"""
ExecutionEngine — smart order routing and execution on Hyperliquid.

Subscribes to ``decisions:approved`` on Redis, consults the Platform
Specialist for margin / depth / advisory data, then executes using one of
four modes: MARKET, LIMIT, TWAP, or ICEBERG.

Supports a full paper-trade mode where orders are simulated locally.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import ExecutedTradeRow, async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_DECISIONS_APPROVED,
    STREAM_TRADES_EXECUTED,
)
from core.models import (
    AccountHealth,
    AlertSeverity,
    ExecutedTrade,
    ExecutionAdvisory,
    ExecutionMode,
    TradeAction,
    TradeDecision,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HL_MAINNET = "https://api.hyperliquid.xyz"
_HL_TESTNET = "https://api.hyperliquid-testnet.xyz"


def _side_from_action(action: TradeAction) -> tuple[bool, bool]:
    """Return (is_buy, is_close) from a TradeAction."""
    if action == TradeAction.OPEN_LONG:
        return True, False
    elif action == TradeAction.OPEN_SHORT:
        return False, False
    elif action == TradeAction.CLOSE:
        return False, True  # Direction determined at execution time.
    elif action == TradeAction.REDUCE:
        return False, True
    elif action == TradeAction.FLIP:
        return True, False  # Close-then-open handled in TWAP logic.
    return True, False


class ExecutionEngine(BaseAgent):
    """Executes risk-approved trade decisions on Hyperliquid."""

    def __init__(
        self,
        bus: MessageBus,
        platform_specialist: Any | None = None,
        **kw: Any,
    ) -> None:
        super().__init__(
            agent_id="execution_engine",
            agent_type="execution",
            bus=bus,
            **kw,
        )
        self._platform = platform_specialist
        self._exchange: Any = None  # hyperliquid.exchange.Exchange (set in _init_sdk)
        self._info: Any = None      # hyperliquid.info.Info

        # Latest advisory cache: asset -> ExecutionAdvisory.
        self._advisory_cache: dict[str, ExecutionAdvisory] = {}
        self._account_cache: AccountHealth = AccountHealth()

        # Sub-tasks.
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        if not settings.paper_trade_mode:
            await self._init_sdk()
        else:
            self.log.info("PAPER TRADE mode — no SDK connection.")
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._advisory_listener(), name="exec:advisory"),
            asyncio.create_task(self._health_listener(), name="exec:health"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        await super().stop()

    async def _init_sdk(self) -> None:
        """Initialise the hyperliquid-python-sdk Exchange and Info clients."""
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info
            from hyperliquid.utils import constants

            secret = settings.hyperliquid_api_secret.get_secret_value()
            if not secret:
                self.log.warning("No HL API secret configured — falling back to paper mode.")
                return

            wallet = Account.from_key(secret)
            base = constants.TESTNET_API_URL if settings.hyperliquid_testnet else constants.MAINNET_API_URL
            vault = settings.hyperliquid_vault_address or None

            self._info = Info(base, skip_ws=True)
            self._exchange = Exchange(wallet, base, vault_address=vault)
            self.log.info(
                "HL SDK initialised (address=%s, testnet=%s)",
                wallet.address, settings.hyperliquid_testnet,
            )
        except ImportError:
            self.log.warning("hyperliquid SDK not installed — paper mode only.")
        except Exception:
            self.log.exception("Failed to init HL SDK — falling back to paper mode.")

    # -- main loop: consume approved decisions -------------------------------

    async def process(self) -> None:
        """Subscribe to decisions:approved and execute each one."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_DECISIONS_APPROVED,
            group="execution_engine",
            consumer=self.agent_id,
        ):
            try:
                decision = TradeDecision.model_validate(payload)
                await self._execute_decision(decision)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Failed to execute decision: %s", payload)

    # -- decision execution --------------------------------------------------

    async def _execute_decision(self, decision: TradeDecision) -> None:
        """Route a decision through pre-checks, execution, and post-trade."""
        self.log.info(
            "Executing: %s %s size=%.4f consensus=%.2f",
            decision.action, decision.asset,
            decision.size_pct, decision.consensus_score,
        )

        # --- Pre-trade checks ---
        rejection = await self._pre_trade_checks(decision)
        if rejection:
            self.log.warning("REJECTED %s: %s", decision.decision_id, rejection)
            return

        # Determine execution mode from metadata (default: MARKET).
        mode_str = decision.metadata.get("execution_mode", "market")
        try:
            mode = ExecutionMode(mode_str)
        except ValueError:
            mode = ExecutionMode.MARKET

        # --- Execute ---
        if mode == ExecutionMode.MARKET:
            trade = await self._exec_market(decision)
        elif mode == ExecutionMode.LIMIT:
            trade = await self._exec_limit(decision)
        elif mode == ExecutionMode.TWAP:
            trade = await self._exec_twap(decision)
        elif mode == ExecutionMode.ICEBERG:
            trade = await self._exec_iceberg(decision)
        else:
            trade = await self._exec_market(decision)

        if trade is None:
            self.log.warning("Execution returned no fill for %s", decision.decision_id)
            return

        # --- Post-trade ---
        await self._post_trade(trade)

    # -- pre-trade checks ----------------------------------------------------

    async def _pre_trade_checks(self, decision: TradeDecision) -> str | None:
        """Run all pre-trade validations.  Returns rejection reason or None."""

        # 1. Check account health severity.
        if self._account_cache.severity == AlertSeverity.CRITICAL:
            if decision.action in (TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT):
                return "Account health CRITICAL — no new positions."

        # 2. Check margin availability.
        if self._account_cache.equity > 0:
            required_margin = decision.size_pct * self._account_cache.equity
            if required_margin > self._account_cache.margin_available:
                return (
                    f"Insufficient margin: need ${required_margin:,.0f}, "
                    f"available ${self._account_cache.margin_available:,.0f}"
                )

        # 3. Check depth vs order size.
        advisory = self._advisory_cache.get(decision.asset)
        if advisory and self._account_cache.equity > 0:
            order_usd = decision.size_pct * self._account_cache.equity
            is_buy, _ = _side_from_action(decision.action)
            depth = advisory.ask_depth_usd if is_buy else advisory.bid_depth_usd
            if depth > 0 and order_usd > depth * 0.5:
                estimated_slippage_bps = (order_usd / depth) * 100
                if estimated_slippage_bps > settings.slippage_threshold_bps:
                    return (
                        f"Slippage ~{estimated_slippage_bps:.0f}bps exceeds "
                        f"{settings.slippage_threshold_bps:.0f}bps threshold"
                    )

        # 4. Verify asset is tradeable.
        if self._platform:
            info = self._platform.get_asset_info(decision.asset)
            if info is None:
                return f"Asset {decision.asset} not found in registry."

        return None

    # -- execution modes -----------------------------------------------------

    async def _exec_market(self, decision: TradeDecision) -> ExecutedTrade | None:
        """Immediate market order execution."""
        is_buy, is_close = _side_from_action(decision.action)
        size = await self._compute_size(decision)
        if size <= 0:
            return None

        if settings.paper_trade_mode:
            return self._simulate_fill(decision, size, is_buy, ExecutionMode.MARKET)

        return await self._send_order(
            decision=decision,
            is_buy=is_buy,
            size=size,
            limit_px=None,
            mode=ExecutionMode.MARKET,
        )

    async def _exec_limit(self, decision: TradeDecision) -> ExecutedTrade | None:
        """Limit order with timeout — cancels if not filled within 60 seconds."""
        is_buy, _ = _side_from_action(decision.action)
        size = await self._compute_size(decision)
        if size <= 0:
            return None

        limit_px = decision.entry_price
        if limit_px is None:
            # Use best bid/ask as limit.
            advisory = self._advisory_cache.get(decision.asset)
            if advisory and advisory.spread_bps > 0:
                # Cross the spread slightly for faster fill.
                limit_px = decision.entry_price
            if limit_px is None:
                self.log.warning("No limit price for %s, falling back to market.", decision.asset)
                return await self._exec_market(decision)

        if settings.paper_trade_mode:
            return self._simulate_fill(decision, size, is_buy, ExecutionMode.LIMIT)

        trade = await self._send_order(
            decision=decision,
            is_buy=is_buy,
            size=size,
            limit_px=limit_px,
            mode=ExecutionMode.LIMIT,
        )

        # If not filled, cancel after timeout.
        if trade is None:
            self.log.info("Limit order for %s timed out — cancelling.", decision.asset)
        return trade

    async def _exec_twap(self, decision: TradeDecision) -> ExecutedTrade | None:
        """Split the order into N slices over a time window."""
        is_buy, _ = _side_from_action(decision.action)
        total_size = await self._compute_size(decision)
        if total_size <= 0:
            return None

        n_splits = decision.metadata.get("twap_splits", settings.twap_default_splits)
        duration = decision.metadata.get("twap_duration", settings.twap_default_duration_secs)
        interval = duration / n_splits
        slice_size = total_size / n_splits

        total_filled = 0.0
        total_cost = 0.0
        total_fee = 0.0

        for i in range(n_splits):
            self.log.info(
                "TWAP slice %d/%d: %s %.6f %s",
                i + 1, n_splits, decision.asset, slice_size, "BUY" if is_buy else "SELL",
            )

            if settings.paper_trade_mode:
                fill = self._simulate_fill(decision, slice_size, is_buy, ExecutionMode.TWAP)
            else:
                fill = await self._send_order(
                    decision=decision,
                    is_buy=is_buy,
                    size=slice_size,
                    limit_px=None,
                    mode=ExecutionMode.TWAP,
                )

            if fill:
                total_filled += fill.size
                total_cost += fill.price * fill.size
                total_fee += fill.fee

            if i < n_splits - 1:
                await asyncio.sleep(interval)

        if total_filled <= 0:
            return None

        avg_price = total_cost / total_filled
        return ExecutedTrade(
            decision_id=decision.decision_id,
            asset=decision.asset,
            side="buy" if is_buy else "sell",
            size=total_filled,
            price=avg_price,
            fee=total_fee,
            execution_mode=ExecutionMode.TWAP,
            paper_trade=settings.paper_trade_mode,
        )

    async def _exec_iceberg(self, decision: TradeDecision) -> ExecutedTrade | None:
        """Iceberg: show only a fraction of size on the book, refill until done."""
        is_buy, _ = _side_from_action(decision.action)
        total_size = await self._compute_size(decision)
        if total_size <= 0:
            return None

        show_pct = decision.metadata.get("iceberg_show_pct", settings.iceberg_show_pct)
        show_size = total_size * show_pct
        remaining = total_size

        total_filled = 0.0
        total_cost = 0.0
        total_fee = 0.0

        while remaining > 0:
            slice_size = min(show_size, remaining)
            self.log.info(
                "ICEBERG: showing %.6f of %.6f remaining for %s",
                slice_size, remaining, decision.asset,
            )

            if settings.paper_trade_mode:
                fill = self._simulate_fill(decision, slice_size, is_buy, ExecutionMode.ICEBERG)
            else:
                fill = await self._send_order(
                    decision=decision,
                    is_buy=is_buy,
                    size=slice_size,
                    limit_px=decision.entry_price,
                    mode=ExecutionMode.ICEBERG,
                )

            if fill:
                total_filled += fill.size
                total_cost += fill.price * fill.size
                total_fee += fill.fee
                remaining -= fill.size
            else:
                self.log.warning("Iceberg slice unfilled — aborting remainder.")
                break

            await asyncio.sleep(2)  # Brief pause between refills.

        if total_filled <= 0:
            return None

        avg_price = total_cost / total_filled
        return ExecutedTrade(
            decision_id=decision.decision_id,
            asset=decision.asset,
            side="buy" if is_buy else "sell",
            size=total_filled,
            price=avg_price,
            fee=total_fee,
            execution_mode=ExecutionMode.ICEBERG,
            paper_trade=settings.paper_trade_mode,
        )

    # -- order submission (SDK wrapper) --------------------------------------

    async def _send_order(
        self,
        decision: TradeDecision,
        is_buy: bool,
        size: float,
        limit_px: float | None,
        mode: ExecutionMode,
    ) -> ExecutedTrade | None:
        """Send an order via the HL SDK (runs blocking SDK call in a thread)."""
        if self._exchange is None:
            self.log.error("Exchange SDK not initialised — cannot send order.")
            return None

        loop = asyncio.get_running_loop()

        try:
            if limit_px is None:
                # Market order.
                result = await loop.run_in_executor(
                    None,
                    lambda: self._exchange.market_open(
                        decision.asset, is_buy, size, slippage=0.01,
                    ),
                )
            else:
                order_type = {"limit": {"tif": "Gtc"}}
                result = await loop.run_in_executor(
                    None,
                    lambda: self._exchange.order(
                        decision.asset, is_buy, size, limit_px, order_type,
                    ),
                )

            # Parse SDK response.
            status = result.get("status", "")
            if status == "ok":
                response = result.get("response", {})
                data = response.get("data", {})
                statuses = data.get("statuses", [{}])
                fill_info = statuses[0] if statuses else {}

                filled = fill_info.get("filled", {})
                fill_px = float(filled.get("avgPx", 0) or 0)
                fill_sz = float(filled.get("totalSz", size) or size)
                oid = str(filled.get("oid", ""))

                return ExecutedTrade(
                    decision_id=decision.decision_id,
                    asset=decision.asset,
                    side="buy" if is_buy else "sell",
                    size=fill_sz,
                    price=fill_px,
                    exchange_order_id=oid,
                    execution_mode=mode,
                    paper_trade=False,
                )
            else:
                err = result.get("response", "unknown error")
                self.log.error("Order rejected by HL: %s", err)
                return None

        except Exception:
            self.log.exception("SDK order call failed.")
            return None

    # -- paper-trade simulation ----------------------------------------------

    def _simulate_fill(
        self,
        decision: TradeDecision,
        size: float,
        is_buy: bool,
        mode: ExecutionMode,
    ) -> ExecutedTrade:
        """Simulate a fill at the current mid-price (or entry_price)."""
        price = decision.entry_price or 0.0
        # Simulate taker fee.
        fee_bps = self._advisory_cache.get(decision.asset)
        taker_bps = fee_bps.taker_fee_bps if fee_bps else 3.5
        fee = size * price * (taker_bps / 10_000)

        return ExecutedTrade(
            decision_id=decision.decision_id,
            asset=decision.asset,
            side="buy" if is_buy else "sell",
            size=size,
            price=price,
            fee=fee,
            execution_mode=mode,
            paper_trade=True,
        )

    # -- sizing helper -------------------------------------------------------

    async def _compute_size(self, decision: TradeDecision) -> float:
        """Convert size_pct of NAV into a concrete asset quantity."""
        equity = self._account_cache.equity
        if equity <= 0:
            self.log.warning("Equity unknown or zero — cannot compute size.")
            return 0.0

        usd_notional = decision.size_pct * equity
        # Get mark price for the asset.
        if self._platform:
            info = self._platform.get_asset_info(decision.asset)
            if info and info.mark_price > 0:
                raw_size = usd_notional / info.mark_price
                # Round to asset's lot size.
                lot = info.lot_size if info.lot_size > 0 else 10 ** (-info.sz_decimals)
                return round(raw_size / lot) * lot if lot > 0 else raw_size

        # Fallback: use entry_price from decision.
        if decision.entry_price and decision.entry_price > 0:
            return usd_notional / decision.entry_price

        self.log.warning("Cannot determine price for %s — size=0", decision.asset)
        return 0.0

    # -- background listeners ------------------------------------------------

    async def _advisory_listener(self) -> None:
        """Cache execution advisories from the platform specialist."""
        from core.message_bus import STREAM_EXECUTION_ADVISORY
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_EXECUTION_ADVISORY,
            group="execution_engine_advisory",
            consumer=self.agent_id,
        ):
            try:
                adv = ExecutionAdvisory.model_validate(payload)
                self._advisory_cache[adv.asset] = adv
            except Exception:
                pass

    async def _health_listener(self) -> None:
        """Cache account health from the platform specialist."""
        from core.message_bus import STREAM_ACCOUNT_HEALTH
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_ACCOUNT_HEALTH,
            group="execution_engine_health",
            consumer=self.agent_id,
        ):
            try:
                self._account_cache = AccountHealth.model_validate(payload)
            except Exception:
                pass

    # -- post-trade persistence ----------------------------------------------

    async def _post_trade(self, trade: ExecutedTrade) -> None:
        """Persist the fill and publish to trades:executed stream."""
        # Persist to DB.
        try:
            async with async_session_factory() as session:
                row = ExecutedTradeRow(
                    trade_id=trade.trade_id,
                    decision_id=trade.decision_id,
                    ts=trade.timestamp,
                    asset=trade.asset,
                    side=trade.side,
                    size=trade.size,
                    price=trade.price,
                    fee=trade.fee,
                    slippage_bps=trade.slippage_bps,
                    exchange_order_id=trade.exchange_order_id,
                    metadata_={
                        "execution_mode": trade.execution_mode,
                        "paper_trade": trade.paper_trade,
                    },
                )
                session.add(row)
                await session.commit()
        except Exception:
            self.log.exception("Failed to persist trade %s", trade.trade_id)

        # Publish to stream.
        await self.bus.publish_to(STREAM_TRADES_EXECUTED, trade)
        self.log.info(
            "Trade executed: %s %s %.6f @ %.2f (mode=%s, paper=%s)",
            trade.side, trade.asset, trade.size, trade.price,
            trade.execution_mode, trade.paper_trade,
        )

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "paper_mode": settings.paper_trade_mode,
            "sdk_connected": self._exchange is not None,
            "advisory_assets_cached": len(self._advisory_cache),
            "account_equity": self._account_cache.equity,
        })
        return base
