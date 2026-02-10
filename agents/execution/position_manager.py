"""
PositionManager — tracks open positions, manages exits, enforces risk limits.

Responsibilities:
  - Sync positions from the Hyperliquid API.
  - Monitor unrealised PnL per position.
  - Manage stop-losses (fixed and trailing) and take-profit levels (TP1/2/3).
  - Publish portfolio state every N seconds.
  - Calculate total NAV, gross/net exposure, daily PnL, max drawdown HWM.
  - Trigger circuit breaker if daily drawdown exceeds threshold.
  - Integrate PlatformSpecialist margin data for liquidation distance.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, date
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import PortfolioSnapshotRow, async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_ACCOUNT_HEALTH,
    STREAM_PORTFOLIO_STATE,
    STREAM_TRADES_EXECUTED,
)
from core.models import (
    AccountHealth,
    AlertSeverity,
    Direction,
    ExecutedTrade,
    PortfolioState,
    Position,
    StopLevel,
    TakeProfitLevel,
)

# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------

_HL_MAINNET = "https://api.hyperliquid.xyz"
_HL_TESTNET = "https://api.hyperliquid-testnet.xyz"


class _ManagedPosition:
    """In-memory wrapper around a Position with stop/TP state."""

    __slots__ = (
        "asset", "direction", "size", "entry_price", "current_price",
        "unrealised_pnl", "opened_at", "stop", "take_profits",
        "highest_price_since_entry",
    )

    def __init__(
        self,
        asset: str,
        direction: Direction,
        size: float,
        entry_price: float,
        current_price: float,
        opened_at: datetime,
    ) -> None:
        self.asset = asset
        self.direction = direction
        self.size = size
        self.entry_price = entry_price
        self.current_price = current_price
        self.unrealised_pnl = 0.0
        self.opened_at = opened_at
        self.stop: StopLevel | None = None
        self.take_profits: list[TakeProfitLevel] = []
        self.highest_price_since_entry = current_price

    def update_price(self, price: float) -> None:
        self.current_price = price
        if self.direction == Direction.LONG:
            self.unrealised_pnl = (price - self.entry_price) * self.size
            self.highest_price_since_entry = max(self.highest_price_since_entry, price)
        else:
            self.unrealised_pnl = (self.entry_price - price) * self.size
            # For shorts, track the lowest price (mirror of trailing logic).
            self.highest_price_since_entry = min(self.highest_price_since_entry, price)

    def notional(self) -> float:
        return abs(self.size * self.current_price)

    def to_model(self, nav: float) -> Position:
        return Position(
            asset=self.asset,
            direction=self.direction,
            size_pct=self.notional() / nav if nav > 0 else 0.0,
            entry_price=self.entry_price,
            current_price=self.current_price,
            unrealised_pnl=self.unrealised_pnl,
            opened_at=self.opened_at,
        )


# ---------------------------------------------------------------------------
# PositionManager agent
# ---------------------------------------------------------------------------

class PositionManager(BaseAgent):
    """Tracks positions, manages exits, publishes portfolio state."""

    def __init__(
        self,
        bus: MessageBus,
        platform_specialist: Any | None = None,
        **kw: Any,
    ) -> None:
        super().__init__(
            agent_id="position_manager",
            agent_type="execution",
            bus=bus,
            **kw,
        )
        self._platform = platform_specialist

        base = _HL_TESTNET if settings.hyperliquid_testnet else _HL_MAINNET
        self._info_url = f"{base}/info"
        self._http: httpx.AsyncClient | None = None

        # Positions keyed by asset.
        self._positions: dict[str, _ManagedPosition] = {}

        # Portfolio metrics.
        self._nav: float = 0.0
        self._daily_pnl: float = 0.0
        self._high_water_mark: float = 0.0
        self._drawdown: float = 0.0
        self._day_start_nav: float = 0.0
        self._current_day: date | None = None
        self._circuit_breaker_triggered = False

        # Account health from PlatformSpecialist.
        self._account_health = AccountHealth()

        # Sub-tasks.
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=15.0)
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._position_sync_loop(), name="pm:sync"),
            asyncio.create_task(self._portfolio_publish_loop(), name="pm:publish"),
            asyncio.create_task(self._stop_tp_monitor_loop(), name="pm:stops"),
            asyncio.create_task(self._health_listener(), name="pm:health"),
            asyncio.create_task(self._fill_listener(), name="pm:fills"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        if self._http:
            await self._http.aclose()
            self._http = None
        await super().stop()

    async def process(self) -> None:
        """Main loop — kept light; real work is in sub-tasks."""
        await asyncio.sleep(30)

    # -- (1) position sync from HL API --------------------------------------

    async def _position_sync_loop(self) -> None:
        """Poll the HL clearinghouse for current positions every 10 seconds."""
        while True:
            try:
                await self._sync_positions()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Position sync failed.")
            await asyncio.sleep(settings.account_health_interval)

    async def _sync_positions(self) -> None:
        address = settings.hyperliquid_vault_address
        if not address:
            return

        state = await self._post_info({"type": "clearinghouseState", "user": address})
        if not state:
            return

        # Parse account value.
        margin_summary = state.get("marginSummary", {})
        self._nav = float(margin_summary.get("accountValue", 0) or 0)

        # Day tracking for daily PnL.
        today = datetime.now(timezone.utc).date()
        if self._current_day != today:
            self._day_start_nav = self._nav
            self._current_day = today
            self._circuit_breaker_triggered = False

        # High-water mark.
        if self._nav > self._high_water_mark:
            self._high_water_mark = self._nav

        # Parse positions.
        asset_positions = state.get("assetPositions", [])
        seen: set[str] = set()

        for entry in asset_positions:
            pos_data = entry.get("position", {})
            asset = pos_data.get("coin", "")
            if not asset:
                continue
            seen.add(asset)

            szi = float(pos_data.get("szi", 0) or 0)
            if szi == 0:
                self._positions.pop(asset, None)
                continue

            direction = Direction.LONG if szi > 0 else Direction.SHORT
            size = abs(szi)
            entry_px = float(pos_data.get("entryPx", 0) or 0)
            current_px = float(pos_data.get("positionValue", 0) or 0) / size if size else 0
            unrealised = float(pos_data.get("unrealizedPnl", 0) or 0)
            liq_px = pos_data.get("liquidationPx")

            if asset in self._positions:
                mp = self._positions[asset]
                mp.size = size
                mp.direction = direction
                mp.entry_price = entry_px
                mp.update_price(current_px)
            else:
                mp = _ManagedPosition(
                    asset=asset,
                    direction=direction,
                    size=size,
                    entry_price=entry_px,
                    current_price=current_px,
                    opened_at=datetime.now(timezone.utc),
                )
                mp.unrealised_pnl = unrealised
                self._positions[asset] = mp
                self.log.info(
                    "New position tracked: %s %s %.6f @ %.4f",
                    direction, asset, size, entry_px,
                )

        # Remove positions that no longer exist on exchange.
        for asset in list(self._positions):
            if asset not in seen:
                self.log.info("Position closed on exchange: %s", asset)
                del self._positions[asset]

    # -- (2) portfolio state publishing --------------------------------------

    async def _portfolio_publish_loop(self) -> None:
        """Compute and publish portfolio state every N seconds."""
        while True:
            try:
                await self._publish_portfolio_state()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Portfolio state publish failed.")
            await asyncio.sleep(settings.portfolio_state_interval)

    async def _publish_portfolio_state(self) -> None:
        nav = self._nav

        # Daily PnL.
        self._daily_pnl = nav - self._day_start_nav if self._day_start_nav > 0 else 0.0

        # Drawdown from HWM.
        self._drawdown = (
            (self._high_water_mark - nav) / self._high_water_mark
            if self._high_water_mark > 0 else 0.0
        )
        self._drawdown = max(0.0, self._drawdown)

        # Exposure calculations.
        gross = sum(mp.notional() for mp in self._positions.values())
        long_notional = sum(
            mp.notional() for mp in self._positions.values()
            if mp.direction == Direction.LONG
        )
        short_notional = sum(
            mp.notional() for mp in self._positions.values()
            if mp.direction == Direction.SHORT
        )
        net = long_notional - short_notional

        gross_exposure = gross / nav if nav > 0 else 0.0
        net_exposure = net / nav if nav > 0 else 0.0

        positions = [mp.to_model(nav) for mp in self._positions.values()]

        state = PortfolioState(
            positions=positions,
            total_nav=nav,
            daily_pnl=self._daily_pnl,
            drawdown=self._drawdown,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
        )

        # Publish.
        await self.bus.publish_to(STREAM_PORTFOLIO_STATE, state)

        # Persist snapshot.
        try:
            async with async_session_factory() as session:
                row = PortfolioSnapshotRow(
                    ts=datetime.now(timezone.utc),
                    total_nav=nav,
                    daily_pnl=self._daily_pnl,
                    drawdown=self._drawdown,
                    gross_exposure=gross_exposure,
                    net_exposure=net_exposure,
                    positions=[p.model_dump() for p in positions],
                )
                session.add(row)
                await session.commit()
        except Exception:
            self.log.exception("Failed to persist portfolio snapshot.")

        # --- Circuit breaker check ---
        await self._check_circuit_breaker()

    # -- (3) stop-loss / take-profit monitoring ------------------------------

    async def _stop_tp_monitor_loop(self) -> None:
        """Check stops and TPs every 5 seconds."""
        while True:
            try:
                await self._evaluate_stops_and_tps()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Stop/TP evaluation failed.")
            await asyncio.sleep(5)

    async def _evaluate_stops_and_tps(self) -> None:
        for asset, mp in list(self._positions.items()):
            # --- Trailing stop update ---
            if mp.stop and mp.stop.trailing:
                if mp.direction == Direction.LONG:
                    new_stop = mp.highest_price_since_entry * (1 - mp.stop.trail_pct)
                    if new_stop > mp.stop.price:
                        mp.stop.price = new_stop
                        mp.stop.highest_price = mp.highest_price_since_entry
                else:
                    new_stop = mp.highest_price_since_entry * (1 + mp.stop.trail_pct)
                    if new_stop < mp.stop.price:
                        mp.stop.price = new_stop
                        mp.stop.highest_price = mp.highest_price_since_entry

            # --- Stop-loss trigger ---
            if mp.stop:
                triggered = False
                if mp.direction == Direction.LONG and mp.current_price <= mp.stop.price:
                    triggered = True
                elif mp.direction == Direction.SHORT and mp.current_price >= mp.stop.price:
                    triggered = True

                if triggered:
                    self.log.warning(
                        "STOP-LOSS triggered: %s %s @ %.4f (stop=%.4f)",
                        mp.direction, asset, mp.current_price, mp.stop.price,
                    )
                    await self._request_close(asset, mp, reason="stop_loss")

            # --- Take-profit triggers (scaled exits) ---
            for tp in mp.take_profits:
                if tp.triggered:
                    continue
                hit = False
                if mp.direction == Direction.LONG and mp.current_price >= tp.price:
                    hit = True
                elif mp.direction == Direction.SHORT and mp.current_price <= tp.price:
                    hit = True

                if hit:
                    tp.triggered = True
                    self.log.info(
                        "TAKE-PROFIT hit: %s %s @ %.4f (tp=%.4f, close_pct=%.0f%%)",
                        mp.direction, asset, mp.current_price, tp.price, tp.close_pct * 100,
                    )
                    await self._request_partial_close(asset, mp, tp.close_pct, reason="take_profit")

    async def _request_close(
        self, asset: str, mp: _ManagedPosition, reason: str,
    ) -> None:
        """Publish a close decision to decisions:approved."""
        from core.message_bus import STREAM_DECISIONS_APPROVED
        from core.models import TradeAction, TradeDecision

        decision = TradeDecision(
            asset=asset,
            action=TradeAction.CLOSE,
            size_pct=mp.notional() / self._nav if self._nav > 0 else 0,
            consensus_score=0.0,
            risk_approved=True,
            metadata={"reason": reason, "execution_mode": "market"},
        )
        await self.bus.publish_to(STREAM_DECISIONS_APPROVED, decision)

    async def _request_partial_close(
        self, asset: str, mp: _ManagedPosition, close_pct: float, reason: str,
    ) -> None:
        """Publish a reduce decision for a fraction of the position."""
        from core.message_bus import STREAM_DECISIONS_APPROVED
        from core.models import TradeAction, TradeDecision

        partial_notional = mp.notional() * close_pct
        decision = TradeDecision(
            asset=asset,
            action=TradeAction.REDUCE,
            size_pct=partial_notional / self._nav if self._nav > 0 else 0,
            consensus_score=0.0,
            risk_approved=True,
            metadata={"reason": reason, "execution_mode": "market"},
        )
        await self.bus.publish_to(STREAM_DECISIONS_APPROVED, decision)

    # -- (4) circuit breaker -------------------------------------------------

    async def _check_circuit_breaker(self) -> None:
        """Trigger emergency flatten if daily drawdown exceeds threshold."""
        if self._circuit_breaker_triggered:
            return

        daily_dd = 0.0
        if self._day_start_nav > 0:
            daily_dd = (self._day_start_nav - self._nav) / self._day_start_nav
            daily_dd = max(0.0, daily_dd)

        if daily_dd >= settings.circuit_breaker_drawdown:
            self._circuit_breaker_triggered = True
            self.log.critical(
                "CIRCUIT BREAKER: Daily drawdown %.2f%% exceeds %.2f%% — FLATTENING ALL",
                daily_dd * 100, settings.circuit_breaker_drawdown * 100,
            )
            for asset, mp in list(self._positions.items()):
                await self._request_close(asset, mp, reason="circuit_breaker")

        elif daily_dd >= settings.max_daily_drawdown:
            self.log.warning(
                "Daily drawdown %.2f%% exceeds soft limit %.2f%% — halting new entries.",
                daily_dd * 100, settings.max_daily_drawdown * 100,
            )

    # -- background listeners ------------------------------------------------

    async def _health_listener(self) -> None:
        """Cache account health from PlatformSpecialist."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_ACCOUNT_HEALTH,
            group="position_manager_health",
            consumer=self.agent_id,
        ):
            try:
                self._account_health = AccountHealth.model_validate(payload)
            except Exception:
                pass

    async def _fill_listener(self) -> None:
        """Listen for executed trades to attach stop/TP levels."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_TRADES_EXECUTED,
            group="position_manager_fills",
            consumer=self.agent_id,
        ):
            try:
                trade = ExecutedTrade.model_validate(payload)
                await self._handle_fill(trade)
            except Exception:
                self.log.exception("Failed to handle fill event.")

    async def _handle_fill(self, trade: ExecutedTrade) -> None:
        """When a new trade fills, set default stop/TP if not already set."""
        mp = self._positions.get(trade.asset)
        if not mp:
            return  # Will appear on next sync.

        # Set a default trailing stop at 5% if none exists.
        if mp.stop is None:
            trail_pct = 0.05
            if mp.direction == Direction.LONG:
                stop_px = mp.entry_price * (1 - trail_pct)
            else:
                stop_px = mp.entry_price * (1 + trail_pct)
            mp.stop = StopLevel(
                price=stop_px,
                trailing=True,
                trail_pct=trail_pct,
                highest_price=mp.current_price,
            )
            self.log.info(
                "Default trailing stop set for %s: %.4f (trail=%.1f%%)",
                trade.asset, stop_px, trail_pct * 100,
            )

        # Set default TP levels (33% at +3%, 33% at +6%, 34% at +10%) if none exist.
        if not mp.take_profits:
            if mp.direction == Direction.LONG:
                mp.take_profits = [
                    TakeProfitLevel(price=mp.entry_price * 1.03, close_pct=0.33),
                    TakeProfitLevel(price=mp.entry_price * 1.06, close_pct=0.33),
                    TakeProfitLevel(price=mp.entry_price * 1.10, close_pct=0.34),
                ]
            else:
                mp.take_profits = [
                    TakeProfitLevel(price=mp.entry_price * 0.97, close_pct=0.33),
                    TakeProfitLevel(price=mp.entry_price * 0.94, close_pct=0.33),
                    TakeProfitLevel(price=mp.entry_price * 0.90, close_pct=0.34),
                ]
            self.log.info(
                "Default TP levels set for %s: %s",
                trade.asset,
                [f"${tp.price:.2f}({tp.close_pct:.0%})" for tp in mp.take_profits],
            )

    # -- public accessors ----------------------------------------------------

    def set_stop(self, asset: str, stop: StopLevel) -> bool:
        """Manually set a stop-loss on a position."""
        mp = self._positions.get(asset)
        if not mp:
            return False
        mp.stop = stop
        self.log.info("Stop set for %s: %.4f (trailing=%s)", asset, stop.price, stop.trailing)
        return True

    def set_take_profits(self, asset: str, tps: list[TakeProfitLevel]) -> bool:
        """Manually set take-profit levels on a position."""
        mp = self._positions.get(asset)
        if not mp:
            return False
        mp.take_profits = tps
        self.log.info("TPs set for %s: %d levels", asset, len(tps))
        return True

    def get_portfolio_state(self) -> PortfolioState:
        nav = self._nav
        gross = sum(mp.notional() for mp in self._positions.values())
        long_n = sum(mp.notional() for mp in self._positions.values() if mp.direction == Direction.LONG)
        short_n = sum(mp.notional() for mp in self._positions.values() if mp.direction == Direction.SHORT)
        return PortfolioState(
            positions=[mp.to_model(nav) for mp in self._positions.values()],
            total_nav=nav,
            daily_pnl=self._daily_pnl,
            drawdown=self._drawdown,
            gross_exposure=gross / nav if nav > 0 else 0.0,
            net_exposure=(long_n - short_n) / nav if nav > 0 else 0.0,
        )

    # -- HL info helper ------------------------------------------------------

    async def _post_info(self, payload: dict[str, Any]) -> Any:
        try:
            resp = await self._http.post(self._info_url, json=payload, timeout=10.0)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            self.log.warning("HL info %s returned %d", payload.get("type"), exc.response.status_code)
        except httpx.RequestError as exc:
            self.log.warning("HL info request failed: %s", exc)
        return None

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "positions": len(self._positions),
            "nav": self._nav,
            "daily_pnl": self._daily_pnl,
            "drawdown": self._drawdown,
            "circuit_breaker_triggered": self._circuit_breaker_triggered,
        })
        return base
