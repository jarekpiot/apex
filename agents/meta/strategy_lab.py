"""
StrategyLab — shadow trading sandbox for evaluating signal quality.

Captures every signal from ``apex:signals``, opens a shadow position at the
current mid-price, and closes it after a configurable horizon (default 24h).
Compares shadow returns per agent vs live performance to flag agents that
should be promoted (increased weight) or demoted.

This agent has NO effect on live trading — it only observes and records.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import PerformanceMetricRow, async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_MARKET_PRICES,
    STREAM_SHADOW_RESULTS,
    STREAM_SIGNALS,
)
from core.models import AgentSignal, PriceUpdate, ShadowTradeResult


# ---------------------------------------------------------------------------
# Internal shadow position
# ---------------------------------------------------------------------------

class _ShadowPosition:
    """A virtual position opened from a signal for evaluation."""

    __slots__ = (
        "signal_id", "agent_id", "asset", "direction", "entry_price",
        "opened_at", "exit_price", "closed_at",
    )

    def __init__(self, signal: AgentSignal, entry_price: float) -> None:
        self.signal_id = signal.signal_id
        self.agent_id = signal.agent_id
        self.asset = signal.asset
        self.direction = signal.direction
        self.entry_price = entry_price
        self.opened_at = datetime.now(timezone.utc)
        self.exit_price: float | None = None
        self.closed_at: datetime | None = None


# ---------------------------------------------------------------------------
# StrategyLab agent
# ---------------------------------------------------------------------------

class StrategyLab(BaseAgent):
    """Shadow trading sandbox — evaluates signal quality without live risk."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="strategy_lab",
            agent_type="meta",
            bus=bus,
            **kw,
        )
        # Open shadow positions: signal_id → _ShadowPosition
        self._open: dict[str, _ShadowPosition] = {}
        # Closed results per agent: agent_id → list of return_bps
        self._results: dict[str, list[float]] = defaultdict(list)
        # Latest mid-prices.
        self._mid_prices: dict[str, float] = {}
        # Counters.
        self._shadows_opened = 0
        self._shadows_closed = 0

        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._price_listener(), name="lab:prices"),
            asyncio.create_task(self._evaluation_loop(), name="lab:eval"),
            asyncio.create_task(self._comparison_loop(), name="lab:compare"),
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

    # -- main loop: consume signals and open shadow positions ----------------

    async def process(self) -> None:
        """Consume AgentSignals from apex:signals, open shadow positions."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_SIGNALS,
            group="strategy_lab",
            consumer=self.agent_id,
        ):
            try:
                signal = AgentSignal.model_validate(payload)

                # Need a price to open a shadow position.
                price = self._mid_prices.get(signal.asset)
                if price is None or price <= 0:
                    continue

                # Skip if we already have an open shadow for this signal.
                if signal.signal_id in self._open:
                    continue

                shadow = _ShadowPosition(signal, price)
                self._open[signal.signal_id] = shadow
                self._shadows_opened += 1
            except Exception:
                self.log.exception("Error processing signal for shadow trade.")

    # -- sub-task: price listener --------------------------------------------

    async def _price_listener(self) -> None:
        """Track mid-prices for shadow PnL calculation."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="lab_prices",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)
            except Exception:
                pass

    # -- sub-task: evaluation loop -------------------------------------------

    async def _evaluation_loop(self) -> None:
        """Close expired shadow positions and compute returns."""
        await asyncio.sleep(30)
        while True:
            try:
                await self._close_expired()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Evaluation loop failed.")
            await asyncio.sleep(60)

    async def _close_expired(self) -> None:
        """Close shadow positions past the holding period."""
        now = datetime.now(timezone.utc)
        horizon = timedelta(hours=settings.strategy_lab_exit_hours)
        to_close: list[str] = []

        for sid, pos in self._open.items():
            if now - pos.opened_at >= horizon:
                to_close.append(sid)

        for sid in to_close:
            pos = self._open.pop(sid)
            current = self._mid_prices.get(pos.asset)
            if current is None or current <= 0 or pos.entry_price <= 0:
                continue

            # Compute return in basis points.
            if pos.direction > 0:
                return_bps = ((current - pos.entry_price) / pos.entry_price) * 10_000
            else:
                return_bps = ((pos.entry_price - current) / pos.entry_price) * 10_000

            holding_hours = (now - pos.opened_at).total_seconds() / 3600

            result = ShadowTradeResult(
                signal_id=pos.signal_id,
                agent_id=pos.agent_id,
                asset=pos.asset,
                direction=pos.direction,
                entry_price=pos.entry_price,
                exit_price=current,
                holding_period_hours=holding_hours,
                return_bps=return_bps,
                is_closed=True,
            )

            self._results[pos.agent_id].append(return_bps)
            self._shadows_closed += 1

            # Publish result.
            await self.bus.publish_to(STREAM_SHADOW_RESULTS, result)

            # Persist to DB.
            try:
                async with async_session_factory() as session:
                    session.add(PerformanceMetricRow(
                        metric_name="shadow_trade",
                        metric_value=return_bps,
                        source="strategy_lab",
                        agent_id=pos.agent_id,
                        asset=pos.asset,
                        metadata_={
                            "signal_id": pos.signal_id,
                            "entry_price": pos.entry_price,
                            "exit_price": current,
                            "holding_hours": holding_hours,
                            "direction": pos.direction,
                        },
                    ))
                    await session.commit()
            except Exception:
                self.log.exception("Failed to persist shadow trade result.")

    # -- sub-task: comparison loop -------------------------------------------

    async def _comparison_loop(self) -> None:
        """Compare shadow vs live performance, flag promotions/demotions."""
        await asyncio.sleep(300)  # Let data accumulate.
        while True:
            try:
                await self._run_comparison()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Comparison loop failed.")
            await asyncio.sleep(3600)

    async def _run_comparison(self) -> None:
        """Check if any agent's shadow performance warrants promotion."""
        min_sample = settings.strategy_lab_min_sample
        threshold = settings.strategy_lab_promotion_threshold

        for agent_id, returns in self._results.items():
            if len(returns) < min_sample:
                continue

            avg_shadow = sum(returns) / len(returns)

            # If shadow average > threshold * 10000 bps (i.e., outperforms by threshold fraction)
            # this is a simplification — in practice you'd compare against live returns
            if avg_shadow > threshold * 100:  # threshold as percentage, returns in bps
                self.log.info(
                    "PROMOTION CANDIDATE: %s — shadow avg=%.1f bps over %d trades",
                    agent_id, avg_shadow, len(returns),
                )

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "open_shadows": len(self._open),
            "shadows_opened": self._shadows_opened,
            "shadows_closed": self._shadows_closed,
            "agents_tracked": len(self._results),
            "prices_tracked": len(self._mid_prices),
        })
        return base
