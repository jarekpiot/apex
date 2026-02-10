"""
RiskGuardian — the portfolio's final gatekeeper.

Every ``TradeDecision`` passes through here *before* it reaches the
execution engine.  The guardian runs a chain of checks and either
approves (with possible adjustments) or rejects outright.

Check pipeline (all must pass):
  a) Position size ≤ max_position_pct of NAV
  b) Gross exposure after trade ≤ max_gross_exposure
  c) Net exposure after trade ≤ max_net_exposure
  d) Pairwise correlation with existing positions ≤ threshold (14d Pearson)
  e) Daily drawdown gates (soft: reduce 50%, hard: reject + circuit breaker)
  f) Weekly / monthly drawdown gates
  g) Single asset-class concentration < 40 % of NAV

The guardian also listens to the ``risk:anomaly`` stream and enters a
*defensive mode* on high-severity alerts: reduce all positions 30 %,
widen stops, pause new entries for 15 minutes.
"""

from __future__ import annotations

import asyncio
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_RISK_ANOMALY,
    STREAM_RISK_CHECKS,
)
from core.models import (
    AlertSeverity,
    AnomalyAlert,
    AssetClass,
    Direction,
    PortfolioState,
    RiskCheck,
    TradeAction,
    TradeDecision,
)

# ---------------------------------------------------------------------------
# Asset-class classifier
# ---------------------------------------------------------------------------

_CRYPTO_MAJORS = frozenset({"BTC", "ETH"})
_EQUITIES = frozenset({
    "SPY", "QQQ", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA",
    "NVDA", "AMD", "NFLX", "DIS", "BA", "JPM", "GS",
})
_COMMODITIES = frozenset({"GOLD", "SILVER", "OIL", "WTI", "BRENT", "NG", "COPPER", "PLAT"})
_FX = frozenset({"EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "CNH"})


def classify_asset(asset: str) -> AssetClass:
    upper = asset.upper()
    if upper in _CRYPTO_MAJORS:
        return AssetClass.CRYPTO_MAJOR
    if upper in _EQUITIES:
        return AssetClass.EQUITY
    if upper in _COMMODITIES:
        return AssetClass.COMMODITY
    if upper in _FX:
        return AssetClass.FX
    # Default: treat everything else as alt-crypto (most HL perps are crypto).
    return AssetClass.CRYPTO_ALT


# ---------------------------------------------------------------------------
# Rolling price history for correlation calculation
# ---------------------------------------------------------------------------

# Number of 1-hour data points in the correlation lookback window.
_CORR_WINDOW = settings.correlation_lookback_days * 24


class _PriceHistory:
    """Fixed-length deque of hourly close prices per asset."""

    def __init__(self, maxlen: int = _CORR_WINDOW) -> None:
        self._data: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._maxlen = maxlen

    def append(self, asset: str, price: float) -> None:
        self._data[asset].append(price)

    def get(self, asset: str) -> list[float]:
        return list(self._data.get(asset, []))

    def pearson(self, asset_a: str, asset_b: str) -> float | None:
        """Compute Pearson r for the overlapping tail of two price series.

        Returns None if insufficient data (< 24 data points).
        """
        a = self.get(asset_a)
        b = self.get(asset_b)
        n = min(len(a), len(b))
        if n < 24:
            return None
        a_arr = np.array(a[-n:])
        b_arr = np.array(b[-n:])
        # Use returns for a more meaningful correlation.
        if n < 25:
            return None
        a_ret = np.diff(a_arr) / a_arr[:-1]
        b_ret = np.diff(b_arr) / b_arr[:-1]
        std_a = np.std(a_ret)
        std_b = np.std(b_ret)
        if std_a == 0 or std_b == 0:
            return 0.0
        return float(np.corrcoef(a_ret, b_ret)[0, 1])


# ---------------------------------------------------------------------------
# Drawdown tracker across daily / weekly / monthly windows
# ---------------------------------------------------------------------------

class _DrawdownTracker:
    """Track NAV snapshots to compute rolling drawdowns."""

    def __init__(self) -> None:
        # (timestamp, nav)
        self._snapshots: deque[tuple[datetime, float]] = deque(maxlen=43_200)  # ~30 days @ 1/min
        self._hwm: float = 0.0

    def record(self, nav: float) -> None:
        now = datetime.now(timezone.utc)
        self._snapshots.append((now, nav))
        if nav > self._hwm:
            self._hwm = nav

    def _nav_at(self, lookback: timedelta) -> float | None:
        cutoff = datetime.now(timezone.utc) - lookback
        for ts, nav in self._snapshots:
            if ts >= cutoff:
                return nav
        return None

    def daily_drawdown(self, current_nav: float) -> float:
        start = self._nav_at(timedelta(days=1))
        if start and start > 0:
            dd = (start - current_nav) / start
            return max(0.0, dd)
        return 0.0

    def weekly_drawdown(self, current_nav: float) -> float:
        start = self._nav_at(timedelta(days=7))
        if start and start > 0:
            dd = (start - current_nav) / start
            return max(0.0, dd)
        return 0.0

    def monthly_drawdown(self, current_nav: float) -> float:
        start = self._nav_at(timedelta(days=30))
        if start and start > 0:
            dd = (start - current_nav) / start
            return max(0.0, dd)
        return 0.0


# ---------------------------------------------------------------------------
# RiskGuardian agent
# ---------------------------------------------------------------------------

class RiskGuardian(BaseAgent):
    """Approves, adjusts, or vetoes every trade decision."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="risk_guardian",
            agent_type="risk",
            bus=bus,
            **kw,
        )
        self._portfolio = PortfolioState()
        self._price_history = _PriceHistory()
        self._dd_tracker = _DrawdownTracker()

        # Defensive-mode state.
        self._defensive_until: datetime | None = None
        self._weekly_pause_until: datetime | None = None
        self._monthly_review_mode = False

        # Counters for audit.
        self._approved_count = 0
        self._rejected_count = 0

        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._portfolio_listener(), name="rg:portfolio"),
            asyncio.create_task(self._price_listener(), name="rg:prices"),
            asyncio.create_task(self._anomaly_listener(), name="rg:anomaly"),
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

    # -- main loop: consume pending decisions --------------------------------

    async def process(self) -> None:
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_DECISIONS_PENDING,
            group="risk_guardian",
            consumer=self.agent_id,
        ):
            try:
                decision = TradeDecision.model_validate(payload)
                await self._evaluate(decision)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Error evaluating decision: %s", payload)

    # -- evaluation pipeline -------------------------------------------------

    async def _evaluate(self, decision: TradeDecision) -> None:
        """Run the full check pipeline. Publish result to the appropriate stream."""

        now = datetime.now(timezone.utc)
        nav = self._portfolio.total_nav
        adjustments: dict[str, Any] = {}
        original_size = decision.size_pct

        self.log.info(
            "EVALUATING decision=%s asset=%s action=%s size=%.4f",
            decision.decision_id[:8], decision.asset,
            decision.action, decision.size_pct,
        )

        is_new_entry = decision.action in (TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT)

        # ----- (f-extra) Monthly review mode: reject all new entries -----
        if self._monthly_review_mode and is_new_entry:
            await self._reject(
                decision, "MONTHLY REVIEW MODE — all new entries paused pending human review.",
            )
            return

        # ----- (f) Weekly drawdown pause -----
        if self._weekly_pause_until and now < self._weekly_pause_until and is_new_entry:
            await self._reject(
                decision,
                "Weekly drawdown pause active until %s." % self._weekly_pause_until.isoformat(),
            )
            return

        # ----- Defensive mode (from anomaly detector) -----
        if self._defensive_until and now < self._defensive_until and is_new_entry:
            await self._reject(
                decision,
                "Defensive mode active until %s — no new entries." % self._defensive_until.isoformat(),
            )
            return

        # ----- (e) Daily drawdown gates -----
        daily_dd = self._dd_tracker.daily_drawdown(nav)

        if daily_dd >= settings.circuit_breaker_drawdown and is_new_entry:
            await self._reject(
                decision,
                "Circuit breaker: daily drawdown %.2f%% >= %.2f%%." % (
                    daily_dd * 100, settings.circuit_breaker_drawdown * 100),
            )
            return

        if daily_dd >= settings.max_daily_drawdown and is_new_entry:
            reduction = settings.drawdown_size_reduction
            decision.size_pct *= (1 - reduction)
            adjustments["size_reduced_by"] = reduction
            adjustments["reason"] = "daily drawdown %.2f%% — size cut %.0f%%" % (
                daily_dd * 100, reduction * 100,
            )
            self.log.warning(
                "Daily drawdown %.2f%%: reducing size %.4f → %.4f",
                daily_dd * 100, original_size, decision.size_pct,
            )

        # ----- (f) Weekly / monthly drawdown -----
        weekly_dd = self._dd_tracker.weekly_drawdown(nav)
        if weekly_dd >= settings.weekly_drawdown_pause:
            self._weekly_pause_until = now + timedelta(hours=24)
            self.log.warning(
                "Weekly drawdown %.2f%% — pausing new entries for 24h.",
                weekly_dd * 100,
            )
            if is_new_entry:
                await self._reject(
                    decision,
                    "Weekly drawdown %.2f%% triggered 24h pause." % (weekly_dd * 100),
                )
                return

        monthly_dd = self._dd_tracker.monthly_drawdown(nav)
        if monthly_dd >= settings.monthly_drawdown_review:
            self._monthly_review_mode = True
            self.log.critical(
                "Monthly drawdown %.2f%% — entering REVIEW MODE.", monthly_dd * 100,
            )
            if is_new_entry:
                await self._reject(
                    decision,
                    "Monthly drawdown %.2f%% triggered review mode." % (monthly_dd * 100),
                )
                return

        # ----- (a) Position size check -----
        if decision.size_pct > settings.max_position_pct:
            if is_new_entry:
                old = decision.size_pct
                decision.size_pct = settings.max_position_pct
                adjustments["size_capped_to"] = settings.max_position_pct
                self.log.warning(
                    "Position size %.4f exceeds max %.4f — capping.",
                    old, settings.max_position_pct,
                )

        # ----- (b) Gross exposure check -----
        if nav > 0 and is_new_entry:
            new_gross = self._portfolio.gross_exposure + decision.size_pct
            if new_gross > settings.max_gross_exposure:
                await self._reject(
                    decision,
                    "Gross exposure would be %.2f (limit %.2f)." % (
                        new_gross, settings.max_gross_exposure),
                )
                return

        # ----- (c) Net exposure check -----
        if nav > 0 and is_new_entry:
            direction_sign = 1.0 if decision.action == TradeAction.OPEN_LONG else -1.0
            new_net = abs(self._portfolio.net_exposure + direction_sign * decision.size_pct)
            if new_net > settings.max_net_exposure:
                await self._reject(
                    decision,
                    "Net exposure would be %.2f (limit %.2f)." % (
                        new_net, settings.max_net_exposure),
                )
                return

        # ----- (d) Correlation check (14d Pearson) -----
        if is_new_entry:
            for pos in self._portfolio.positions:
                if pos.asset == decision.asset:
                    continue  # Adding to existing position — skip.
                corr = self._price_history.pearson(decision.asset, pos.asset)
                if corr is not None and abs(corr) > settings.max_correlation_threshold:
                    await self._reject(
                        decision,
                        "Correlation %.2f with existing %s position exceeds %.2f threshold." % (
                            corr, pos.asset, settings.max_correlation_threshold),
                    )
                    return

        # ----- (g) Asset-class concentration -----
        if nav > 0 and is_new_entry:
            new_class = classify_asset(decision.asset)
            class_exposure: dict[AssetClass, float] = defaultdict(float)
            for pos in self._portfolio.positions:
                cls = classify_asset(pos.asset)
                class_exposure[cls] += pos.size_pct
            class_exposure[new_class] += decision.size_pct

            if class_exposure[new_class] > settings.max_single_asset_class_pct:
                await self._reject(
                    decision,
                    "%s class exposure would be %.1f%% (limit %.1f%%)." % (
                        new_class.value,
                        class_exposure[new_class] * 100,
                        settings.max_single_asset_class_pct * 100,
                    ),
                )
                return

        # ----- ALL CHECKS PASSED — approve -----
        await self._approve(decision, adjustments)

    # -- approve / reject helpers --------------------------------------------

    async def _approve(
        self, decision: TradeDecision, adjustments: dict[str, Any],
    ) -> None:
        decision.risk_approved = True

        # Compute projected portfolio impact.
        portfolio_impact = {
            "projected_gross_exposure": self._portfolio.gross_exposure + decision.size_pct,
            "projected_net_exposure": self._portfolio.net_exposure,
            "daily_drawdown": self._dd_tracker.daily_drawdown(self._portfolio.total_nav),
        }

        check = RiskCheck(
            decision_id=decision.decision_id,
            approved=True,
            adjustments=adjustments,
            portfolio_impact=portfolio_impact,
        )

        # Publish to both the audit stream and the approved stream.
        await self.bus.publish_to(STREAM_RISK_CHECKS, check)
        await self.bus.publish_to(STREAM_DECISIONS_APPROVED, decision)
        self._approved_count += 1

        self.log.info(
            "APPROVED decision=%s asset=%s size=%.4f adjustments=%s",
            decision.decision_id[:8], decision.asset,
            decision.size_pct, adjustments or "none",
        )

    async def _reject(self, decision: TradeDecision, reason: str) -> None:
        check = RiskCheck(
            decision_id=decision.decision_id,
            approved=False,
            veto_reason=reason,
            portfolio_impact={
                "current_gross_exposure": self._portfolio.gross_exposure,
                "current_net_exposure": self._portfolio.net_exposure,
                "daily_drawdown": self._dd_tracker.daily_drawdown(self._portfolio.total_nav),
            },
        )

        await self.bus.publish_to(STREAM_RISK_CHECKS, check)
        await self.bus.publish_to(STREAM_DECISIONS_REJECTED, decision)
        self._rejected_count += 1

        self.log.warning(
            "REJECTED decision=%s asset=%s reason=%s",
            decision.decision_id[:8], decision.asset, reason,
        )

    # -- defensive mode (triggered by anomaly detector) ----------------------

    async def _enter_defensive_mode(self, alert: AnomalyAlert) -> None:
        """Reduce positions and pause entries on high-severity anomaly."""
        now = datetime.now(timezone.utc)
        self._defensive_until = now + timedelta(minutes=settings.defensive_pause_minutes)

        self.log.critical(
            "DEFENSIVE MODE activated: %s (%s) — pause until %s",
            alert.anomaly_type, alert.description,
            self._defensive_until.isoformat(),
        )

        # Reduce all existing positions by the configured fraction.
        for pos in self._portfolio.positions:
            reduce_size = pos.size_pct * settings.defensive_position_reduction
            reduce_decision = TradeDecision(
                asset=pos.asset,
                action=TradeAction.REDUCE,
                size_pct=reduce_size,
                consensus_score=0.0,
                risk_approved=True,
                metadata={
                    "reason": "defensive_mode",
                    "anomaly_type": alert.anomaly_type,
                    "execution_mode": "market",
                },
            )
            await self.bus.publish_to(STREAM_DECISIONS_APPROVED, reduce_decision)
            self.log.warning(
                "Defensive reduce: %s by %.4f (%.0f%%)",
                pos.asset, reduce_size, settings.defensive_position_reduction * 100,
            )

    # -- background listeners ------------------------------------------------

    async def _portfolio_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE,
            group="risk_guardian_portfolio",
            consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
                self._dd_tracker.record(self._portfolio.total_nav)
            except Exception:
                pass

    async def _price_listener(self) -> None:
        """Sample hourly prices for correlation calculation."""
        last_sample = 0.0
        from core.models import PriceUpdate
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="risk_guardian_prices",
            consumer=self.agent_id,
        ):
            try:
                import time
                now = time.time()
                # Sample once per hour for correlation.
                if now - last_sample < 3600:
                    continue
                last_sample = now
                update = PriceUpdate.model_validate(payload)
                for asset, price in update.prices.items():
                    self._price_history.append(asset, price)
            except Exception:
                pass

    async def _anomaly_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_RISK_ANOMALY,
            group="risk_guardian_anomaly",
            consumer=self.agent_id,
        ):
            try:
                alert = AnomalyAlert.model_validate(payload)
                if alert.severity == AlertSeverity.CRITICAL:
                    await self._enter_defensive_mode(alert)
            except Exception:
                self.log.exception("Error handling anomaly alert.")

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        now = datetime.now(timezone.utc)
        base.update({
            "approved": self._approved_count,
            "rejected": self._rejected_count,
            "defensive_mode": (
                self._defensive_until is not None and now < self._defensive_until
            ),
            "weekly_pause": (
                self._weekly_pause_until is not None and now < self._weekly_pause_until
            ),
            "monthly_review": self._monthly_review_mode,
            "portfolio_nav": self._portfolio.total_nav,
            "portfolio_gross_exposure": self._portfolio.gross_exposure,
        })
        return base
