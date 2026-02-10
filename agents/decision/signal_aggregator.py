"""
SignalAggregator — the CIO's real-time signal inbox.

Subscribes to ``apex:signals`` and all data streams, maintains a per-asset
``SignalMatrix`` with deduplication, expiry, conflict detection, and urgency
ranking.  Publishes the compiled matrix to ``cio:signal_matrix`` every
``signal_matrix_interval`` seconds.

This agent is pure data processing — no decisions, no LLM calls.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.agent_registry import AGENT_REGISTRY
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_CIO_SIGNAL_MATRIX,
    STREAM_DATA_MACRO,
    STREAM_DATA_ONCHAIN,
    STREAM_DATA_REGIME,
    STREAM_DATA_SENTIMENT,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_RISK_ANOMALY,
    STREAM_SIGNALS,
)
from core.models import (
    AgentSignal,
    AnomalyAlert,
    MarketRegime,
    PortfolioState,
    PriceUpdate,
    RegimeState,
)
from pydantic import BaseModel, Field


class AssetSignalEntry(BaseModel):
    """Single signal in the matrix."""

    agent_id: str
    direction: float
    conviction: float
    timeframe: str
    reasoning: str = ""
    timestamp: datetime
    expires_at: datetime | None = None


class AssetMatrix(BaseModel):
    """Aggregated signal state for one asset."""

    asset: str
    signals: list[AssetSignalEntry] = Field(default_factory=list)
    net_direction: float = 0.0
    avg_conviction: float = 0.0
    signal_count: int = 0
    has_conflict: bool = False
    urgency: float = 0.0
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SignalMatrixSnapshot(BaseModel):
    """Full matrix broadcast to the CIO."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    assets: dict[str, AssetMatrix] = Field(default_factory=dict)
    regime: MarketRegime = MarketRegime.LOW_VOLATILITY
    regime_confidence: float = 0.0
    active_anomalies: int = 0
    portfolio_positions: int = 0
    total_nav: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class SignalAggregator(BaseAgent):
    """Maintains a real-time signal matrix and publishes to the CIO."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="signal_aggregator",
            agent_type="decision",
            bus=bus,
            **kw,
        )
        # Per-asset signal buffers: asset → agent_id → signal.
        self._signals: dict[str, dict[str, AssetSignalEntry]] = defaultdict(dict)
        self._regime = MarketRegime.LOW_VOLATILITY
        self._regime_confidence = 0.0
        self._portfolio = PortfolioState()
        self._mid_prices: dict[str, float] = {}
        self._recent_anomalies: list[AnomalyAlert] = []
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._signal_listener(), name="sa:sig"),
            asyncio.create_task(self._regime_listener(), name="sa:regime"),
            asyncio.create_task(self._portfolio_listener(), name="sa:port"),
            asyncio.create_task(self._price_listener(), name="sa:px"),
            asyncio.create_task(self._anomaly_listener(), name="sa:anomaly"),
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

    async def process(self) -> None:
        """Periodically compile and publish the signal matrix."""
        await asyncio.sleep(settings.signal_matrix_interval)
        now = datetime.now(timezone.utc)
        self._prune_stale(now)

        snapshot = self._compile_matrix(now)
        await self.bus.publish_to(STREAM_CIO_SIGNAL_MATRIX, snapshot)
        self.log.debug(
            "Signal matrix published: %d assets, regime=%s",
            len(snapshot.assets), snapshot.regime,
        )

    def _prune_stale(self, now: datetime) -> None:
        """Remove expired signals."""
        max_age = timedelta(seconds=settings.orch_max_signal_age)
        for asset in list(self._signals.keys()):
            buf = self._signals[asset]
            stale = [
                aid for aid, sig in buf.items()
                if (sig.expires_at and sig.expires_at < now)
                or (now - sig.timestamp > max_age)
            ]
            for aid in stale:
                del buf[aid]
            if not buf:
                del self._signals[asset]

    def _compile_matrix(self, now: datetime) -> SignalMatrixSnapshot:
        """Build the full matrix snapshot."""
        assets: dict[str, AssetMatrix] = {}

        for asset, buf in self._signals.items():
            entries = list(buf.values())
            if not entries:
                continue

            directions = [e.direction for e in entries]
            convictions = [e.conviction for e in entries]
            net_dir = sum(d * c for d, c in zip(directions, convictions))
            total_c = sum(convictions) or 1.0
            net_dir /= total_c

            # Conflict: signals pulling in opposite directions.
            has_long = any(d > 0.1 for d in directions)
            has_short = any(d < -0.1 for d in directions)
            has_conflict = has_long and has_short

            # Urgency: higher conviction + more signals + recency.
            avg_conv = sum(convictions) / len(convictions)
            recency_bonus = sum(
                1.0 / max((now - e.timestamp).total_seconds(), 60)
                for e in entries
            )
            urgency = avg_conv * len(entries) * min(recency_bonus, 1.0)

            assets[asset] = AssetMatrix(
                asset=asset,
                signals=entries,
                net_direction=round(net_dir, 4),
                avg_conviction=round(avg_conv, 4),
                signal_count=len(entries),
                has_conflict=has_conflict,
                urgency=round(urgency, 4),
                last_updated=max(e.timestamp for e in entries),
            )

        # Filter recent anomalies.
        recent_anom = [
            a for a in self._recent_anomalies
            if (now - a.timestamp).total_seconds() < 900
        ]

        return SignalMatrixSnapshot(
            assets=assets,
            regime=self._regime,
            regime_confidence=self._regime_confidence,
            active_anomalies=len(recent_anom),
            portfolio_positions=len(self._portfolio.positions),
            total_nav=self._portfolio.total_nav,
        )

    # -- listeners -----------------------------------------------------------

    async def _signal_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_SIGNALS, group="signal_aggregator", consumer=self.agent_id,
        ):
            try:
                sig = AgentSignal.model_validate(payload)
                entry = AssetSignalEntry(
                    agent_id=sig.agent_id,
                    direction=sig.direction,
                    conviction=sig.conviction,
                    timeframe=sig.timeframe,
                    reasoning=sig.reasoning,
                    timestamp=sig.timestamp,
                    expires_at=sig.expires_at,
                )
                self._signals[sig.asset][sig.agent_id] = entry
            except Exception:
                pass

    async def _regime_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_DATA_REGIME, group="sa_regime", consumer=self.agent_id,
        ):
            try:
                state = RegimeState.model_validate(payload)
                self._regime = state.regime
                self._regime_confidence = state.confidence
            except Exception:
                pass

    async def _portfolio_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE, group="sa_portfolio", consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    async def _price_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES, group="sa_prices", consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)
            except Exception:
                pass

    async def _anomaly_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_RISK_ANOMALY, group="sa_anomaly", consumer=self.agent_id,
        ):
            try:
                alert = AnomalyAlert.model_validate(payload)
                self._recent_anomalies.append(alert)
                if len(self._recent_anomalies) > 50:
                    self._recent_anomalies = self._recent_anomalies[-50:]
            except Exception:
                pass

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "assets_tracked": len(self._signals),
            "total_signals": sum(len(b) for b in self._signals.values()),
            "regime": self._regime,
        })
        return base
