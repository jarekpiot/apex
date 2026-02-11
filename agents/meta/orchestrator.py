"""
MetaOrchestrator — the ensemble brain of APEX.

Aggregates signals from all analysis agents, runs weighted consensus voting,
and dispatches trade decisions to the risk pipeline.

Flow:
  1. Collects AgentSignals from ``apex:signals``
  2. Buffers the latest signal per (agent, asset), pruning stale entries
  3. Every ``orch_voting_interval`` seconds runs a voting round
  4. Computes weighted consensus: ``registry_weight × conviction × direction``
  5. If |consensus| ≥ threshold **and** quorum met → creates a TradeDecision
  6. Decision published to ``decisions:pending`` → RiskGuardian → ExecutionEngine

Position awareness:
  - Subscribes to ``portfolio:state`` to know current positions
  - No existing position → OPEN_LONG / OPEN_SHORT based on consensus
  - Existing position aligns with consensus → skip (already exposed)
  - Existing position opposes consensus → CLOSE (or FLIP at high conviction)
  - Asset already decided within cooldown → skip

Sizing:
  ``size = orch_base_size_pct × |consensus| × avg_conviction``
  capped at ``max_position_pct``.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.agent_registry import AGENT_REGISTRY, voting_agents
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_SIGNALS,
)
from core.models import (
    AgentSignal,
    Direction,
    PortfolioState,
    PriceUpdate,
    Timeframe,
    TradeAction,
    TradeDecision,
)


# ---------------------------------------------------------------------------
# Per-asset signal buffer
# ---------------------------------------------------------------------------

class _SignalBuffer:
    """Keeps the *latest* signal per agent for a single asset.

    Only signals from agents with ``default_weight > 0`` are stored.
    Expired and stale signals are pruned automatically during reads.
    """

    __slots__ = ("_signals",)

    def __init__(self) -> None:
        # agent_id → AgentSignal
        self._signals: dict[str, AgentSignal] = {}

    def add(self, signal: AgentSignal) -> None:
        existing = self._signals.get(signal.agent_id)
        if existing is None or signal.timestamp >= existing.timestamp:
            self._signals[signal.agent_id] = signal

    def active(self, now: datetime, max_age_s: int) -> list[AgentSignal]:
        """Return non-expired, non-stale signals."""
        cutoff = now - timedelta(seconds=max_age_s)
        return [
            s for s in self._signals.values()
            if (s.expires_at is None or s.expires_at > now)
            and s.timestamp >= cutoff
        ]

    def prune(self, now: datetime, max_age_s: int) -> None:
        cutoff = now - timedelta(seconds=max_age_s)
        self._signals = {
            aid: s for aid, s in self._signals.items()
            if (s.expires_at is None or s.expires_at > now)
            and s.timestamp >= cutoff
        }

    def __len__(self) -> int:
        return len(self._signals)


# ---------------------------------------------------------------------------
# MetaOrchestrator agent
# ---------------------------------------------------------------------------

class MetaOrchestrator(BaseAgent):
    """Aggregates signals, runs consensus voting, dispatches decisions."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="meta_orchestrator",
            agent_type="meta",
            bus=bus,
            **kw,
        )
        # Per-asset signal buffers.
        self._buffers: dict[str, _SignalBuffer] = defaultdict(_SignalBuffer)

        # Latest portfolio snapshot.
        self._portfolio = PortfolioState()

        # Latest mid-prices for entry price.
        self._mid_prices: dict[str, float] = {}

        # Cooldown tracking: asset → monotonic time of last decision.
        self._last_decision_time: dict[str, float] = {}

        # Rejection tracking: asset → monotonic time of last rejection.
        self._last_rejection_time: dict[str, float] = {}

        # Agent weight lookup (cached from registry).
        self._weights: dict[str, float] = {
            entry.agent_id: entry.default_weight
            for entry in voting_agents()
        }

        self._decisions_sent = 0
        self._voting_rounds = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._voting_loop(), name="orch:vote"),
            asyncio.create_task(self._portfolio_listener(), name="orch:port"),
            asyncio.create_task(self._price_listener(), name="orch:px"),
            asyncio.create_task(self._rejection_listener(), name="orch:rej"),
            asyncio.create_task(self._weight_refresh_loop(), name="orch:weights"),
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

    # -- main loop: ingest signals -------------------------------------------

    async def process(self) -> None:
        """Consume AgentSignals from the shared signal stream."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_SIGNALS,
            group="meta_orchestrator",
            consumer=self.agent_id,
        ):
            try:
                signal = AgentSignal.model_validate(payload)

                # Only buffer signals from voting agents.
                if signal.agent_id not in self._weights:
                    continue

                self._buffers[signal.asset].add(signal)
            except Exception:
                self.log.exception("Error ingesting signal.")

    # -- periodic voting round -----------------------------------------------

    async def _voting_loop(self) -> None:
        """Every orch_voting_interval seconds, evaluate consensus per asset."""
        await asyncio.sleep(15)  # Let signals accumulate after boot.
        while True:
            try:
                await self._run_voting_round()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Voting round failed.")
            await asyncio.sleep(settings.orch_voting_interval)

    async def _run_voting_round(self) -> None:
        now = datetime.now(timezone.utc)
        mono_now = time.monotonic()
        max_age = settings.orch_max_signal_age
        self._voting_rounds += 1

        for asset, buf in list(self._buffers.items()):
            # Prune stale signals.
            buf.prune(now, max_age)
            signals = buf.active(now, max_age)

            if len(signals) < settings.orch_min_quorum:
                continue

            # Cooldown: skip if we recently decided on this asset.
            last_dec = self._last_decision_time.get(asset, 0.0)
            if mono_now - last_dec < settings.orch_decision_cooldown:
                continue

            # Rejection back-off: double the cooldown after rejection.
            last_rej = self._last_rejection_time.get(asset, 0.0)
            if mono_now - last_rej < settings.orch_decision_cooldown * 2:
                continue

            # --- Weighted consensus ---
            consensus, avg_conviction, dominant_tf, agent_count = (
                self._compute_consensus(signals)
            )

            if abs(consensus) < settings.orch_min_consensus:
                continue

            # --- Position awareness ---
            action = self._choose_action(asset, consensus)
            if action is None:
                continue  # Already aligned — nothing to do.

            # --- Sizing ---
            size_pct = self._compute_size(consensus, avg_conviction)

            # --- Build decision ---
            entry_price = self._mid_prices.get(asset)

            decision = TradeDecision(
                asset=asset,
                action=action,
                size_pct=size_pct,
                consensus_score=consensus,
                entry_price=entry_price,
                contributing_signals=[s.signal_id for s in signals],
                metadata={
                    "source": "meta_orchestrator",
                    "agent_count": agent_count,
                    "avg_conviction": round(avg_conviction, 4),
                    "dominant_timeframe": dominant_tf,
                    "voting_round": self._voting_rounds,
                },
            )

            await self.bus.publish_to(STREAM_DECISIONS_PENDING, decision)
            self._last_decision_time[asset] = mono_now
            self._decisions_sent += 1

            self.log.info(
                "DECISION: %s %s consensus=%.3f size=%.4f agents=%d tf=%s",
                action, asset, consensus, size_pct, agent_count, dominant_tf,
            )

    # -- consensus computation -----------------------------------------------

    def _compute_consensus(
        self, signals: list[AgentSignal],
    ) -> tuple[float, float, str, int]:
        """Weighted average of signal directions.

        Returns (consensus, avg_conviction, dominant_timeframe, agent_count).

        consensus:  −1..+1, weighted direction.
        avg_conviction: mean conviction of contributing signals.
        dominant_timeframe: the timeframe with the most weight.
        agent_count: distinct agents that contributed.
        """
        total_weight = 0.0
        weighted_dir = 0.0
        conviction_sum = 0.0
        tf_weight: dict[str, float] = defaultdict(float)

        for sig in signals:
            w = self._weights.get(sig.agent_id, 0.05)
            # Effective weight = registry weight × signal conviction.
            eff = w * sig.conviction
            weighted_dir += sig.direction * eff
            total_weight += eff
            conviction_sum += sig.conviction
            tf_weight[sig.timeframe] += eff

        if total_weight == 0:
            return 0.0, 0.0, Timeframe.INTRADAY, 0

        consensus = weighted_dir / total_weight
        consensus = max(-1.0, min(1.0, consensus))
        avg_conviction = conviction_sum / len(signals)

        dominant_tf = max(tf_weight, key=tf_weight.get)  # type: ignore[arg-type]

        return consensus, avg_conviction, dominant_tf, len(signals)

    # -- position-aware action selection -------------------------------------

    def _choose_action(self, asset: str, consensus: float) -> TradeAction | None:
        """Decide what action to take given current portfolio.

        Returns None if the existing position already aligns with the
        consensus direction (no action needed).
        """
        existing = self._find_position(asset)

        if existing is None:
            # No position → open in the consensus direction.
            return TradeAction.OPEN_LONG if consensus > 0 else TradeAction.OPEN_SHORT

        # Already have a position in this asset.
        if existing.direction == Direction.LONG and consensus > 0:
            return None  # Already long and consensus is bullish.
        if existing.direction == Direction.SHORT and consensus < 0:
            return None  # Already short and consensus is bearish.

        # Position opposes consensus.
        if abs(consensus) >= settings.orch_flip_consensus:
            # Strong opposing consensus → flip.
            return TradeAction.FLIP
        else:
            # Moderate opposition → close first, let next round re-enter.
            return TradeAction.CLOSE

    def _find_position(self, asset: str) -> Any:
        """Find existing position for the asset, or None."""
        for pos in self._portfolio.positions:
            if pos.asset == asset:
                return pos
        return None

    # -- sizing --------------------------------------------------------------

    def _compute_size(self, consensus: float, avg_conviction: float) -> float:
        """Position size proportional to consensus strength × conviction."""
        raw = settings.orch_base_size_pct * abs(consensus) * max(avg_conviction, 0.1)
        return min(raw, settings.max_position_pct)

    # -- background listeners ------------------------------------------------

    async def _portfolio_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE,
            group="meta_orch_portfolio",
            consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    async def _price_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="meta_orch_prices",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)
            except Exception:
                pass

    async def _weight_refresh_loop(self) -> None:
        """Read dynamic weights from Redis hash every 60s.

        Falls back to registry defaults if the hash doesn't exist yet
        (i.e., PerformanceAuditor hasn't run).
        """
        await asyncio.sleep(30)
        while True:
            try:
                raw = await self.bus.redis.hgetall("apex:agent_weights")
                if raw:
                    for k, v in raw.items():
                        key = k.decode() if isinstance(k, bytes) else k
                        val = v.decode() if isinstance(v, bytes) else v
                        try:
                            self._weights[key] = float(val)
                        except (ValueError, TypeError):
                            pass
                    self.log.debug("Refreshed %d dynamic weights from Redis.", len(raw))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # Redis unavailable — keep using current weights.
            await asyncio.sleep(60)

    async def _rejection_listener(self) -> None:
        """Track recently rejected decisions for back-off."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_DECISIONS_REJECTED,
            group="meta_orch_rejections",
            consumer=self.agent_id,
        ):
            try:
                decision = TradeDecision.model_validate(payload)
                # Only track our own rejections.
                if decision.metadata.get("source") == "meta_orchestrator":
                    self._last_rejection_time[decision.asset] = time.monotonic()
                    self.log.info(
                        "Decision for %s rejected — backing off.", decision.asset,
                    )
            except Exception:
                pass

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "assets_with_signals": len(self._buffers),
            "decisions_sent": self._decisions_sent,
            "voting_rounds": self._voting_rounds,
            "portfolio_positions": len(self._portfolio.positions),
            "mid_prices_tracked": len(self._mid_prices),
        })
        return base
