"""
PerformanceAuditor — tracks per-agent performance and computes dynamic weights.

Subscribes to trades, decisions, signals, and prices to build a rolling
performance picture of every agent.  Every ``perf_weight_update_interval``
seconds it recalculates recommended weights and writes them to:
  - Redis hash ``apex:agent_weights`` (hot path for MetaOrchestrator)
  - ``agent_weights`` DB table (persistence / audit)

Weight algorithm:
  raw = default_weight * (1 + 0.5 * max(0, rolling_sharpe))
  smoothed via EMA, clamped ±perf_max_weight_change per update, normalised.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.agent_registry import AGENT_REGISTRY, voting_agents
from config.settings import settings
from core.database import AgentWeightRow, PerformanceMetricRow, async_session_factory
from core.gamification import GamificationEngine, SignalOutcome
from core.message_bus import (
    MessageBus,
    STREAM_AGENT_RANKINGS,
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_REJECTED,
    STREAM_MARKET_PRICES,
    STREAM_PERFORMANCE_REPORTS,
    STREAM_SIGNALS,
    STREAM_TRADES_EXECUTED,
    STREAM_WEIGHT_UPDATES,
)
from core.models import (
    AgentPerformanceReport,
    AgentRankingSnapshot,
    AgentSignal,
    ExecutedTrade,
    PriceUpdate,
    RiskCheck,
    TradeDecision,
    WeightUpdate,
)


# ---------------------------------------------------------------------------
# Internal tracking structures
# ---------------------------------------------------------------------------

class _TradeRecord:
    """Links an executed trade back to its contributing agents."""

    __slots__ = (
        "trade_id", "decision_id", "asset", "side", "size", "entry_price",
        "fee", "timestamp", "contributing_agents", "exit_price", "pnl_bps",
        "is_closed",
    )

    def __init__(self, trade: ExecutedTrade, contributing_agents: list[str]) -> None:
        self.trade_id = trade.trade_id
        self.decision_id = trade.decision_id
        self.asset = trade.asset
        self.side = trade.side
        self.size = trade.size
        self.entry_price = trade.price
        self.fee = trade.fee
        self.timestamp = trade.timestamp
        self.contributing_agents = contributing_agents
        self.exit_price: float | None = None
        self.pnl_bps: float = 0.0
        self.is_closed = False


class _AgentStats:
    """Accumulates rolling stats for a single agent."""

    __slots__ = (
        "total_signals", "signals_traded", "wins", "losses",
        "returns_bps", "pnl_total",
    )

    def __init__(self) -> None:
        self.total_signals = 0
        self.signals_traded = 0
        self.wins = 0
        self.losses = 0
        self.returns_bps: list[float] = []
        self.pnl_total = 0.0

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        return self.wins / total if total > 0 else 0.0

    @property
    def avg_return_bps(self) -> float:
        return sum(self.returns_bps) / len(self.returns_bps) if self.returns_bps else 0.0

    @property
    def sharpe_ratio(self) -> float:
        if len(self.returns_bps) < 2:
            return 0.0
        mean = sum(self.returns_bps) / len(self.returns_bps)
        variance = sum((r - mean) ** 2 for r in self.returns_bps) / (len(self.returns_bps) - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        return mean / std if std > 0 else 0.0

    @property
    def signal_to_trade_conversion(self) -> float:
        return self.signals_traded / self.total_signals if self.total_signals > 0 else 0.0


# ---------------------------------------------------------------------------
# PerformanceAuditor agent
# ---------------------------------------------------------------------------

class PerformanceAuditor(BaseAgent):
    """Tracks per-agent performance and computes dynamic consensus weights."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="performance_auditor",
            agent_type="meta",
            bus=bus,
            **kw,
        )
        # Trade tracking.
        self._open_trades: dict[str, _TradeRecord] = {}  # trade_id → record
        self._closed_trades: list[_TradeRecord] = []

        # Decision tracking: decision_id → contributing agent_ids.
        self._decision_agents: dict[str, list[str]] = {}

        # Signal cache: signal_id → AgentSignal (for gamification outcome).
        self._signal_cache: dict[str, AgentSignal] = {}

        # Signal counting per agent.
        self._signal_counts: dict[str, int] = defaultdict(int)

        # Decision outcome tracking.
        self._approvals = 0
        self._rejections = 0

        # Latest prices.
        self._mid_prices: dict[str, float] = {}

        # Current weights.
        self._current_weights: dict[str, float] = {
            e.agent_id: e.default_weight for e in voting_agents()
        }

        # Gamification engine.
        self.gamification = GamificationEngine()

        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._trade_listener(), name="perf:trades"),
            asyncio.create_task(self._decision_listener(), name="perf:decisions"),
            asyncio.create_task(self._price_listener(), name="perf:prices"),
            asyncio.create_task(self._audit_loop(), name="perf:audit"),
            asyncio.create_task(self._weight_update_loop(), name="perf:weights"),
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

    # -- main loop: ingest signals for counting ------------------------------

    async def process(self) -> None:
        """Count signals per agent from the shared signal stream."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_SIGNALS,
            group="performance_auditor",
            consumer=self.agent_id,
        ):
            try:
                signal = AgentSignal.model_validate(payload)
                self._signal_counts[signal.agent_id] += 1
                # Cache for gamification outcome recording.
                self._signal_cache[signal.signal_id] = signal
                # Cap cache size.
                if len(self._signal_cache) > 5000:
                    oldest = sorted(self._signal_cache.keys())[:1000]
                    for k in oldest:
                        del self._signal_cache[k]
            except Exception:
                self.log.exception("Error ingesting signal for counting.")

    # -- sub-task: trade listener --------------------------------------------

    async def _trade_listener(self) -> None:
        """Capture executed trades and map to contributing agents."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_TRADES_EXECUTED,
            group="perf_trades",
            consumer=self.agent_id,
        ):
            try:
                trade = ExecutedTrade.model_validate(payload)
                agents = self._decision_agents.get(trade.decision_id, [])
                record = _TradeRecord(trade, agents)
                self._open_trades[trade.trade_id] = record

                # Mark agents as having traded.
                for aid in agents:
                    self._signal_counts.setdefault(aid, 0)
            except Exception:
                self.log.exception("Error processing executed trade.")

    # -- sub-task: decision listener -----------------------------------------

    async def _decision_listener(self) -> None:
        """Track approval/rejection and map decision → contributing agents."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_DECISIONS_APPROVED,
            group="perf_decisions_approved",
            consumer=self.agent_id,
        ):
            try:
                decision = TradeDecision.model_validate(payload)
                self._decision_agents[decision.decision_id] = list(
                    set(decision.contributing_signals)  # signal_ids, not agent_ids
                )
                self._approvals += 1
            except Exception:
                self.log.exception("Error processing approved decision.")

    # -- sub-task: price listener --------------------------------------------

    async def _price_listener(self) -> None:
        """Track mid-prices for open trade PnL calculation."""
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="perf_prices",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)

                # Update PnL on open trades.
                for record in self._open_trades.values():
                    if record.asset in update.prices and not record.is_closed:
                        current = update.prices[record.asset]
                        if record.entry_price > 0:
                            if record.side in ("buy", "long"):
                                record.pnl_bps = ((current - record.entry_price) / record.entry_price) * 10_000
                            else:
                                record.pnl_bps = ((record.entry_price - current) / record.entry_price) * 10_000
            except Exception:
                pass

    # -- sub-task: audit loop ------------------------------------------------

    async def _audit_loop(self) -> None:
        """Compute per-agent rolling metrics and persist."""
        await asyncio.sleep(60)  # Let data accumulate.
        while True:
            try:
                await self._run_audit()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Audit loop failed.")
            await asyncio.sleep(settings.perf_audit_interval)

    async def _run_audit(self) -> None:
        """Compute and publish performance reports for each agent."""
        # Close trades older than exit horizon.
        cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.strategy_lab_exit_hours)
        to_close = [
            tid for tid, rec in self._open_trades.items()
            if rec.timestamp < cutoff and not rec.is_closed
        ]
        for tid in to_close:
            rec = self._open_trades[tid]
            current = self._mid_prices.get(rec.asset)
            if current and rec.entry_price > 0:
                if rec.side in ("buy", "long"):
                    rec.pnl_bps = ((current - rec.entry_price) / rec.entry_price) * 10_000
                else:
                    rec.pnl_bps = ((rec.entry_price - current) / rec.entry_price) * 10_000
                rec.exit_price = current
            rec.is_closed = True
            self._closed_trades.append(rec)
            del self._open_trades[tid]

            # Feed gamification: record outcome for contributing agents.
            correct = rec.pnl_bps > 0
            actual_move = rec.pnl_bps / 100.0  # bps → percent
            primary = rec.contributing_agents[0] if rec.contributing_agents else ""
            for aid in rec.contributing_agents:
                outcome = SignalOutcome(
                    signal_id=rec.trade_id,
                    agent_id=aid,
                    asset=rec.asset,
                    predicted_direction=1.0 if rec.side in ("buy", "long") else -1.0,
                    actual_move_pct=actual_move,
                    correct=correct,
                    was_primary_driver=(aid == primary),
                    pnl_contribution=rec.pnl_bps,
                )
                self.gamification.record_outcome(outcome)

        # Build per-agent stats from closed trades.
        agent_stats: dict[str, _AgentStats] = defaultdict(_AgentStats)
        for rec in self._closed_trades:
            for aid in rec.contributing_agents:
                stats = agent_stats[aid]
                stats.signals_traded += 1
                stats.returns_bps.append(rec.pnl_bps)
                stats.pnl_total += rec.pnl_bps
                if rec.pnl_bps > 0:
                    stats.wins += 1
                else:
                    stats.losses += 1

        # Merge signal counts.
        for aid, count in self._signal_counts.items():
            agent_stats[aid].total_signals = count

        # Build and publish reports.
        for aid, stats in agent_stats.items():
            report = AgentPerformanceReport(
                agent_id=aid,
                window_days=settings.perf_rolling_window_days,
                total_signals=stats.total_signals,
                signals_traded=stats.signals_traded,
                signal_to_trade_conversion=stats.signal_to_trade_conversion,
                win_rate=stats.win_rate,
                avg_return_bps=stats.avg_return_bps,
                sharpe_ratio=stats.sharpe_ratio,
                total_pnl=stats.pnl_total,
                recommended_weight=self._compute_recommended_weight(aid, stats),
            )
            await self.bus.publish_to(STREAM_PERFORMANCE_REPORTS, report)

            # Persist to DB.
            try:
                async with async_session_factory() as session:
                    session.add(PerformanceMetricRow(
                        metric_name="agent_performance",
                        metric_value=stats.sharpe_ratio,
                        source="performance_auditor",
                        agent_id=aid,
                        metadata_={
                            "win_rate": stats.win_rate,
                            "avg_return_bps": stats.avg_return_bps,
                            "total_signals": stats.total_signals,
                            "signals_traded": stats.signals_traded,
                        },
                    ))
                    await session.commit()
            except Exception:
                self.log.exception("Failed to persist performance metric for %s.", aid)

    def _compute_recommended_weight(self, agent_id: str, stats: _AgentStats) -> float:
        """Compute recommended weight based on rolling Sharpe."""
        entry = AGENT_REGISTRY.get(agent_id)
        default = entry.default_weight if entry else 0.05
        sharpe = stats.sharpe_ratio
        raw = default * (1.0 + 0.5 * max(0.0, sharpe))
        return max(0.01, raw)

    # -- sub-task: weight update loop ----------------------------------------

    async def _weight_update_loop(self) -> None:
        """Recalculate dynamic weights and write to Redis + DB."""
        await asyncio.sleep(120)  # Let audit data accumulate.
        while True:
            try:
                await self._update_weights()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Weight update failed.")
            await asyncio.sleep(settings.perf_weight_update_interval)

    async def _update_weights(self) -> None:
        """EMA-smooth and normalise recommended weights, write to Redis hash."""
        # Build per-agent stats from closed trades.
        agent_stats: dict[str, _AgentStats] = defaultdict(_AgentStats)
        for rec in self._closed_trades:
            for aid in rec.contributing_agents:
                stats = agent_stats[aid]
                stats.signals_traded += 1
                stats.returns_bps.append(rec.pnl_bps)
                if rec.pnl_bps > 0:
                    stats.wins += 1
                else:
                    stats.losses += 1

        # Only update if we have enough data.
        voting = {e.agent_id: e.default_weight for e in voting_agents()}
        new_weights: dict[str, float] = {}
        alpha = settings.perf_weight_ema_alpha
        max_change = settings.perf_max_weight_change

        for aid, default_w in voting.items():
            stats = agent_stats.get(aid)
            if stats and stats.signals_traded >= settings.perf_min_trades_for_weight:
                recommended = self._compute_recommended_weight(aid, stats)
                # EMA smoothing.
                current = self._current_weights.get(aid, default_w)
                smoothed = alpha * recommended + (1 - alpha) * current
                # Clamp change.
                delta = smoothed - current
                delta = max(-max_change, min(max_change, delta))
                new_weights[aid] = current + delta
            else:
                new_weights[aid] = self._current_weights.get(aid, default_w)

        # Normalise so weights sum ≈ 1.
        total = sum(new_weights.values())
        if total > 0:
            new_weights = {aid: w / total for aid, w in new_weights.items()}

        # Write to Redis hash.
        try:
            redis = self.bus.redis
            pipe = redis.pipeline()
            for aid, w in new_weights.items():
                pipe.hset("apex:agent_weights", aid, str(w))
            await pipe.execute()
        except Exception:
            self.log.exception("Failed to write weights to Redis.")

        # Persist to DB.
        try:
            async with async_session_factory() as session:
                for aid, w in new_weights.items():
                    prev = self._current_weights.get(aid, 0.0)
                    session.add(AgentWeightRow(
                        agent_id=aid,
                        weight=w,
                        previous_weight=prev,
                        reason="performance_audit",
                    ))
                await session.commit()
        except Exception:
            self.log.exception("Failed to persist weight update to DB.")

        # Publish update event.
        update = WeightUpdate(
            weights=new_weights,
            reason="periodic_performance_audit",
        )
        await self.bus.publish_to(STREAM_WEIGHT_UPDATES, update)

        self._current_weights = new_weights
        self.log.info("Dynamic weights updated: %d agents.", len(new_weights))

        # Feed Sharpe ratios to gamification engine for rank checks.
        for aid, stats in agent_stats.items():
            self.gamification.set_sharpe(aid, stats.sharpe_ratio)

        # Publish agent rankings.
        await self._publish_rankings()

    async def _publish_rankings(self) -> None:
        """Publish current agent rankings to the rankings stream."""
        leaderboard = self.gamification.get_leaderboard()
        rankings = []
        for p in leaderboard:
            total = p.signals_30d
            wr = p.wins_30d / total if total > 0 else 0.0
            rankings.append({
                "agent_id": p.agent_id,
                "rank": p.rank,
                "xp": p.xp,
                "level": p.level,
                "win_streak": p.current_win_streak,
                "loss_streak": p.current_loss_streak,
                "win_rate": round(wr, 4),
                "multiplier": p.signal_weight_multiplier,
                "on_probation": p.on_probation,
                "benched": p.benched,
                "best_regime": p.best_regime,
                "best_asset_class": p.best_asset_class,
            })
        snapshot = AgentRankingSnapshot(rankings=rankings)
        await self.bus.publish_to(STREAM_AGENT_RANKINGS, snapshot)

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "open_trades": len(self._open_trades),
            "closed_trades": len(self._closed_trades),
            "agents_tracked": len(self._signal_counts),
            "approvals": self._approvals,
            "rejections": self._rejections,
            "current_weights": dict(self._current_weights),
            "gamification_profiles": len(self.gamification.profiles),
        })
        return base
