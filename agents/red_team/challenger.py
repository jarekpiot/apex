"""
RedTeamChallenger — devil's advocate for every proposed trade.

Subscribes to ``decisions:pending`` and applies heuristic challenges
to every trade decision before risk approval.  Publishes
``RedTeamChallenge`` objects to ``apex:red_team`` for audit.

This agent does NOT block the decision pipeline — it provides an
advisory counter-thesis. Future versions can feed challenges back
to the MetaOrchestrator to modulate conviction.

Challenge types:
  a) Low consensus — weak agreement among agents
  b) Liquidity concern — is the asset liquid enough?
  c) Correlation risk — adding correlated exposure
  d) Regime mismatch — trading against the macro regime
  e) Timing risk — high volatility / unusual hour
  f) Contrarian data — opposing signals exist
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.agent_registry import AGENT_REGISTRY
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_DECISIONS_PENDING,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_RED_TEAM,
    STREAM_RISK_ANOMALY,
)
from core.models import (
    AnomalyAlert,
    ChallengeType,
    PortfolioState,
    PriceUpdate,
    RedTeamChallenge,
    RedTeamRecommendation,
    TradeAction,
    TradeDecision,
)


class RedTeamChallenger(BaseAgent):
    """Devil's advocate — challenges every proposed trade."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="red_team",
            agent_type="red_team",
            bus=bus,
            **kw,
        )
        self._portfolio = PortfolioState()
        self._mid_prices: dict[str, float] = {}
        self._recent_anomalies: list[AnomalyAlert] = []
        self._challenges_issued = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._portfolio_listener(), name="rt:port"),
            asyncio.create_task(self._price_listener(), name="rt:px"),
            asyncio.create_task(self._anomaly_listener(), name="rt:anomaly"),
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

    # -- main loop: challenge every pending decision -------------------------

    async def process(self) -> None:
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_DECISIONS_PENDING,
            group="red_team",
            consumer=self.agent_id,
        ):
            try:
                decision = TradeDecision.model_validate(payload)
                await self._challenge(decision)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Error challenging decision.")

    async def _challenge(self, decision: TradeDecision) -> None:
        """Run all challenge heuristics and publish the most severe."""
        challenges: list[tuple[ChallengeType, float, str, list[str]]] = []

        challenges.append(self._check_consensus(decision))
        challenges.append(self._check_liquidity(decision))
        challenges.append(self._check_correlation(decision))
        challenges.append(self._check_volatility(decision))
        challenges.append(self._check_timing(decision))
        challenges.append(self._check_contrarian(decision))

        # Filter out non-issues (severity = 0).
        active = [(ct, sev, thesis, ev) for ct, sev, thesis, ev in challenges if sev > 0.1]

        if not active:
            return

        # Pick the most severe.
        active.sort(key=lambda x: x[1], reverse=True)
        top = active[0]
        total_severity = sum(sev for _, sev, _, _ in active)

        # Determine recommendation.
        if total_severity > 1.5:
            rec = RedTeamRecommendation.VETO
            adj = -0.5
        elif total_severity > 0.8:
            rec = RedTeamRecommendation.REDUCE_CONVICTION
            adj = -0.3
        elif total_severity > 0.4:
            rec = RedTeamRecommendation.PAUSE_AND_REVIEW
            adj = -0.15
        else:
            rec = RedTeamRecommendation.PROCEED
            adj = 0.0

        challenge = RedTeamChallenge(
            decision_id=decision.decision_id,
            challenge_type=top[0],
            severity=min(total_severity, 1.0),
            counter_thesis=top[2],
            evidence=[ev for _, _, _, evs in active for ev in evs],
            recommendation=rec,
            conviction_adjustment=adj,
        )

        await self.bus.publish_to(STREAM_RED_TEAM, challenge)
        self._challenges_issued += 1

        self.log.info(
            "CHALLENGE %s: %s sev=%.2f rec=%s adj=%.2f — %s",
            decision.decision_id[:8], top[0], total_severity, rec, adj, top[2],
        )

    # -- challenge heuristics ------------------------------------------------

    def _check_consensus(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Low consensus = agents disagree → risky."""
        cs = abs(d.consensus_score)
        n_agents = d.metadata.get("agent_count", 0)
        evidence: list[str] = []

        if cs < 0.4:
            severity = 0.5 * (1 - cs / 0.4)
            evidence.append("Consensus score %.2f is weak" % cs)
            if n_agents < 3:
                severity += 0.2
                evidence.append("Only %d agents voted" % n_agents)
            return (ChallengeType.CONTRARIAN_DATA, severity,
                    "Weak consensus (%.2f) — agents disagree" % cs, evidence)
        return (ChallengeType.CONTRARIAN_DATA, 0.0, "", [])

    def _check_liquidity(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Check if the asset has enough price data (proxy for liquidity)."""
        evidence: list[str] = []
        price = self._mid_prices.get(d.asset)
        if price is None:
            return (ChallengeType.LIQUIDITY_CONCERN, 0.4,
                    "No price data for %s — liquidity unknown" % d.asset,
                    ["Asset not in mid-price feed"])

        # Large position in potentially illiquid asset.
        if d.size_pct > 0.015:
            evidence.append("Size %.2f%% may exceed book depth" % (d.size_pct * 100))
            return (ChallengeType.LIQUIDITY_CONCERN, 0.3,
                    "Large position (%.2f%% of NAV) may face slippage" % (d.size_pct * 100),
                    evidence)
        return (ChallengeType.LIQUIDITY_CONCERN, 0.0, "", [])

    def _check_correlation(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Check if this adds correlated exposure."""
        evidence: list[str] = []
        is_new_entry = d.action in (TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT)
        if not is_new_entry:
            return (ChallengeType.CORRELATION_RISK, 0.0, "", [])

        # Count existing positions in the same direction.
        same_dir = 0
        for pos in self._portfolio.positions:
            if d.action == TradeAction.OPEN_LONG and pos.direction.value == "long":
                same_dir += 1
            elif d.action == TradeAction.OPEN_SHORT and pos.direction.value == "short":
                same_dir += 1

        if same_dir >= 4:
            evidence.append("%d existing positions in same direction" % same_dir)
            return (ChallengeType.CORRELATION_RISK, 0.4,
                    "Portfolio already has %d positions in same direction" % same_dir,
                    evidence)
        return (ChallengeType.CORRELATION_RISK, 0.0, "", [])

    def _check_volatility(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Check if recent anomalies suggest high volatility."""
        evidence: list[str] = []
        now = datetime.now(timezone.utc)
        recent = [a for a in self._recent_anomalies
                  if (now - a.timestamp).total_seconds() < 900]

        if len(recent) >= 2:
            types = set(a.anomaly_type for a in recent)
            evidence.extend("%s: %s" % (a.anomaly_type, a.description) for a in recent[:3])
            severity = min(len(recent) * 0.2, 0.6)
            return (ChallengeType.TIMING_RISK, severity,
                    "%d anomalies in last 15min — volatile environment" % len(recent),
                    evidence)
        return (ChallengeType.TIMING_RISK, 0.0, "", [])

    def _check_timing(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Weekend / off-hours = lower liquidity."""
        now = datetime.now(timezone.utc)
        evidence: list[str] = []

        if now.weekday() >= 5:  # Saturday/Sunday
            evidence.append("Weekend: lower liquidity and wider spreads")
            return (ChallengeType.TIMING_RISK, 0.2,
                    "Weekend trading — expect wider spreads", evidence)
        return (ChallengeType.TIMING_RISK, 0.0, "", [])

    def _check_contrarian(
        self, d: TradeDecision,
    ) -> tuple[ChallengeType, float, str, list[str]]:
        """Check if the trade is against the portfolio's existing direction."""
        evidence: list[str] = []
        existing = None
        for pos in self._portfolio.positions:
            if pos.asset == d.asset:
                existing = pos
                break

        if existing is not None and d.action == TradeAction.FLIP:
            evidence.append("Flipping existing %s position" % existing.direction)
            return (ChallengeType.REGIME_MISMATCH, 0.3,
                    "Position flip for %s — ensure regime change is confirmed" % d.asset,
                    evidence)
        return (ChallengeType.REGIME_MISMATCH, 0.0, "", [])

    # -- background listeners ------------------------------------------------

    async def _portfolio_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE, group="red_team_portfolio",
            consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    async def _price_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES, group="red_team_prices",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)
            except Exception:
                pass

    async def _anomaly_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_RISK_ANOMALY, group="red_team_anomaly",
            consumer=self.agent_id,
        ):
            try:
                alert = AnomalyAlert.model_validate(payload)
                self._recent_anomalies.append(alert)
                # Keep only last 20.
                if len(self._recent_anomalies) > 20:
                    self._recent_anomalies = self._recent_anomalies[-20:]
            except Exception:
                pass

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"challenges_issued": self._challenges_issued,
                     "recent_anomalies": len(self._recent_anomalies)})
        return base
