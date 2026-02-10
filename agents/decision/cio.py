"""
ChiefInvestmentOfficer — top-level orchestrator for the APEX fund.

Uses Claude for complex reasoning. Runs a continuous leadership loop:

  A. **Briefing** (every 5 min): Reads the SignalAggregator's compiled signal
     matrix, produces a ``MarketBrief`` (regime, themes, opportunities, risks,
     information gaps).

  B. **Research Assignment**: Publishes ``ResearchTask`` to ``cio:research_tasks``
     targeting specific agents, collects results with timeout.

  C. **Trade Thesis Generation**: Creates ``InvestmentThesis`` with edge, catalyst,
     entry/stop/TP using Claude.

  D. **Investment Committee (5-phase debate)**:
       Phase 1 (15s) — thesis presentation
       Phase 2 (45s) — expert review with ``ExpertOpinion``
       Phase 3 (30s) — Red Team challenge
       Phase 4 (30s) — CIO deliberation / decision
       Phase 5 (10s) — position sizing via PortfolioAllocator

     CIO can override anyone EXCEPT RiskGuardian and PlatformSpecialist.
     3 consecutive Red Team overrides triggers mandatory human review.

  E. **Portfolio Review** (every 2 min): Reviews all open positions —
     hold, tighten_stop, reduce_size, add_to_position, close_position.

  F. **Priority Setting**: Maintains ``StrategyPriority`` published to
     ``cio:priorities``.

  G. **Daily Summary**: End-of-session summary via Claude with P&L
     attribution, agent grades, lessons learned.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import ICRecordRow, DailySummaryRow, async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_CIO_PRIORITIES,
    STREAM_CIO_RESEARCH_TASKS,
    STREAM_CIO_SIGNAL_MATRIX,
    STREAM_DATA_REGIME,
    STREAM_DECISIONS_PENDING,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_TRADES_EXECUTED,
)
from core.models import (
    DailySummary,
    Direction,
    ExpertOpinion,
    ICDecision,
    InvestmentCommitteeRecord,
    InvestmentThesis,
    MarketBrief,
    MarketRegime,
    PortfolioState,
    PositionAction,
    PositionReview,
    PriceUpdate,
    RedTeamChallengeV2,
    RegimeState,
    StrategyPriority,
    Timeframe,
    TradeAction,
    TradeDecision,
    TradeProposal,
)


_CIO_SYSTEM = """You are the Chief Investment Officer (CIO) of an AI-powered crypto hedge fund
trading perpetual futures on Hyperliquid.

You have access to signals from: technical analysts, fundamental analysts,
sentiment scrapers, on-chain flow monitors, macro regime classifiers,
and funding rate arbitrage scanners.

Your responsibilities:
1. Synthesise all signals into a coherent market view
2. Identify high-conviction trading opportunities
3. Generate investment theses with clear edge, catalyst, and risk factors
4. Make final trading decisions through Investment Committee debates
5. Manage risk by reviewing open positions

Be specific, quantitative, and decisive. Cite data when possible.
Express conviction as a number 0.0-1.0.

Respond ONLY with valid JSON matching the requested schema."""


class ChiefInvestmentOfficer(BaseAgent):
    """The top-level CIO — leads the entire agent team."""

    def __init__(
        self,
        bus: MessageBus,
        red_team: Any = None,
        allocator: Any = None,
        **kw: Any,
    ) -> None:
        super().__init__(
            agent_id="cio",
            agent_type="decision",
            bus=bus,
            **kw,
        )
        self._red_team = red_team          # RedTeamStrategist instance.
        self._allocator = allocator        # PortfolioAllocator instance.
        self._client: Any = None           # anthropic.AsyncAnthropic

        # State.
        self._signal_matrix: dict[str, Any] = {}
        self._regime = MarketRegime.LOW_VOLATILITY
        self._regime_confidence = 0.0
        self._portfolio = PortfolioState()
        self._mid_prices: dict[str, float] = {}
        self._latest_brief: MarketBrief | None = None
        self._priorities = StrategyPriority()

        # IC tracking.
        self._consecutive_rt_overrides = 0
        self._ic_records: list[InvestmentCommitteeRecord] = []
        self._decisions_made = 0
        self._theses_generated = 0
        self._last_daily_summary: str = ""

        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        api_key = settings.anthropic_api_key.get_secret_value()
        if api_key:
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(api_key=api_key)
            except ImportError:
                self.log.warning("anthropic SDK not installed — CIO in passive mode.")
        else:
            self.log.warning("No Anthropic API key — CIO in passive mode.")

        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._briefing_loop(), name="cio:brief"),
            asyncio.create_task(self._portfolio_review_loop(), name="cio:review"),
            asyncio.create_task(self._priority_loop(), name="cio:prio"),
            asyncio.create_task(self._matrix_listener(), name="cio:matrix"),
            asyncio.create_task(self._regime_listener(), name="cio:regime"),
            asyncio.create_task(self._portfolio_listener(), name="cio:port"),
            asyncio.create_task(self._price_listener(), name="cio:px"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        if self._client:
            await self._client.close()
            self._client = None
        await super().stop()

    async def process(self) -> None:
        """Main loop — daily summary generation."""
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        if now.hour == settings.cio_daily_summary_hour and self._last_daily_summary != today_str:
            await self._generate_daily_summary()
            self._last_daily_summary = today_str

    # -- A. Briefing ---------------------------------------------------------

    async def _briefing_loop(self) -> None:
        await asyncio.sleep(30)  # Let signals accumulate.
        while True:
            try:
                brief = await self._generate_brief()
                if brief:
                    self._latest_brief = brief
                    # Identify opportunities and potentially start IC debates.
                    await self._evaluate_opportunities(brief)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Briefing failed.")
            await asyncio.sleep(settings.cio_briefing_interval)

    async def _generate_brief(self) -> MarketBrief | None:
        """Synthesise the signal matrix into a market brief."""
        if not self._signal_matrix:
            return None

        if self._client is None:
            return self._heuristic_brief()

        matrix_summary = self._summarise_matrix()
        prompt = (
            f"Generate a market brief from this signal matrix:\n\n"
            f"{matrix_summary}\n\n"
            f"Current regime: {self._regime} (confidence: {self._regime_confidence:.2f})\n"
            f"Portfolio: {len(self._portfolio.positions)} positions, "
            f"NAV={self._portfolio.total_nav:.0f}, "
            f"drawdown={self._portfolio.drawdown:.2%}\n\n"
            f"Respond with JSON:\n"
            f'{{"themes": ["..."], "opportunities": [{{"asset": "...", "direction": "long/short", '
            f'"conviction": 0.0-1.0, "reasoning": "..."}}], '
            f'"risks": ["..."], "information_gaps": ["..."], "summary": "..."}}'
        )

        try:
            msg = await self._client.messages.create(
                model=settings.cio_model,
                max_tokens=2048,
                system=_CIO_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            data = self._parse_json(msg.content[0].text)

            brief = MarketBrief(
                regime=self._regime,
                regime_confidence=self._regime_confidence,
                themes=data.get("themes", []),
                opportunities=data.get("opportunities", []),
                risks=data.get("risks", []),
                information_gaps=data.get("information_gaps", []),
                summary=data.get("summary", ""),
            )
            self.log.info(
                "BRIEF: %d themes, %d opportunities, %d risks",
                len(brief.themes), len(brief.opportunities), len(brief.risks),
            )
            return brief

        except Exception:
            self.log.exception("Brief generation failed, using heuristic.")
            return self._heuristic_brief()

    def _heuristic_brief(self) -> MarketBrief:
        """Fallback brief from signal matrix without LLM."""
        opportunities: list[dict[str, Any]] = []
        assets_data = self._signal_matrix.get("assets", {})

        for asset, data in assets_data.items():
            if isinstance(data, dict):
                net_dir = data.get("net_direction", 0)
                avg_conv = data.get("avg_conviction", 0)
                sig_count = data.get("signal_count", 0)

                if abs(net_dir) > 0.3 and avg_conv > 0.4 and sig_count >= 2:
                    opportunities.append({
                        "asset": asset,
                        "direction": "long" if net_dir > 0 else "short",
                        "conviction": round(avg_conv, 2),
                        "reasoning": f"Net direction {net_dir:.2f} from {sig_count} signals",
                    })

        return MarketBrief(
            regime=self._regime,
            regime_confidence=self._regime_confidence,
            opportunities=opportunities,
            summary=f"Regime: {self._regime}. {len(opportunities)} opportunities identified.",
        )

    # -- C. Thesis Generation ------------------------------------------------

    async def _evaluate_opportunities(self, brief: MarketBrief) -> None:
        """Turn high-conviction opportunities into IC debates."""
        for opp in brief.opportunities:
            conv = opp.get("conviction", 0)
            if conv < 0.5:
                continue

            asset = opp.get("asset", "")
            direction_str = opp.get("direction", "long")
            direction = Direction.LONG if direction_str == "long" else Direction.SHORT

            # Don't duplicate existing positions in same direction.
            existing = [p for p in self._portfolio.positions if p.asset == asset]
            if existing and existing[0].direction.value == direction_str:
                continue

            thesis = await self._generate_thesis(asset, direction, opp)
            if thesis:
                await self._run_ic_debate(thesis)

    async def _generate_thesis(
        self,
        asset: str,
        direction: Direction,
        opportunity: dict[str, Any],
    ) -> InvestmentThesis | None:
        """Generate a full investment thesis."""
        entry_price = self._mid_prices.get(asset)

        if self._client is None:
            thesis = InvestmentThesis(
                asset=asset,
                direction=direction,
                edge_description=opportunity.get("reasoning", "Signal consensus"),
                catalyst="Multi-agent signal alignment",
                conviction=opportunity.get("conviction", 0.5),
                entry_price=entry_price,
                timeframe=Timeframe.INTRADAY,
                risk_factors=["Market regime change", "Liquidity risk"],
                invalidation_criteria=["Signal consensus reversal"],
            )
            self._theses_generated += 1
            return thesis

        prompt = (
            f"Generate an investment thesis for {direction.upper()} {asset}.\n\n"
            f"Signal data: {json.dumps(opportunity)}\n"
            f"Current price: {entry_price}\n"
            f"Regime: {self._regime}\n\n"
            f"Respond with JSON:\n"
            f'{{"edge_description": "...", "catalyst": "...", '
            f'"supporting_signals": ["..."], "risk_factors": ["..."], '
            f'"invalidation_criteria": ["..."], '
            f'"entry_price": null, "stop_loss": null, "take_profit": null, '
            f'"timeframe": "intraday", "conviction": 0.0-1.0}}'
        )

        try:
            msg = await self._client.messages.create(
                model=settings.cio_model,
                max_tokens=1024,
                system=_CIO_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            data = self._parse_json(msg.content[0].text)

            thesis = InvestmentThesis(
                asset=asset,
                direction=direction,
                edge_description=data.get("edge_description", ""),
                catalyst=data.get("catalyst", ""),
                supporting_signals=data.get("supporting_signals", []),
                risk_factors=data.get("risk_factors", []),
                invalidation_criteria=data.get("invalidation_criteria", []),
                entry_price=data.get("entry_price") or entry_price,
                stop_loss=data.get("stop_loss"),
                take_profit=data.get("take_profit"),
                timeframe=Timeframe(data.get("timeframe", "intraday")),
                conviction=float(data.get("conviction", 0.5)),
            )
            self._theses_generated += 1
            self.log.info("THESIS: %s %s conv=%.2f — %s",
                          direction, asset, thesis.conviction,
                          thesis.edge_description[:80])
            return thesis

        except Exception:
            self.log.exception("Thesis generation failed.")
            return None

    # -- D. Investment Committee Debate --------------------------------------

    async def _run_ic_debate(self, thesis: InvestmentThesis) -> None:
        """Execute the 5-phase IC debate."""
        start_time = asyncio.get_event_loop().time()
        record = InvestmentCommitteeRecord(thesis=thesis)

        # Phase 1: Thesis Presentation (15s budget, instant here).
        self.log.info("IC PHASE 1: Presenting thesis for %s %s",
                      thesis.direction, thesis.asset)

        # Phase 2: Expert Review (45s budget).
        experts = await self._gather_expert_opinions(thesis)
        record.expert_opinions = experts

        # Phase 3: Red Team Challenge (30s budget).
        rt_challenge = await self._get_red_team_challenge(thesis)
        if rt_challenge:
            record.red_team_challenges = [rt_challenge]

        # Phase 4: CIO Deliberation (30s budget).
        decision, reasoning = await self._deliberate(thesis, experts, rt_challenge)
        record.cio_decision = decision
        record.cio_reasoning = reasoning

        # Track Red Team override pattern.
        if rt_challenge and rt_challenge.recommendation in (
            RedTeamRecommendation.VETO,
            RedTeamRecommendation.PAUSE_AND_REVIEW,
        ) and decision == ICDecision.APPROVE:
            self._consecutive_rt_overrides += 1
            if self._consecutive_rt_overrides >= settings.cio_red_team_override_limit:
                record.human_review_required = True
                record.cio_decision = ICDecision.DEFER
                record.cio_reasoning += " [DEFERRED: 3 consecutive RT overrides — human review required]"
                self.log.warning("HUMAN REVIEW REQUIRED: %d consecutive Red Team overrides",
                                 self._consecutive_rt_overrides)
        else:
            self._consecutive_rt_overrides = 0

        record.consecutive_red_team_overrides = self._consecutive_rt_overrides

        # Phase 5: Position Sizing.
        if record.cio_decision == ICDecision.APPROVE:
            proposal = self._size_position(thesis, experts)
            record.final_proposal = proposal

            # Submit to risk pipeline.
            trade_decision = TradeDecision(
                asset=thesis.asset,
                action=proposal.action,
                size_pct=proposal.size_pct,
                entry_price=proposal.entry_price,
                stop_loss=proposal.stop_loss,
                take_profit=proposal.take_profit,
                consensus_score=thesis.conviction,
                contributing_signals=thesis.supporting_signals,
                metadata={
                    "source": "cio",
                    "thesis_id": thesis.thesis_id,
                    "ic_record_id": record.record_id,
                    "debate_quality": self._debate_quality(experts, rt_challenge),
                },
            )
            await self.bus.publish_to(STREAM_DECISIONS_PENDING, trade_decision)
            self._decisions_made += 1
            self.log.info(
                "IC APPROVED: %s %s size=%.4f — %s",
                proposal.action, thesis.asset, proposal.size_pct, reasoning[:80],
            )
        else:
            self.log.info("IC %s: %s — %s", decision, thesis.asset, reasoning[:80])

        # Record timing.
        record.debate_duration_seconds = asyncio.get_event_loop().time() - start_time

        # Persist IC record.
        await self._persist_ic_record(record)
        self._ic_records.append(record)
        if len(self._ic_records) > 100:
            self._ic_records = self._ic_records[-100:]

    async def _gather_expert_opinions(
        self, thesis: InvestmentThesis,
    ) -> list[ExpertOpinion]:
        """Gather expert opinions from specialised agents (simulated via signal matrix)."""
        opinions: list[ExpertOpinion] = []

        # Use signal matrix data as proxy for expert opinions.
        assets_data = self._signal_matrix.get("assets", {})
        asset_data = assets_data.get(thesis.asset, {})

        if isinstance(asset_data, dict):
            for sig in asset_data.get("signals", []):
                if isinstance(sig, dict):
                    direction = sig.get("direction", 0)
                    supports = (direction > 0 and thesis.direction == Direction.LONG) or \
                               (direction < 0 and thesis.direction == Direction.SHORT)
                    opinions.append(ExpertOpinion(
                        agent_id=sig.get("agent_id", "unknown"),
                        role=sig.get("agent_id", "analyst"),
                        assessment=sig.get("reasoning", "Signal-based assessment"),
                        conviction=sig.get("conviction", 0.5),
                        supports_thesis=supports,
                        concerns=[] if supports else ["Signal opposes thesis direction"],
                    ))

        return opinions

    async def _get_red_team_challenge(
        self, thesis: InvestmentThesis,
    ) -> RedTeamChallengeV2 | None:
        """Get Red Team challenge if the strategist is available."""
        if self._red_team is None:
            return None
        try:
            context = f"Regime: {self._regime}, NAV: {self._portfolio.total_nav:.0f}"
            return await asyncio.wait_for(
                self._red_team.challenge_thesis(thesis, context),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            self.log.warning("Red Team challenge timed out.")
            return None
        except Exception:
            self.log.exception("Red Team challenge failed.")
            return None

    async def _deliberate(
        self,
        thesis: InvestmentThesis,
        experts: list[ExpertOpinion],
        rt_challenge: RedTeamChallengeV2 | None,
    ) -> tuple[ICDecision, str]:
        """CIO deliberation: weigh evidence and decide."""
        # Count support / oppose.
        support_count = sum(1 for e in experts if e.supports_thesis)
        oppose_count = len(experts) - support_count
        avg_conviction = (
            sum(e.conviction for e in experts) / len(experts)
            if experts else 0.0
        )

        # Red Team severity.
        rt_severity = rt_challenge.severity if rt_challenge else 0.0
        rt_recommends_veto = (
            rt_challenge is not None and
            rt_challenge.recommendation in (
                RedTeamRecommendation.VETO,
                RedTeamRecommendation.PAUSE_AND_REVIEW,
            )
        )

        # LLM deliberation if available.
        if self._client is not None:
            try:
                return await self._llm_deliberate(thesis, experts, rt_challenge)
            except Exception:
                self.log.exception("LLM deliberation failed, using heuristic.")

        # Heuristic decision.
        if rt_severity > 0.8 and rt_recommends_veto:
            return ICDecision.REJECT, f"Red Team veto (severity={rt_severity:.2f})"

        if oppose_count > support_count and avg_conviction < 0.4:
            return ICDecision.REJECT, f"Majority oppose ({oppose_count}/{len(experts)})"

        if thesis.conviction < 0.3:
            return ICDecision.REJECT, f"Low conviction ({thesis.conviction:.2f})"

        if self._portfolio.drawdown > settings.max_daily_drawdown * 0.8:
            return ICDecision.REDUCE, f"Near drawdown limit ({self._portfolio.drawdown:.2%})"

        if support_count >= oppose_count and thesis.conviction >= 0.5:
            return ICDecision.APPROVE, f"Consensus support ({support_count}/{len(experts)}), conv={thesis.conviction:.2f}"

        if thesis.conviction >= 0.7 and rt_severity < 0.5:
            return ICDecision.APPROVE, f"High conviction override ({thesis.conviction:.2f})"

        return ICDecision.DEFER, "Insufficient evidence for or against"

    async def _llm_deliberate(
        self,
        thesis: InvestmentThesis,
        experts: list[ExpertOpinion],
        rt_challenge: RedTeamChallengeV2 | None,
    ) -> tuple[ICDecision, str]:
        """Use Claude for the CIO's deliberation."""
        expert_summary = "\n".join(
            f"  - {e.agent_id}: {'SUPPORTS' if e.supports_thesis else 'OPPOSES'} "
            f"(conv={e.conviction:.2f}) — {e.assessment[:100]}"
            for e in experts
        )
        rt_text = ""
        if rt_challenge:
            rt_text = (
                f"\nRed Team Challenge (severity={rt_challenge.severity:.2f}):\n"
                f"  {rt_challenge.challenge_text}\n"
                f"  Recommendation: {rt_challenge.recommendation}\n"
            )

        prompt = (
            f"IC Deliberation for {thesis.direction.upper()} {thesis.asset}:\n\n"
            f"Thesis: {thesis.edge_description}\n"
            f"Conviction: {thesis.conviction:.2f}\n"
            f"Entry: {thesis.entry_price}, Stop: {thesis.stop_loss}, TP: {thesis.take_profit}\n\n"
            f"Expert Opinions:\n{expert_summary}\n"
            f"{rt_text}\n"
            f"Portfolio: drawdown={self._portfolio.drawdown:.2%}, "
            f"positions={len(self._portfolio.positions)}\n"
            f"Regime: {self._regime}\n\n"
            f"IMPORTANT: You CANNOT override Risk Guardian or Platform Specialist.\n\n"
            f'Respond with JSON: {{"decision": "approve|reject|reduce|defer", "reasoning": "..."}}'
        )

        msg = await self._client.messages.create(
            model=settings.cio_model,
            max_tokens=512,
            system=_CIO_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        data = self._parse_json(msg.content[0].text)
        decision = ICDecision(data.get("decision", "defer"))
        reasoning = data.get("reasoning", "")
        return decision, reasoning

    # -- E. Portfolio Review -------------------------------------------------

    async def _portfolio_review_loop(self) -> None:
        await asyncio.sleep(60)
        while True:
            try:
                await self._review_positions()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Portfolio review failed.")
            await asyncio.sleep(settings.cio_portfolio_review_interval)

    async def _review_positions(self) -> None:
        """Review all open positions and publish actions."""
        if not self._portfolio.positions:
            return

        for pos in self._portfolio.positions:
            review = self._heuristic_position_review(pos)
            if review.action != PositionAction.HOLD:
                self.log.info(
                    "POSITION REVIEW: %s %s → %s — %s",
                    pos.asset, pos.direction, review.action, review.reasoning,
                )
                # Convert to trade decision if closing/reducing.
                if review.action == PositionAction.CLOSE_POSITION:
                    decision = TradeDecision(
                        asset=pos.asset,
                        action=TradeAction.CLOSE,
                        size_pct=pos.size_pct,
                        consensus_score=0.0,
                        metadata={"source": "cio_review", "reason": review.reasoning},
                    )
                    await self.bus.publish_to(STREAM_DECISIONS_PENDING, decision)
                elif review.action == PositionAction.REDUCE_SIZE:
                    decision = TradeDecision(
                        asset=pos.asset,
                        action=TradeAction.REDUCE,
                        size_pct=pos.size_pct * abs(review.size_adjustment_pct),
                        consensus_score=0.0,
                        metadata={"source": "cio_review", "reason": review.reasoning},
                    )
                    await self.bus.publish_to(STREAM_DECISIONS_PENDING, decision)

    def _heuristic_position_review(self, pos: Any) -> PositionReview:
        """Simple heuristic review of an open position."""
        pnl_pct = pos.unrealised_pnl / max(self._portfolio.total_nav, 1) * 100

        # Take profit if large unrealised gain.
        if pnl_pct > 3.0:
            return PositionReview(
                asset=pos.asset,
                action=PositionAction.REDUCE_SIZE,
                reasoning=f"Unrealised PnL {pnl_pct:.1f}% — taking partial profit",
                size_adjustment_pct=-0.5,
            )

        # Cut losses.
        if pnl_pct < -2.0:
            return PositionReview(
                asset=pos.asset,
                action=PositionAction.CLOSE_POSITION,
                reasoning=f"Unrealised loss {pnl_pct:.1f}% exceeds threshold",
            )

        # Tighten stop on winners.
        if pnl_pct > 1.0:
            return PositionReview(
                asset=pos.asset,
                action=PositionAction.TIGHTEN_STOP,
                reasoning=f"In profit {pnl_pct:.1f}% — tightening stop",
                new_stop_loss=pos.entry_price,
            )

        return PositionReview(asset=pos.asset, action=PositionAction.HOLD)

    # -- F. Priority Setting -------------------------------------------------

    async def _priority_loop(self) -> None:
        await asyncio.sleep(45)
        while True:
            try:
                prio = self._compute_priorities()
                self._priorities = prio
                await self.bus.publish_to(STREAM_CIO_PRIORITIES, prio)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Priority update failed.")
            await asyncio.sleep(settings.cio_priority_interval)

    def _compute_priorities(self) -> StrategyPriority:
        """Compute current strategic priorities."""
        # Focus on assets with most signals.
        assets_data = self._signal_matrix.get("assets", {})
        focus = sorted(
            assets_data.keys(),
            key=lambda a: assets_data[a].get("signal_count", 0)
            if isinstance(assets_data[a], dict) else 0,
            reverse=True,
        )[:5]

        # Risk budget based on drawdown.
        dd = self._portfolio.drawdown
        if dd > settings.max_daily_drawdown * 0.8:
            risk_budget = 0.2
            priority = 5  # Defensive.
        elif dd > settings.max_daily_drawdown * 0.5:
            risk_budget = 0.5
            priority = 3
        else:
            risk_budget = 1.0
            priority = 1  # Full risk-on.

        return StrategyPriority(
            priority_level=priority,
            focus_assets=focus,
            regime_context=self._regime,
            risk_budget_pct=risk_budget,
            notes=f"Regime={self._regime}, DD={dd:.2%}, positions={len(self._portfolio.positions)}",
        )

    # -- G. Daily Summary ----------------------------------------------------

    async def _generate_daily_summary(self) -> None:
        """Generate end-of-day summary."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")

        summary = DailySummary(
            date=today,
            total_pnl=self._portfolio.daily_pnl,
            total_pnl_pct=(
                self._portfolio.daily_pnl / max(self._portfolio.total_nav, 1) * 100
            ),
            trades_executed=self._decisions_made,
            regime_summary=f"Primary regime: {self._regime}",
            lessons_learned=[
                f"IC debates: {len(self._ic_records)} total",
                f"Red Team override count: {self._consecutive_rt_overrides}",
            ],
        )

        # Persist.
        try:
            async with async_session_factory() as session:
                row = DailySummaryRow(
                    date=today,
                    total_pnl=summary.total_pnl,
                    trades_executed=summary.trades_executed,
                    summary=summary.model_dump(),
                )
                session.add(row)
                await session.commit()
        except Exception:
            self.log.exception("Failed to persist daily summary.")

        self.log.info("DAILY SUMMARY: PnL=%.2f, trades=%d", summary.total_pnl, summary.trades_executed)

    # -- helpers -------------------------------------------------------------

    def _size_position(
        self,
        thesis: InvestmentThesis,
        experts: list[ExpertOpinion],
    ) -> TradeProposal:
        """Use PortfolioAllocator or fallback sizing."""
        debate_quality = sum(e.conviction for e in experts) / max(len(experts), 1)

        if self._allocator:
            return self._allocator.compute_size(
                thesis=thesis,
                regime=self._regime,
                debate_quality=debate_quality,
            )

        # Fallback: simple conviction-based sizing.
        raw = settings.orch_base_size_pct * thesis.conviction
        size = min(raw, settings.max_position_pct)
        action = TradeAction.OPEN_LONG if thesis.direction == Direction.LONG else TradeAction.OPEN_SHORT

        return TradeProposal(
            thesis_id=thesis.thesis_id,
            asset=thesis.asset,
            action=action,
            size_pct=round(size, 6),
            entry_price=thesis.entry_price,
            stop_loss=thesis.stop_loss,
            take_profit=thesis.take_profit,
            reasoning=f"Fallback sizing: conviction={thesis.conviction:.2f}",
        )

    def _debate_quality(
        self,
        experts: list[ExpertOpinion],
        rt_challenge: RedTeamChallengeV2 | None,
    ) -> float:
        """Score the quality of an IC debate 0..1."""
        score = 0.0
        if experts:
            score += 0.3 * min(len(experts) / 4, 1.0)  # More experts = better.
            score += 0.2 * (sum(e.conviction for e in experts) / len(experts))
        if rt_challenge:
            score += 0.3  # Red Team participated.
            score += 0.2 * rt_challenge.confidence
        return min(score, 1.0)

    def _summarise_matrix(self) -> str:
        """Summarise signal matrix for LLM prompt."""
        assets_data = self._signal_matrix.get("assets", {})
        lines: list[str] = []
        for asset, data in sorted(assets_data.items()):
            if isinstance(data, dict):
                lines.append(
                    f"  {asset}: dir={data.get('net_direction', 0):.2f} "
                    f"conv={data.get('avg_conviction', 0):.2f} "
                    f"signals={data.get('signal_count', 0)} "
                    f"conflict={'YES' if data.get('has_conflict') else 'no'}"
                )
        return "\n".join(lines) if lines else "No signals available."

    async def _persist_ic_record(self, record: InvestmentCommitteeRecord) -> None:
        """Persist IC record to database."""
        try:
            async with async_session_factory() as session:
                row = ICRecordRow(
                    record_id=record.record_id,
                    asset=record.thesis.asset if record.thesis else "",
                    decision=record.cio_decision.value,
                    thesis=record.thesis.model_dump() if record.thesis else {},
                    debate=record.model_dump(),
                )
                session.add(row)
                await session.commit()
        except Exception:
            self.log.exception("Failed to persist IC record.")

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        """Parse JSON from LLM response, handling markdown code blocks."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            if "```" in text:
                json_part = text.split("```")[1]
                if json_part.startswith("json"):
                    json_part = json_part[4:]
                return json.loads(json_part.strip())
            raise

    # -- listeners -----------------------------------------------------------

    async def _matrix_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_CIO_SIGNAL_MATRIX, group="cio_matrix", consumer=self.agent_id,
        ):
            try:
                self._signal_matrix = payload
            except Exception:
                pass

    async def _regime_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_DATA_REGIME, group="cio_regime", consumer=self.agent_id,
        ):
            try:
                state = RegimeState.model_validate(payload)
                self._regime = state.regime
                self._regime_confidence = state.confidence
            except Exception:
                pass

    async def _portfolio_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE, group="cio_portfolio", consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    async def _price_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES, group="cio_prices", consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)
            except Exception:
                pass

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "regime": self._regime,
            "decisions_made": self._decisions_made,
            "theses_generated": self._theses_generated,
            "ic_records": len(self._ic_records),
            "consecutive_rt_overrides": self._consecutive_rt_overrides,
            "llm_available": self._client is not None,
            "portfolio_positions": len(self._portfolio.positions),
        })
        return base
