"""
RedTeamStrategist — LLM-powered adversarial challenger for IC debates.

Uses Claude (configurable model, default Sonnet) with an adversarial system
prompt — a DIFFERENT model context than the CIO to avoid confirmation bias.

Challenge categories:
  1. counter_evidence    — data contradicting the thesis
  2. assumption_flaw     — questionable assumptions
  3. missing_context     — important data not considered
  4. adverse_scenario    — what could go wrong
  5. timing_risk         — why now is a bad time
  6. survivorship_bias   — selection bias in supporting data
  7. crowded_trade       — too many participants, reflexivity risk

Outputs ``RedTeamChallengeV2`` with severity and recommendation.
Tracks own accuracy over time.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus
from core.models import (
    ChallengeCategory,
    InvestmentThesis,
    RedTeamChallengeV2,
    RedTeamRecommendation,
)


_ADVERSARIAL_SYSTEM = """You are the Red Team Strategist for a crypto hedge fund.
Your SOLE PURPOSE is to find flaws, risks, and problems with proposed trades.
You are adversarial by design — your job is to protect the fund from bad trades.

You must be specific, data-driven, and cite concrete counter-evidence.
Do NOT be generically negative — provide actionable, targeted challenges.

Respond with a JSON object:
{
  "category": "counter_evidence|assumption_flaw|missing_context|adverse_scenario|timing_risk|survivorship_bias|crowded_trade",
  "severity": 0.0-1.0,
  "challenge_text": "specific challenge",
  "counter_evidence": ["point1", "point2"],
  "recommendation": "proceed|reduce_conviction|pause_and_review|veto",
  "confidence": 0.0-1.0
}"""


class RedTeamStrategist(BaseAgent):
    """LLM-powered adversarial challenger for Investment Committee debates."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="red_team_strategist",
            agent_type="decision",
            bus=bus,
            **kw,
        )
        self._client: Any = None  # anthropic.AsyncAnthropic
        self._challenges_issued = 0
        self._accuracy_hits = 0
        self._accuracy_total = 0

    async def start(self) -> None:
        api_key = settings.anthropic_api_key.get_secret_value()
        if api_key:
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(api_key=api_key)
            except ImportError:
                self.log.warning("anthropic SDK not installed.")
        else:
            self.log.warning("No Anthropic API key — Red Team running in heuristic mode.")
        await super().start()

    async def stop(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
        await super().stop()

    async def process(self) -> None:
        """Idle loop — challenges are invoked by the CIO during IC debates."""
        await asyncio.sleep(3600)

    async def challenge_thesis(
        self,
        thesis: InvestmentThesis,
        market_context: str = "",
    ) -> RedTeamChallengeV2:
        """Generate an adversarial challenge for an investment thesis.

        Called by the CIO during Phase 3 of the IC debate.
        Falls back to heuristic challenges if LLM is unavailable.
        """
        if self._client is None:
            return self._heuristic_challenge(thesis)

        try:
            return await self._llm_challenge(thesis, market_context)
        except Exception:
            self.log.exception("LLM challenge failed, falling back to heuristic.")
            return self._heuristic_challenge(thesis)

    async def _llm_challenge(
        self,
        thesis: InvestmentThesis,
        market_context: str,
    ) -> RedTeamChallengeV2:
        """Use Claude to generate a challenge."""
        thesis_text = (
            f"Asset: {thesis.asset}\n"
            f"Direction: {thesis.direction}\n"
            f"Edge: {thesis.edge_description}\n"
            f"Catalyst: {thesis.catalyst}\n"
            f"Conviction: {thesis.conviction}\n"
            f"Timeframe: {thesis.timeframe}\n"
            f"Risk factors: {', '.join(thesis.risk_factors)}\n"
            f"Entry: {thesis.entry_price}, Stop: {thesis.stop_loss}, TP: {thesis.take_profit}\n"
        )
        if market_context:
            thesis_text += f"\nMarket context:\n{market_context}\n"

        msg = await self._client.messages.create(
            model=settings.cio_red_team_model,
            max_tokens=1024,
            system=_ADVERSARIAL_SYSTEM,
            messages=[{"role": "user", "content": f"Challenge this thesis:\n\n{thesis_text}"}],
        )

        text = msg.content[0].text
        # Parse JSON from response.
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code block.
            if "```" in text:
                json_part = text.split("```")[1]
                if json_part.startswith("json"):
                    json_part = json_part[4:]
                data = json.loads(json_part.strip())
            else:
                raise

        challenge = RedTeamChallengeV2(
            thesis_id=thesis.thesis_id,
            category=ChallengeCategory(data.get("category", "counter_evidence")),
            severity=float(data.get("severity", 0.5)),
            challenge_text=data.get("challenge_text", ""),
            counter_evidence=data.get("counter_evidence", []),
            recommendation=RedTeamRecommendation(data.get("recommendation", "proceed")),
            confidence=float(data.get("confidence", 0.5)),
        )

        self._challenges_issued += 1
        self.log.info(
            "RT CHALLENGE [%s]: sev=%.2f rec=%s — %s",
            challenge.category, challenge.severity,
            challenge.recommendation, challenge.challenge_text[:100],
        )
        return challenge

    def _heuristic_challenge(self, thesis: InvestmentThesis) -> RedTeamChallengeV2:
        """Fallback heuristic challenges when LLM is unavailable."""
        challenges: list[tuple[ChallengeCategory, float, str, list[str]]] = []

        # Low conviction.
        if thesis.conviction < 0.4:
            challenges.append((
                ChallengeCategory.ASSUMPTION_FLAW, 0.5,
                f"Conviction is only {thesis.conviction:.2f} — thesis lacks strong evidence.",
                ["Low conviction indicates uncertainty"],
            ))

        # No stop loss.
        if thesis.stop_loss is None:
            challenges.append((
                ChallengeCategory.MISSING_CONTEXT, 0.6,
                "No stop-loss defined — unbounded downside risk.",
                ["Risk management requires defined exits"],
            ))

        # No risk factors acknowledged.
        if not thesis.risk_factors:
            challenges.append((
                ChallengeCategory.ASSUMPTION_FLAW, 0.4,
                "No risk factors identified — every trade has risks.",
                ["Missing risk analysis"],
            ))

        # Short timeframe + large conviction gap.
        if thesis.timeframe == "scalp" and thesis.conviction > 0.7:
            challenges.append((
                ChallengeCategory.TIMING_RISK, 0.3,
                "High conviction on a scalp timeframe — noise may dominate.",
                ["Scalp trades have lower signal-to-noise"],
            ))

        if not challenges:
            challenges.append((
                ChallengeCategory.ADVERSE_SCENARIO, 0.2,
                "What happens if the broader market moves against this thesis?",
                ["Systematic risk not addressed"],
            ))

        # Pick the most severe.
        challenges.sort(key=lambda x: x[1], reverse=True)
        top = challenges[0]

        severity = top[1]
        if severity > 0.6:
            rec = RedTeamRecommendation.REDUCE_CONVICTION
        elif severity > 0.4:
            rec = RedTeamRecommendation.PAUSE_AND_REVIEW
        else:
            rec = RedTeamRecommendation.PROCEED

        self._challenges_issued += 1
        return RedTeamChallengeV2(
            thesis_id=thesis.thesis_id,
            category=top[0],
            severity=severity,
            challenge_text=top[2],
            counter_evidence=top[3],
            recommendation=rec,
            confidence=0.5,
        )

    def record_outcome(self, challenge_id: str, thesis_was_profitable: bool) -> None:
        """Track accuracy: was the Red Team's concern warranted?"""
        self._accuracy_total += 1
        if not thesis_was_profitable:
            self._accuracy_hits += 1

    @property
    def accuracy(self) -> float:
        if self._accuracy_total == 0:
            return 0.0
        return self._accuracy_hits / self._accuracy_total

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "challenges_issued": self._challenges_issued,
            "accuracy": round(self.accuracy, 3),
            "accuracy_samples": self._accuracy_total,
            "llm_available": self._client is not None,
        })
        return base
