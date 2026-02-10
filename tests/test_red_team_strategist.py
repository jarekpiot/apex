"""
Tests for the RedTeamStrategist agent.

Validates:
  - Heuristic challenges: low conviction, no stop loss, no risk factors, scalp timing
  - Default fallback challenge (adverse_scenario)
  - Severity-to-recommendation mapping
  - Accuracy tracking
  - LLM JSON parsing (raw + markdown code block)
  - Health reporting
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agents.decision.red_team import RedTeamStrategist
from core.models import (
    ChallengeCategory,
    Direction,
    InvestmentThesis,
    RedTeamChallengeV2,
    RedTeamRecommendation,
    Timeframe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def agent(mock_bus):
    return RedTeamStrategist(bus=mock_bus)


def _make_thesis(
    conviction: float = 0.8,
    stop_loss: float | None = 49000.0,
    risk_factors: list[str] | None = None,
    timeframe: Timeframe = Timeframe.INTRADAY,
    direction: Direction = Direction.LONG,
    asset: str = "BTC",
) -> InvestmentThesis:
    return InvestmentThesis(
        asset=asset,
        direction=direction,
        conviction=conviction,
        stop_loss=stop_loss,
        timeframe=timeframe,
        risk_factors=risk_factors if risk_factors is not None else ["market risk"],
        entry_price=50000.0,
        take_profit=52000.0,
    )


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def test_instantiation(mock_bus):
    agent = RedTeamStrategist(bus=mock_bus)
    assert agent.agent_id == "red_team_strategist"
    assert agent.agent_type == "decision"
    assert agent._challenges_issued == 0
    assert agent._client is None


# ---------------------------------------------------------------------------
# Heuristic challenges
# ---------------------------------------------------------------------------

def test_heuristic_low_conviction(agent):
    thesis = _make_thesis(conviction=0.3)
    c = agent._heuristic_challenge(thesis)
    assert c.category == ChallengeCategory.ASSUMPTION_FLAW
    assert c.severity == 0.5
    assert "conviction" in c.challenge_text.lower()


def test_heuristic_no_stop_loss(agent):
    thesis = _make_thesis(stop_loss=None)
    c = agent._heuristic_challenge(thesis)
    assert c.category == ChallengeCategory.MISSING_CONTEXT
    assert c.severity == 0.6
    assert "stop" in c.challenge_text.lower()


def test_heuristic_no_risk_factors(agent):
    thesis = _make_thesis(risk_factors=[])
    c = agent._heuristic_challenge(thesis)
    assert c.category == ChallengeCategory.ASSUMPTION_FLAW
    assert c.severity == 0.4


def test_heuristic_scalp_high_conviction(agent):
    thesis = _make_thesis(timeframe=Timeframe.SCALP, conviction=0.9)
    c = agent._heuristic_challenge(thesis)
    assert c.category == ChallengeCategory.TIMING_RISK


def test_heuristic_default_fallback(agent):
    """Well-formed thesis should get a generic adverse_scenario challenge."""
    thesis = _make_thesis(
        conviction=0.7, stop_loss=49000.0,
        risk_factors=["market risk"], timeframe=Timeframe.SWING,
    )
    c = agent._heuristic_challenge(thesis)
    assert c.category == ChallengeCategory.ADVERSE_SCENARIO
    assert c.severity == 0.2


def test_heuristic_picks_most_severe(agent):
    """When multiple issues exist, the most severe challenge should win."""
    thesis = _make_thesis(conviction=0.3, stop_loss=None, risk_factors=[])
    c = agent._heuristic_challenge(thesis)
    # Missing stop loss (0.6) > low conviction (0.5) > no risk factors (0.4).
    assert c.severity == 0.6
    assert c.category == ChallengeCategory.MISSING_CONTEXT


# ---------------------------------------------------------------------------
# Recommendation mapping
# ---------------------------------------------------------------------------

def test_recommendation_high_severity(agent):
    """severity = 0.6 falls in > 0.4 band → PAUSE_AND_REVIEW."""
    thesis = _make_thesis(stop_loss=None)  # severity 0.6
    c = agent._heuristic_challenge(thesis)
    # 0.6 is not > 0.6, so it falls into the > 0.4 band.
    assert c.recommendation == RedTeamRecommendation.PAUSE_AND_REVIEW


def test_recommendation_medium_severity(agent):
    """severity > 0.4 → PAUSE_AND_REVIEW."""
    thesis = _make_thesis(conviction=0.3)  # severity 0.5
    c = agent._heuristic_challenge(thesis)
    assert c.recommendation == RedTeamRecommendation.PAUSE_AND_REVIEW


def test_recommendation_low_severity(agent):
    """severity <= 0.4 → PROCEED."""
    thesis = _make_thesis(conviction=0.7, timeframe=Timeframe.SWING)
    c = agent._heuristic_challenge(thesis)
    # Default fallback: severity=0.2.
    assert c.recommendation == RedTeamRecommendation.PROCEED


# ---------------------------------------------------------------------------
# challenge_thesis fallback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_challenge_thesis_no_client_uses_heuristic(agent):
    """Without LLM client, challenge_thesis falls back to heuristic."""
    assert agent._client is None
    thesis = _make_thesis()
    c = await agent.challenge_thesis(thesis)
    assert isinstance(c, RedTeamChallengeV2)
    assert c.thesis_id == thesis.thesis_id


@pytest.mark.asyncio
async def test_challenge_thesis_llm_exception_fallback(agent):
    """If LLM call fails, should fall back to heuristic."""
    agent._client = MagicMock()
    agent._client.messages = MagicMock()
    agent._client.messages.create = AsyncMock(side_effect=RuntimeError("API error"))

    thesis = _make_thesis()
    c = await agent.challenge_thesis(thesis)
    assert isinstance(c, RedTeamChallengeV2)


# ---------------------------------------------------------------------------
# LLM challenge — JSON parsing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_llm_challenge_parses_raw_json(agent):
    """Test parsing raw JSON from Claude response."""
    response_data = {
        "category": "counter_evidence",
        "severity": 0.7,
        "challenge_text": "BTC is overbought on daily RSI",
        "counter_evidence": ["RSI > 70", "Volume declining"],
        "recommendation": "reduce_conviction",
        "confidence": 0.8,
    }
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=json.dumps(response_data))]
    agent._client = MagicMock()
    agent._client.messages = MagicMock()
    agent._client.messages.create = AsyncMock(return_value=mock_msg)

    thesis = _make_thesis()
    c = await agent._llm_challenge(thesis, "test context")
    assert c.category == ChallengeCategory.COUNTER_EVIDENCE
    assert c.severity == 0.7
    assert c.recommendation == RedTeamRecommendation.REDUCE_CONVICTION
    assert len(c.counter_evidence) == 2


@pytest.mark.asyncio
async def test_llm_challenge_parses_markdown_json(agent):
    """Test parsing JSON inside markdown code blocks."""
    response_data = {
        "category": "timing_risk",
        "severity": 0.5,
        "challenge_text": "Market close in 2 hours",
        "counter_evidence": [],
        "recommendation": "pause_and_review",
        "confidence": 0.6,
    }
    text = f"Here is my analysis:\n```json\n{json.dumps(response_data)}\n```"
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=text)]
    agent._client = MagicMock()
    agent._client.messages = MagicMock()
    agent._client.messages.create = AsyncMock(return_value=mock_msg)

    thesis = _make_thesis()
    c = await agent._llm_challenge(thesis, "")
    assert c.category == ChallengeCategory.TIMING_RISK
    assert c.recommendation == RedTeamRecommendation.PAUSE_AND_REVIEW


# ---------------------------------------------------------------------------
# Challenges issued counter
# ---------------------------------------------------------------------------

def test_challenges_issued_increments(agent):
    thesis = _make_thesis()
    assert agent._challenges_issued == 0
    agent._heuristic_challenge(thesis)
    assert agent._challenges_issued == 1
    agent._heuristic_challenge(thesis)
    assert agent._challenges_issued == 2


# ---------------------------------------------------------------------------
# Accuracy tracking
# ---------------------------------------------------------------------------

def test_accuracy_no_samples(agent):
    assert agent.accuracy == 0.0


def test_accuracy_all_hits(agent):
    agent.record_outcome("c1", thesis_was_profitable=False)
    agent.record_outcome("c2", thesis_was_profitable=False)
    assert agent.accuracy == 1.0


def test_accuracy_mixed(agent):
    agent.record_outcome("c1", thesis_was_profitable=False)   # hit
    agent.record_outcome("c2", thesis_was_profitable=True)    # miss
    agent.record_outcome("c3", thesis_was_profitable=False)   # hit
    assert abs(agent.accuracy - 2 / 3) < 0.01


def test_accuracy_no_hits(agent):
    agent.record_outcome("c1", thesis_was_profitable=True)
    assert agent.accuracy == 0.0


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health_default(agent):
    h = agent.health()
    assert h["challenges_issued"] == 0
    assert h["accuracy"] == 0.0
    assert h["accuracy_samples"] == 0
    assert h["llm_available"] is False


def test_health_with_data(agent):
    agent._challenges_issued = 5
    agent._accuracy_hits = 3
    agent._accuracy_total = 4
    agent._client = MagicMock()  # Simulate LLM available.

    h = agent.health()
    assert h["challenges_issued"] == 5
    assert h["accuracy"] == 0.75
    assert h["accuracy_samples"] == 4
    assert h["llm_available"] is True
