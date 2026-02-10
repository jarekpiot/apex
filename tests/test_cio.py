"""
Tests for the ChiefInvestmentOfficer agent.

Validates:
  - Heuristic brief generation
  - Opportunity evaluation filters
  - Deliberation heuristics (all branches)
  - Position review heuristics (profit taking, loss cutting, stop tightening, hold)
  - Priority computation
  - _parse_json (raw JSON and markdown code blocks)
  - Debate quality scoring
  - Red Team override tracking and human review trigger
  - Position sizing fallback
  - Health reporting
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agents.decision.cio import ChiefInvestmentOfficer
from core.models import (
    ChallengeCategory,
    Direction,
    ExpertOpinion,
    ICDecision,
    InvestmentThesis,
    MarketBrief,
    MarketRegime,
    PortfolioState,
    Position,
    PositionAction,
    RedTeamChallengeV2,
    RedTeamRecommendation,
    StrategyPriority,
    Timeframe,
    TradeAction,
    TradeProposal,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def agent(mock_bus):
    a = ChiefInvestmentOfficer(bus=mock_bus)
    a._persist_ic_record = AsyncMock()  # No DB in tests.
    return a


def _make_thesis(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    conviction: float = 0.7,
    entry_price: float = 50000.0,
    stop_loss: float = 49000.0,
    take_profit: float = 52000.0,
) -> InvestmentThesis:
    return InvestmentThesis(
        asset=asset,
        direction=direction,
        conviction=conviction,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        timeframe=Timeframe.INTRADAY,
        risk_factors=["market risk"],
    )


def _make_expert(
    agent_id: str = "ta",
    conviction: float = 0.7,
    supports: bool = True,
) -> ExpertOpinion:
    return ExpertOpinion(
        agent_id=agent_id,
        role="analyst",
        assessment="Test assessment",
        conviction=conviction,
        supports_thesis=supports,
    )


def _make_rt_challenge(
    severity: float = 0.5,
    recommendation: RedTeamRecommendation = RedTeamRecommendation.PROCEED,
    confidence: float = 0.6,
) -> RedTeamChallengeV2:
    return RedTeamChallengeV2(
        category=ChallengeCategory.COUNTER_EVIDENCE,
        severity=severity,
        challenge_text="Test challenge",
        recommendation=recommendation,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def test_instantiation(mock_bus):
    agent = ChiefInvestmentOfficer(bus=mock_bus)
    assert agent.agent_id == "cio"
    assert agent.agent_type == "decision"
    assert agent._client is None
    assert agent._consecutive_rt_overrides == 0
    assert agent._decisions_made == 0


def test_instantiation_with_wiring(mock_bus):
    rt = MagicMock()
    alloc = MagicMock()
    agent = ChiefInvestmentOfficer(bus=mock_bus, red_team=rt, allocator=alloc)
    assert agent._red_team is rt
    assert agent._allocator is alloc


# ---------------------------------------------------------------------------
# _parse_json
# ---------------------------------------------------------------------------

def test_parse_json_raw():
    data = {"decision": "approve", "reasoning": "strong thesis"}
    result = ChiefInvestmentOfficer._parse_json(json.dumps(data))
    assert result["decision"] == "approve"


def test_parse_json_markdown_code_block():
    data = {"decision": "reject", "reasoning": "weak"}
    text = f"Analysis:\n```json\n{json.dumps(data)}\n```"
    result = ChiefInvestmentOfficer._parse_json(text)
    assert result["decision"] == "reject"


def test_parse_json_markdown_no_json_prefix():
    data = {"foo": "bar"}
    text = f"Result:\n```\n{json.dumps(data)}\n```"
    result = ChiefInvestmentOfficer._parse_json(text)
    assert result["foo"] == "bar"


def test_parse_json_invalid_raises():
    with pytest.raises(json.JSONDecodeError):
        ChiefInvestmentOfficer._parse_json("not json at all")


# ---------------------------------------------------------------------------
# _heuristic_brief
# ---------------------------------------------------------------------------

def test_heuristic_brief_empty_matrix(agent):
    agent._signal_matrix = {}
    brief = agent._heuristic_brief()
    assert isinstance(brief, MarketBrief)
    assert len(brief.opportunities) == 0


def test_heuristic_brief_finds_opportunity(agent):
    agent._signal_matrix = {
        "assets": {
            "BTC": {
                "net_direction": 0.6,
                "avg_conviction": 0.7,
                "signal_count": 3,
            },
        },
    }
    agent._regime = MarketRegime.TRENDING_UP
    brief = agent._heuristic_brief()
    assert len(brief.opportunities) == 1
    assert brief.opportunities[0]["asset"] == "BTC"
    assert brief.opportunities[0]["direction"] == "long"


def test_heuristic_brief_filters_low_direction(agent):
    agent._signal_matrix = {
        "assets": {
            "ETH": {
                "net_direction": 0.1,  # Below 0.3 threshold.
                "avg_conviction": 0.8,
                "signal_count": 4,
            },
        },
    }
    brief = agent._heuristic_brief()
    assert len(brief.opportunities) == 0


def test_heuristic_brief_filters_low_conviction(agent):
    agent._signal_matrix = {
        "assets": {
            "SOL": {
                "net_direction": 0.5,
                "avg_conviction": 0.2,  # Below 0.4 threshold.
                "signal_count": 3,
            },
        },
    }
    brief = agent._heuristic_brief()
    assert len(brief.opportunities) == 0


def test_heuristic_brief_filters_few_signals(agent):
    agent._signal_matrix = {
        "assets": {
            "AVAX": {
                "net_direction": 0.5,
                "avg_conviction": 0.7,
                "signal_count": 1,  # Below 2 threshold.
            },
        },
    }
    brief = agent._heuristic_brief()
    assert len(brief.opportunities) == 0


def test_heuristic_brief_short_direction(agent):
    agent._signal_matrix = {
        "assets": {
            "DOGE": {
                "net_direction": -0.5,
                "avg_conviction": 0.6,
                "signal_count": 2,
            },
        },
    }
    brief = agent._heuristic_brief()
    assert brief.opportunities[0]["direction"] == "short"


# ---------------------------------------------------------------------------
# _evaluate_opportunities
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_evaluate_opportunities_skips_low_conviction(agent):
    """Opportunities with conviction < 0.5 should be skipped."""
    brief = MarketBrief(
        opportunities=[{"asset": "BTC", "direction": "long", "conviction": 0.3}],
    )
    agent._generate_thesis = AsyncMock()
    await agent._evaluate_opportunities(brief)
    agent._generate_thesis.assert_not_called()


@pytest.mark.asyncio
async def test_evaluate_opportunities_skips_existing_position(agent):
    """Should skip if already have same-direction position."""
    agent._portfolio = PortfolioState(
        positions=[
            Position(
                asset="BTC", direction=Direction.LONG, size_pct=0.02,
                entry_price=50000, current_price=51000, unrealised_pnl=200,
                opened_at=datetime.now(timezone.utc),
            ),
        ],
    )
    brief = MarketBrief(
        opportunities=[{"asset": "BTC", "direction": "long", "conviction": 0.7}],
    )
    agent._generate_thesis = AsyncMock()
    await agent._evaluate_opportunities(brief)
    agent._generate_thesis.assert_not_called()


@pytest.mark.asyncio
async def test_evaluate_opportunities_generates_thesis(agent):
    """Valid opportunity should trigger thesis + IC debate."""
    brief = MarketBrief(
        opportunities=[{"asset": "ETH", "direction": "long", "conviction": 0.7}],
    )
    thesis = _make_thesis(asset="ETH")
    agent._generate_thesis = AsyncMock(return_value=thesis)
    agent._run_ic_debate = AsyncMock()

    await agent._evaluate_opportunities(brief)
    agent._generate_thesis.assert_called_once()
    agent._run_ic_debate.assert_called_once_with(thesis)


# ---------------------------------------------------------------------------
# _generate_thesis — heuristic fallback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generate_thesis_heuristic(agent):
    """Without LLM client, generates heuristic thesis."""
    assert agent._client is None
    thesis = await agent._generate_thesis(
        "BTC", Direction.LONG, {"conviction": 0.6, "reasoning": "test"},
    )
    assert thesis is not None
    assert thesis.asset == "BTC"
    assert thesis.direction == Direction.LONG
    assert thesis.conviction == 0.6
    assert agent._theses_generated == 1


# ---------------------------------------------------------------------------
# _deliberate — heuristic paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deliberate_rt_veto_high_severity(agent):
    """Red Team veto with severity > 0.8 → REJECT."""
    thesis = _make_thesis()
    experts = [_make_expert(supports=True)]
    rt = _make_rt_challenge(severity=0.9, recommendation=RedTeamRecommendation.VETO)
    decision, reason = await agent._deliberate(thesis, experts, rt)
    assert decision == ICDecision.REJECT
    assert "Red Team veto" in reason


@pytest.mark.asyncio
async def test_deliberate_majority_oppose(agent):
    """Majority opposing + low avg conviction → REJECT."""
    thesis = _make_thesis(conviction=0.5)
    experts = [
        _make_expert(agent_id="a", supports=False, conviction=0.3),
        _make_expert(agent_id="b", supports=False, conviction=0.3),
        _make_expert(agent_id="c", supports=True, conviction=0.4),
    ]
    decision, reason = await agent._deliberate(thesis, experts, None)
    assert decision == ICDecision.REJECT
    assert "Majority oppose" in reason


@pytest.mark.asyncio
async def test_deliberate_low_conviction(agent):
    """Thesis conviction < 0.3 → REJECT."""
    thesis = _make_thesis(conviction=0.2)
    experts = [_make_expert(supports=True)]
    decision, reason = await agent._deliberate(thesis, experts, None)
    assert decision == ICDecision.REJECT
    assert "Low conviction" in reason


@pytest.mark.asyncio
async def test_deliberate_near_drawdown_limit(agent):
    """Near drawdown limit → REDUCE."""
    thesis = _make_thesis(conviction=0.6)
    agent._portfolio = PortfolioState(drawdown=0.09)  # > 0.10 * 0.8 = 0.08
    experts = [_make_expert(supports=True)]
    decision, reason = await agent._deliberate(thesis, experts, None)
    assert decision == ICDecision.REDUCE
    assert "drawdown" in reason.lower()


@pytest.mark.asyncio
async def test_deliberate_consensus_approve(agent):
    """Support majority + conviction >= 0.5 → APPROVE."""
    thesis = _make_thesis(conviction=0.6)
    experts = [
        _make_expert(agent_id="a", supports=True),
        _make_expert(agent_id="b", supports=True),
    ]
    decision, reason = await agent._deliberate(thesis, experts, None)
    assert decision == ICDecision.APPROVE
    assert "Consensus" in reason


@pytest.mark.asyncio
async def test_deliberate_high_conviction_override(agent):
    """High conviction (>= 0.7) + low RT severity → APPROVE."""
    thesis = _make_thesis(conviction=0.8)
    experts = [
        _make_expert(agent_id="a", supports=False, conviction=0.5),
        _make_expert(agent_id="b", supports=False, conviction=0.5),
        _make_expert(agent_id="c", supports=True, conviction=0.5),
    ]
    # Majority oppose but avg_conviction=0.5 (not < 0.4), so "majority oppose" doesn't trigger.
    # High conviction (0.8 >= 0.7) + low RT severity (0.3 < 0.5) → APPROVE.
    rt = _make_rt_challenge(severity=0.3)
    decision, reason = await agent._deliberate(thesis, experts, rt)
    assert decision == ICDecision.APPROVE
    assert "High conviction" in reason


@pytest.mark.asyncio
async def test_deliberate_defer_default(agent):
    """No strong signal either way → DEFER."""
    thesis = _make_thesis(conviction=0.45)
    experts = [
        _make_expert(agent_id="a", supports=True, conviction=0.5),
        _make_expert(agent_id="b", supports=False, conviction=0.5),
    ]
    rt = _make_rt_challenge(severity=0.5, recommendation=RedTeamRecommendation.PROCEED)
    decision, reason = await agent._deliberate(thesis, experts, rt)
    assert decision == ICDecision.DEFER
    assert "Insufficient" in reason


# ---------------------------------------------------------------------------
# Red Team override tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rt_override_tracking(agent):
    """Consecutive RT overrides should be tracked."""
    thesis = _make_thesis(conviction=0.8)
    rt_mock = MagicMock()
    rt_mock.challenge_thesis = AsyncMock(return_value=_make_rt_challenge(
        severity=0.3, recommendation=RedTeamRecommendation.VETO,
    ))
    agent._red_team = rt_mock
    agent._allocator = None  # Use fallback sizing.

    # First two overrides — should approve (high conviction override).
    for _ in range(2):
        await agent._run_ic_debate(thesis)

    assert agent._consecutive_rt_overrides == 2


@pytest.mark.asyncio
async def test_rt_override_limit_triggers_defer(agent):
    """3 consecutive RT overrides → human_review_required, decision deferred."""
    thesis = _make_thesis(conviction=0.8)
    rt = MagicMock()
    rt.challenge_thesis = AsyncMock(return_value=_make_rt_challenge(
        severity=0.3, recommendation=RedTeamRecommendation.VETO,
    ))
    agent._red_team = rt
    # No allocator — uses fallback sizing.
    agent._allocator = None

    for _ in range(3):
        await agent._run_ic_debate(thesis)

    assert agent._consecutive_rt_overrides == 3
    # The 3rd debate should have been deferred.
    last_record = agent._ic_records[-1]
    assert last_record.human_review_required is True
    assert last_record.cio_decision == ICDecision.DEFER


@pytest.mark.asyncio
async def test_rt_override_resets_on_non_override(agent):
    """Override counter resets when CIO agrees with RT."""
    agent._consecutive_rt_overrides = 2
    thesis = _make_thesis(conviction=0.2)  # Low conviction → REJECT.
    await agent._run_ic_debate(thesis)
    assert agent._consecutive_rt_overrides == 0


# ---------------------------------------------------------------------------
# _heuristic_position_review
# ---------------------------------------------------------------------------

def test_position_review_take_profit(agent):
    """PnL > 3% → REDUCE_SIZE."""
    agent._portfolio = PortfolioState(total_nav=100_000)
    pos = Position(
        asset="BTC", direction=Direction.LONG, size_pct=0.02,
        entry_price=50000, current_price=55000,
        unrealised_pnl=3500,  # 3.5% of NAV.
        opened_at=datetime.now(timezone.utc),
    )
    review = agent._heuristic_position_review(pos)
    assert review.action == PositionAction.REDUCE_SIZE
    assert review.size_adjustment_pct == -0.5


def test_position_review_cut_loss(agent):
    """PnL < -2% → CLOSE_POSITION."""
    agent._portfolio = PortfolioState(total_nav=100_000)
    pos = Position(
        asset="ETH", direction=Direction.LONG, size_pct=0.02,
        entry_price=3000, current_price=2800,
        unrealised_pnl=-2500,  # -2.5% of NAV.
        opened_at=datetime.now(timezone.utc),
    )
    review = agent._heuristic_position_review(pos)
    assert review.action == PositionAction.CLOSE_POSITION


def test_position_review_tighten_stop(agent):
    """PnL between 1-3% → TIGHTEN_STOP."""
    agent._portfolio = PortfolioState(total_nav=100_000)
    pos = Position(
        asset="SOL", direction=Direction.LONG, size_pct=0.02,
        entry_price=100, current_price=110,
        unrealised_pnl=1500,  # 1.5% of NAV.
        opened_at=datetime.now(timezone.utc),
    )
    review = agent._heuristic_position_review(pos)
    assert review.action == PositionAction.TIGHTEN_STOP
    assert review.new_stop_loss == pos.entry_price


def test_position_review_hold(agent):
    """PnL in normal range → HOLD."""
    agent._portfolio = PortfolioState(total_nav=100_000)
    pos = Position(
        asset="AVAX", direction=Direction.LONG, size_pct=0.02,
        entry_price=30, current_price=30.5,
        unrealised_pnl=500,  # 0.5% of NAV.
        opened_at=datetime.now(timezone.utc),
    )
    review = agent._heuristic_position_review(pos)
    assert review.action == PositionAction.HOLD


# ---------------------------------------------------------------------------
# _review_positions integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_review_positions_publishes_close(agent, mock_bus):
    """Position review should publish CLOSE decision for losers."""
    agent._portfolio = PortfolioState(
        total_nav=100_000,
        positions=[
            Position(
                asset="DOGE", direction=Direction.LONG, size_pct=0.02,
                entry_price=0.10, current_price=0.08,
                unrealised_pnl=-2500,
                opened_at=datetime.now(timezone.utc),
            ),
        ],
    )
    await agent._review_positions()
    published = mock_bus.get_published("decisions:pending")
    assert len(published) == 1
    assert published[0].action == TradeAction.CLOSE


@pytest.mark.asyncio
async def test_review_positions_publishes_reduce(agent, mock_bus):
    """Position review should publish REDUCE decision for big winners."""
    agent._portfolio = PortfolioState(
        total_nav=100_000,
        positions=[
            Position(
                asset="BTC", direction=Direction.LONG, size_pct=0.02,
                entry_price=50000, current_price=55000,
                unrealised_pnl=3500,
                opened_at=datetime.now(timezone.utc),
            ),
        ],
    )
    await agent._review_positions()
    published = mock_bus.get_published("decisions:pending")
    assert len(published) == 1
    assert published[0].action == TradeAction.REDUCE


@pytest.mark.asyncio
async def test_review_positions_no_positions_no_publish(agent, mock_bus):
    """No positions → nothing published."""
    agent._portfolio = PortfolioState(total_nav=100_000)
    await agent._review_positions()
    assert len(mock_bus.get_published("decisions:pending")) == 0


# ---------------------------------------------------------------------------
# _compute_priorities
# ---------------------------------------------------------------------------

def test_priorities_normal(agent):
    agent._signal_matrix = {
        "assets": {
            "BTC": {"signal_count": 5},
            "ETH": {"signal_count": 3},
            "SOL": {"signal_count": 1},
        },
    }
    agent._portfolio = PortfolioState(drawdown=0.02)
    prio = agent._compute_priorities()
    assert prio.priority_level == 1
    assert prio.risk_budget_pct == 1.0
    assert prio.focus_assets[0] == "BTC"


def test_priorities_high_drawdown(agent):
    agent._signal_matrix = {"assets": {"BTC": {"signal_count": 1}}}
    agent._portfolio = PortfolioState(drawdown=0.09)
    prio = agent._compute_priorities()
    assert prio.priority_level == 5
    assert prio.risk_budget_pct == 0.2


def test_priorities_medium_drawdown(agent):
    agent._signal_matrix = {"assets": {"BTC": {"signal_count": 1}}}
    agent._portfolio = PortfolioState(drawdown=0.06)
    prio = agent._compute_priorities()
    assert prio.priority_level == 3
    assert prio.risk_budget_pct == 0.5


# ---------------------------------------------------------------------------
# _debate_quality
# ---------------------------------------------------------------------------

def test_debate_quality_no_experts_no_rt(agent):
    q = agent._debate_quality([], None)
    assert q == 0.0


def test_debate_quality_with_experts(agent):
    experts = [
        _make_expert(agent_id="a", conviction=0.8),
        _make_expert(agent_id="b", conviction=0.6),
    ]
    q = agent._debate_quality(experts, None)
    # 0.3 * min(2/4, 1.0) + 0.2 * avg_conviction
    # = 0.3 * 0.5 + 0.2 * 0.7 = 0.15 + 0.14 = 0.29
    assert abs(q - 0.29) < 0.01


def test_debate_quality_with_rt(agent):
    experts = [_make_expert(conviction=0.7)]
    rt = _make_rt_challenge(confidence=0.8)
    q = agent._debate_quality(experts, rt)
    # 0.3 * min(1/4, 1.0) + 0.2 * 0.7 + 0.3 + 0.2 * 0.8
    # = 0.075 + 0.14 + 0.3 + 0.16 = 0.675
    assert abs(q - 0.675) < 0.01


def test_debate_quality_capped_at_1(agent):
    experts = [_make_expert(conviction=1.0) for _ in range(8)]
    rt = _make_rt_challenge(confidence=1.0)
    q = agent._debate_quality(experts, rt)
    assert q <= 1.0


# ---------------------------------------------------------------------------
# _size_position
# ---------------------------------------------------------------------------

def test_size_position_with_allocator(agent):
    """Should delegate to PortfolioAllocator when available."""
    mock_allocator = MagicMock()
    mock_proposal = TradeProposal(
        asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.015,
    )
    mock_allocator.compute_size.return_value = mock_proposal
    agent._allocator = mock_allocator

    thesis = _make_thesis()
    experts = [_make_expert()]
    result = agent._size_position(thesis, experts)
    assert result is mock_proposal
    mock_allocator.compute_size.assert_called_once()


def test_size_position_fallback(agent):
    """Without allocator, uses simple conviction-based sizing."""
    agent._allocator = None
    thesis = _make_thesis(conviction=0.7)
    experts = [_make_expert()]
    result = agent._size_position(thesis, experts)
    assert result.asset == "BTC"
    assert result.action == TradeAction.OPEN_LONG
    assert result.size_pct > 0


def test_size_position_fallback_short(agent):
    agent._allocator = None
    thesis = _make_thesis(direction=Direction.SHORT)
    result = agent._size_position(thesis, [])
    assert result.action == TradeAction.OPEN_SHORT


# ---------------------------------------------------------------------------
# _gather_expert_opinions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_expert_opinions(agent):
    agent._signal_matrix = {
        "assets": {
            "BTC": {
                "signals": [
                    {"agent_id": "ta", "direction": 0.5, "conviction": 0.8, "reasoning": "bullish"},
                    {"agent_id": "fa", "direction": -0.3, "conviction": 0.6, "reasoning": "bearish"},
                ],
            },
        },
    }
    thesis = _make_thesis(direction=Direction.LONG)
    opinions = await agent._gather_expert_opinions(thesis)
    assert len(opinions) == 2
    # ta supports (positive direction + LONG thesis).
    assert opinions[0].supports_thesis is True
    # fa opposes (negative direction + LONG thesis).
    assert opinions[1].supports_thesis is False


# ---------------------------------------------------------------------------
# _get_red_team_challenge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_red_team_challenge_no_rt(agent):
    agent._red_team = None
    result = await agent._get_red_team_challenge(_make_thesis())
    assert result is None


@pytest.mark.asyncio
async def test_get_red_team_challenge_success(agent):
    rt = MagicMock()
    challenge = _make_rt_challenge()
    rt.challenge_thesis = AsyncMock(return_value=challenge)
    agent._red_team = rt

    result = await agent._get_red_team_challenge(_make_thesis())
    assert result is challenge


@pytest.mark.asyncio
async def test_get_red_team_challenge_timeout(agent):
    """Timeout should return None, not raise."""
    import asyncio

    async def slow_challenge(*a, **kw):
        await asyncio.sleep(100)

    rt = MagicMock()
    rt.challenge_thesis = slow_challenge
    agent._red_team = rt

    result = await agent._get_red_team_challenge(_make_thesis())
    assert result is None


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health(agent):
    agent._decisions_made = 5
    agent._theses_generated = 8
    agent._consecutive_rt_overrides = 1
    agent._regime = MarketRegime.TRENDING_DOWN

    h = agent.health()
    assert h["decisions_made"] == 5
    assert h["theses_generated"] == 8
    assert h["consecutive_rt_overrides"] == 1
    assert h["regime"] == MarketRegime.TRENDING_DOWN
    assert h["llm_available"] is False
