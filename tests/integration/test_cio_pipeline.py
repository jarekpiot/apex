"""
Integration tests for the CIO decision pipeline.

CIO path:
  SignalAggregator -> cio:signal_matrix -> CIO (Claude reasoning)
    -> IC debate -> decisions:pending -> RiskGuardian -> decisions:approved
    -> ExecutionEngine -> trades:executed

All LLM calls (anthropic) are mocked.  DB is mocked via autouse _patch_db
fixture in tests/integration/conftest.py.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.message_bus import (
    STREAM_CIO_SIGNAL_MATRIX,
    STREAM_DATA_REGIME,
    STREAM_DECISIONS_PENDING,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_SIGNALS,
)
from core.models import (
    AgentSignal,
    ChallengeCategory,
    Direction,
    ICDecision,
    InvestmentThesis,
    MarketRegime,
    PortfolioState,
    RedTeamChallengeV2,
    RedTeamRecommendation,
    Timeframe,
    TradeAction,
    TradeDecision,
    TradeProposal,
)
from tests.integration.conftest import (
    TrackingBus,
    make_decision,
    make_portfolio,
    make_signal,
    make_signal_aggregator,
    make_cio,
    make_portfolio_allocator,
    make_red_team_strategist,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_thesis(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    conviction: float = 0.75,
    entry_price: float = 50000.0,
    stop_loss: float = 48000.0,
    take_profit: float = 55000.0,
) -> InvestmentThesis:
    return InvestmentThesis(
        asset=asset,
        direction=direction,
        edge_description="Strong multi-TF breakout with volume confirmation",
        catalyst="Spot ETF inflows accelerating",
        supporting_signals=["technical_analyst", "on_chain_flow"],
        risk_factors=["Regime shift to risk-off", "Liquidity drain"],
        invalidation_criteria=["Break below 48k support"],
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        timeframe=Timeframe.INTRADAY,
        conviction=conviction,
    )


def _make_rt_challenge(
    thesis_id: str = "",
    severity: float = 0.5,
    recommendation: RedTeamRecommendation = RedTeamRecommendation.PROCEED,
) -> RedTeamChallengeV2:
    return RedTeamChallengeV2(
        thesis_id=thesis_id,
        category=ChallengeCategory.ADVERSE_SCENARIO,
        severity=severity,
        challenge_text="Market could reverse sharply on macro news",
        counter_evidence=["Recent FOMC minutes hawkish"],
        recommendation=recommendation,
        confidence=0.6,
    )


# ---------------------------------------------------------------------------
# 1. SignalAggregator produces a matrix
# ---------------------------------------------------------------------------

async def test_signal_aggregator_produces_matrix(tracking_bus: TrackingBus):
    """Inject signals from multiple agents. Trigger the aggregator's process()
    cycle (which compiles + publishes).  Assert SignalMatrixSnapshot appears on
    STREAM_CIO_SIGNAL_MATRIX."""
    agg = make_signal_aggregator(tracking_bus)

    # Populate internal signal buffers directly (bypasses listener loop).
    from agents.decision.signal_aggregator import AssetSignalEntry

    now = datetime.now(timezone.utc)
    agg._signals["BTC"]["technical_analyst"] = AssetSignalEntry(
        agent_id="technical_analyst",
        direction=0.8,
        conviction=0.85,
        timeframe="intraday",
        reasoning="Breakout above resistance",
        timestamp=now,
    )
    agg._signals["BTC"]["funding_arb"] = AssetSignalEntry(
        agent_id="funding_arb",
        direction=0.5,
        conviction=0.6,
        timeframe="intraday",
        reasoning="Positive funding rate differential",
        timestamp=now,
    )
    agg._signals["ETH"]["on_chain_flow"] = AssetSignalEntry(
        agent_id="on_chain_flow",
        direction=-0.3,
        conviction=0.7,
        timeframe="swing",
        reasoning="TVL declining across L2s",
        timestamp=now,
    )

    # Patch sleep so process() returns immediately after publishing.
    with patch("agents.decision.signal_aggregator.asyncio.sleep", new_callable=AsyncMock):
        await agg.process()

    published = tracking_bus.get_published(STREAM_CIO_SIGNAL_MATRIX)
    assert len(published) == 1, f"Expected 1 matrix snapshot, got {len(published)}"

    snapshot = published[0]
    # SignalMatrixSnapshot is a Pydantic model; check fields.
    assert "BTC" in snapshot.assets
    assert "ETH" in snapshot.assets
    assert snapshot.assets["BTC"].signal_count == 2
    assert snapshot.assets["ETH"].signal_count == 1


# ---------------------------------------------------------------------------
# 2. SignalAggregator deduplicates same-agent signals
# ---------------------------------------------------------------------------

async def test_signal_aggregator_deduplicates(tracking_bus: TrackingBus):
    """Inject two signals from the same agent for the same asset.  Assert the
    matrix only retains the latest."""
    agg = make_signal_aggregator(tracking_bus)

    from agents.decision.signal_aggregator import AssetSignalEntry

    old_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    new_time = datetime.now(timezone.utc)

    # First signal (older).
    agg._signals["BTC"]["technical_analyst"] = AssetSignalEntry(
        agent_id="technical_analyst",
        direction=0.3,
        conviction=0.4,
        timeframe="intraday",
        timestamp=old_time,
    )

    # Second signal (newer) from the same agent — overwrites.
    agg._signals["BTC"]["technical_analyst"] = AssetSignalEntry(
        agent_id="technical_analyst",
        direction=0.9,
        conviction=0.95,
        timeframe="intraday",
        timestamp=new_time,
    )

    with patch("agents.decision.signal_aggregator.asyncio.sleep", new_callable=AsyncMock):
        await agg.process()

    published = tracking_bus.get_published(STREAM_CIO_SIGNAL_MATRIX)
    assert len(published) == 1

    btc_matrix = published[0].assets["BTC"]
    assert btc_matrix.signal_count == 1, "Dedup failed — expected exactly 1 signal"

    entry = btc_matrix.signals[0]
    assert entry.direction == 0.9, "Should keep latest signal direction"
    assert entry.conviction == 0.95, "Should keep latest signal conviction"


# ---------------------------------------------------------------------------
# 3. SignalAggregator detects conflicts
# ---------------------------------------------------------------------------

async def test_signal_aggregator_detects_conflict(tracking_bus: TrackingBus):
    """Inject one bullish and one bearish signal for the same asset.  Assert
    the resulting AssetMatrix has has_conflict=True."""
    agg = make_signal_aggregator(tracking_bus)

    from agents.decision.signal_aggregator import AssetSignalEntry

    now = datetime.now(timezone.utc)

    agg._signals["SOL"]["technical_analyst"] = AssetSignalEntry(
        agent_id="technical_analyst",
        direction=0.8,
        conviction=0.7,
        timeframe="intraday",
        timestamp=now,
    )
    agg._signals["SOL"]["sentiment_social"] = AssetSignalEntry(
        agent_id="sentiment_social",
        direction=-0.6,
        conviction=0.65,
        timeframe="intraday",
        timestamp=now,
    )

    with patch("agents.decision.signal_aggregator.asyncio.sleep", new_callable=AsyncMock):
        await agg.process()

    published = tracking_bus.get_published(STREAM_CIO_SIGNAL_MATRIX)
    assert len(published) == 1

    sol_matrix = published[0].assets["SOL"]
    assert sol_matrix.has_conflict is True, "Expected conflict flag when signals oppose"
    assert sol_matrix.signal_count == 2


# ---------------------------------------------------------------------------
# 4. CIO instantiation and wiring
# ---------------------------------------------------------------------------

async def test_cio_instantiation(tracking_bus: TrackingBus):
    """Create CIO with red_team and allocator.  Verify basic identity."""
    rt = make_red_team_strategist(tracking_bus)
    alloc = make_portfolio_allocator(tracking_bus)
    cio = make_cio(tracking_bus, red_team=rt, allocator=alloc)

    assert cio.agent_id == "cio"
    assert cio.agent_type == "decision"
    assert cio._red_team is rt
    assert cio._allocator is alloc


# ---------------------------------------------------------------------------
# 5. CIO decision reaches the risk pipeline
# ---------------------------------------------------------------------------

async def test_cio_decision_reaches_risk_pipeline(tracking_bus: TrackingBus):
    """Mock the CIO's LLM to be None (heuristic mode).  Feed it a signal
    matrix with a high-conviction opportunity and run the IC debate.
    Assert a TradeDecision is published to STREAM_DECISIONS_PENDING."""
    alloc = make_portfolio_allocator(tracking_bus)
    cio = make_cio(tracking_bus, allocator=alloc)

    # CIO in heuristic mode (no LLM client).
    cio._client = None

    # Supply a signal matrix that will produce a high-conviction opportunity
    # when _heuristic_brief() scans it.
    cio._signal_matrix = {
        "assets": {
            "BTC": {
                "net_direction": 0.7,
                "avg_conviction": 0.8,
                "signal_count": 3,
                "has_conflict": False,
                "signals": [
                    {
                        "agent_id": "technical_analyst",
                        "direction": 0.8,
                        "conviction": 0.85,
                        "reasoning": "Breakout confirmed",
                    },
                    {
                        "agent_id": "on_chain_flow",
                        "direction": 0.6,
                        "conviction": 0.7,
                        "reasoning": "TVL rising",
                    },
                    {
                        "agent_id": "funding_arb",
                        "direction": 0.5,
                        "conviction": 0.6,
                        "reasoning": "Funding rate favourable",
                    },
                ],
            },
        },
    }
    cio._mid_prices = {"BTC": 50000.0}

    # Generate brief + evaluate opportunities (triggers IC debate).
    brief = cio._heuristic_brief()
    assert len(brief.opportunities) >= 1, "Heuristic brief should find BTC opportunity"

    await cio._evaluate_opportunities(brief)

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) >= 1, "CIO should publish at least one TradeDecision"

    decision = published[0]
    assert isinstance(decision, TradeDecision)
    assert decision.asset == "BTC"
    assert decision.metadata.get("source") == "cio"
    assert decision.size_pct > 0


# ---------------------------------------------------------------------------
# 6. CIO respects Red Team veto
# ---------------------------------------------------------------------------

async def test_cio_respects_red_team_veto(tracking_bus: TrackingBus):
    """Mock the Red Team to return a high-severity VETO.  The CIO's heuristic
    deliberation should reject the trade when severity > 0.8."""
    rt = make_red_team_strategist(tracking_bus)
    alloc = make_portfolio_allocator(tracking_bus)
    cio = make_cio(tracking_bus, red_team=rt, allocator=alloc)

    # No LLM — heuristic deliberation.
    cio._client = None

    thesis = _make_thesis(conviction=0.7)

    # Mock red_team.challenge_thesis to return a severe VETO.
    veto_challenge = _make_rt_challenge(
        thesis_id=thesis.thesis_id,
        severity=0.9,
        recommendation=RedTeamRecommendation.VETO,
    )
    rt.challenge_thesis = AsyncMock(return_value=veto_challenge)

    # Run the IC debate.
    await cio._run_ic_debate(thesis)

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)

    # The heuristic _deliberate path checks: if severity > 0.8 and VETO -> REJECT.
    # Therefore no TradeDecision should appear on decisions:pending.
    assert len(published) == 0, (
        "CIO should NOT publish a trade when Red Team issues high-severity VETO. "
        f"Got {len(published)} decisions."
    )


# ---------------------------------------------------------------------------
# 7. PortfolioAllocator Kelly sizing
# ---------------------------------------------------------------------------

async def test_portfolio_allocator_kelly_sizing(tracking_bus: TrackingBus):
    """Create a PortfolioAllocator and call compute_size().  Verify position
    size follows the Half-Kelly formula with adjustments."""
    alloc = make_portfolio_allocator(tracking_bus)

    thesis = _make_thesis(conviction=0.8, asset="ETH")

    proposal = alloc.compute_size(
        thesis=thesis,
        regime=MarketRegime.LOW_VOLATILITY,
        debate_quality=0.7,
        win_rate=0.55,
        avg_win_loss_ratio=1.5,
    )

    assert isinstance(proposal, TradeProposal)
    assert proposal.asset == "ETH"
    assert proposal.action == TradeAction.OPEN_LONG

    # Verify Half-Kelly formula:
    # f* = (p*b - q) / b  where p=0.55, q=0.45, b=1.5
    # f* = (0.55 * 1.5 - 0.45) / 1.5 = (0.825 - 0.45) / 1.5 = 0.25
    # half_kelly = 0.25 * 0.5 = 0.125
    expected_full_kelly = (0.55 * 1.5 - 0.45) / 1.5
    expected_half_kelly = expected_full_kelly * 0.5  # settings.kelly_fraction=0.5

    assert proposal.kelly_fraction == pytest.approx(expected_half_kelly, abs=1e-4), (
        f"Kelly fraction should be ~{expected_half_kelly:.4f}, got {proposal.kelly_fraction}"
    )

    # Final size = kelly * conviction * debate_mult * regime_mult * dd_mult * corr_mult
    # conviction=0.8, debate_mult=0.5+0.5*0.7=0.85, regime_mult(LOW_VOL)=0.9,
    # dd_mult=1.0 (no drawdown), corr_mult=1.0 (no positions)
    expected_raw = expected_half_kelly * 0.8 * 0.85 * 0.9 * 1.0 * 1.0
    # Capped at max_position_pct (0.02).
    from config.settings import settings
    expected_capped = min(expected_raw, settings.max_position_pct)
    expected_final = max(0.001, expected_capped)

    assert proposal.size_pct == pytest.approx(expected_final, abs=1e-4), (
        f"Final size should be ~{expected_final:.6f}, got {proposal.size_pct}"
    )

    assert proposal.size_pct > 0
    assert proposal.size_pct <= settings.max_position_pct
    assert "Kelly=" in proposal.reasoning


# ---------------------------------------------------------------------------
# Additional pipeline tests
# ---------------------------------------------------------------------------

async def test_cio_consecutive_rt_overrides_trigger_defer(tracking_bus: TrackingBus):
    """If the CIO overrides the Red Team 3 consecutive times, the 3rd IC debate
    should set human_review_required and DEFER the decision."""
    rt = make_red_team_strategist(tracking_bus)
    alloc = make_portfolio_allocator(tracking_bus)
    cio = make_cio(tracking_bus, red_team=rt, allocator=alloc)
    cio._client = None
    cio._mid_prices = {"BTC": 50000.0}

    # Red Team always recommends VETO with moderate severity (0.6) --
    # low enough that the heuristic path will APPROVE, creating an override.
    def _make_moderate_veto(thesis: InvestmentThesis) -> RedTeamChallengeV2:
        return _make_rt_challenge(
            thesis_id=thesis.thesis_id,
            severity=0.6,
            recommendation=RedTeamRecommendation.VETO,
        )

    rt.challenge_thesis = AsyncMock(side_effect=lambda t, ctx="": _make_moderate_veto(t))

    # Run 3 IC debates with high-conviction theses that the heuristic will approve.
    for i in range(3):
        thesis = _make_thesis(conviction=0.8)

        # Inject expert signal data so heuristic has support votes.
        cio._signal_matrix = {
            "assets": {
                "BTC": {
                    "signals": [
                        {"agent_id": "ta", "direction": 0.8, "conviction": 0.8,
                         "reasoning": "Bullish"},
                        {"agent_id": "onchain", "direction": 0.7, "conviction": 0.7,
                         "reasoning": "TVL up"},
                    ],
                },
            },
        }

        await cio._run_ic_debate(thesis)

    # After 3 overrides, the counter should have reached the limit.
    assert cio._consecutive_rt_overrides >= 3

    # The last IC record should have human_review_required=True.
    last_record = cio._ic_records[-1]
    assert last_record.human_review_required is True
    assert last_record.cio_decision == ICDecision.DEFER


async def test_signal_aggregator_no_conflict_when_aligned(tracking_bus: TrackingBus):
    """When all signals agree in direction, has_conflict should be False."""
    agg = make_signal_aggregator(tracking_bus)

    from agents.decision.signal_aggregator import AssetSignalEntry

    now = datetime.now(timezone.utc)

    # Two bullish signals -- both > 0.1 threshold.
    agg._signals["BTC"]["ta"] = AssetSignalEntry(
        agent_id="ta", direction=0.6, conviction=0.7,
        timeframe="intraday", timestamp=now,
    )
    agg._signals["BTC"]["onchain"] = AssetSignalEntry(
        agent_id="onchain", direction=0.4, conviction=0.5,
        timeframe="swing", timestamp=now,
    )

    with patch("agents.decision.signal_aggregator.asyncio.sleep", new_callable=AsyncMock):
        await agg.process()

    published = tracking_bus.get_published(STREAM_CIO_SIGNAL_MATRIX)
    btc_matrix = published[0].assets["BTC"]
    assert btc_matrix.has_conflict is False
    assert btc_matrix.net_direction > 0


async def test_cio_heuristic_brief_skips_low_conviction(tracking_bus: TrackingBus):
    """_evaluate_opportunities skips opportunities with conviction < 0.5."""
    cio = make_cio(tracking_bus)
    cio._client = None

    # One opportunity with low conviction.
    cio._signal_matrix = {
        "assets": {
            "DOGE": {
                "net_direction": 0.5,
                "avg_conviction": 0.3,
                "signal_count": 2,
                "has_conflict": False,
            },
        },
    }

    brief = cio._heuristic_brief()

    # avg_conviction 0.3 < 0.4 threshold in _heuristic_brief, so no opportunity.
    await cio._evaluate_opportunities(brief)

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) == 0, "Low-conviction opportunities should be skipped"


async def test_portfolio_allocator_drawdown_dampening(tracking_bus: TrackingBus):
    """When the portfolio has significant drawdown, the raw (pre-cap) position
    size should be smaller.  We compare conviction_adjusted_size to avoid the
    max_position_pct cap masking the difference."""
    alloc = make_portfolio_allocator(tracking_bus)

    # Simulate a portfolio near drawdown limit.
    alloc._portfolio = PortfolioState(
        total_nav=100_000.0,
        drawdown=0.08,  # 8% -- above 50% of max_daily_drawdown (0.10)
    )

    thesis = _make_thesis(conviction=0.8)

    proposal_with_dd = alloc.compute_size(
        thesis=thesis,
        regime=MarketRegime.LOW_VOLATILITY,
        debate_quality=0.7,
    )

    # Reset to no drawdown.
    alloc._portfolio = PortfolioState(total_nav=100_000.0, drawdown=0.0)

    proposal_no_dd = alloc.compute_size(
        thesis=thesis,
        regime=MarketRegime.LOW_VOLATILITY,
        debate_quality=0.7,
    )

    # Raw sizes before the max_position_pct cap are stored in conviction_adjusted_size.
    assert proposal_with_dd.conviction_adjusted_size < proposal_no_dd.conviction_adjusted_size, (
        "Raw position size should be smaller when portfolio has drawdown. "
        f"With dd: {proposal_with_dd.conviction_adjusted_size}, "
        f"without dd: {proposal_no_dd.conviction_adjusted_size}"
    )


async def test_portfolio_allocator_regime_adjustment(tracking_bus: TrackingBus):
    """HIGH_VOLATILITY regime should reduce position size compared to LOW_VOLATILITY.
    We compare conviction_adjusted_size (pre-cap) because both raw values may exceed
    max_position_pct and get clamped to the same ceiling."""
    alloc = make_portfolio_allocator(tracking_bus)
    thesis = _make_thesis(conviction=0.8)

    proposal_low_vol = alloc.compute_size(
        thesis=thesis,
        regime=MarketRegime.LOW_VOLATILITY,
        debate_quality=0.7,
    )

    proposal_high_vol = alloc.compute_size(
        thesis=thesis,
        regime=MarketRegime.HIGH_VOLATILITY,
        debate_quality=0.7,
    )

    assert proposal_high_vol.conviction_adjusted_size < proposal_low_vol.conviction_adjusted_size, (
        "HIGH_VOLATILITY regime should yield smaller raw size than LOW_VOLATILITY. "
        f"High vol: {proposal_high_vol.conviction_adjusted_size}, "
        f"Low vol: {proposal_low_vol.conviction_adjusted_size}"
    )
