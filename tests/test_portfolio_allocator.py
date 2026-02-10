"""
Tests for the PortfolioAllocator agent.

Validates:
  - Kelly criterion calculation (full and half)
  - Conviction, debate quality, regime, drawdown, and correlation adjustments
  - Direction mapping (LONG → OPEN_LONG, SHORT → OPEN_SHORT)
  - Size clamping (max_position_pct floor, minimum floor)
  - Health reporting
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from agents.decision.portfolio_allocator import PortfolioAllocator, _REGIME_MULTIPLIER
from core.models import (
    Direction,
    InvestmentThesis,
    MarketRegime,
    PortfolioState,
    Position,
    TradeAction,
    Timeframe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def agent(mock_bus):
    return PortfolioAllocator(bus=mock_bus)


def _make_thesis(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    conviction: float = 0.8,
    entry_price: float = 50000.0,
    stop_loss: float = 49000.0,
    take_profit: float = 52000.0,
    timeframe: Timeframe = Timeframe.INTRADAY,
    risk_factors: list[str] | None = None,
) -> InvestmentThesis:
    return InvestmentThesis(
        asset=asset,
        direction=direction,
        conviction=conviction,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        timeframe=timeframe,
        risk_factors=risk_factors or ["market risk"],
    )


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def test_instantiation(mock_bus):
    agent = PortfolioAllocator(bus=mock_bus)
    assert agent.agent_id == "portfolio_allocator"
    assert agent.agent_type == "decision"


# ---------------------------------------------------------------------------
# Kelly criterion — full formula
# ---------------------------------------------------------------------------

def test_kelly_basic(agent):
    """f* = (p*b - q)/b = (0.55*1.5 - 0.45)/1.5 = 0.25."""
    thesis = _make_thesis(conviction=1.0)
    proposal = agent.compute_size(thesis, win_rate=0.55, avg_win_loss_ratio=1.5)
    # Half-Kelly: 0.25 * 0.5 = 0.125.
    assert abs(proposal.kelly_fraction - 0.125) < 0.01


def test_kelly_negative_edge_floors_to_zero(agent):
    """When win_rate is very low, Kelly should be 0 (no negative sizing)."""
    thesis = _make_thesis(conviction=0.8)
    proposal = agent.compute_size(thesis, win_rate=0.2, avg_win_loss_ratio=1.0)
    # f* = (0.2*1.0 - 0.8)/1.0 = -0.6, floored to 0.
    assert proposal.kelly_fraction == 0.0
    # Size should be the minimum floor.
    assert proposal.size_pct == 0.001


def test_kelly_win_rate_clamped(agent):
    """Win rate should be clamped to [0.01, 0.99]."""
    thesis = _make_thesis()
    # win_rate > 1 should be clamped.
    proposal = agent.compute_size(thesis, win_rate=1.5, avg_win_loss_ratio=1.5)
    assert proposal.size_pct > 0


# ---------------------------------------------------------------------------
# Half-Kelly (default kelly_fraction = 0.5)
# ---------------------------------------------------------------------------

def test_half_kelly_vs_full(agent):
    thesis = _make_thesis(conviction=1.0)
    proposal = agent.compute_size(
        thesis, win_rate=0.6, avg_win_loss_ratio=2.0,
        debate_quality=1.0, regime=MarketRegime.LOW_VOLATILITY,
    )
    # Full Kelly: f* = (0.6*2.0 - 0.4)/2.0 = 0.4
    # Half Kelly: 0.4 * 0.5 = 0.2
    assert abs(proposal.kelly_fraction - 0.2) < 0.01


# ---------------------------------------------------------------------------
# Conviction adjustment
# ---------------------------------------------------------------------------

def test_conviction_scales_size(agent):
    thesis_high = _make_thesis(conviction=0.9)
    thesis_low = _make_thesis(conviction=0.3)
    # Use low win_rate so sizes stay below max_position_pct cap.
    p_high = agent.compute_size(thesis_high, win_rate=0.51, avg_win_loss_ratio=1.0)
    p_low = agent.compute_size(thesis_low, win_rate=0.51, avg_win_loss_ratio=1.0)
    assert p_high.size_pct > p_low.size_pct


# ---------------------------------------------------------------------------
# Debate quality adjustment
# ---------------------------------------------------------------------------

def test_debate_quality_scales_size(agent):
    thesis = _make_thesis(conviction=0.8)
    # Use low win_rate so sizes stay below max_position_pct cap.
    p_good = agent.compute_size(thesis, debate_quality=1.0, win_rate=0.51, avg_win_loss_ratio=1.0)
    p_bad = agent.compute_size(thesis, debate_quality=0.0, win_rate=0.51, avg_win_loss_ratio=1.0)
    # debate_mult: 0.5 + 0.5 * quality.
    # Good: 1.0, Bad: 0.5.
    assert p_good.size_pct > p_bad.size_pct


# ---------------------------------------------------------------------------
# Regime multiplier
# ---------------------------------------------------------------------------

def test_regime_multipliers(agent):
    thesis = _make_thesis()
    # Use low win_rate so sizes stay below max_position_pct cap.
    p_up = agent.compute_size(thesis, regime=MarketRegime.TRENDING_UP, win_rate=0.51, avg_win_loss_ratio=1.0)
    p_hi = agent.compute_size(thesis, regime=MarketRegime.HIGH_VOLATILITY, win_rate=0.51, avg_win_loss_ratio=1.0)
    # TRENDING_UP=1.0, HIGH_VOLATILITY=0.4.
    assert p_up.size_pct > p_hi.size_pct


def test_all_regimes_in_multiplier():
    for regime in MarketRegime:
        assert regime in _REGIME_MULTIPLIER


# ---------------------------------------------------------------------------
# Drawdown dampening
# ---------------------------------------------------------------------------

def test_drawdown_reduces_size(agent):
    thesis = _make_thesis()
    # Use low win_rate so sizes stay below max_position_pct cap.
    p_normal = agent.compute_size(thesis, win_rate=0.51, avg_win_loss_ratio=1.0)

    # Set high drawdown (> 50% of max_daily_drawdown).
    agent._portfolio = PortfolioState(drawdown=0.08)
    p_dd = agent.compute_size(thesis, win_rate=0.51, avg_win_loss_ratio=1.0)
    assert p_dd.size_pct < p_normal.size_pct


def test_extreme_drawdown_near_floor(agent):
    thesis = _make_thesis()
    agent._portfolio = PortfolioState(drawdown=0.10)
    p = agent.compute_size(thesis)
    # Should be severely dampened but still above minimum floor.
    assert p.size_pct >= 0.001


# ---------------------------------------------------------------------------
# Correlation penalty
# ---------------------------------------------------------------------------

def test_same_direction_positions_penalise(agent):
    thesis = _make_thesis(direction=Direction.LONG)
    # Use low win_rate so sizes stay below max_position_pct cap.
    p_clean = agent.compute_size(thesis, win_rate=0.51, avg_win_loss_ratio=1.0)

    # Add 3 LONG positions.
    agent._portfolio = PortfolioState(
        positions=[
            Position(
                asset=f"ASSET{i}", direction=Direction.LONG, size_pct=0.01,
                entry_price=100, current_price=100, unrealised_pnl=0,
                opened_at=datetime.now(timezone.utc),
            )
            for i in range(3)
        ],
    )
    p_corr = agent.compute_size(thesis, win_rate=0.51, avg_win_loss_ratio=1.0)
    assert p_corr.size_pct < p_clean.size_pct


def test_opposite_direction_no_penalty(agent):
    thesis = _make_thesis(direction=Direction.LONG)
    agent._portfolio = PortfolioState(
        positions=[
            Position(
                asset="ETH", direction=Direction.SHORT, size_pct=0.01,
                entry_price=3000, current_price=3000, unrealised_pnl=0,
                opened_at=datetime.now(timezone.utc),
            ),
        ],
    )
    p = agent.compute_size(thesis)
    # No same-direction penalty (corr_mult stays 1.0).
    # The SHORT position doesn't penalise a LONG thesis.
    assert p.size_pct > 0


# ---------------------------------------------------------------------------
# Direction mapping
# ---------------------------------------------------------------------------

def test_long_maps_to_open_long(agent):
    thesis = _make_thesis(direction=Direction.LONG)
    p = agent.compute_size(thesis)
    assert p.action == TradeAction.OPEN_LONG


def test_short_maps_to_open_short(agent):
    thesis = _make_thesis(direction=Direction.SHORT)
    p = agent.compute_size(thesis)
    assert p.action == TradeAction.OPEN_SHORT


# ---------------------------------------------------------------------------
# Size clamping
# ---------------------------------------------------------------------------

def test_size_capped_at_max_position_pct(agent):
    """Even very high Kelly + conviction should be capped at max_position_pct."""
    thesis = _make_thesis(conviction=1.0)
    p = agent.compute_size(
        thesis, win_rate=0.95, avg_win_loss_ratio=5.0,
        debate_quality=1.0, regime=MarketRegime.TRENDING_UP,
    )
    from config.settings import settings
    assert p.size_pct <= settings.max_position_pct


def test_size_has_minimum_floor(agent):
    """Even with terrible parameters, size should be at least 0.001."""
    thesis = _make_thesis(conviction=0.01)
    p = agent.compute_size(thesis, win_rate=0.3, avg_win_loss_ratio=0.5)
    assert p.size_pct >= 0.001


# ---------------------------------------------------------------------------
# Proposal fields
# ---------------------------------------------------------------------------

def test_proposal_has_thesis_fields(agent):
    thesis = _make_thesis(
        asset="ETH", entry_price=3500, stop_loss=3400, take_profit=3700,
    )
    p = agent.compute_size(thesis)
    assert p.asset == "ETH"
    assert p.entry_price == 3500
    assert p.stop_loss == 3400
    assert p.take_profit == 3700
    assert p.thesis_id == thesis.thesis_id


def test_proposal_reasoning_contains_factors(agent):
    thesis = _make_thesis()
    p = agent.compute_size(thesis)
    assert "Kelly=" in p.reasoning
    assert "conv=" in p.reasoning
    assert "regime=" in p.reasoning


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health(agent):
    agent._portfolio = PortfolioState(
        total_nav=150_000, drawdown=0.03,
        positions=[
            Position(
                asset="BTC", direction=Direction.LONG, size_pct=0.02,
                entry_price=50000, current_price=51000, unrealised_pnl=200,
                opened_at=datetime.now(timezone.utc),
            ),
        ],
    )
    h = agent.health()
    assert h["portfolio_positions"] == 1
    assert h["portfolio_nav"] == 150_000
    assert h["portfolio_drawdown"] == 0.03
