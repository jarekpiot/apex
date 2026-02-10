"""
Tests for the SignalAggregator agent.

Validates:
  - Signal ingestion and deduplication (same agent overwrites)
  - Stale signal pruning
  - Matrix compilation: net_direction, avg_conviction, conflict detection, urgency
  - Snapshot metadata (regime, anomalies, portfolio)
  - Health reporting
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from agents.decision.signal_aggregator import (
    AssetMatrix,
    AssetSignalEntry,
    SignalAggregator,
    SignalMatrixSnapshot,
)
from core.models import (
    AnomalyAlert,
    AnomalyType,
    AlertSeverity,
    MarketRegime,
    PortfolioState,
    Position,
    Direction,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def agent(mock_bus):
    return SignalAggregator(bus=mock_bus)


def _now():
    return datetime.now(timezone.utc)


def _make_entry(
    agent_id: str = "ta",
    direction: float = 0.5,
    conviction: float = 0.8,
    age_seconds: float = 10.0,
    timeframe: str = "intraday",
) -> AssetSignalEntry:
    ts = _now() - timedelta(seconds=age_seconds)
    return AssetSignalEntry(
        agent_id=agent_id,
        direction=direction,
        conviction=conviction,
        timeframe=timeframe,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def test_instantiation(mock_bus):
    agent = SignalAggregator(bus=mock_bus)
    assert agent.agent_id == "signal_aggregator"
    assert agent.agent_type == "decision"
    assert len(agent._signals) == 0
    assert agent._regime == MarketRegime.LOW_VOLATILITY


# ---------------------------------------------------------------------------
# Signal deduplication
# ---------------------------------------------------------------------------

def test_signal_dedup_same_agent_overwrites(agent):
    """Adding a signal from the same agent for the same asset should overwrite."""
    agent._signals["BTC"]["ta"] = _make_entry(agent_id="ta", direction=0.5)
    agent._signals["BTC"]["ta"] = _make_entry(agent_id="ta", direction=-0.3)
    assert len(agent._signals["BTC"]) == 1
    assert agent._signals["BTC"]["ta"].direction == -0.3


def test_signal_different_agents_coexist(agent):
    agent._signals["BTC"]["ta"] = _make_entry(agent_id="ta")
    agent._signals["BTC"]["fa"] = _make_entry(agent_id="fa")
    assert len(agent._signals["BTC"]) == 2


def test_signals_different_assets(agent):
    agent._signals["BTC"]["ta"] = _make_entry(agent_id="ta")
    agent._signals["ETH"]["ta"] = _make_entry(agent_id="ta")
    assert "BTC" in agent._signals
    assert "ETH" in agent._signals


# ---------------------------------------------------------------------------
# _prune_stale
# ---------------------------------------------------------------------------

def test_prune_stale_removes_old_signals(agent):
    """Signals older than orch_max_signal_age should be pruned."""
    old = _make_entry(agent_id="ta", age_seconds=99999)
    agent._signals["BTC"]["ta"] = old
    agent._prune_stale(_now())
    assert "BTC" not in agent._signals


def test_prune_stale_keeps_fresh_signals(agent):
    fresh = _make_entry(agent_id="ta", age_seconds=5)
    agent._signals["BTC"]["ta"] = fresh
    agent._prune_stale(_now())
    assert "BTC" in agent._signals


def test_prune_stale_removes_expired_signals(agent):
    """Signal with expires_at in the past should be pruned."""
    entry = _make_entry(agent_id="ta", age_seconds=5)
    entry.expires_at = _now() - timedelta(seconds=1)
    agent._signals["BTC"]["ta"] = entry
    agent._prune_stale(_now())
    assert "BTC" not in agent._signals


def test_prune_stale_keeps_signals_not_expired(agent):
    entry = _make_entry(agent_id="ta", age_seconds=5)
    entry.expires_at = _now() + timedelta(hours=1)
    agent._signals["BTC"]["ta"] = entry
    agent._prune_stale(_now())
    assert "BTC" in agent._signals


def test_prune_stale_removes_asset_key_when_empty(agent):
    old = _make_entry(agent_id="ta", age_seconds=99999)
    agent._signals["BTC"]["ta"] = old
    agent._prune_stale(_now())
    assert "BTC" not in agent._signals


# ---------------------------------------------------------------------------
# _compile_matrix — net_direction
# ---------------------------------------------------------------------------

def test_compile_matrix_single_long_signal(agent):
    agent._signals["BTC"]["ta"] = _make_entry(direction=0.8, conviction=1.0)
    snap = agent._compile_matrix(_now())
    assert "BTC" in snap.assets
    # net_direction = (0.8 * 1.0) / 1.0 = 0.8
    assert abs(snap.assets["BTC"].net_direction - 0.8) < 0.01


def test_compile_matrix_opposing_signals(agent):
    """Two opposing signals should partially cancel."""
    agent._signals["BTC"]["bull"] = _make_entry(agent_id="bull", direction=0.6, conviction=0.8)
    agent._signals["BTC"]["bear"] = _make_entry(agent_id="bear", direction=-0.4, conviction=0.6)
    snap = agent._compile_matrix(_now())
    am = snap.assets["BTC"]
    # net = (0.6*0.8 + (-0.4)*0.6) / (0.8+0.6) = (0.48-0.24)/1.4 = 0.1714
    assert abs(am.net_direction - 0.1714) < 0.01


def test_compile_matrix_empty(agent):
    snap = agent._compile_matrix(_now())
    assert len(snap.assets) == 0


# ---------------------------------------------------------------------------
# _compile_matrix — conflict detection
# ---------------------------------------------------------------------------

def test_conflict_detected_with_opposing_signals(agent):
    agent._signals["BTC"]["bull"] = _make_entry(agent_id="bull", direction=0.5)
    agent._signals["BTC"]["bear"] = _make_entry(agent_id="bear", direction=-0.5)
    snap = agent._compile_matrix(_now())
    assert snap.assets["BTC"].has_conflict is True


def test_no_conflict_aligned_signals(agent):
    agent._signals["BTC"]["ta1"] = _make_entry(agent_id="ta1", direction=0.5)
    agent._signals["BTC"]["ta2"] = _make_entry(agent_id="ta2", direction=0.3)
    snap = agent._compile_matrix(_now())
    assert snap.assets["BTC"].has_conflict is False


def test_no_conflict_near_zero_direction(agent):
    """Signals very close to zero should not trigger conflict (threshold 0.1)."""
    agent._signals["BTC"]["a"] = _make_entry(agent_id="a", direction=0.05)
    agent._signals["BTC"]["b"] = _make_entry(agent_id="b", direction=-0.05)
    snap = agent._compile_matrix(_now())
    assert snap.assets["BTC"].has_conflict is False


# ---------------------------------------------------------------------------
# _compile_matrix — avg_conviction & signal_count
# ---------------------------------------------------------------------------

def test_avg_conviction(agent):
    agent._signals["BTC"]["a"] = _make_entry(agent_id="a", conviction=0.8)
    agent._signals["BTC"]["b"] = _make_entry(agent_id="b", conviction=0.4)
    snap = agent._compile_matrix(_now())
    assert abs(snap.assets["BTC"].avg_conviction - 0.6) < 0.01


def test_signal_count(agent):
    agent._signals["BTC"]["a"] = _make_entry(agent_id="a")
    agent._signals["BTC"]["b"] = _make_entry(agent_id="b")
    agent._signals["BTC"]["c"] = _make_entry(agent_id="c")
    snap = agent._compile_matrix(_now())
    assert snap.assets["BTC"].signal_count == 3


# ---------------------------------------------------------------------------
# _compile_matrix — urgency
# ---------------------------------------------------------------------------

def test_urgency_increases_with_more_signals(agent):
    agent._signals["BTC"]["a"] = _make_entry(agent_id="a", conviction=0.8, age_seconds=5)
    snap1 = agent._compile_matrix(_now())

    agent._signals["BTC"]["b"] = _make_entry(agent_id="b", conviction=0.8, age_seconds=5)
    snap2 = agent._compile_matrix(_now())

    assert snap2.assets["BTC"].urgency >= snap1.assets["BTC"].urgency


# ---------------------------------------------------------------------------
# _compile_matrix — snapshot metadata
# ---------------------------------------------------------------------------

def test_snapshot_regime(agent):
    agent._regime = MarketRegime.HIGH_VOLATILITY
    agent._regime_confidence = 0.9
    snap = agent._compile_matrix(_now())
    assert snap.regime == MarketRegime.HIGH_VOLATILITY
    assert snap.regime_confidence == 0.9


def test_snapshot_portfolio_info(agent):
    agent._portfolio = PortfolioState(
        total_nav=200_000,
        positions=[
            Position(
                asset="BTC", direction=Direction.LONG, size_pct=0.02,
                entry_price=50000, current_price=51000, unrealised_pnl=200,
                opened_at=_now(),
            ),
        ],
    )
    snap = agent._compile_matrix(_now())
    assert snap.portfolio_positions == 1
    assert snap.total_nav == 200_000


def test_snapshot_anomalies_recent(agent):
    """Only anomalies within 15 minutes should be counted."""
    recent = AnomalyAlert(
        anomaly_type=AnomalyType.VOLUME_SPIKE,
        severity=AlertSeverity.WARNING,
        asset="BTC",
        timestamp=_now() - timedelta(minutes=5),
    )
    old = AnomalyAlert(
        anomaly_type=AnomalyType.PRICE_DEVIATION,
        severity=AlertSeverity.CRITICAL,
        asset="ETH",
        timestamp=_now() - timedelta(minutes=20),
    )
    agent._recent_anomalies = [recent, old]
    snap = agent._compile_matrix(_now())
    assert snap.active_anomalies == 1  # Only the recent one.


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health(agent):
    agent._signals["BTC"]["ta"] = _make_entry(agent_id="ta")
    agent._signals["ETH"]["fa"] = _make_entry(agent_id="fa")
    agent._regime = MarketRegime.TRENDING_UP

    h = agent.health()
    assert h["assets_tracked"] == 2
    assert h["total_signals"] == 2
    assert h["regime"] == MarketRegime.TRENDING_UP
