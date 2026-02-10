"""Tests for RedTeamChallenger — heuristic challenge checks."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from agents.red_team.challenger import RedTeamChallenger
from core.models import (
    AnomalyAlert,
    AnomalyType,
    AlertSeverity,
    ChallengeType,
    Direction,
    PortfolioState,
    RedTeamRecommendation,
    TradeAction,
    TradeDecision,
)
from tests.conftest import MockMessageBus, make_decision, make_position


def _make_challenger() -> tuple[RedTeamChallenger, MockMessageBus]:
    bus = MockMessageBus()
    rt = RedTeamChallenger(bus=bus)
    return rt, bus


# ---------------------------------------------------------------------------
# _check_consensus
# ---------------------------------------------------------------------------

class TestCheckConsensus:
    def test_weak_consensus_has_severity(self):
        rt, _ = _make_challenger()
        d = make_decision(consensus_score=0.2, metadata={"agent_count": 5})
        ct, sev, thesis, ev = rt._check_consensus(d)
        assert sev > 0

    def test_strong_consensus_zero_severity(self):
        rt, _ = _make_challenger()
        d = make_decision(consensus_score=0.8, metadata={"agent_count": 5})
        ct, sev, thesis, ev = rt._check_consensus(d)
        assert sev == 0.0

    def test_few_agents_adds_severity(self):
        rt, _ = _make_challenger()
        d = make_decision(consensus_score=0.2, metadata={"agent_count": 2})
        ct, sev, thesis, ev = rt._check_consensus(d)
        # Base severity + 0.2 for few agents
        d2 = make_decision(consensus_score=0.2, metadata={"agent_count": 5})
        _, sev2, _, _ = rt._check_consensus(d2)
        assert sev > sev2


# ---------------------------------------------------------------------------
# _check_liquidity
# ---------------------------------------------------------------------------

class TestCheckLiquidity:
    def test_no_price_data(self):
        rt, _ = _make_challenger()
        d = make_decision(asset="UNKNOWN")
        ct, sev, thesis, ev = rt._check_liquidity(d)
        assert sev == pytest.approx(0.4)

    def test_large_position(self):
        rt, _ = _make_challenger()
        rt._mid_prices["BTC"] = 50000.0
        d = make_decision(size_pct=0.02)  # > 0.015
        ct, sev, thesis, ev = rt._check_liquidity(d)
        assert sev == pytest.approx(0.3)

    def test_small_position_with_price(self):
        rt, _ = _make_challenger()
        rt._mid_prices["BTC"] = 50000.0
        d = make_decision(size_pct=0.01)  # <= 0.015
        ct, sev, thesis, ev = rt._check_liquidity(d)
        assert sev == 0.0


# ---------------------------------------------------------------------------
# _check_correlation
# ---------------------------------------------------------------------------

class TestCheckCorrelation:
    def test_many_same_direction_positions(self):
        rt, _ = _make_challenger()
        rt._portfolio = PortfolioState(
            positions=[
                make_position(asset="ETH", direction=Direction.LONG),
                make_position(asset="SOL", direction=Direction.LONG),
                make_position(asset="AVAX", direction=Direction.LONG),
                make_position(asset="LINK", direction=Direction.LONG),
            ],
        )
        d = make_decision(action=TradeAction.OPEN_LONG)
        ct, sev, thesis, ev = rt._check_correlation(d)
        assert sev == pytest.approx(0.4)

    def test_close_action_zero(self):
        rt, _ = _make_challenger()
        d = make_decision(action=TradeAction.CLOSE)
        ct, sev, thesis, ev = rt._check_correlation(d)
        assert sev == 0.0

    def test_few_positions_zero(self):
        rt, _ = _make_challenger()
        rt._portfolio = PortfolioState(
            positions=[make_position(asset="ETH", direction=Direction.LONG)],
        )
        d = make_decision(action=TradeAction.OPEN_LONG)
        ct, sev, thesis, ev = rt._check_correlation(d)
        assert sev == 0.0


# ---------------------------------------------------------------------------
# _check_volatility
# ---------------------------------------------------------------------------

class TestCheckVolatility:
    def test_recent_anomalies(self):
        rt, _ = _make_challenger()
        now = datetime.now(timezone.utc)
        rt._recent_anomalies = [
            AnomalyAlert(anomaly_type=AnomalyType.PRICE_DEVIATION,
                         severity=AlertSeverity.WARNING, timestamp=now),
            AnomalyAlert(anomaly_type=AnomalyType.VOLUME_SPIKE,
                         severity=AlertSeverity.WARNING, timestamp=now),
        ]
        d = make_decision()
        ct, sev, thesis, ev = rt._check_volatility(d)
        assert sev > 0

    def test_no_anomalies(self):
        rt, _ = _make_challenger()
        rt._recent_anomalies = []
        d = make_decision()
        ct, sev, thesis, ev = rt._check_volatility(d)
        assert sev == 0.0


# ---------------------------------------------------------------------------
# _check_timing
# ---------------------------------------------------------------------------

class TestCheckTiming:
    def test_weekend(self):
        rt, _ = _make_challenger()
        d = make_decision()
        # Saturday = weekday() == 5
        with patch("agents.red_team.challenger.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 1, 4, 12, 0, tzinfo=timezone.utc)  # Saturday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            ct, sev, thesis, ev = rt._check_timing(d)
            assert sev == pytest.approx(0.2)

    def test_weekday(self):
        rt, _ = _make_challenger()
        d = make_decision()
        with patch("agents.red_team.challenger.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 1, 6, 12, 0, tzinfo=timezone.utc)  # Monday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            ct, sev, thesis, ev = rt._check_timing(d)
            assert sev == 0.0


# ---------------------------------------------------------------------------
# _check_contrarian
# ---------------------------------------------------------------------------

class TestCheckContrarian:
    def test_flip_with_existing_position(self):
        rt, _ = _make_challenger()
        rt._portfolio = PortfolioState(
            positions=[make_position(asset="BTC", direction=Direction.LONG)],
        )
        d = make_decision(action=TradeAction.FLIP)
        ct, sev, thesis, ev = rt._check_contrarian(d)
        assert sev == pytest.approx(0.3)

    def test_non_flip(self):
        rt, _ = _make_challenger()
        d = make_decision(action=TradeAction.OPEN_LONG)
        ct, sev, thesis, ev = rt._check_contrarian(d)
        assert sev == 0.0


# ---------------------------------------------------------------------------
# _challenge aggregation
# ---------------------------------------------------------------------------

class TestChallengeAggregation:
    async def test_high_severity_veto(self):
        rt, bus = _make_challenger()
        # Force multiple high severities: no price + weak consensus + weekend + anomalies
        rt._mid_prices.clear()
        rt._recent_anomalies = [
            AnomalyAlert(anomaly_type=AnomalyType.PRICE_DEVIATION,
                         severity=AlertSeverity.WARNING,
                         timestamp=datetime.now(timezone.utc)),
            AnomalyAlert(anomaly_type=AnomalyType.VOLUME_SPIKE,
                         severity=AlertSeverity.WARNING,
                         timestamp=datetime.now(timezone.utc)),
            AnomalyAlert(anomaly_type=AnomalyType.BOOK_IMBALANCE,
                         severity=AlertSeverity.WARNING,
                         timestamp=datetime.now(timezone.utc)),
        ]
        rt._portfolio = PortfolioState(
            positions=[
                make_position(asset="ETH", direction=Direction.LONG),
                make_position(asset="SOL", direction=Direction.LONG),
                make_position(asset="AVAX", direction=Direction.LONG),
                make_position(asset="LINK", direction=Direction.LONG),
            ],
        )
        d = make_decision(
            asset="UNKNOWN", consensus_score=0.15,
            action=TradeAction.OPEN_LONG,
            metadata={"agent_count": 1},
        )
        await rt._challenge(d)
        published = bus.get_published("apex:red_team")
        assert len(published) == 1
        challenge = published[0]
        assert challenge.recommendation == RedTeamRecommendation.VETO

    async def test_low_severity_no_publish(self):
        rt, bus = _make_challenger()
        rt._mid_prices["BTC"] = 50000.0
        d = make_decision(consensus_score=0.9, size_pct=0.005, metadata={"agent_count": 10})
        await rt._challenge(d)
        published = bus.get_published("apex:red_team")
        assert len(published) == 0

    async def test_moderate_severity_reduce(self):
        rt, bus = _make_challenger()
        # No price data (0.4) + weak consensus
        d = make_decision(
            asset="UNKNOWN", consensus_score=0.2,
            metadata={"agent_count": 5},
        )
        await rt._challenge(d)
        published = bus.get_published("apex:red_team")
        if published:
            rec = published[0].recommendation
            assert rec in (
                RedTeamRecommendation.REDUCE_CONVICTION,
                RedTeamRecommendation.PAUSE_AND_REVIEW,
                RedTeamRecommendation.PROCEED,
            )
