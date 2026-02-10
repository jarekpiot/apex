"""Tests for MetaOrchestrator — signal buffering, consensus voting, action selection, sizing."""

import pytest
from datetime import datetime, timedelta, timezone

from agents.meta.orchestrator import MetaOrchestrator, _SignalBuffer
from config.settings import settings
from core.models import AgentSignal, Direction, Position, PortfolioState, Timeframe, TradeAction
from tests.conftest import MockMessageBus, make_signal, make_position


# ---------------------------------------------------------------------------
# _SignalBuffer tests
# ---------------------------------------------------------------------------

class TestSignalBuffer:
    def test_add_replaces_older(self):
        buf = _SignalBuffer()
        old = make_signal(agent_id="a1", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc))
        new = make_signal(agent_id="a1", timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc))
        buf.add(old)
        buf.add(new)
        assert len(buf) == 1
        signals = buf.active(datetime(2025, 1, 2, tzinfo=timezone.utc), max_age_s=999999)
        assert signals[0].timestamp == new.timestamp

    def test_add_keeps_newer(self):
        buf = _SignalBuffer()
        new = make_signal(agent_id="a1", timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc))
        old = make_signal(agent_id="a1", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc))
        buf.add(new)
        buf.add(old)
        signals = buf.active(datetime(2025, 1, 2, tzinfo=timezone.utc), max_age_s=999999)
        assert signals[0].timestamp == new.timestamp

    def test_active_excludes_expired(self):
        buf = _SignalBuffer()
        expired = make_signal(
            agent_id="a1",
            expires_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        buf.add(expired)
        now = datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert len(buf.active(now, max_age_s=999999)) == 0

    def test_active_excludes_stale(self):
        buf = _SignalBuffer()
        old = make_signal(
            agent_id="a1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        buf.add(old)
        now = datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert len(buf.active(now, max_age_s=3600)) == 0  # 1 hour max age

    def test_prune_removes_stale(self):
        buf = _SignalBuffer()
        old = make_signal(
            agent_id="a1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        buf.add(old)
        assert len(buf) == 1
        buf.prune(datetime(2025, 1, 2, tzinfo=timezone.utc), max_age_s=3600)
        assert len(buf) == 0


# ---------------------------------------------------------------------------
# _compute_consensus tests
# ---------------------------------------------------------------------------

class TestComputeConsensus:
    def _make_orch(self) -> MetaOrchestrator:
        bus = MockMessageBus()
        orch = MetaOrchestrator(bus=bus)
        return orch

    def test_single_agent(self):
        orch = self._make_orch()
        orch._weights = {"agent_a": 0.20}
        sig = make_signal(agent_id="agent_a", direction=0.8, conviction=0.9)
        consensus, avg_conv, tf, count = orch._compute_consensus([sig])
        # direction = 0.8, weight = 0.20, eff = 0.20 * 0.9 = 0.18
        # consensus = (0.8 * 0.18) / 0.18 = 0.8
        assert abs(consensus - 0.8) < 0.01
        assert count == 1

    def test_two_agents_agree(self):
        orch = self._make_orch()
        orch._weights = {"a": 0.20, "b": 0.20}
        s1 = make_signal(agent_id="a", direction=0.6, conviction=0.8)
        s2 = make_signal(agent_id="b", direction=0.4, conviction=0.7)
        consensus, _, _, count = orch._compute_consensus([s1, s2])
        assert consensus > 0
        assert count == 2

    def test_two_agents_disagree(self):
        orch = self._make_orch()
        orch._weights = {"a": 0.20, "b": 0.20}
        s1 = make_signal(agent_id="a", direction=0.8, conviction=0.8)
        s2 = make_signal(agent_id="b", direction=-0.8, conviction=0.8)
        consensus, _, _, count = orch._compute_consensus([s1, s2])
        assert abs(consensus) < 0.01  # Cancel out

    def test_high_conviction_dominates(self):
        orch = self._make_orch()
        orch._weights = {"a": 0.20, "b": 0.20}
        s1 = make_signal(agent_id="a", direction=0.8, conviction=1.0)
        s2 = make_signal(agent_id="b", direction=-0.3, conviction=0.1)
        consensus, _, _, _ = orch._compute_consensus([s1, s2])
        assert consensus > 0.5  # High-conviction agent dominates

    def test_agent_count_correct(self):
        orch = self._make_orch()
        orch._weights = {"a": 0.1, "b": 0.1, "c": 0.1}
        signals = [
            make_signal(agent_id="a", direction=0.5, conviction=0.5),
            make_signal(agent_id="b", direction=0.5, conviction=0.5),
            make_signal(agent_id="c", direction=0.5, conviction=0.5),
        ]
        _, _, _, count = orch._compute_consensus(signals)
        assert count == 3

    def test_zero_total_weight(self):
        orch = self._make_orch()
        orch._weights = {"a": 0.0}
        sig = make_signal(agent_id="a", direction=0.5, conviction=0.0)
        consensus, avg_conv, tf, count = orch._compute_consensus([sig])
        assert consensus == 0.0
        assert count == 0


# ---------------------------------------------------------------------------
# _choose_action tests
# ---------------------------------------------------------------------------

class TestChooseAction:
    def _make_orch(self) -> MetaOrchestrator:
        bus = MockMessageBus()
        return MetaOrchestrator(bus=bus)

    def test_no_position_positive_consensus(self):
        orch = self._make_orch()
        action = orch._choose_action("BTC", 0.5)
        assert action == TradeAction.OPEN_LONG

    def test_no_position_negative_consensus(self):
        orch = self._make_orch()
        action = orch._choose_action("BTC", -0.5)
        assert action == TradeAction.OPEN_SHORT

    def test_long_position_positive_consensus_aligned(self):
        orch = self._make_orch()
        orch._portfolio = PortfolioState(
            positions=[make_position(asset="BTC", direction=Direction.LONG)],
        )
        action = orch._choose_action("BTC", 0.5)
        assert action is None  # Already aligned

    def test_short_position_negative_consensus_aligned(self):
        orch = self._make_orch()
        orch._portfolio = PortfolioState(
            positions=[make_position(asset="BTC", direction=Direction.SHORT)],
        )
        action = orch._choose_action("BTC", -0.5)
        assert action is None

    def test_long_position_weak_negative_consensus_close(self):
        orch = self._make_orch()
        orch._portfolio = PortfolioState(
            positions=[make_position(asset="BTC", direction=Direction.LONG)],
        )
        action = orch._choose_action("BTC", -0.3)
        assert action == TradeAction.CLOSE

    def test_long_position_strong_negative_consensus_flip(self):
        orch = self._make_orch()
        orch._portfolio = PortfolioState(
            positions=[make_position(asset="BTC", direction=Direction.LONG)],
        )
        # orch_flip_consensus default is 0.60
        action = orch._choose_action("BTC", -0.65)
        assert action == TradeAction.FLIP


# ---------------------------------------------------------------------------
# _compute_size tests
# ---------------------------------------------------------------------------

class TestComputeSize:
    def _make_orch(self) -> MetaOrchestrator:
        bus = MockMessageBus()
        return MetaOrchestrator(bus=bus)

    def test_normal_case(self):
        orch = self._make_orch()
        # base=0.01, |consensus|=0.5, conviction=0.8
        size = orch._compute_size(0.5, 0.8)
        expected = 0.01 * 0.5 * 0.8
        assert abs(size - expected) < 1e-6

    def test_capped_at_max_position(self):
        orch = self._make_orch()
        # base=0.01, |consensus|=1.0, conviction=1.0 → 0.01
        # max_position_pct=0.02, so 0.01 < 0.02 → not capped
        size = orch._compute_size(1.0, 1.0)
        assert size <= settings.max_position_pct

    def test_low_conviction_floor(self):
        orch = self._make_orch()
        # conviction=0.0 → max(0.0, 0.1) = 0.1
        size = orch._compute_size(1.0, 0.0)
        expected = 0.01 * 1.0 * 0.1
        assert abs(size - expected) < 1e-6
