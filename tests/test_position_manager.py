"""Tests for PositionManager — _ManagedPosition and portfolio state."""

import pytest
from datetime import datetime, timezone

from agents.execution.position_manager import PositionManager, _ManagedPosition
from core.models import Direction
from tests.conftest import MockMessageBus


def _make_mgr() -> PositionManager:
    return PositionManager(bus=MockMessageBus(), platform_specialist=None)


# ---------------------------------------------------------------------------
# _ManagedPosition
# ---------------------------------------------------------------------------

class TestManagedPosition:
    def test_update_price_long_pnl(self):
        mp = _ManagedPosition(
            asset="BTC", direction=Direction.LONG, size=1.0,
            entry_price=50000.0, current_price=50000.0,
            opened_at=datetime.now(timezone.utc),
        )
        mp.update_price(51000.0)
        assert mp.unrealised_pnl == pytest.approx(1000.0)
        assert mp.highest_price_since_entry == 51000.0

    def test_update_price_short_pnl(self):
        mp = _ManagedPosition(
            asset="BTC", direction=Direction.SHORT, size=1.0,
            entry_price=50000.0, current_price=50000.0,
            opened_at=datetime.now(timezone.utc),
        )
        mp.update_price(49000.0)
        assert mp.unrealised_pnl == pytest.approx(1000.0)
        # For shorts, highest_price_since_entry tracks lowest
        assert mp.highest_price_since_entry == 49000.0

    def test_notional(self):
        mp = _ManagedPosition(
            asset="BTC", direction=Direction.LONG, size=2.0,
            entry_price=50000.0, current_price=51000.0,
            opened_at=datetime.now(timezone.utc),
        )
        assert mp.notional() == pytest.approx(102000.0)

    def test_to_model_fields(self):
        mp = _ManagedPosition(
            asset="ETH", direction=Direction.LONG, size=10.0,
            entry_price=3000.0, current_price=3100.0,
            opened_at=datetime.now(timezone.utc),
        )
        mp.update_price(3100.0)
        pos = mp.to_model(nav=100_000.0)
        assert pos.asset == "ETH"
        assert pos.direction == Direction.LONG
        assert pos.entry_price == 3000.0
        assert pos.current_price == 3100.0
        assert pos.size_pct == pytest.approx(31000.0 / 100_000.0)

    def test_to_model_zero_nav(self):
        mp = _ManagedPosition(
            asset="BTC", direction=Direction.LONG, size=1.0,
            entry_price=50000.0, current_price=50000.0,
            opened_at=datetime.now(timezone.utc),
        )
        pos = mp.to_model(nav=0.0)
        assert pos.size_pct == 0.0


# ---------------------------------------------------------------------------
# get_portfolio_state
# ---------------------------------------------------------------------------

class TestGetPortfolioState:
    def test_empty_portfolio(self):
        mgr = _make_mgr()
        state = mgr.get_portfolio_state()
        assert state.total_nav == 0.0
        assert state.gross_exposure == 0.0
        assert state.net_exposure == 0.0
        assert len(state.positions) == 0

    def test_with_positions(self):
        mgr = _make_mgr()
        mgr._nav = 100_000.0
        mgr._positions["BTC"] = _ManagedPosition(
            asset="BTC", direction=Direction.LONG, size=1.0,
            entry_price=50000.0, current_price=50000.0,
            opened_at=datetime.now(timezone.utc),
        )
        mgr._positions["ETH"] = _ManagedPosition(
            asset="ETH", direction=Direction.SHORT, size=10.0,
            entry_price=3000.0, current_price=3000.0,
            opened_at=datetime.now(timezone.utc),
        )
        state = mgr.get_portfolio_state()
        assert len(state.positions) == 2
        # gross = |50000| + |30000| = 80000 → 0.8
        assert state.gross_exposure == pytest.approx(0.8)
        # net = 50000 - 30000 = 20000 → 0.2
        assert state.net_exposure == pytest.approx(0.2)
