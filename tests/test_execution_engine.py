"""Tests for ExecutionEngine — side mapping, simulate fill, pre-trade checks."""

import pytest

from agents.execution.engine import ExecutionEngine, _side_from_action
from core.models import (
    AccountHealth,
    AlertSeverity,
    ExecutionAdvisory,
    ExecutionMode,
    TradeAction,
    TradeDecision,
)
from tests.conftest import MockMessageBus, make_decision


def _make_engine() -> ExecutionEngine:
    bus = MockMessageBus()
    return ExecutionEngine(bus=bus, platform_specialist=None)


# ---------------------------------------------------------------------------
# _side_from_action
# ---------------------------------------------------------------------------

class TestSideFromAction:
    def test_open_long(self):
        is_buy, is_close = _side_from_action(TradeAction.OPEN_LONG)
        assert is_buy is True
        assert is_close is False

    def test_open_short(self):
        is_buy, is_close = _side_from_action(TradeAction.OPEN_SHORT)
        assert is_buy is False
        assert is_close is False

    def test_close(self):
        is_buy, is_close = _side_from_action(TradeAction.CLOSE)
        assert is_buy is False
        assert is_close is True

    def test_reduce(self):
        is_buy, is_close = _side_from_action(TradeAction.REDUCE)
        assert is_buy is False
        assert is_close is True

    def test_flip(self):
        is_buy, is_close = _side_from_action(TradeAction.FLIP)
        assert is_buy is True
        assert is_close is False


# ---------------------------------------------------------------------------
# _simulate_fill
# ---------------------------------------------------------------------------

class TestSimulateFill:
    def test_correct_fields(self):
        engine = _make_engine()
        d = make_decision(entry_price=50000.0)
        fill = engine._simulate_fill(d, size=0.1, is_buy=True, mode=ExecutionMode.MARKET)
        assert fill.asset == "BTC"
        assert fill.side == "buy"
        assert fill.size == 0.1
        assert fill.price == 50000.0
        assert fill.paper_trade is True

    def test_sell_side(self):
        engine = _make_engine()
        d = make_decision(action=TradeAction.OPEN_SHORT, entry_price=50000.0)
        fill = engine._simulate_fill(d, size=0.05, is_buy=False, mode=ExecutionMode.MARKET)
        assert fill.side == "sell"

    def test_fee_calculation(self):
        engine = _make_engine()
        d = make_decision(entry_price=50000.0)
        fill = engine._simulate_fill(d, size=1.0, is_buy=True, mode=ExecutionMode.MARKET)
        # Default taker fee: 3.5 bps
        expected_fee = 1.0 * 50000.0 * (3.5 / 10_000)
        assert fill.fee == pytest.approx(expected_fee)

    def test_with_advisory_fee(self):
        engine = _make_engine()
        engine._advisory_cache["BTC"] = ExecutionAdvisory(
            asset="BTC", taker_fee_bps=2.0,
        )
        d = make_decision(entry_price=50000.0)
        fill = engine._simulate_fill(d, size=1.0, is_buy=True, mode=ExecutionMode.MARKET)
        expected_fee = 1.0 * 50000.0 * (2.0 / 10_000)
        assert fill.fee == pytest.approx(expected_fee)


# ---------------------------------------------------------------------------
# _pre_trade_checks
# ---------------------------------------------------------------------------

class TestPreTradeChecks:
    async def test_critical_health_blocks_entry(self):
        engine = _make_engine()
        engine._account_cache = AccountHealth(
            severity=AlertSeverity.CRITICAL,
            equity=100_000, margin_available=50_000,
        )
        d = make_decision(action=TradeAction.OPEN_LONG)
        reason = await engine._pre_trade_checks(d)
        assert reason is not None
        assert "CRITICAL" in reason

    async def test_critical_health_allows_close(self):
        engine = _make_engine()
        engine._account_cache = AccountHealth(
            severity=AlertSeverity.CRITICAL,
            equity=100_000, margin_available=50_000,
        )
        d = make_decision(action=TradeAction.CLOSE)
        reason = await engine._pre_trade_checks(d)
        assert reason is None

    async def test_insufficient_margin(self):
        engine = _make_engine()
        engine._account_cache = AccountHealth(
            equity=100_000, margin_available=500,  # Only $500 available
        )
        d = make_decision(size_pct=0.02)  # Needs $2000
        reason = await engine._pre_trade_checks(d)
        assert reason is not None
        assert "margin" in reason.lower()

    async def test_excessive_slippage(self):
        engine = _make_engine()
        engine._account_cache = AccountHealth(equity=100_000, margin_available=50_000)
        engine._advisory_cache["BTC"] = ExecutionAdvisory(
            asset="BTC", ask_depth_usd=100.0, bid_depth_usd=100.0,  # Very thin book
        )
        d = make_decision(size_pct=0.02, action=TradeAction.OPEN_LONG)
        reason = await engine._pre_trade_checks(d)
        assert reason is not None
        assert "lippage" in reason.lower()

    async def test_valid_passes(self):
        engine = _make_engine()
        engine._account_cache = AccountHealth(
            equity=100_000, margin_available=50_000,
        )
        d = make_decision(size_pct=0.01)
        reason = await engine._pre_trade_checks(d)
        assert reason is None
