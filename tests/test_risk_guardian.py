"""
Tests for the RiskGuardian agent — classify_asset, _PriceHistory,
_DrawdownTracker, and the full _evaluate pipeline.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from agents.risk.guardian import (
    RiskGuardian,
    _DrawdownTracker,
    _PriceHistory,
    classify_asset,
)
from core.models import (
    AlertSeverity,
    AnomalyType,
    AssetClass,
    Direction,
    TradeAction,
)
from tests.conftest import (
    MockMessageBus,
    make_anomaly,
    make_decision,
    make_portfolio,
    make_position,
)


# =========================================================================
# 1. classify_asset
# =========================================================================


class TestClassifyAsset:
    def test_btc_is_crypto_major(self):
        assert classify_asset("BTC") == AssetClass.CRYPTO_MAJOR

    def test_eth_is_crypto_major(self):
        assert classify_asset("ETH") == AssetClass.CRYPTO_MAJOR

    def test_spy_is_equity(self):
        assert classify_asset("SPY") == AssetClass.EQUITY

    def test_aapl_is_equity(self):
        assert classify_asset("AAPL") == AssetClass.EQUITY

    def test_nvda_is_equity(self):
        assert classify_asset("NVDA") == AssetClass.EQUITY

    def test_gold_is_commodity(self):
        assert classify_asset("GOLD") == AssetClass.COMMODITY

    def test_silver_is_commodity(self):
        assert classify_asset("SILVER") == AssetClass.COMMODITY

    def test_oil_is_commodity(self):
        assert classify_asset("OIL") == AssetClass.COMMODITY

    def test_eur_is_fx(self):
        assert classify_asset("EUR") == AssetClass.FX

    def test_jpy_is_fx(self):
        assert classify_asset("JPY") == AssetClass.FX

    def test_gbp_is_fx(self):
        assert classify_asset("GBP") == AssetClass.FX

    def test_sol_is_crypto_alt(self):
        assert classify_asset("SOL") == AssetClass.CRYPTO_ALT

    def test_doge_is_crypto_alt(self):
        assert classify_asset("DOGE") == AssetClass.CRYPTO_ALT

    def test_case_insensitive(self):
        assert classify_asset("btc") == AssetClass.CRYPTO_MAJOR
        assert classify_asset("Eth") == AssetClass.CRYPTO_MAJOR
        assert classify_asset("gold") == AssetClass.COMMODITY


# =========================================================================
# 2. _PriceHistory
# =========================================================================


class TestPriceHistory:
    def test_append_and_get(self):
        ph = _PriceHistory(maxlen=100)
        ph.append("BTC", 50000.0)
        ph.append("BTC", 51000.0)
        assert ph.get("BTC") == [50000.0, 51000.0]

    def test_get_unknown_asset_returns_empty(self):
        ph = _PriceHistory(maxlen=100)
        assert ph.get("NONEXISTENT") == []

    def test_maxlen_enforced(self):
        ph = _PriceHistory(maxlen=5)
        for i in range(10):
            ph.append("BTC", float(i))
        assert len(ph.get("BTC")) == 5
        assert ph.get("BTC") == [5.0, 6.0, 7.0, 8.0, 9.0]

    def test_pearson_returns_none_insufficient_data(self):
        ph = _PriceHistory(maxlen=100)
        # Only 20 data points -- below the 25 minimum
        for i in range(20):
            ph.append("BTC", 50000.0 + i * 10)
            ph.append("ETH", 3000.0 + i * 5)
        assert ph.pearson("BTC", "ETH") is None

    def test_pearson_identical_series_near_one(self):
        ph = _PriceHistory(maxlen=200)
        for i in range(50):
            price = 100.0 + i * 0.5
            ph.append("A", price)
            ph.append("B", price)
        corr = ph.pearson("A", "B")
        assert corr is not None
        assert corr == pytest.approx(1.0, abs=0.01)

    def test_pearson_inverse_series_near_neg_one(self):
        """Returns-based correlation: when A goes up by r%, B goes down by r%."""
        ph = _PriceHistory(maxlen=200)
        import math
        base_a = 100.0
        base_b = 100.0
        for i in range(50):
            # Create returns that are perfectly negatively correlated:
            # A grows by a varying pct, B shrinks by the same pct.
            pct = 0.01 * math.sin(i * 0.5)  # oscillating returns
            base_a *= (1 + pct)
            base_b *= (1 - pct)
            ph.append("A", base_a)
            ph.append("B", base_b)
        corr = ph.pearson("A", "B")
        assert corr is not None
        assert corr == pytest.approx(-1.0, abs=0.05)

    def test_pearson_constant_prices_returns_zero(self):
        ph = _PriceHistory(maxlen=200)
        for _ in range(50):
            ph.append("X", 100.0)
            ph.append("Y", 200.0)
        corr = ph.pearson("X", "Y")
        assert corr == 0.0

    def test_pearson_one_unknown_asset_returns_none(self):
        ph = _PriceHistory(maxlen=200)
        for i in range(50):
            ph.append("A", 100.0 + i)
        assert ph.pearson("A", "UNKNOWN") is None


# =========================================================================
# 3. _DrawdownTracker
# =========================================================================


class TestDrawdownTracker:
    def test_daily_drawdown_no_history_returns_zero(self):
        dd = _DrawdownTracker()
        assert dd.daily_drawdown(100_000.0) == 0.0

    def test_weekly_drawdown_no_history_returns_zero(self):
        dd = _DrawdownTracker()
        assert dd.weekly_drawdown(100_000.0) == 0.0

    def test_monthly_drawdown_no_history_returns_zero(self):
        dd = _DrawdownTracker()
        assert dd.monthly_drawdown(100_000.0) == 0.0

    def test_daily_drawdown_with_loss(self):
        dd = _DrawdownTracker()
        # Insert a snapshot 12 hours ago with NAV = 100_000
        past = datetime.now(timezone.utc) - timedelta(hours=12)
        dd._snapshots.append((past, 100_000.0))
        # Current NAV = 90_000 -> 10% drawdown
        result = dd.daily_drawdown(90_000.0)
        assert result == pytest.approx(0.10, abs=0.001)

    def test_daily_drawdown_with_gain_returns_zero(self):
        dd = _DrawdownTracker()
        past = datetime.now(timezone.utc) - timedelta(hours=12)
        dd._snapshots.append((past, 100_000.0))
        # Current NAV is higher than the past snapshot -> no drawdown
        result = dd.daily_drawdown(110_000.0)
        assert result == 0.0

    def test_weekly_drawdown_with_history(self):
        dd = _DrawdownTracker()
        past = datetime.now(timezone.utc) - timedelta(days=5)
        dd._snapshots.append((past, 100_000.0))
        result = dd.weekly_drawdown(80_000.0)
        assert result == pytest.approx(0.20, abs=0.001)

    def test_monthly_drawdown_with_history(self):
        dd = _DrawdownTracker()
        past = datetime.now(timezone.utc) - timedelta(days=20)
        dd._snapshots.append((past, 100_000.0))
        result = dd.monthly_drawdown(75_000.0)
        assert result == pytest.approx(0.25, abs=0.001)

    def test_record_updates_hwm(self):
        dd = _DrawdownTracker()
        dd.record(100_000.0)
        assert dd._hwm == 100_000.0
        dd.record(110_000.0)
        assert dd._hwm == 110_000.0
        dd.record(105_000.0)
        # HWM should not decrease
        assert dd._hwm == 110_000.0

    def test_old_snapshot_outside_daily_window_ignored(self):
        dd = _DrawdownTracker()
        # Snapshot from 2 days ago should not appear in daily lookback
        old = datetime.now(timezone.utc) - timedelta(days=2)
        dd._snapshots.append((old, 100_000.0))
        # No snapshot within the 1-day window -> returns 0
        assert dd.daily_drawdown(90_000.0) == 0.0


# =========================================================================
# 4. RiskGuardian._evaluate integration tests
# =========================================================================


def _make_guardian(bus: MockMessageBus) -> RiskGuardian:
    """Create a RiskGuardian wired to a MockMessageBus."""
    return RiskGuardian(bus=bus)


class TestEvaluateApproval:
    @pytest.mark.asyncio
    async def test_approves_valid_small_open_long(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1
        assert approved[0].asset == "BTC"
        assert approved[0].risk_approved is True

    @pytest.mark.asyncio
    async def test_caps_oversized_position(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)

        # size_pct=0.05 exceeds max_position_pct=0.02
        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.05,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1
        # The size should have been capped to max_position_pct (0.02)
        assert approved[0].size_pct == pytest.approx(0.02, abs=0.001)

        # Also check that adjustments were published in the risk check
        risk_checks = mock_bus.get_published("apex:risk_checks")
        assert len(risk_checks) == 1
        assert risk_checks[0].adjustments.get("size_capped_to") == 0.02

    @pytest.mark.asyncio
    async def test_rejects_gross_exposure_breach(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # Gross exposure already at 2.99 -- adding 0.02 would exceed 3.0
        guardian._portfolio = make_portfolio(
            total_nav=100_000, gross_exposure=2.99,
        )

        decision = make_decision(
            asset="SOL", action=TradeAction.OPEN_LONG, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1
        assert mock_bus.get_published("decisions:approved") == []

    @pytest.mark.asyncio
    async def test_rejects_net_exposure_breach(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # Net exposure already at 0.99 -- adding 0.02 LONG would exceed 1.0
        guardian._portfolio = make_portfolio(
            total_nav=100_000, net_exposure=0.99,
        )

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1

    @pytest.mark.asyncio
    async def test_reduces_size_on_daily_drawdown_soft_limit(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=89_000)

        # Insert snapshot from 12h ago with NAV=100k -> daily dd = 11% > 10% soft limit
        past = datetime.now(timezone.utc) - timedelta(hours=12)
        guardian._dd_tracker._snapshots.append((past, 100_000.0))

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1
        # Size should be reduced by 50% (drawdown_size_reduction=0.50)
        assert approved[0].size_pct == pytest.approx(0.01, abs=0.001)

    @pytest.mark.asyncio
    async def test_rejects_on_circuit_breaker_drawdown(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=84_000)

        # Insert snapshot from 12h ago with NAV=100k -> daily dd = 16% > 15% circuit breaker
        past = datetime.now(timezone.utc) - timedelta(hours=12)
        guardian._dd_tracker._snapshots.append((past, 100_000.0))

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1
        assert mock_bus.get_published("decisions:approved") == []

    @pytest.mark.asyncio
    async def test_rejects_in_defensive_mode(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)
        # Set defensive mode to expire in the future
        guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=10)

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1
        assert mock_bus.get_published("decisions:approved") == []

    @pytest.mark.asyncio
    async def test_close_allowed_in_defensive_mode(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)
        guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=10)

        decision = make_decision(
            asset="BTC", action=TradeAction.CLOSE, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1

    @pytest.mark.asyncio
    async def test_reduce_allowed_in_defensive_mode(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)
        guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=10)

        decision = make_decision(
            asset="BTC", action=TradeAction.REDUCE, size_pct=0.005,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1

    @pytest.mark.asyncio
    async def test_rejects_high_correlation(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # Existing position in BTC
        guardian._portfolio = make_portfolio(
            total_nav=100_000,
            positions=[make_position(asset="BTC", size_pct=0.02)],
        )

        # Populate price history with highly correlated data (same trend)
        for i in range(50):
            price = 100.0 + i * 2.0
            guardian._price_history.append("BTC", price)
            guardian._price_history.append("ETH", price * 0.06)  # Same direction

        decision = make_decision(
            asset="ETH", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1

    @pytest.mark.asyncio
    async def test_rejects_asset_class_concentration(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # Already have 39% in crypto alts (SOL). Adding 2% DOGE would push to 41%.
        guardian._portfolio = make_portfolio(
            total_nav=100_000,
            positions=[make_position(asset="SOL", size_pct=0.39)],
        )

        decision = make_decision(
            asset="DOGE", action=TradeAction.OPEN_LONG, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1

    @pytest.mark.asyncio
    async def test_approves_when_asset_class_within_limit(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # 30% in crypto alts. Adding 2% stays under 40%.
        guardian._portfolio = make_portfolio(
            total_nav=100_000,
            positions=[make_position(asset="SOL", size_pct=0.30)],
        )

        decision = make_decision(
            asset="DOGE", action=TradeAction.OPEN_LONG, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1


class TestEnterDefensiveMode:
    @pytest.mark.asyncio
    async def test_enter_defensive_mode_publishes_reduce_decisions(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(
            total_nav=100_000,
            positions=[
                make_position(asset="BTC", size_pct=0.05),
                make_position(asset="ETH", size_pct=0.03),
            ],
        )

        alert = make_anomaly(
            anomaly_type=AnomalyType.PRICE_DEVIATION,
            severity=AlertSeverity.CRITICAL,
            asset="BTC",
        )

        await guardian._enter_defensive_mode(alert)

        approved = mock_bus.get_published("decisions:approved")
        # Should publish REDUCE for each open position
        assert len(approved) == 2
        for d in approved:
            assert d.action == TradeAction.REDUCE
            assert d.risk_approved is True
            assert d.metadata.get("reason") == "defensive_mode"

        # Check that the reduce sizes are 30% of original
        sizes = sorted(d.size_pct for d in approved)
        assert sizes[0] == pytest.approx(0.03 * 0.30, abs=0.0001)
        assert sizes[1] == pytest.approx(0.05 * 0.30, abs=0.0001)

    @pytest.mark.asyncio
    async def test_enter_defensive_mode_sets_pause_timer(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)

        alert = make_anomaly(severity=AlertSeverity.CRITICAL)
        await guardian._enter_defensive_mode(alert)

        assert guardian._defensive_until is not None
        # Should be ~15 minutes in the future
        now = datetime.now(timezone.utc)
        delta = (guardian._defensive_until - now).total_seconds()
        assert 14 * 60 < delta <= 15 * 60 + 5


class TestEvaluateEdgeCases:
    @pytest.mark.asyncio
    async def test_open_short_counts_against_net_exposure(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        # Net exposure at -0.99 (heavily short).  A new short pushes past -1.0.
        guardian._portfolio = make_portfolio(
            total_nav=100_000, net_exposure=-0.99,
        )

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_SHORT, size_pct=0.02,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1

    @pytest.mark.asyncio
    async def test_weekly_drawdown_rejects_and_sets_pause(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=79_000)

        # 6 days ago NAV was 100k -> weekly dd = 21% > 20%
        past = datetime.now(timezone.utc) - timedelta(days=6)
        guardian._dd_tracker._snapshots.append((past, 100_000.0))

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1
        # Pause should be set
        assert guardian._weekly_pause_until is not None

    @pytest.mark.asyncio
    async def test_monthly_drawdown_enters_review_mode(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=74_000)

        # 20 days ago NAV was 100k -> monthly dd = 26% > 25%
        past = datetime.now(timezone.utc) - timedelta(days=20)
        guardian._dd_tracker._snapshots.append((past, 100_000.0))

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1
        assert guardian._monthly_review_mode is True

    @pytest.mark.asyncio
    async def test_monthly_review_mode_rejects_new_entries(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)
        guardian._monthly_review_mode = True

        decision = make_decision(
            asset="ETH", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        rejected = mock_bus.get_published("decisions:rejected")
        assert len(rejected) == 1

    @pytest.mark.asyncio
    async def test_approved_counter_increments(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)

        assert guardian._approved_count == 0
        await guardian._evaluate(make_decision(size_pct=0.01))
        assert guardian._approved_count == 1

    @pytest.mark.asyncio
    async def test_rejected_counter_increments(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000, gross_exposure=2.99)

        assert guardian._rejected_count == 0
        await guardian._evaluate(make_decision(size_pct=0.02))
        assert guardian._rejected_count == 1

    @pytest.mark.asyncio
    async def test_risk_check_published_on_approval(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(total_nav=100_000)

        decision = make_decision(asset="BTC", size_pct=0.01)
        await guardian._evaluate(decision)

        risk_checks = mock_bus.get_published("apex:risk_checks")
        assert len(risk_checks) == 1
        assert risk_checks[0].approved is True
        assert risk_checks[0].decision_id == decision.decision_id

    @pytest.mark.asyncio
    async def test_risk_check_published_on_rejection(self, mock_bus):
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(
            total_nav=100_000, gross_exposure=2.99,
        )

        decision = make_decision(asset="SOL", size_pct=0.02)
        await guardian._evaluate(decision)

        risk_checks = mock_bus.get_published("apex:risk_checks")
        assert len(risk_checks) == 1
        assert risk_checks[0].approved is False
        assert risk_checks[0].veto_reason is not None

    @pytest.mark.asyncio
    async def test_correlation_skip_same_asset_position(self, mock_bus):
        """Adding to an existing BTC position should skip correlation check for BTC."""
        guardian = _make_guardian(mock_bus)
        guardian._portfolio = make_portfolio(
            total_nav=100_000,
            positions=[make_position(asset="BTC", size_pct=0.01)],
        )

        # Populate price history -- even though correlation would be high,
        # the check should skip because the asset matches.
        for i in range(50):
            guardian._price_history.append("BTC", 100.0 + i)

        decision = make_decision(
            asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01,
        )
        await guardian._evaluate(decision)

        # Should still approve because correlation is only checked against
        # OTHER assets in the portfolio.
        approved = mock_bus.get_published("decisions:approved")
        assert len(approved) == 1
