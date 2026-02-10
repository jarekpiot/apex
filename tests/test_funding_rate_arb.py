"""Tests for FundingRateArb — _FundingHistory, conviction, breakeven."""

import pytest

from agents.analysis.funding_arb import FundingRateArb, _FundingHistory


# ---------------------------------------------------------------------------
# _FundingHistory
# ---------------------------------------------------------------------------

class TestFundingHistory:
    def test_mean(self):
        h = _FundingHistory(maxlen=100)
        for i in range(10):
            h.append(float(i), 0.001)
        assert h.mean() == pytest.approx(0.001)

    def test_mean_varying(self):
        h = _FundingHistory(maxlen=100)
        h.append(1.0, 0.002)
        h.append(2.0, 0.004)
        assert h.mean() == pytest.approx(0.003)

    def test_std(self):
        h = _FundingHistory(maxlen=100)
        for i in range(10):
            h.append(float(i), 0.001 * (i % 2))
        assert h.std() > 0

    def test_std_single_value(self):
        h = _FundingHistory(maxlen=100)
        h.append(1.0, 0.001)
        assert h.std() == 0.0

    def test_trend_positive(self):
        h = _FundingHistory(maxlen=100)
        for i in range(10):
            h.append(float(i), 0.001 * (i + 1))  # Increasing rates
        assert h.trend() > 0

    def test_trend_negative(self):
        h = _FundingHistory(maxlen=100)
        for i in range(10):
            h.append(float(i), 0.01 - 0.001 * i)  # Decreasing rates
        assert h.trend() < 0

    def test_trend_insufficient_data(self):
        h = _FundingHistory(maxlen=100)
        h.append(1.0, 0.001)
        assert h.trend() == 0.0

    def test_is_stable_all_same_sign(self):
        h = _FundingHistory(maxlen=100)
        for i in range(15):
            h.append(float(i), 0.001)  # All positive
        assert h.is_stable() is True

    def test_is_stable_sign_flip(self):
        h = _FundingHistory(maxlen=100)
        for i in range(15):
            h.append(float(i), 0.001 if i % 2 == 0 else -0.001)
        assert h.is_stable() is False

    def test_is_stable_insufficient(self):
        h = _FundingHistory(maxlen=100)
        for i in range(5):
            h.append(float(i), 0.001)
        assert h.is_stable() is False


# ---------------------------------------------------------------------------
# _calc_conviction
# ---------------------------------------------------------------------------

class TestCalcConviction:
    def test_high_yield_stable(self):
        h = _FundingHistory(maxlen=100)
        for i in range(60):
            h.append(float(i), 0.002)  # Stable positive
        # Annualised yield: 0.002 * 3 * 365 = 2.19 → base = min(2.19*2, 0.5) = 0.5
        conviction = FundingRateArb._calc_conviction(2.19, h, is_cross_exchange=True)
        assert conviction > 0.5

    def test_low_yield(self):
        h = _FundingHistory(maxlen=100)
        for i in range(20):
            h.append(float(i), 0.0001)
        # Annualised: 0.0001 * 1095 = 0.1095 → base = min(0.1095*2, 0.5) = 0.219
        # + stability_bonus 0.2 + data_bonus 0.2 = 0.619
        conviction = FundingRateArb._calc_conviction(0.1095, h, is_cross_exchange=False)
        assert conviction < 0.8  # Lower than high-yield scenario

    def test_cross_exchange_bonus(self):
        h = _FundingHistory(maxlen=100)
        for i in range(20):
            h.append(float(i), 0.001)
        yield_val = 0.5
        conv_cross = FundingRateArb._calc_conviction(yield_val, h, is_cross_exchange=True)
        conv_single = FundingRateArb._calc_conviction(yield_val, h, is_cross_exchange=False)
        assert conv_cross > conv_single

    def test_trend_penalty(self):
        h = _FundingHistory(maxlen=100)
        # Positive rates converging toward zero
        for i in range(10):
            h.append(float(i), 0.005 - 0.0004 * i)
        conviction = FundingRateArb._calc_conviction(0.5, h, is_cross_exchange=False)
        # Should have some trend penalty applied
        assert 0.0 <= conviction <= 1.0

    def test_clamped_0_1(self):
        h = _FundingHistory(maxlen=100)
        for i in range(60):
            h.append(float(i), 0.01)  # Very high, stable
        conviction = FundingRateArb._calc_conviction(100.0, h, is_cross_exchange=True)
        assert conviction <= 1.0


# ---------------------------------------------------------------------------
# _calc_breakeven
# ---------------------------------------------------------------------------

class TestCalcBreakeven:
    def test_positive_diff(self):
        # round_trip_cost = 10 bps = 0.001
        # rate_diff = 0.0005 → periods = 0.001/0.0005 = 2 → hours = 16
        hours = FundingRateArb._calc_breakeven(0.0005)
        assert hours == pytest.approx(16.0)

    def test_zero_diff(self):
        hours = FundingRateArb._calc_breakeven(0.0)
        assert hours == float("inf")
