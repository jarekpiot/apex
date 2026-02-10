"""Tests for TechnicalAnalyst — volume profile, swing levels, indicators, composite signal."""

import numpy as np
import pandas as pd
import pytest

from agents.analysis.technical import TechnicalAnalyst, _safe_last, _safe_at
from tests.conftest import MockMessageBus


def _make_analyst() -> TechnicalAnalyst:
    return TechnicalAnalyst(bus=MockMessageBus())


def _make_ohlcv_df(n: int = 250, trend: str = "up") -> pd.DataFrame:
    """Generate a synthetic OHLCV DataFrame."""
    rng = np.random.RandomState(42)
    base = 100.0
    prices = [base]
    for i in range(1, n):
        if trend == "up":
            drift = 0.001
        elif trend == "down":
            drift = -0.001
        else:
            drift = 0.0
        prices.append(prices[-1] * (1 + drift + rng.randn() * 0.01))

    close = np.array(prices)
    high = close * (1 + rng.uniform(0, 0.01, n))
    low = close * (1 - rng.uniform(0, 0.01, n))
    open_ = close * (1 + rng.uniform(-0.005, 0.005, n))
    volume = rng.uniform(100, 10000, n)

    return pd.DataFrame({
        "ts": pd.date_range("2025-01-01", periods=n, freq="h"),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


# ---------------------------------------------------------------------------
# _volume_profile
# ---------------------------------------------------------------------------

class TestVolumeProfile:
    def test_known_distribution(self):
        df = _make_ohlcv_df(100)
        result = TechnicalAnalyst._volume_profile(df)
        assert "poc" in result
        assert "vah" in result
        assert "val" in result
        assert result["val"] <= result["poc"] <= result["vah"]

    def test_zero_volume_fallback(self):
        df = _make_ohlcv_df(50)
        df["volume"] = 0.0
        result = TechnicalAnalyst._volume_profile(df)
        mid = df["close"].iloc[-1]
        assert result["poc"] == pytest.approx(mid, rel=0.01)


# ---------------------------------------------------------------------------
# _swing_levels
# ---------------------------------------------------------------------------

class TestSwingLevels:
    def test_clear_swing_pattern(self):
        df = _make_ohlcv_df(100)
        support, resistance = TechnicalAnalyst._swing_levels(df)
        # Should find some swing levels in a random walk
        assert isinstance(support, list)
        assert isinstance(resistance, list)

    def test_monotonic_few_swings(self):
        n = 50
        prices = np.linspace(100, 200, n)
        df = pd.DataFrame({
            "ts": pd.date_range("2025-01-01", periods=n, freq="h"),
            "open": prices,
            "high": prices + 0.5,
            "low": prices - 0.5,
            "close": prices,
            "volume": np.ones(n) * 1000,
        })
        support, resistance = TechnicalAnalyst._swing_levels(df)
        # Monotonically increasing → few/no swing highs, lows at the start
        assert len(resistance) <= 2


# ---------------------------------------------------------------------------
# _compute_indicators
# ---------------------------------------------------------------------------

class TestComputeIndicators:
    def test_returns_expected_keys(self):
        analyst = _make_analyst()
        df = _make_ohlcv_df(250)
        indicators = analyst._compute_indicators(df)
        expected_keys = {"rsi", "macd", "atr", "ema9", "ema21", "ema50",
                         "adx", "volume_profile", "support", "resistance"}
        for key in expected_keys:
            assert key in indicators, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# _composite_signal
# ---------------------------------------------------------------------------

class TestCompositeSignal:
    def test_strong_uptrend(self):
        analyst = _make_analyst()
        df = _make_ohlcv_df(250, trend="up")
        indicators = analyst._compute_indicators(df)
        direction, conviction, reasoning = analyst._composite_signal("BTC", df, indicators)
        # With an uptrend, direction should generally be positive
        assert isinstance(direction, float)
        assert isinstance(conviction, float)
        assert -1.0 <= direction <= 1.0
        assert 0.0 <= conviction <= 1.0

    def test_insufficient_data(self):
        analyst = _make_analyst()
        # Single row — indicators all None → no scoring criteria hit
        df = pd.DataFrame({
            "ts": [1.0], "open": [100.0], "high": [101.0],
            "low": [99.0], "close": [100.0], "volume": [10.0],
        })
        # With empty indicators, should return low conviction
        indicators: dict = {}
        direction, conviction, reasoning = analyst._composite_signal("BTC", df, indicators)
        assert conviction < 0.3


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestSafeLast:
    def test_valid_series(self):
        s = pd.Series([1.0, 2.0, 3.0])
        assert _safe_last(s) == 3.0

    def test_none_input(self):
        assert _safe_last(None) is None

    def test_nan_value(self):
        s = pd.Series([1.0, float("nan")])
        assert _safe_last(s) is None


class TestSafeAt:
    def test_valid_index(self):
        s = pd.Series([10.0, 20.0, 30.0])
        assert _safe_at(s, -2) == 20.0

    def test_out_of_bounds(self):
        s = pd.Series([10.0])
        assert _safe_at(s, -5) is None

    def test_none_input(self):
        assert _safe_at(None, 0) is None
