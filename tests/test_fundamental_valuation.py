"""Tests for FundamentalValuation — _evaluate scoring."""

import pytest

from agents.fundamental.valuation import FundamentalValuation


class TestEvaluate:
    def test_low_nvt_bullish(self):
        # NVT < 15 → bullish
        direction, conviction, reason = FundamentalValuation._evaluate(
            "BTC", nvt=10.0, tvl_ratio=0, pct_24h=0, pct_7d=0,
        )
        assert direction > 0

    def test_high_nvt_bearish(self):
        # NVT > 100 → bearish
        direction, conviction, reason = FundamentalValuation._evaluate(
            "BTC", nvt=150.0, tvl_ratio=0, pct_24h=0, pct_7d=0,
        )
        assert direction < 0

    def test_low_tvl_ratio_bullish(self):
        # tvl_ratio < 3 → bullish
        direction, conviction, reason = FundamentalValuation._evaluate(
            "AAVE", nvt=50.0, tvl_ratio=2.0, pct_24h=0, pct_7d=0,
        )
        # NVT is neutral (50 is between 15 and 100), TVL adds +0.4
        assert direction > 0

    def test_high_tvl_ratio_bearish(self):
        # tvl_ratio > 20 → bearish
        direction, conviction, reason = FundamentalValuation._evaluate(
            "TOKEN", nvt=50.0, tvl_ratio=30.0, pct_24h=0, pct_7d=0,
        )
        assert direction < 0

    def test_strong_24h_gain(self):
        direction, conviction, reason = FundamentalValuation._evaluate(
            "BTC", nvt=50.0, tvl_ratio=0, pct_24h=10.0, pct_7d=0,
        )
        assert direction > 0

    def test_strong_24h_loss(self):
        direction, conviction, reason = FundamentalValuation._evaluate(
            "BTC", nvt=50.0, tvl_ratio=0, pct_24h=-10.0, pct_7d=0,
        )
        assert direction < 0

    def test_conviction_capped(self):
        # With very strong signals, conviction should not exceed 0.8
        _, conviction, _ = FundamentalValuation._evaluate(
            "BTC", nvt=5.0, tvl_ratio=1.0, pct_24h=20.0, pct_7d=50.0,
        )
        assert conviction <= 0.8
