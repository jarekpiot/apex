"""Tests for HedgingEngine — fallback hedge selection and vol proxy."""

import pytest
import numpy as np

from agents.risk.hedging import HedgingEngine
from tests.conftest import MockMessageBus


def _make_engine() -> HedgingEngine:
    return HedgingEngine(bus=MockMessageBus())


class TestFallbackHedge:
    def test_long_heavy_returns_gold(self):
        he = _make_engine()
        he._mid_prices = {"GOLD": 2000.0, "SPY": 500.0, "BTC": 60000.0}
        result = he._fallback_hedge(is_long_heavy=True)
        assert result == "GOLD"

    def test_short_heavy_returns_btc(self):
        he = _make_engine()
        he._mid_prices = {"BTC": 60000.0, "ETH": 3000.0, "GOLD": 2000.0}
        result = he._fallback_hedge(is_long_heavy=False)
        assert result == "BTC"

    def test_no_candidates_returns_none(self):
        he = _make_engine()
        he._mid_prices = {}
        result = he._fallback_hedge(is_long_heavy=True)
        assert result is None

    def test_long_heavy_no_gold_uses_diversifier(self):
        he = _make_engine()
        he._mid_prices = {"SILVER": 25.0}
        result = he._fallback_hedge(is_long_heavy=True)
        # Should try diversifiers: GOLD, SILVER, SPY, EUR
        assert result == "SILVER"


class TestComputeVolProxy:
    def test_sufficient_data(self):
        he = _make_engine()
        # Generate 30 hourly prices for BTC and ETH
        rng = np.random.RandomState(42)
        btc_prices = [50000.0]
        eth_prices = [3000.0]
        for _ in range(29):
            btc_prices.append(btc_prices[-1] * (1 + rng.randn() * 0.02))
            eth_prices.append(eth_prices[-1] * (1 + rng.randn() * 0.02))

        from collections import deque
        he._hourly_prices["BTC"] = deque(btc_prices, maxlen=168)
        he._hourly_prices["ETH"] = deque(eth_prices, maxlen=168)

        vol = he._compute_vol_proxy()
        assert vol is not None
        assert vol > 0

    def test_insufficient_data(self):
        he = _make_engine()
        from collections import deque
        he._hourly_prices["BTC"] = deque([50000.0, 50100.0], maxlen=168)
        he._hourly_prices["ETH"] = deque([3000.0, 3010.0], maxlen=168)

        vol = he._compute_vol_proxy()
        assert vol is None
