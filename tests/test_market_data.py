"""Tests for MarketDataCollector — _CandleBuffer."""

import time
import pytest
from unittest.mock import patch

from agents.ingestion.market_data import _CandleBuffer


class TestCandleBuffer:
    def test_first_tick_sets_ohlc(self):
        buf = _CandleBuffer("BTC", "1m", 60)
        result = buf.update(50000.0)
        assert result is None  # No completed candle yet
        assert buf.open == 50000.0
        assert buf.high == 50000.0
        assert buf.low == 50000.0
        assert buf.close == 50000.0
        assert buf.tick_count == 1

    def test_tracks_high_low(self):
        buf = _CandleBuffer("BTC", "1m", 60)
        buf.update(50000.0)
        buf.update(51000.0)
        buf.update(49000.0)
        buf.update(50500.0)
        assert buf.high == 51000.0
        assert buf.low == 49000.0
        assert buf.close == 50500.0

    def test_accumulates_volume(self):
        buf = _CandleBuffer("BTC", "1m", 60)
        buf.update(50000.0, volume=100.0)
        buf.update(50100.0, volume=200.0)
        assert buf.volume == 300.0

    def test_period_roll_emits_row(self):
        # Use a very short timeframe (1 second) and advance time
        buf = _CandleBuffer("BTC", "1s", 1)
        buf.update(50000.0)

        # Force the period to be in the past
        buf.period_start = time.time() - 2

        result = buf.update(51000.0)
        assert result is not None
        assert result.asset == "BTC"
        assert result.open == 50000.0
        assert result.close == 50000.0

    def test_no_roll_returns_none(self):
        buf = _CandleBuffer("BTC", "1h", 3600)
        buf.update(50000.0)
        result = buf.update(50100.0)
        assert result is None

    def test_empty_buffer_no_roll(self):
        buf = _CandleBuffer("BTC", "1s", 1)
        buf.period_start = time.time() - 2
        # tick_count is 0, so even with period roll, no candle emitted
        result = buf.update(50000.0)
        assert result is None
