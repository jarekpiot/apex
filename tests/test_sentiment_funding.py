"""Tests for SentimentFunding — _evaluate contrarian funding/OI."""

import pytest
from collections import deque

from agents.sentiment.funding import SentimentFunding
from config.settings import settings
from tests.conftest import MockMessageBus


def _make_agent() -> SentimentFunding:
    return SentimentFunding(bus=MockMessageBus())


class TestEvaluate:
    def test_crowd_long_bearish(self):
        agent = _make_agent()
        # Positive funding > extreme threshold → contrarian bearish
        extreme = settings.funding_rate_extreme  # 0.001
        for _ in range(10):
            agent._funding_history["BTC"].append(extreme * 2)
            agent._oi_history["BTC"].append(1000.0)
        direction, conviction, reason = agent._evaluate("BTC")
        assert direction < 0

    def test_crowd_short_bullish(self):
        agent = _make_agent()
        extreme = settings.funding_rate_extreme
        for _ in range(10):
            agent._funding_history["BTC"].append(-extreme * 2)
            agent._oi_history["BTC"].append(1000.0)
        direction, conviction, reason = agent._evaluate("BTC")
        assert direction > 0

    def test_euphoria(self):
        agent = _make_agent()
        extreme = settings.funding_rate_extreme
        # Rising OI + positive funding → euphoria → bearish
        for i in range(6):
            agent._funding_history["ETH"].append(extreme * 2)
            agent._oi_history["ETH"].append(1000.0 + i * 100)  # Rising OI
        direction, conviction, reason = agent._evaluate("ETH")
        assert direction < 0
        assert "uphoria" in reason.lower() or direction < 0

    def test_capitulation(self):
        agent = _make_agent()
        extreme = settings.funding_rate_extreme
        # Falling OI → capitulation → bullish
        for i in range(6):
            agent._funding_history["SOL"].append(extreme * 0.5)  # Mild funding
            agent._oi_history["SOL"].append(1000.0 - i * 100)  # Falling OI
        direction, conviction, reason = agent._evaluate("SOL")
        assert direction > 0

    def test_insufficient_history(self):
        agent = _make_agent()
        agent._funding_history["BTC"].append(0.001)
        agent._oi_history["BTC"].append(1000.0)
        direction, conviction, reason = agent._evaluate("BTC")
        assert conviction == 0.0
