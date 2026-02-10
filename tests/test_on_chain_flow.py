"""Tests for OnChainFlow — stablecoin signal and TVL momentum."""

import pytest
from collections import deque
import time

from agents.on_chain.flow import OnChainFlow
from tests.conftest import MockMessageBus


def _make_agent() -> OnChainFlow:
    return OnChainFlow(bus=MockMessageBus())


class TestStablecoinSignal:
    def test_rising(self):
        agent = _make_agent()
        now = time.time()
        # 6 samples with rising mcap (>0.1% change)
        for i in range(6):
            agent._stablecoin_mcap_history.append((now - (5 - i) * 600, 100_000 + i * 200))
        direction, conv, reason = agent._stablecoin_signal()
        assert direction > 0
        assert conv > 0

    def test_falling(self):
        agent = _make_agent()
        now = time.time()
        for i in range(6):
            agent._stablecoin_mcap_history.append((now - (5 - i) * 600, 100_000 - i * 200))
        direction, conv, reason = agent._stablecoin_signal()
        assert direction < 0

    def test_flat(self):
        agent = _make_agent()
        now = time.time()
        for i in range(6):
            agent._stablecoin_mcap_history.append((now - (5 - i) * 600, 100_000))
        direction, conv, reason = agent._stablecoin_signal()
        assert conv == 0.0

    def test_insufficient_history(self):
        agent = _make_agent()
        now = time.time()
        agent._stablecoin_mcap_history.append((now, 100_000))
        direction, conv, reason = agent._stablecoin_signal()
        assert conv == 0.0


class TestTvlMomentum:
    def test_rising(self):
        agent = _make_agent()
        now = time.time()
        for i in range(6):
            agent._tvl_history["Ethereum"].append((now - (5 - i) * 600, 50e9 + i * 1e9))
        direction, conv, reason = agent._tvl_momentum("Ethereum")
        assert direction > 0
        assert conv > 0

    def test_falling(self):
        agent = _make_agent()
        now = time.time()
        for i in range(6):
            agent._tvl_history["Solana"].append((now - (5 - i) * 600, 10e9 - i * 0.5e9))
        direction, conv, reason = agent._tvl_momentum("Solana")
        assert direction < 0

    def test_flat(self):
        agent = _make_agent()
        now = time.time()
        for i in range(6):
            agent._tvl_history["Polygon"].append((now - (5 - i) * 600, 5e9))
        direction, conv, reason = agent._tvl_momentum("Polygon")
        assert conv == 0.0

    def test_insufficient(self):
        agent = _make_agent()
        direction, conv, reason = agent._tvl_momentum("Unknown")
        assert conv == 0.0
