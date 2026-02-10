"""Tests for MacroRegime — _classify_regime."""

import pytest
from collections import deque

from agents.macro.regime import MacroRegime
from tests.conftest import MockMessageBus


def _make_agent() -> MacroRegime:
    return MacroRegime(bus=MockMessageBus())


class TestClassifyRegime:
    def test_risk_on(self):
        """Falling DXY + normal yield curve + low VIX → risk_on."""
        agent = _make_agent()
        # Falling DXY: recent < older by > 0.5%
        agent._dxy_history.extend([110.0, 109.5, 109.0])
        regime, score, reason = agent._classify_regime({
            "dxy": 109.0, "us10y": 4.5, "us2y": 3.5, "vix": 12.0,
        })
        assert regime == "risk_on"
        assert score > 0

    def test_risk_off(self):
        """Rising DXY + inverted yield curve + high VIX → risk_off."""
        agent = _make_agent()
        agent._dxy_history.extend([100.0, 101.0, 102.0])
        regime, score, reason = agent._classify_regime({
            "dxy": 102.0, "us10y": 3.5, "us2y": 4.0, "vix": 30.0,
        })
        assert regime == "risk_off"
        assert score < 0

    def test_neutral_mixed(self):
        """Mixed signals → neutral."""
        agent = _make_agent()
        agent._dxy_history.extend([100.0, 100.0, 100.0])
        regime, score, reason = agent._classify_regime({
            "dxy": 100.0, "us10y": 4.0, "us2y": 3.8, "vix": 18.0,
        })
        assert regime == "neutral"

    def test_insufficient_data(self):
        agent = _make_agent()
        regime, score, reason = agent._classify_regime({})
        assert regime == "neutral"
        assert score == 0.0

    def test_dxy_falling(self):
        agent = _make_agent()
        agent._dxy_history.extend([110.0, 109.0, 108.0])
        regime, score, reason = agent._classify_regime({"dxy": 108.0})
        assert score > 0

    def test_dxy_rising(self):
        agent = _make_agent()
        agent._dxy_history.extend([100.0, 101.0, 102.0])
        regime, score, reason = agent._classify_regime({"dxy": 102.0})
        assert score < 0

    def test_yield_curve_normal(self):
        agent = _make_agent()
        regime, score, reason = agent._classify_regime({
            "us10y": 5.0, "us2y": 4.0,
        })
        # Spread = 1.0 > 0.5 → bullish
        assert score > 0

    def test_yield_curve_inverted(self):
        agent = _make_agent()
        regime, score, reason = agent._classify_regime({
            "us10y": 3.5, "us2y": 4.0,
        })
        # Spread = -0.5 < -0.2 → bearish
        assert score < 0
