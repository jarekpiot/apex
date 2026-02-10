"""
Parametrized lifecycle tests for all 16 APEX agents.

Validates that every agent:
  1. Instantiates correctly with the expected agent_id, agent_type, and bus.
  2. Returns a well-formed health dict with running=False before start.
  3. Transitions _running to True after start().
  4. Transitions _running to False and _task to None after stop().
  5. Tolerates being started twice without raising.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agents.ingestion.market_data import MarketDataCollector
from agents.ingestion.platform_specialist import PlatformSpecialist
from agents.execution.engine import ExecutionEngine
from agents.execution.position_manager import PositionManager
from agents.risk.guardian import RiskGuardian
from agents.risk.anomaly import AnomalyDetector
from agents.risk.hedging import HedgingEngine
from agents.analysis.technical import TechnicalAnalyst
from agents.analysis.funding_arb import FundingRateArb
from agents.fundamental.valuation import FundamentalValuation
from agents.on_chain.flow import OnChainFlow
from agents.sentiment.social import SentimentSocial
from agents.sentiment.funding import SentimentFunding
from agents.macro.regime import MacroRegime
from agents.red_team.challenger import RedTeamChallenger
from agents.meta.orchestrator import MetaOrchestrator


# ---------------------------------------------------------------------------
# Agent descriptors: (class, expected_agent_id, expected_agent_type, extra_kwargs)
# ---------------------------------------------------------------------------

AGENT_SPECS: list[tuple[type, str, str, dict[str, Any]]] = [
    (MarketDataCollector,  "market_data",           "ingestion",   {}),
    (PlatformSpecialist,   "platform_specialist",   "ingestion",   {}),
    (ExecutionEngine,      "execution_engine",       "execution",   {"platform_specialist": None}),
    (PositionManager,      "position_manager",       "execution",   {"platform_specialist": None}),
    (RiskGuardian,         "risk_guardian",           "risk",        {}),
    (AnomalyDetector,      "anomaly_detector",       "risk",        {}),
    (HedgingEngine,        "hedging_engine",         "risk",        {}),
    (TechnicalAnalyst,     "technical_analyst",       "technical",   {}),
    (FundingRateArb,       "funding_arb",             "technical",   {}),
    (FundamentalValuation, "fundamental_valuation",   "fundamental", {}),
    (OnChainFlow,          "on_chain_flow",           "on_chain",    {}),
    (SentimentSocial,      "sentiment_social",        "sentiment",   {}),
    (SentimentFunding,     "sentiment_funding",       "sentiment",   {}),
    (MacroRegime,          "macro_regime",            "macro",       {}),
    (RedTeamChallenger,    "red_team",                "red_team",    {}),
    (MetaOrchestrator,     "meta_orchestrator",       "meta",        {}),
]

# Use the class name as the test-id for readable pytest output.
_IDS = [spec[0].__name__ for spec in AGENT_SPECS]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_agent(cls: type, bus: Any, kwargs: dict[str, Any]) -> Any:
    """Instantiate an agent class with the mock bus and any extra kwargs."""
    return cls(bus=bus, **kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cls, expected_id, expected_type, kwargs", AGENT_SPECS, ids=_IDS)
def test_instantiation(cls, expected_id, expected_type, kwargs, mock_bus):
    """Constructor succeeds and sets agent_id, agent_type, and bus correctly."""
    agent = _make_agent(cls, mock_bus, kwargs)

    assert agent.agent_id == expected_id
    assert agent.agent_type == expected_type
    assert agent.bus is mock_bus


@pytest.mark.parametrize("cls, expected_id, expected_type, kwargs", AGENT_SPECS, ids=_IDS)
def test_health_default(cls, expected_id, expected_type, kwargs, mock_bus):
    """health() returns a dict with the expected keys and running=False before start."""
    agent = _make_agent(cls, mock_bus, kwargs)
    h = agent.health()

    assert isinstance(h, dict)
    assert h["agent_id"] == expected_id
    assert h["agent_type"] == expected_type
    assert h["running"] is False
    assert "last_heartbeat" in h
    assert h["last_heartbeat"] is None


@pytest.mark.parametrize("cls, expected_id, expected_type, kwargs", AGENT_SPECS, ids=_IDS)
async def test_start_sets_running(cls, expected_id, expected_type, kwargs, mock_bus):
    """After start(), _running is True and _task is not None."""
    agent = _make_agent(cls, mock_bus, kwargs)
    try:
        await agent.start()

        assert agent._running is True
        assert agent._task is not None
    finally:
        await agent.stop()


@pytest.mark.parametrize("cls, expected_id, expected_type, kwargs", AGENT_SPECS, ids=_IDS)
async def test_stop_clears_running(cls, expected_id, expected_type, kwargs, mock_bus):
    """After start() + stop(), _running is False and _task is None."""
    agent = _make_agent(cls, mock_bus, kwargs)
    try:
        await agent.start()
    finally:
        await agent.stop()

    assert agent._running is False
    assert agent._task is None


@pytest.mark.parametrize("cls, expected_id, expected_type, kwargs", AGENT_SPECS, ids=_IDS)
async def test_double_start_idempotent(cls, expected_id, expected_type, kwargs, mock_bus):
    """Calling start() twice does not raise and the agent remains running."""
    agent = _make_agent(cls, mock_bus, kwargs)
    try:
        await agent.start()
        # Second start should be a no-op (BaseAgent guards with if self._running).
        await agent.start()

        assert agent._running is True
    finally:
        await agent.stop()
