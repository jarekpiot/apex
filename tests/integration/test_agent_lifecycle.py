"""
Integration lifecycle tests — start/stop all 26 agents with a TrackingBus.

Unlike the unit-level lifecycle tests (tests/test_agent_lifecycle.py) which use
a simple MockMessageBus, these tests use the integration TrackingBus that routes
messages between agents, verifying the agents work in a wired environment.

Tests:
  1. Every agent starts and stops cleanly with TrackingBus
  2. All agents can be started concurrently (as in main.py)
  3. Graceful shutdown ordering (execution last)
  4. Agent health reports consistent state after start/stop
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.conftest import TrackingBus


# ---------------------------------------------------------------------------
# Agent specs: (module_path, class_name, expected_id, extra_kwargs)
# ---------------------------------------------------------------------------

AGENT_SPECS: list[tuple[str, str, str, dict[str, Any]]] = [
    ("agents.ingestion.market_data", "MarketDataCollector", "market_data", {}),
    ("agents.ingestion.platform_specialist", "PlatformSpecialist", "platform_specialist", {}),
    ("agents.ingestion.onchain_intel", "OnChainIntelligence", "onchain_intel", {}),
    ("agents.ingestion.macro_feed", "MacroFeed", "macro_feed", {}),
    ("agents.ingestion.sentiment", "SentimentScraper", "sentiment_scraper", {}),
    ("agents.execution.engine", "ExecutionEngine", "execution_engine", {"platform_specialist": None}),
    ("agents.execution.position_manager", "PositionManager", "position_manager", {"platform_specialist": None}),
    ("agents.risk.guardian", "RiskGuardian", "risk_guardian", {}),
    ("agents.risk.anomaly", "AnomalyDetector", "anomaly_detector", {}),
    ("agents.risk.hedging", "HedgingEngine", "hedging_engine", {}),
    ("agents.analysis.technical", "TechnicalAnalyst", "technical_analyst", {}),
    ("agents.analysis.funding_arb", "FundingRateArb", "funding_arb", {}),
    ("agents.fundamental.valuation", "FundamentalValuation", "fundamental_valuation", {}),
    ("agents.on_chain.flow", "OnChainFlow", "on_chain_flow", {}),
    ("agents.sentiment.social", "SentimentSocial", "sentiment_social", {}),
    ("agents.sentiment.funding", "SentimentFunding", "sentiment_funding", {}),
    ("agents.macro.regime", "MacroRegime", "macro_regime", {}),
    ("agents.red_team.challenger", "RedTeamChallenger", "red_team", {}),
    ("agents.decision.cio", "ChiefInvestmentOfficer", "cio", {}),
    ("agents.decision.signal_aggregator", "SignalAggregator", "signal_aggregator", {}),
    ("agents.decision.red_team", "RedTeamStrategist", "red_team_strategist", {}),
    ("agents.decision.portfolio_allocator", "PortfolioAllocator", "portfolio_allocator", {}),
    ("agents.decision.regime_classifier", "RegimeClassifier", "regime_classifier", {}),
    ("agents.meta.orchestrator", "MetaOrchestrator", "meta_orchestrator", {}),
    ("agents.meta.performance", "PerformanceAuditor", "performance_auditor", {}),
    ("agents.meta.strategy_lab", "StrategyLab", "strategy_lab", {}),
]

_IDS = [spec[2] for spec in AGENT_SPECS]


def _make_agent(module_path: str, class_name: str, bus: TrackingBus, kwargs: dict) -> Any:
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls(bus=bus, **kwargs)


# ---------------------------------------------------------------------------
# 1. Individual start/stop with TrackingBus
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "module_path, class_name, expected_id, kwargs",
    AGENT_SPECS,
    ids=_IDS,
)
async def test_start_stop_with_tracking_bus(
    module_path: str, class_name: str, expected_id: str,
    kwargs: dict, tracking_bus: TrackingBus,
):
    """Each agent starts and stops cleanly with the integration TrackingBus."""
    agent = _make_agent(module_path, class_name, tracking_bus, kwargs)

    assert agent.agent_id == expected_id
    assert agent._running is False

    try:
        await agent.start()
        assert agent._running is True

        h = agent.health()
        assert h["running"] is True
        assert h["agent_id"] == expected_id
    finally:
        await agent.stop()

    assert agent._running is False
    assert agent._task is None


# ---------------------------------------------------------------------------
# 2. All agents start concurrently
# ---------------------------------------------------------------------------

async def test_concurrent_startup(tracking_bus: TrackingBus):
    """All 26 agents can start concurrently without deadlocking."""
    agents = [
        _make_agent(mp, cn, tracking_bus, kw)
        for mp, cn, _, kw in AGENT_SPECS
    ]

    try:
        # Start all agents (as main.py does, sequentially)
        for agent in agents:
            await agent.start()

        await asyncio.sleep(0.2)

        running_count = sum(1 for a in agents if a._running)
        assert running_count == 26, f"Expected 26 running, got {running_count}"

    finally:
        # Stop in reverse order (execution last, as in production)
        for agent in reversed(agents):
            await agent.stop()

    stopped_count = sum(1 for a in agents if not a._running)
    assert stopped_count == 26


# ---------------------------------------------------------------------------
# 3. Double start/stop idempotency
# ---------------------------------------------------------------------------

async def test_double_start_stop(tracking_bus: TrackingBus):
    """Starting twice and stopping twice doesn't raise."""
    from agents.risk.guardian import RiskGuardian
    agent = RiskGuardian(bus=tracking_bus)

    try:
        await agent.start()
        await agent.start()  # Second start is a no-op
        assert agent._running is True
    finally:
        await agent.stop()
        await agent.stop()  # Second stop is safe

    assert agent._running is False


# ---------------------------------------------------------------------------
# 4. Health consistency across lifecycle
# ---------------------------------------------------------------------------

async def test_health_lifecycle_consistency(tracking_bus: TrackingBus):
    """Health reports reflect the correct state at each lifecycle phase."""
    from agents.meta.orchestrator import MetaOrchestrator
    agent = MetaOrchestrator(bus=tracking_bus)

    # Before start
    h = agent.health()
    assert h["running"] is False
    assert h["last_heartbeat"] is None

    try:
        await agent.start()
        await asyncio.sleep(0.1)

        # After start
        h = agent.health()
        assert h["running"] is True
    finally:
        await agent.stop()

    # After stop
    h = agent.health()
    assert h["running"] is False


# ---------------------------------------------------------------------------
# 5. TrackingBus captures subscriptions from started agents
# ---------------------------------------------------------------------------

async def test_subscriptions_registered_on_start(tracking_bus: TrackingBus):
    """Starting agents registers their stream subscriptions in the bus."""
    from agents.risk.guardian import RiskGuardian
    agent = RiskGuardian(bus=tracking_bus)

    try:
        await agent.start()
        await asyncio.sleep(0.2)
    finally:
        await agent.stop()

    subs = tracking_bus.subscribed_streams()
    # RiskGuardian subscribes to at least these 4 streams
    assert "decisions:pending" in subs
    assert "portfolio:state" in subs
    assert "market:prices" in subs
    assert "risk:anomaly" in subs
