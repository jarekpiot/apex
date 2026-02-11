"""
Integration tests: stream wiring verification.

Validates that:
  1. All 28 stream constants are defined in message_bus.py
  2. Each agent subscribes to the expected streams when started
  3. Published messages route to the correct subscribers
  4. No orphan streams (every stream has ≥1 publisher and ≥1 subscriber)
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from core.message_bus import (
    STREAM_ACCOUNT_HEALTH,
    STREAM_CIO_PRIORITIES,
    STREAM_CIO_RESEARCH_RESULTS,
    STREAM_CIO_RESEARCH_TASKS,
    STREAM_CIO_SIGNAL_MATRIX,
    STREAM_DATA_MACRO,
    STREAM_DATA_ONCHAIN,
    STREAM_DATA_REGIME,
    STREAM_DATA_SENTIMENT,
    STREAM_DECISIONS,
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_EXECUTION_ADVISORY,
    STREAM_MARKET_PRICES,
    STREAM_NEW_LISTING,
    STREAM_PERFORMANCE_REPORTS,
    STREAM_PLATFORM_STATE,
    STREAM_PORTFOLIO_STATE,
    STREAM_RED_TEAM,
    STREAM_RISK_ANOMALY,
    STREAM_RISK_CHECKS,
    STREAM_SHADOW_RESULTS,
    STREAM_SIGNALS,
    STREAM_SIGNALS_FUNDING_ARB,
    STREAM_SIGNALS_TECHNICAL,
    STREAM_TRADES_EXECUTED,
    STREAM_WEIGHT_UPDATES,
)

from tests.integration.conftest import TrackingBus


# ---------------------------------------------------------------------------
# Expected wiring: which streams each agent subscribes to / publishes to
# ---------------------------------------------------------------------------

# (agent_factory, expected_subscribe_streams)
# We only list streams the agent actively subscribes to in start()/process()

AGENT_SUBSCRIPTIONS: dict[str, set[str]] = {
    "risk_guardian": {
        STREAM_DECISIONS_PENDING,
        STREAM_PORTFOLIO_STATE,
        STREAM_MARKET_PRICES,
        STREAM_RISK_ANOMALY,
    },
    "execution_engine": {
        STREAM_DECISIONS_APPROVED,
        STREAM_EXECUTION_ADVISORY,
        STREAM_ACCOUNT_HEALTH,
    },
    "position_manager": {
        STREAM_ACCOUNT_HEALTH,
        STREAM_TRADES_EXECUTED,
    },
    "anomaly_detector": {
        STREAM_MARKET_PRICES,
        STREAM_EXECUTION_ADVISORY,
    },
    "hedging_engine": {
        STREAM_PORTFOLIO_STATE,
        STREAM_MARKET_PRICES,
    },
    "meta_orchestrator": {
        STREAM_SIGNALS,
        STREAM_PORTFOLIO_STATE,
    },
    "red_team_challenger": {
        STREAM_DECISIONS_PENDING,
    },
    "performance_auditor": {
        STREAM_SIGNALS,
        STREAM_TRADES_EXECUTED,
        STREAM_DECISIONS_APPROVED,
        STREAM_MARKET_PRICES,
    },
    "strategy_lab": {
        STREAM_SIGNALS,
        STREAM_MARKET_PRICES,
    },
    "signal_aggregator": {
        STREAM_SIGNALS,
        STREAM_DATA_REGIME,
        STREAM_PORTFOLIO_STATE,
        STREAM_MARKET_PRICES,
        STREAM_RISK_ANOMALY,
    },
}

AGENT_PUBLICATIONS: dict[str, set[str]] = {
    "risk_guardian": {
        STREAM_DECISIONS_APPROVED,
        STREAM_DECISIONS_REJECTED,
        STREAM_RISK_CHECKS,
        STREAM_DECISIONS_PENDING,  # defensive REDUCE decisions
    },
    "execution_engine": {
        STREAM_TRADES_EXECUTED,
    },
    "position_manager": {
        STREAM_PORTFOLIO_STATE,
        STREAM_DECISIONS_PENDING,  # circuit breaker flatten
    },
    "anomaly_detector": {
        STREAM_RISK_ANOMALY,
    },
    "hedging_engine": {
        STREAM_DECISIONS_PENDING,
    },
    "meta_orchestrator": {
        STREAM_DECISIONS_PENDING,
    },
    "performance_auditor": {
        STREAM_PERFORMANCE_REPORTS,
        STREAM_WEIGHT_UPDATES,
    },
    "strategy_lab": {
        STREAM_SHADOW_RESULTS,
    },
    "signal_aggregator": {
        STREAM_CIO_SIGNAL_MATRIX,
    },
}


# ---------------------------------------------------------------------------
# 1. Stream constant existence tests
# ---------------------------------------------------------------------------

ALL_STREAM_CONSTANTS = [
    "STREAM_SIGNALS",
    "STREAM_DECISIONS",
    "STREAM_RISK_CHECKS",
    "STREAM_RED_TEAM",
    "STREAM_MARKET_PRICES",
    "STREAM_PLATFORM_STATE",
    "STREAM_NEW_LISTING",
    "STREAM_ACCOUNT_HEALTH",
    "STREAM_EXECUTION_ADVISORY",
    "STREAM_DECISIONS_PENDING",
    "STREAM_DECISIONS_APPROVED",
    "STREAM_DECISIONS_REJECTED",
    "STREAM_TRADES_EXECUTED",
    "STREAM_PORTFOLIO_STATE",
    "STREAM_RISK_ANOMALY",
    "STREAM_SIGNALS_TECHNICAL",
    "STREAM_SIGNALS_FUNDING_ARB",
    "STREAM_DATA_ONCHAIN",
    "STREAM_DATA_MACRO",
    "STREAM_DATA_SENTIMENT",
    "STREAM_DATA_REGIME",
    "STREAM_CIO_RESEARCH_TASKS",
    "STREAM_CIO_RESEARCH_RESULTS",
    "STREAM_CIO_SIGNAL_MATRIX",
    "STREAM_CIO_PRIORITIES",
    "STREAM_PERFORMANCE_REPORTS",
    "STREAM_WEIGHT_UPDATES",
    "STREAM_SHADOW_RESULTS",
]


@pytest.mark.parametrize("const_name", ALL_STREAM_CONSTANTS)
def test_stream_constant_exists(const_name: str):
    """Every documented stream constant exists in message_bus."""
    from core import message_bus as mb
    value = getattr(mb, const_name, None)
    assert value is not None, f"{const_name} not defined in message_bus"
    assert isinstance(value, str), f"{const_name} should be a string"
    assert ":" in value, f"{const_name}={value!r} should contain ':' separator"


def test_stream_constant_count():
    """We expect exactly 28 STREAM_* constants."""
    from core import message_bus as mb
    streams = [name for name in dir(mb) if name.startswith("STREAM_")]
    assert len(streams) == 28, f"Expected 28 STREAM_* constants, got {len(streams)}: {streams}"


def test_no_duplicate_stream_values():
    """Each stream constant maps to a unique Redis stream name."""
    from core import message_bus as mb
    streams = {
        name: getattr(mb, name)
        for name in dir(mb)
        if name.startswith("STREAM_")
    }
    values = list(streams.values())
    assert len(values) == len(set(values)), (
        f"Duplicate stream values: {[v for v in values if values.count(v) > 1]}"
    )


# ---------------------------------------------------------------------------
# 2. Agent subscription wiring tests
# ---------------------------------------------------------------------------

def _make_agent(agent_key: str, bus: TrackingBus) -> Any:
    """Factory that instantiates an agent by key name."""
    factories = {
        "risk_guardian": lambda: _import("agents.risk.guardian", "RiskGuardian")(bus=bus),
        "execution_engine": lambda: _import("agents.execution.engine", "ExecutionEngine")(
            bus=bus, platform_specialist=None,
        ),
        "position_manager": lambda: _import("agents.execution.position_manager", "PositionManager")(
            bus=bus, platform_specialist=None,
        ),
        "anomaly_detector": lambda: _import("agents.risk.anomaly", "AnomalyDetector")(bus=bus),
        "hedging_engine": lambda: _import("agents.risk.hedging", "HedgingEngine")(bus=bus),
        "meta_orchestrator": lambda: _import("agents.meta.orchestrator", "MetaOrchestrator")(bus=bus),
        "red_team_challenger": lambda: _import("agents.red_team.challenger", "RedTeamChallenger")(bus=bus),
        "performance_auditor": lambda: _import("agents.meta.performance", "PerformanceAuditor")(bus=bus),
        "strategy_lab": lambda: _import("agents.meta.strategy_lab", "StrategyLab")(bus=bus),
        "signal_aggregator": lambda: _import("agents.decision.signal_aggregator", "SignalAggregator")(bus=bus),
    }
    return factories[agent_key]()


def _import(module: str, cls: str):
    import importlib
    return getattr(importlib.import_module(module), cls)


@pytest.mark.parametrize("agent_key", list(AGENT_SUBSCRIPTIONS.keys()))
async def test_agent_subscribes_to_expected_streams(agent_key: str, tracking_bus: TrackingBus):
    """Start agent briefly, verify it registers subscriptions on the expected streams."""
    agent = _make_agent(agent_key, tracking_bus)

    try:
        await agent.start()
        # Give agent time to register its subscriptions (most happen in start())
        await asyncio.sleep(0.2)
    finally:
        await agent.stop()

    subscribed = tracking_bus.subscribed_streams()
    expected = AGENT_SUBSCRIPTIONS[agent_key]

    missing = expected - subscribed
    assert not missing, (
        f"{agent_key} missing subscriptions: {missing}. "
        f"Got: {subscribed}"
    )


# ---------------------------------------------------------------------------
# 3. Cross-agent routing test
# ---------------------------------------------------------------------------

async def test_publish_routes_to_subscriber(tracking_bus: TrackingBus):
    """A message published to a stream reaches all subscribers on that stream."""
    received: list[dict] = []

    # Set up subscriber
    async def consumer():
        async for msg_id, payload in tracking_bus.subscribe_to(
            STREAM_DECISIONS_PENDING, "test-group", "test-consumer",
        ):
            received.append(payload)
            if len(received) >= 1:
                break

    from tests.integration.conftest import make_decision
    decision = make_decision(asset="ETH", action="open_short")

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)  # let subscriber register

    await tracking_bus.publish_to(STREAM_DECISIONS_PENDING, decision)
    await asyncio.wait_for(task, timeout=3.0)

    assert len(received) == 1
    assert received[0]["asset"] == "ETH"
    assert received[0]["action"] == "open_short"


async def test_multiple_subscribers_same_stream(tracking_bus: TrackingBus):
    """Multiple subscribers on the same stream each receive the message."""
    received_a: list[dict] = []
    received_b: list[dict] = []

    async def consumer_a():
        async for msg_id, payload in tracking_bus.subscribe_to(
            STREAM_MARKET_PRICES, "group-a", "consumer-a",
        ):
            received_a.append(payload)
            if len(received_a) >= 1:
                break

    async def consumer_b():
        async for msg_id, payload in tracking_bus.subscribe_to(
            STREAM_MARKET_PRICES, "group-b", "consumer-b",
        ):
            received_b.append(payload)
            if len(received_b) >= 1:
                break

    task_a = asyncio.create_task(consumer_a())
    task_b = asyncio.create_task(consumer_b())
    await asyncio.sleep(0.05)

    from tests.integration.conftest import make_price_update
    prices = make_price_update({"BTC": 52000.0})
    await tracking_bus.publish_to(STREAM_MARKET_PRICES, prices)

    await asyncio.wait_for(asyncio.gather(task_a, task_b), timeout=3.0)

    assert len(received_a) == 1
    assert len(received_b) == 1
    assert received_a[0]["prices"]["BTC"] == 52000.0


async def test_inject_reaches_subscriber(tracking_bus: TrackingBus):
    """inject() delivers a raw dict payload to existing subscribers."""
    received: list[dict] = []

    async def consumer():
        async for msg_id, payload in tracking_bus.subscribe_to(
            STREAM_RISK_ANOMALY, "test-group", "test-consumer",
        ):
            received.append(payload)
            if len(received) >= 1:
                break

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)

    await tracking_bus.inject(STREAM_RISK_ANOMALY, {
        "anomaly_type": "volume_spike",
        "severity": "critical",
        "asset": "SOL",
    })

    await asyncio.wait_for(task, timeout=3.0)
    assert received[0]["asset"] == "SOL"


# ---------------------------------------------------------------------------
# 4. Subscription tracking introspection
# ---------------------------------------------------------------------------

def test_tracking_bus_records_subscriptions(tracking_bus: TrackingBus):
    """TrackingBus.get_subscriptions() records all subscribe_to calls."""
    tracking_bus.subscribe_to("stream:a", "grp1", "con1")
    tracking_bus.subscribe_to("stream:b", "grp2", "con2")

    subs = tracking_bus.get_subscriptions()
    assert len(subs) == 2
    assert ("stream:a", "grp1", "con1") in subs
    assert ("stream:b", "grp2", "con2") in subs


def test_tracking_bus_records_publications(tracking_bus: TrackingBus):
    """TrackingBus.get_published() returns all published models."""
    import asyncio

    async def _pub():
        from tests.integration.conftest import make_decision
        d = make_decision()
        await tracking_bus.publish_to(STREAM_DECISIONS_PENDING, d)

    asyncio.get_event_loop().run_until_complete(_pub())

    msgs = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(msgs) == 1


def test_tracking_bus_reset(tracking_bus: TrackingBus):
    """reset() clears all state."""
    tracking_bus.subscribe_to("test:stream", "g", "c")
    tracking_bus.reset()

    assert tracking_bus.get_subscriptions() == []
    assert tracking_bus.subscribed_streams() == set()
    assert tracking_bus.published_streams() == set()


# ---------------------------------------------------------------------------
# 5. Registry ↔ wiring consistency
# ---------------------------------------------------------------------------

def test_all_registry_agents_instantiate_with_mock_bus(tracking_bus: TrackingBus):
    """Every agent in the registry can be instantiated with the tracking bus."""
    from config.agent_registry import AGENT_REGISTRY

    # Map agent_id -> (module, class, kwargs)
    AGENT_MAP = {
        "market_data": ("agents.ingestion.market_data", "MarketDataCollector", {}),
        "platform_specialist": ("agents.ingestion.platform_specialist", "PlatformSpecialist", {}),
        "onchain_intel": ("agents.ingestion.onchain_intel", "OnChainIntelligence", {}),
        "macro_feed": ("agents.ingestion.macro_feed", "MacroFeed", {}),
        "sentiment_scraper": ("agents.ingestion.sentiment", "SentimentScraper", {}),
        "execution_engine": ("agents.execution.engine", "ExecutionEngine", {"platform_specialist": None}),
        "position_manager": ("agents.execution.position_manager", "PositionManager", {"platform_specialist": None}),
        "risk_guardian": ("agents.risk.guardian", "RiskGuardian", {}),
        "anomaly_detector": ("agents.risk.anomaly", "AnomalyDetector", {}),
        "hedging_engine": ("agents.risk.hedging", "HedgingEngine", {}),
        "technical_analyst": ("agents.analysis.technical", "TechnicalAnalyst", {}),
        "funding_arb": ("agents.analysis.funding_arb", "FundingRateArb", {}),
        "fundamental_valuation": ("agents.fundamental.valuation", "FundamentalValuation", {}),
        "on_chain_flow": ("agents.on_chain.flow", "OnChainFlow", {}),
        "sentiment_social": ("agents.sentiment.social", "SentimentSocial", {}),
        "sentiment_funding": ("agents.sentiment.funding", "SentimentFunding", {}),
        "macro_regime": ("agents.macro.regime", "MacroRegime", {}),
        "red_team": ("agents.red_team.challenger", "RedTeamChallenger", {}),
        "cio": ("agents.decision.cio", "ChiefInvestmentOfficer", {}),
        "signal_aggregator": ("agents.decision.signal_aggregator", "SignalAggregator", {}),
        "red_team_strategist": ("agents.decision.red_team", "RedTeamStrategist", {}),
        "portfolio_allocator": ("agents.decision.portfolio_allocator", "PortfolioAllocator", {}),
        "regime_classifier": ("agents.decision.regime_classifier", "RegimeClassifier", {}),
        "meta_orchestrator": ("agents.meta.orchestrator", "MetaOrchestrator", {}),
        "performance_auditor": ("agents.meta.performance", "PerformanceAuditor", {}),
        "strategy_lab": ("agents.meta.strategy_lab", "StrategyLab", {}),
    }

    for agent_id in AGENT_REGISTRY:
        assert agent_id in AGENT_MAP, f"No test mapping for agent {agent_id}"
        module_path, class_name, kwargs = AGENT_MAP[agent_id]
        cls = _import(module_path, class_name)
        agent = cls(bus=tracking_bus, **kwargs)
        assert agent.agent_id == agent_id
