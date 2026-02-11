"""
Integration test fixtures for APEX.

Provides TrackingBus (routes messages between agents), agent factories
with proper constructor wiring, and helpers for inject/drain patterns.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import pytest

from core.models import (
    AgentSignal,
    AnomalyAlert,
    AnomalyType,
    AlertSeverity,
    Direction,
    ExecutedTrade,
    ExecutionMode,
    HedgeSuggestion,
    PortfolioState,
    Position,
    PriceUpdate,
    RiskCheck,
    Timeframe,
    TradeAction,
    TradeDecision,
)


# ---------------------------------------------------------------------------
# TrackingBus — enhanced mock that routes messages between agents
# ---------------------------------------------------------------------------

class TrackingBus:
    """Mock message bus that routes published messages to subscribers.

    - publish_to() delivers payload to all subscriber queues on that stream
    - subscribe_to() returns an async iterator fed from a per-subscriber queue
    - Tracks all calls for assertion in tests
    """

    def __init__(self) -> None:
        self.published: dict[str, list[Any]] = {}
        self._subscriber_queues: dict[str, list[asyncio.Queue]] = {}
        self._subscription_log: list[tuple[str, str, str]] = []
        self._publish_log: list[tuple[str, Any]] = []

    # -- lifecycle -----------------------------------------------------------

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def health_check(self) -> bool:
        return True

    async def ensure_group(self, stream: str, group: str) -> None:
        pass

    # -- publishing ----------------------------------------------------------

    async def publish_to(self, stream: str, model: Any) -> str:
        self.published.setdefault(stream, []).append(model)
        self._publish_log.append((stream, model))

        if hasattr(model, "model_dump_json"):
            payload = json.loads(model.model_dump_json())
        elif isinstance(model, dict):
            payload = model
        else:
            payload = {"data": str(model)}

        for q in self._subscriber_queues.get(stream, []):
            await q.put(("msg-id", payload))

        return "msg-id"

    async def publish_signal(self, signal: Any) -> str:
        return await self.publish_to("apex:signals", signal)

    async def publish_decision(self, decision: Any) -> str:
        return await self.publish_to("apex:decisions", decision)

    async def publish_risk_check(self, check: Any) -> str:
        return await self.publish_to("apex:risk_checks", check)

    async def publish_red_team(self, challenge: Any) -> str:
        return await self.publish_to("apex:red_team", challenge)

    # -- subscribing ---------------------------------------------------------

    def subscribe_to(
        self, stream: str, group: str, consumer: str, **kw: Any,
    ):
        self._subscription_log.append((stream, group, consumer))
        q: asyncio.Queue = asyncio.Queue()
        self._subscriber_queues.setdefault(stream, []).append(q)
        return self._iter_queue(q)

    def subscribe_signals(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:signals", group, consumer, **kw)

    def subscribe_decisions(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:decisions", group, consumer, **kw)

    def subscribe_risk_checks(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:risk_checks", group, consumer, **kw)

    def subscribe_red_team(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:red_team", group, consumer, **kw)

    async def _iter_queue(self, q: asyncio.Queue):
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=30.0)
                yield item
            except asyncio.TimeoutError:
                continue

    # -- test helpers --------------------------------------------------------

    async def inject(self, stream: str, payload: dict) -> None:
        """Push a raw dict into all subscriber queues for *stream*."""
        for q in self._subscriber_queues.get(stream, []):
            await q.put(("injected-id", payload))

    def get_published(self, stream: str) -> list[Any]:
        return self.published.get(stream, [])

    def get_subscriptions(self) -> list[tuple[str, str, str]]:
        """Return list of (stream, group, consumer) tuples."""
        return list(self._subscription_log)

    def subscribed_streams(self) -> set[str]:
        """Return set of stream names that have been subscribed to."""
        return {s for s, _, _ in self._subscription_log}

    def published_streams(self) -> set[str]:
        """Return set of stream names that have been published to."""
        return set(self.published.keys())

    async def drain(self, stream: str, timeout: float = 2.0) -> list[Any]:
        """Wait up to *timeout* seconds, collecting all published messages on *stream*."""
        result = list(self.get_published(stream))
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
            current = self.get_published(stream)
            if len(current) > len(result):
                result = list(current)
        return result

    def reset(self) -> None:
        """Clear all tracked state."""
        self.published.clear()
        self._subscriber_queues.clear()
        self._subscription_log.clear()
        self._publish_log.clear()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tracking_bus() -> TrackingBus:
    return TrackingBus()


class _FakeSession:
    """No-op async DB session for integration tests."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def add(self, obj: Any) -> None:
        pass

    def add_all(self, objs: Any) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def execute(self, stmt: Any) -> Any:
        from unittest.mock import MagicMock
        return MagicMock(
            scalars=MagicMock(
                return_value=MagicMock(all=MagicMock(return_value=[]))
            )
        )


def _fake_session_factory():
    return _FakeSession()


@pytest.fixture(autouse=True)
def _patch_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("core.database.async_session_factory", _fake_session_factory)


# ---------------------------------------------------------------------------
# Agent factory helpers
# ---------------------------------------------------------------------------

def make_risk_guardian(bus: TrackingBus):
    from agents.risk.guardian import RiskGuardian
    return RiskGuardian(bus=bus)


def make_execution_engine(bus: TrackingBus, platform_specialist=None):
    from agents.execution.engine import ExecutionEngine
    return ExecutionEngine(bus=bus, platform_specialist=platform_specialist)


def make_position_manager(bus: TrackingBus, platform_specialist=None):
    from agents.execution.position_manager import PositionManager
    return PositionManager(bus=bus, platform_specialist=platform_specialist)


def make_anomaly_detector(bus: TrackingBus):
    from agents.risk.anomaly import AnomalyDetector
    return AnomalyDetector(bus=bus)


def make_hedging_engine(bus: TrackingBus):
    from agents.risk.hedging import HedgingEngine
    return HedgingEngine(bus=bus)


def make_meta_orchestrator(bus: TrackingBus):
    from agents.meta.orchestrator import MetaOrchestrator
    return MetaOrchestrator(bus=bus)


def make_cio(bus: TrackingBus, red_team=None, allocator=None):
    from agents.decision.cio import ChiefInvestmentOfficer
    return ChiefInvestmentOfficer(bus=bus, red_team=red_team, allocator=allocator)


def make_signal_aggregator(bus: TrackingBus):
    from agents.decision.signal_aggregator import SignalAggregator
    return SignalAggregator(bus=bus)


def make_red_team_strategist(bus: TrackingBus):
    from agents.decision.red_team import RedTeamStrategist
    return RedTeamStrategist(bus=bus)


def make_portfolio_allocator(bus: TrackingBus):
    from agents.decision.portfolio_allocator import PortfolioAllocator
    return PortfolioAllocator(bus=bus)


# ---------------------------------------------------------------------------
# Model factories (integration-specific — richer defaults than unit tests)
# ---------------------------------------------------------------------------

def make_decision(
    asset: str = "BTC",
    action: TradeAction = TradeAction.OPEN_LONG,
    size_pct: float = 0.01,
    consensus_score: float = 0.6,
    entry_price: float = 50000.0,
    **kw: Any,
) -> TradeDecision:
    return TradeDecision(
        asset=asset,
        action=action,
        size_pct=size_pct,
        consensus_score=consensus_score,
        entry_price=entry_price,
        **kw,
    )


def make_signal(
    asset: str = "BTC",
    direction: float = 0.7,
    conviction: float = 0.8,
    agent_id: str = "technical_analyst",
    timeframe: Timeframe = Timeframe.INTRADAY,
    **kw: Any,
) -> AgentSignal:
    return AgentSignal(
        agent_id=agent_id,
        asset=asset,
        direction=direction,
        conviction=conviction,
        timeframe=timeframe,
        **kw,
    )


def make_portfolio(
    positions: list[Position] | None = None,
    total_nav: float = 100_000.0,
    daily_pnl: float = 0.0,
    drawdown: float = 0.0,
    gross_exposure: float = 0.0,
    net_exposure: float = 0.0,
    **kw: Any,
) -> PortfolioState:
    return PortfolioState(
        positions=positions or [],
        total_nav=total_nav,
        daily_pnl=daily_pnl,
        drawdown=drawdown,
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        **kw,
    )


def make_position(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    size_pct: float = 0.02,
    entry_price: float = 50000.0,
    current_price: float = 51000.0,
) -> Position:
    pnl_sign = 1.0 if direction == Direction.LONG else -1.0
    return Position(
        asset=asset,
        direction=direction,
        size_pct=size_pct,
        entry_price=entry_price,
        current_price=current_price,
        unrealised_pnl=(current_price - entry_price) * pnl_sign * size_pct * 100_000,
        opened_at=datetime.now(timezone.utc),
    )


def make_price_update(prices: dict[str, float] | None = None) -> PriceUpdate:
    return PriceUpdate(prices=prices or {"BTC": 50000.0, "ETH": 3000.0})


def make_anomaly(
    anomaly_type: AnomalyType = AnomalyType.PRICE_DEVIATION,
    severity: AlertSeverity = AlertSeverity.CRITICAL,
    asset: str = "BTC",
    value: float = 5.0,
    threshold: float = 3.0,
    **kw: Any,
) -> AnomalyAlert:
    return AnomalyAlert(
        anomaly_type=anomaly_type,
        severity=severity,
        asset=asset,
        value=value,
        threshold=threshold,
        description=f"{anomaly_type} on {asset}: {value} > {threshold}",
        **kw,
    )


def make_executed_trade(
    asset: str = "BTC",
    decision_id: str = "test-decision-1",
    side: str = "buy",
    size: float = 0.1,
    price: float = 50000.0,
    **kw: Any,
) -> ExecutedTrade:
    return ExecutedTrade(
        decision_id=decision_id,
        asset=asset,
        side=side,
        size=size,
        price=price,
        paper_trade=True,
        **kw,
    )
