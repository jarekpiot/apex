"""
Shared fixtures for the APEX test suite.

Provides:
- MockMessageBus — captures publishes, no Redis needed
- Mock DB session factory — no Postgres needed
- Model factories for building test data quickly
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.models import (
    AgentSignal,
    AlertSeverity,
    AnomalyAlert,
    AnomalyType,
    Direction,
    PortfolioState,
    Position,
    Timeframe,
    TradeAction,
    TradeDecision,
)


# ---------------------------------------------------------------------------
# MockMessageBus
# ---------------------------------------------------------------------------

class MockMessageBus:
    """In-memory message bus that captures all publishes."""

    def __init__(self) -> None:
        self.published: dict[str, list[Any]] = {}
        self._queues: dict[str, asyncio.Queue[tuple[str, dict]]] = {}

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def health_check(self) -> bool:
        return True

    async def ensure_group(self, stream: str, group: str) -> None:
        pass

    async def publish_to(self, stream: str, model: Any) -> str:
        self.published.setdefault(stream, []).append(model)
        return "mock-id"

    async def publish_signal(self, signal: Any) -> str:
        return await self.publish_to("apex:signals", signal)

    async def publish_decision(self, decision: Any) -> str:
        return await self.publish_to("apex:decisions", decision)

    async def publish_risk_check(self, check: Any) -> str:
        return await self.publish_to("apex:risk_checks", check)

    async def publish_red_team(self, challenge: Any) -> str:
        return await self.publish_to("apex:red_team", challenge)

    def subscribe_to(self, stream: str, group: str, consumer: str, **kw: Any):
        return self._empty_iter()

    def subscribe_signals(self, group: str, consumer: str, **kw: Any):
        return self._empty_iter()

    def subscribe_decisions(self, group: str, consumer: str, **kw: Any):
        return self._empty_iter()

    def subscribe_risk_checks(self, group: str, consumer: str, **kw: Any):
        return self._empty_iter()

    def subscribe_red_team(self, group: str, consumer: str, **kw: Any):
        return self._empty_iter()

    async def _empty_iter(self):
        # Yield nothing; the caller's async-for will block, but tests
        # cancel via stop() so this is fine.
        while True:
            await asyncio.sleep(3600)
            yield  # pragma: no cover

    async def inject(self, stream: str, payload: dict) -> None:
        q = self._queues.setdefault(stream, asyncio.Queue())
        await q.put(("injected-id", payload))

    def get_published(self, stream: str) -> list[Any]:
        return self.published.get(stream, [])


@pytest.fixture
def mock_bus() -> MockMessageBus:
    return MockMessageBus()


# ---------------------------------------------------------------------------
# Mock DB — autouse so no agent ever touches real Postgres
# ---------------------------------------------------------------------------

class _FakeSession:
    """Fake async session that no-ops all DB operations."""

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
        return MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[]))))


def _fake_session_factory():
    return _FakeSession()


@pytest.fixture(autouse=True)
def _patch_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Globally replace the async session factory with a no-op version."""
    monkeypatch.setattr("core.database.async_session_factory", _fake_session_factory)


# ---------------------------------------------------------------------------
# Model factories
# ---------------------------------------------------------------------------

def make_signal(
    asset: str = "BTC",
    direction: float = 0.5,
    conviction: float = 0.8,
    agent_id: str = "test_agent",
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


def make_decision(
    asset: str = "BTC",
    action: TradeAction = TradeAction.OPEN_LONG,
    size_pct: float = 0.01,
    consensus_score: float = 0.5,
    **kw: Any,
) -> TradeDecision:
    return TradeDecision(
        asset=asset,
        action=action,
        size_pct=size_pct,
        consensus_score=consensus_score,
        **kw,
    )


def make_portfolio(
    positions: list[Position] | None = None,
    total_nav: float = 100_000,
    **kw: Any,
) -> PortfolioState:
    return PortfolioState(
        positions=positions or [],
        total_nav=total_nav,
        **kw,
    )


def make_position(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    size_pct: float = 0.02,
    entry_price: float = 50000.0,
    current_price: float = 51000.0,
) -> Position:
    return Position(
        asset=asset,
        direction=direction,
        size_pct=size_pct,
        entry_price=entry_price,
        current_price=current_price,
        unrealised_pnl=(current_price - entry_price) * size_pct * 100_000,
        opened_at=datetime.now(timezone.utc),
    )


def make_anomaly(
    anomaly_type: AnomalyType = AnomalyType.PRICE_DEVIATION,
    severity: AlertSeverity = AlertSeverity.CRITICAL,
    asset: str = "BTC",
    **kw: Any,
) -> AnomalyAlert:
    return AnomalyAlert(
        anomaly_type=anomaly_type,
        severity=severity,
        asset=asset,
        **kw,
    )
