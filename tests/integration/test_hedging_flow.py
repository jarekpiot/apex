"""
Integration tests for APEX HedgingEngine flow.

Validates:
  1. HedgingEngine instantiation and identity
  2. Hedge triggers when net exposure exceeds threshold
  3. No hedge when exposure is within threshold
  4. Hedge decisions route through the risk pipeline (decisions:pending)
  5. Cooldown prevents rapid-fire hedge proposals
  6. Correct stream subscriptions (portfolio:state + market:prices)
  7. Long bias produces SHORT hedge
  8. Short bias produces LONG hedge
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import pytest

from core.message_bus import (
    STREAM_DECISIONS_PENDING,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
)
from core.models import (
    Direction,
    PortfolioState,
    Position,
    TradeAction,
    TradeDecision,
)
from tests.integration.conftest import (
    TrackingBus,
    make_portfolio,
    make_position,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_engine(bus: TrackingBus):
    """Import and instantiate HedgingEngine with a TrackingBus."""
    from agents.risk.hedging import HedgingEngine

    return HedgingEngine(bus=bus)


def _populate_price_history(engine, assets: dict[str, list[float]]) -> None:
    """Fill ``_hourly_prices`` with enough data for correlation calculations.

    *assets* maps asset name to a list of hourly price samples.
    """
    for asset, prices in assets.items():
        for p in prices:
            engine._hourly_prices[asset].append(p)


def _make_price_series(base: float, n: int = 30, step: float = 100.0) -> list[float]:
    """Generate a simple upward-trending price series starting at *base*."""
    return [base + i * step for i in range(n)]


# ---------------------------------------------------------------------------
# 1. Instantiation
# ---------------------------------------------------------------------------


async def test_hedging_engine_instantiation(tracking_bus: TrackingBus):
    """HedgingEngine has the expected agent_id and agent_type."""
    engine = _build_engine(tracking_bus)
    assert engine.agent_id == "hedging_engine"
    assert engine.agent_type == "risk"


# ---------------------------------------------------------------------------
# 2. Hedge trigger on high net exposure
# ---------------------------------------------------------------------------


async def test_hedge_trigger_on_high_net_exposure(tracking_bus: TrackingBus):
    """When net_exposure exceeds the threshold (0.50), a TradeDecision is published."""
    engine = _build_engine(tracking_bus)

    # Set portfolio with high net exposure (0.70 > 0.50 threshold).
    engine._portfolio = make_portfolio(
        net_exposure=0.70,
        positions=[make_position("BTC", direction=Direction.LONG, size_pct=0.04)],
        total_nav=100_000,
    )

    # Provide mid-prices so fallback hedge selection can find candidates.
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0, "SPY": 450.0}

    # Provide enough hourly price data for correlation.
    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30, step=100),
        "ETH": _make_price_series(3000, n=30, step=10),
        "GOLD": _make_price_series(1900, n=30, step=-5),  # inverse trend
        "SPY": _make_price_series(450, n=30, step=1),
    })

    # Call the internal evaluation method directly.
    await engine._evaluate_hedge()

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) == 1, (
        f"Expected exactly 1 hedge decision, got {len(published)}"
    )

    decision = published[0]
    assert isinstance(decision, TradeDecision)
    assert decision.metadata.get("source") == "hedging_engine"


# ---------------------------------------------------------------------------
# 3. No hedge when exposure is within threshold
# ---------------------------------------------------------------------------


async def test_no_hedge_when_exposure_within_threshold(tracking_bus: TrackingBus):
    """When net_exposure is below the threshold (0.30 < 0.50), no hedge is published."""
    engine = _build_engine(tracking_bus)

    engine._portfolio = make_portfolio(
        net_exposure=0.30,
        positions=[make_position("BTC", direction=Direction.LONG, size_pct=0.02)],
        total_nav=100_000,
    )
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0}

    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30),
        "GOLD": _make_price_series(1900, n=30, step=-5),
    })

    await engine._evaluate_hedge()

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) == 0, (
        "No hedge should be published when net exposure is within threshold"
    )


# ---------------------------------------------------------------------------
# 4. Hedge decision goes through risk pipeline
# ---------------------------------------------------------------------------


async def test_hedge_decision_goes_through_risk_pipeline(tracking_bus: TrackingBus):
    """Hedge decisions are published to STREAM_DECISIONS_PENDING, not approved directly.

    This ensures every hedge proposal passes through RiskGuardian's 7-check pipeline.
    """
    engine = _build_engine(tracking_bus)

    engine._portfolio = make_portfolio(
        net_exposure=0.80,
        positions=[make_position("BTC", direction=Direction.LONG, size_pct=0.04)],
        total_nav=100_000,
    )
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0}

    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30),
        "GOLD": _make_price_series(1900, n=30, step=-5),
    })

    await engine._evaluate_hedge()

    # Must publish to decisions:pending (the risk pipeline entry point).
    assert len(tracking_bus.get_published(STREAM_DECISIONS_PENDING)) >= 1

    # Must NOT publish to decisions:approved (hedging engine cannot bypass risk).
    from core.message_bus import STREAM_DECISIONS_APPROVED
    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0, (
        "HedgingEngine must not publish directly to decisions:approved"
    )


# ---------------------------------------------------------------------------
# 5. Hedge cooldown
# ---------------------------------------------------------------------------


async def test_hedge_cooldown(tracking_bus: TrackingBus):
    """After a hedge proposal, the next evaluation within the 5-min cooldown
    should NOT produce another proposal.
    """
    engine = _build_engine(tracking_bus)

    engine._portfolio = make_portfolio(
        net_exposure=0.70,
        positions=[make_position("BTC", direction=Direction.LONG, size_pct=0.04)],
        total_nav=100_000,
    )
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0}

    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30),
        "GOLD": _make_price_series(1900, n=30, step=-5),
    })

    # First evaluation should produce a hedge.
    await engine._evaluate_hedge()
    assert len(tracking_bus.get_published(STREAM_DECISIONS_PENDING)) == 1

    # Second evaluation immediately after should be suppressed by cooldown.
    await engine._evaluate_hedge()
    assert len(tracking_bus.get_published(STREAM_DECISIONS_PENDING)) == 1, (
        "Cooldown should prevent a second hedge proposal within 5 minutes"
    )


# ---------------------------------------------------------------------------
# 6. Subscription wiring
# ---------------------------------------------------------------------------


async def test_hedging_engine_subscribes_correctly(tracking_bus: TrackingBus):
    """HedgingEngine subscribes to portfolio:state and market:prices."""
    engine = _build_engine(tracking_bus)

    try:
        await engine.start()
        # Allow the agent's background tasks to register their subscriptions.
        await asyncio.sleep(0.2)
    finally:
        await engine.stop()

    subscribed = tracking_bus.subscribed_streams()
    assert STREAM_PORTFOLIO_STATE in subscribed, (
        f"Missing {STREAM_PORTFOLIO_STATE} subscription. Got: {subscribed}"
    )
    assert STREAM_MARKET_PRICES in subscribed, (
        f"Missing {STREAM_MARKET_PRICES} subscription. Got: {subscribed}"
    )


# ---------------------------------------------------------------------------
# 7. Hedge for long bias
# ---------------------------------------------------------------------------


async def test_hedge_for_long_bias(tracking_bus: TrackingBus):
    """When portfolio is heavily long, the hedge should propose a SHORT."""
    engine = _build_engine(tracking_bus)

    engine._portfolio = make_portfolio(
        net_exposure=0.80,
        positions=[
            make_position("BTC", direction=Direction.LONG, size_pct=0.03),
            make_position("ETH", direction=Direction.LONG, size_pct=0.02),
        ],
        total_nav=100_000,
    )
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0, "SPY": 450.0}

    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30, step=100),
        "ETH": _make_price_series(3000, n=30, step=10),
        "GOLD": _make_price_series(1900, n=30, step=-5),
        "SPY": _make_price_series(450, n=30, step=-1),
    })

    await engine._evaluate_hedge()

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) == 1

    decision = published[0]
    assert isinstance(decision, TradeDecision)
    assert decision.action == TradeAction.OPEN_SHORT, (
        f"Long-biased portfolio should trigger OPEN_SHORT hedge, got {decision.action}"
    )


# ---------------------------------------------------------------------------
# 8. Hedge for short bias
# ---------------------------------------------------------------------------


async def test_hedge_for_short_bias(tracking_bus: TrackingBus):
    """When portfolio is heavily short, the hedge should propose a LONG."""
    engine = _build_engine(tracking_bus)

    engine._portfolio = make_portfolio(
        net_exposure=-0.70,
        positions=[
            make_position("BTC", direction=Direction.SHORT, size_pct=0.03),
            make_position("ETH", direction=Direction.SHORT, size_pct=0.02),
        ],
        total_nav=100_000,
    )
    engine._mid_prices = {"BTC": 50000.0, "ETH": 3000.0, "GOLD": 1900.0, "SPY": 450.0}

    _populate_price_history(engine, {
        "BTC": _make_price_series(50000, n=30, step=100),
        "ETH": _make_price_series(3000, n=30, step=10),
        "GOLD": _make_price_series(1900, n=30, step=-5),
        "SPY": _make_price_series(450, n=30, step=-1),
    })

    await engine._evaluate_hedge()

    published = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(published) == 1

    decision = published[0]
    assert isinstance(decision, TradeDecision)
    assert decision.action == TradeAction.OPEN_LONG, (
        f"Short-biased portfolio should trigger OPEN_LONG hedge, got {decision.action}"
    )
