"""
Integration tests for APEX safety rails: circuit breakers, drawdown gates,
and defensive mode.

These tests exercise the RiskGuardian evaluation pipeline end-to-end using
TrackingBus (no real Redis) and the autouse ``_patch_db`` fixture from the
integration conftest.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.message_bus import (
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_RISK_CHECKS,
)
from core.models import (
    AlertSeverity,
    AnomalyAlert,
    AnomalyType,
    Direction,
    PortfolioState,
    Position,
    TradeAction,
    TradeDecision,
)
from tests.integration.conftest import (
    TrackingBus,
    make_anomaly,
    make_decision,
    make_portfolio,
    make_position,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_guardian(bus: TrackingBus):
    """Instantiate a RiskGuardian wired to the TrackingBus."""
    from agents.risk.guardian import RiskGuardian
    return RiskGuardian(bus=bus)


def _seed_daily_drawdown(guardian, start_nav: float, hours_ago: float = 12.0) -> None:
    """Plant a historical NAV snapshot so daily_drawdown() returns a real value.

    ``start_nav`` is the NAV recorded ``hours_ago`` hours in the past.
    The *current* NAV comes from ``guardian._portfolio.total_nav`` at evaluate time.
    """
    past = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    guardian._dd_tracker._snapshots.append((past, start_nav))


def _seed_weekly_drawdown(guardian, start_nav: float, days_ago: float = 5.0) -> None:
    """Plant a NAV snapshot several days ago for weekly drawdown."""
    past = datetime.now(timezone.utc) - timedelta(days=days_ago)
    guardian._dd_tracker._snapshots.append((past, start_nav))


# ---------------------------------------------------------------------------
# 1. Circuit breaker rejects new entries at >= 15% daily drawdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_circuit_breaker_rejects_new_entry(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # Portfolio currently at 84_000 NAV (a 16% drop from 100_000).
    guardian._portfolio = make_portfolio(total_nav=84_000)
    _seed_daily_drawdown(guardian, start_nav=100_000)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.OPEN_LONG,
        size_pct=0.01,
    )
    await guardian._evaluate(decision)

    # Should be rejected.
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1
    assert rejected[0].decision_id == decision.decision_id

    # The RiskCheck audit record should note the circuit breaker.
    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    assert "Circuit breaker" in (checks[0].veto_reason or "")

    # Nothing on the approved stream.
    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 2. Soft drawdown (10-15%) reduces size by 50% but still approves
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_soft_drawdown_reduces_size(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # 12% daily drawdown: NAV was 100_000 12h ago, now 88_000.
    guardian._portfolio = make_portfolio(total_nav=88_000)
    _seed_daily_drawdown(guardian, start_nav=100_000)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,
    )
    await guardian._evaluate(decision)

    # Should be approved with reduced size.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 1
    assert approved[0].decision_id == decision.decision_id

    # 0.02 * (1 - 0.50) = 0.01
    assert approved[0].size_pct == pytest.approx(0.01, abs=1e-6)

    # Audit record should show the adjustment.
    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is True
    assert checks[0].adjustments.get("size_reduced_by") == 0.50


# ---------------------------------------------------------------------------
# 3. CLOSE orders bypass circuit breaker
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_orders_bypass_circuit_breaker(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # 16% daily drawdown -- circuit breaker territory.
    guardian._portfolio = make_portfolio(total_nav=84_000)
    _seed_daily_drawdown(guardian, start_nav=100_000)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.CLOSE,
        size_pct=0.02,
    )
    await guardian._evaluate(decision)

    # CLOSE should be approved even during circuit breaker.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 1
    assert approved[0].decision_id == decision.decision_id
    assert approved[0].action == TradeAction.CLOSE

    # Nothing rejected.
    assert len(tracking_bus.get_published(STREAM_DECISIONS_REJECTED)) == 0


# ---------------------------------------------------------------------------
# 4. Defensive mode rejects new entries
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_defensive_mode_rejects_new_entries(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)
    guardian._portfolio = make_portfolio(total_nav=100_000)

    # Simulate defensive mode being active.
    guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=15)

    decision = make_decision(
        asset="ETH",
        action=TradeAction.OPEN_LONG,
        size_pct=0.01,
    )
    await guardian._evaluate(decision)

    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1
    assert rejected[0].decision_id == decision.decision_id

    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    assert "Defensive mode" in (checks[0].veto_reason or "")

    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 5. Defensive mode reduces existing positions by 30%
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_defensive_mode_reduces_positions(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    pos_btc = make_position(asset="BTC", size_pct=0.05, direction=Direction.LONG)
    pos_eth = make_position(asset="ETH", size_pct=0.03, direction=Direction.SHORT)
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        positions=[pos_btc, pos_eth],
    )

    alert = make_anomaly(
        anomaly_type=AnomalyType.PRICE_DEVIATION,
        severity=AlertSeverity.CRITICAL,
        asset="BTC",
    )

    await guardian._enter_defensive_mode(alert)

    # Should have published 2 REDUCE decisions to decisions:approved.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 2

    # Both should be REDUCE actions, risk_approved=True, correct reduction sizes.
    assets_reduced = {d.asset: d for d in approved}
    assert "BTC" in assets_reduced
    assert "ETH" in assets_reduced

    # 30% of 0.05 = 0.015 for BTC
    assert assets_reduced["BTC"].action == TradeAction.REDUCE
    assert assets_reduced["BTC"].risk_approved is True
    assert assets_reduced["BTC"].size_pct == pytest.approx(0.05 * 0.30, abs=1e-6)

    # 30% of 0.03 = 0.009 for ETH
    assert assets_reduced["ETH"].action == TradeAction.REDUCE
    assert assets_reduced["ETH"].risk_approved is True
    assert assets_reduced["ETH"].size_pct == pytest.approx(0.03 * 0.30, abs=1e-6)

    # Defensive mode should now be set.
    assert guardian._defensive_until is not None
    assert guardian._defensive_until > datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 6. Gross exposure rejection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gross_exposure_rejection(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # Current gross = 2.99. Even a capped size of 0.02 pushes to 3.01 > max 3.0.
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        gross_exposure=2.99,
    )

    decision = make_decision(
        asset="BTC",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,
    )
    await guardian._evaluate(decision)

    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1

    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    assert "Gross exposure" in (checks[0].veto_reason or "")

    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 7. Net exposure rejection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_net_exposure_rejection(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # Current net = 0.99. OPEN_LONG with 0.02 pushes to 1.01 > max 1.0.
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        net_exposure=0.99,
    )

    decision = make_decision(
        asset="ETH",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,
    )
    await guardian._evaluate(decision)

    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1

    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    assert "Net exposure" in (checks[0].veto_reason or "")

    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 8. Asset-class concentration rejection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_asset_class_concentration_rejection(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # Existing position: SOL is a crypto_alt with 35% size.
    pos_sol = make_position(asset="SOL", size_pct=0.35, direction=Direction.LONG)
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        positions=[pos_sol],
    )

    # New OPEN_LONG for AVAX (also crypto_alt). 0.35 + 0.02 = 0.37 <= 0.40 -- should pass.
    # Use a size that would push over 40%: 0.35 + 0.06 = 0.41 > 0.40.
    decision = make_decision(
        asset="AVAX",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,  # Will be kept since <= max_position_pct
    )
    # We need combined to exceed 0.40. With SOL at 0.35 and AVAX at 0.02 = 0.37, that won't trip.
    # Set SOL to 0.39 so combined is 0.41.
    pos_sol_high = make_position(asset="SOL", size_pct=0.39, direction=Direction.LONG)
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        positions=[pos_sol_high],
    )

    await guardian._evaluate(decision)

    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1

    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    assert "class exposure" in (checks[0].veto_reason or "").lower()

    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 9. Weekly drawdown pause triggers and rejects
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_weekly_pause_triggers(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)

    # 22% weekly drawdown: was 100_000 five days ago, now 78_000.
    guardian._portfolio = make_portfolio(total_nav=78_000)
    _seed_weekly_drawdown(guardian, start_nav=100_000, days_ago=5.0)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.OPEN_LONG,
        size_pct=0.01,
    )
    await guardian._evaluate(decision)

    # Should be rejected.
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1

    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is False
    veto = checks[0].veto_reason or ""
    assert "weekly" in veto.lower() or "Weekly" in veto

    # Guardian should have set the weekly pause.
    assert guardian._weekly_pause_until is not None
    assert guardian._weekly_pause_until > datetime.now(timezone.utc)

    assert len(tracking_bus.get_published(STREAM_DECISIONS_APPROVED)) == 0


# ---------------------------------------------------------------------------
# 10. Position size capping (> max_position_pct gets capped, not rejected)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_position_size_capping(tracking_bus: TrackingBus) -> None:
    guardian = _make_guardian(tracking_bus)
    guardian._portfolio = make_portfolio(total_nav=100_000)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.OPEN_LONG,
        size_pct=0.05,  # > max 0.02
    )
    await guardian._evaluate(decision)

    # Should be approved with capped size.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 1
    assert approved[0].decision_id == decision.decision_id
    assert approved[0].size_pct == pytest.approx(0.02, abs=1e-6)

    # Audit record should show the cap.
    checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(checks) == 1
    assert checks[0].approved is True
    assert checks[0].adjustments.get("size_capped_to") == 0.02

    # Nothing rejected.
    assert len(tracking_bus.get_published(STREAM_DECISIONS_REJECTED)) == 0
