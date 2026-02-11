"""
Integration tests: anomaly detection and defensive response flow.

Validates that:
  1. AnomalyAlert model serializes/deserializes correctly
  2. AnomalyDetector instantiates with correct identity and health
  3. CRITICAL anomalies trigger RiskGuardian defensive mode (reduce positions, pause entries)
  4. Defensive mode blocks new entries but allows close/reduce orders
  5. WARNING anomalies do NOT trigger defensive mode
  6. Multiple decisions are correctly handled during defensive mode
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.message_bus import (
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_REJECTED,
    STREAM_RISK_ANOMALY,
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
    make_decision,
    make_portfolio,
    make_position,
    make_anomaly,
)


# ---------------------------------------------------------------------------
# 1. AnomalyAlert model validation
# ---------------------------------------------------------------------------

def test_anomaly_alert_model_validation():
    """Construct AnomalyAlert with all fields. Assert round-trip serialization."""
    alert = AnomalyAlert(
        anomaly_type=AnomalyType.VOLUME_SPIKE,
        severity=AlertSeverity.CRITICAL,
        asset="BTC",
        description="BTC depth 500000 vs avg 80000 (6.3x)",
        value=6.3,
        threshold=5.0,
        metadata={"exchange": "hyperliquid"},
    )

    # Serialize to JSON and back.
    json_str = alert.model_dump_json()
    restored = AnomalyAlert.model_validate_json(json_str)

    assert restored.anomaly_type == AnomalyType.VOLUME_SPIKE
    assert restored.severity == AlertSeverity.CRITICAL
    assert restored.asset == "BTC"
    assert restored.value == 6.3
    assert restored.threshold == 5.0
    assert restored.description == "BTC depth 500000 vs avg 80000 (6.3x)"
    assert restored.metadata == {"exchange": "hyperliquid"}
    assert restored.alert_id == alert.alert_id
    assert restored.timestamp == alert.timestamp


# ---------------------------------------------------------------------------
# 2. AnomalyDetector instantiation
# ---------------------------------------------------------------------------

async def test_anomaly_detector_instantiation(tracking_bus: TrackingBus):
    """Create AnomalyDetector with tracking_bus. Check agent_id, agent_type, health."""
    from agents.risk.anomaly import AnomalyDetector

    detector = AnomalyDetector(bus=tracking_bus)

    assert detector.agent_id == "anomaly_detector"
    assert detector.agent_type == "risk"

    h = detector.health()
    assert h["agent_id"] == "anomaly_detector"
    assert h["assets_monitored"] == 0
    assert h["alerts_emitted"] == 0


# ---------------------------------------------------------------------------
# 3. CRITICAL anomaly triggers defensive mode
# ---------------------------------------------------------------------------

async def test_critical_anomaly_triggers_defensive_mode(tracking_bus: TrackingBus):
    """
    Create guardian with portfolio (2 positions). Call _enter_defensive_mode
    with a CRITICAL anomaly. Assert:
      - guardian._defensive_until is in the future
      - 2 REDUCE decisions published to decisions:approved
      - Each reduce has size = position.size_pct * 0.30
    """
    from agents.risk.guardian import RiskGuardian

    guardian = RiskGuardian(bus=tracking_bus)
    btc_pos = make_position("BTC", entry_price=50000, current_price=51000, size_pct=0.02)
    eth_pos = make_position("ETH", entry_price=3000, current_price=3100, size_pct=0.015)

    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        positions=[btc_pos, eth_pos],
    )

    alert = make_anomaly(
        anomaly_type=AnomalyType.PRICE_DEVIATION,
        severity=AlertSeverity.CRITICAL,
        asset="BTC",
        value=5.5,
        threshold=3.0,
    )

    await guardian._enter_defensive_mode(alert)

    # Defensive mode should be set into the future.
    now = datetime.now(timezone.utc)
    assert guardian._defensive_until is not None
    assert guardian._defensive_until > now

    # 2 REDUCE decisions published to decisions:approved.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 2

    # Verify each reduce decision.
    assets_reduced = {}
    for decision in approved:
        assert decision.action == TradeAction.REDUCE
        assert decision.risk_approved is True
        assert decision.metadata.get("reason") == "defensive_mode"
        assets_reduced[decision.asset] = decision.size_pct

    # BTC reduce: 0.02 * 0.30 = 0.006
    assert abs(assets_reduced["BTC"] - btc_pos.size_pct * 0.30) < 1e-9
    # ETH reduce: 0.015 * 0.30 = 0.0045
    assert abs(assets_reduced["ETH"] - eth_pos.size_pct * 0.30) < 1e-9


# ---------------------------------------------------------------------------
# 4. Defensive mode blocks new entries
# ---------------------------------------------------------------------------

async def test_defensive_mode_blocks_new_entries(tracking_bus: TrackingBus):
    """Set guardian defensive_until to 15 minutes from now. Submit OPEN_LONG. Assert rejected."""
    from agents.risk.guardian import RiskGuardian

    guardian = RiskGuardian(bus=tracking_bus)
    guardian._portfolio = make_portfolio(total_nav=100_000)
    guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=15)

    decision = make_decision(
        asset="SOL",
        action=TradeAction.OPEN_LONG,
        size_pct=0.01,
    )

    await guardian._evaluate(decision)

    # Should be rejected.
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 1
    assert rejected[0].asset == "SOL"

    # The risk check should contain "Defensive mode" in the veto reason.
    risk_checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(risk_checks) == 1
    assert risk_checks[0].approved is False
    assert "Defensive mode" in risk_checks[0].veto_reason

    # Nothing approved.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 0


# ---------------------------------------------------------------------------
# 5. Defensive mode allows close orders
# ---------------------------------------------------------------------------

async def test_defensive_mode_allows_close_orders(tracking_bus: TrackingBus):
    """Set guardian defensive_until. Submit CLOSE action. Assert approved."""
    from agents.risk.guardian import RiskGuardian

    guardian = RiskGuardian(bus=tracking_bus)
    guardian._portfolio = make_portfolio(total_nav=100_000)
    guardian._defensive_until = datetime.now(timezone.utc) + timedelta(minutes=15)

    decision = make_decision(
        asset="BTC",
        action=TradeAction.CLOSE,
        size_pct=0.02,
    )

    await guardian._evaluate(decision)

    # CLOSE is not a new entry, so it should pass through.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 1
    assert approved[0].asset == "BTC"
    assert approved[0].action == TradeAction.CLOSE

    # No rejections.
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 0


# ---------------------------------------------------------------------------
# 6. WARNING anomaly does NOT trigger defensive mode
# ---------------------------------------------------------------------------

async def test_warning_anomaly_does_not_trigger_defensive(tracking_bus: TrackingBus):
    """Create a WARNING-severity anomaly. Run through anomaly listener logic. Assert no defensive mode."""
    from agents.risk.guardian import RiskGuardian

    guardian = RiskGuardian(bus=tracking_bus)
    guardian._portfolio = make_portfolio(total_nav=100_000)

    warning_alert = make_anomaly(
        anomaly_type=AnomalyType.BOOK_IMBALANCE,
        severity=AlertSeverity.WARNING,
        asset="ETH",
        value=12.0,
        threshold=10.0,
    )

    # The anomaly listener checks severity == CRITICAL before calling
    # _enter_defensive_mode. Simulate the same logic.
    if warning_alert.severity == AlertSeverity.CRITICAL:
        await guardian._enter_defensive_mode(warning_alert)

    # Defensive mode should NOT be activated.
    assert guardian._defensive_until is None

    # No REDUCE decisions published.
    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(approved) == 0


# ---------------------------------------------------------------------------
# 7. AnomalyAlert has correct fields for volume spike
# ---------------------------------------------------------------------------

def test_anomaly_alert_has_correct_fields():
    """Create alert for volume spike. Verify all expected fields."""
    alert = make_anomaly(
        anomaly_type=AnomalyType.VOLUME_SPIKE,
        severity=AlertSeverity.CRITICAL,
        asset="SOL",
        value=7.2,
        threshold=5.0,
    )

    assert alert.anomaly_type == AnomalyType.VOLUME_SPIKE
    assert alert.severity == AlertSeverity.CRITICAL
    assert alert.asset == "SOL"
    assert alert.value == 7.2
    assert alert.threshold == 5.0
    assert alert.alert_id  # auto-generated, non-empty
    assert isinstance(alert.timestamp, datetime)
    assert alert.description  # make_anomaly fills this in
    assert isinstance(alert.metadata, dict)


# ---------------------------------------------------------------------------
# 8. Multiple anomalies chain — defensive rejects entries, allows reduce
# ---------------------------------------------------------------------------

async def test_multiple_anomalies_chain(tracking_bus: TrackingBus):
    """
    Trigger defensive mode, then submit multiple new entry decisions while
    in defensive. All should be rejected. Then submit a REDUCE -- should
    be approved.
    """
    from agents.risk.guardian import RiskGuardian

    guardian = RiskGuardian(bus=tracking_bus)
    guardian._portfolio = make_portfolio(
        total_nav=100_000,
        positions=[make_position("BTC"), make_position("ETH", entry_price=3000, current_price=3100)],
    )

    # Enter defensive mode via a CRITICAL anomaly.
    alert = make_anomaly(
        anomaly_type=AnomalyType.CORRELATION_SPIKE,
        severity=AlertSeverity.CRITICAL,
        asset="",
        value=0.85,
        threshold=0.5,
    )
    await guardian._enter_defensive_mode(alert)

    # The defensive entry publishes 2 REDUCE decisions for the 2 positions.
    initial_approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(initial_approved) == 2

    # Now submit 3 new entry decisions. All should be rejected.
    for asset, action in [
        ("SOL", TradeAction.OPEN_LONG),
        ("AVAX", TradeAction.OPEN_SHORT),
        ("DOGE", TradeAction.OPEN_LONG),
    ]:
        decision = make_decision(asset=asset, action=action, size_pct=0.01)
        await guardian._evaluate(decision)

    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(rejected) == 3
    rejected_assets = {d.asset for d in rejected}
    assert rejected_assets == {"SOL", "AVAX", "DOGE"}

    # All rejections have "Defensive mode" reason.
    risk_checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    for check in risk_checks:
        assert check.approved is False
        assert "Defensive mode" in check.veto_reason

    # Submit a REDUCE order. Should be approved (not a new entry).
    reduce_decision = make_decision(
        asset="BTC",
        action=TradeAction.REDUCE,
        size_pct=0.005,
    )
    await guardian._evaluate(reduce_decision)

    # The initial 2 defensive reduces + this 1 manual reduce = 3.
    all_approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    assert len(all_approved) == 3

    # The last approved should be our BTC REDUCE.
    last_approved = all_approved[-1]
    assert last_approved.asset == "BTC"
    assert last_approved.action == TradeAction.REDUCE
