"""
Integration tests for the legacy pipeline:
  MetaOrchestrator -> RiskGuardian -> ExecutionEngine

Flow: AgentSignals on apex:signals -> MetaOrchestrator (weighted consensus)
  -> TradeDecision on decisions:pending -> RiskGuardian (7-check gate)
  -> decisions:approved / decisions:rejected -> ExecutionEngine
  -> ExecutedTrade on trades:executed
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from config.settings import settings
from core.message_bus import (
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_RISK_CHECKS,
    STREAM_SIGNALS,
    STREAM_TRADES_EXECUTED,
)
from core.models import (
    AccountHealth,
    Direction,
    ExecutedTrade,
    Timeframe,
    TradeAction,
    TradeDecision,
)
from tests.integration.conftest import (
    TrackingBus,
    make_decision,
    make_portfolio,
    make_position,
    make_signal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_orchestrator(bus: TrackingBus):
    from agents.meta.orchestrator import MetaOrchestrator
    return MetaOrchestrator(bus=bus)


def _make_guardian(bus: TrackingBus):
    from agents.risk.guardian import RiskGuardian
    return RiskGuardian(bus=bus)


def _make_engine(bus: TrackingBus):
    from agents.execution.engine import ExecutionEngine
    return ExecutionEngine(bus=bus, platform_specialist=None)


def _inject_signal_into_orchestrator(orch, signal):
    """Add a signal directly into the orchestrator's internal buffer."""
    if signal.agent_id not in orch._weights:
        raise ValueError(
            f"Agent {signal.agent_id!r} not in voting_agents. "
            f"Available: {list(orch._weights.keys())}"
        )
    orch._buffers[signal.asset].add(signal)


# ---------------------------------------------------------------------------
# 1. Signal -> Decision flow (MetaOrchestrator)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_signal_to_decision_flow(tracking_bus: TrackingBus):
    """Inject 2+ signals from different voting agents for the same asset.
    Run orchestrator voting round. Assert TradeDecision on decisions:pending."""
    orch = _make_orchestrator(tracking_bus)

    # Set a mid price so the decision has an entry_price.
    orch._mid_prices["ETH"] = 3000.0

    # Two bullish signals from different voting agents (quorum = 2).
    sig1 = make_signal(
        asset="ETH", direction=0.8, conviction=0.9,
        agent_id="technical_analyst", timeframe=Timeframe.INTRADAY,
    )
    sig2 = make_signal(
        asset="ETH", direction=0.6, conviction=0.7,
        agent_id="funding_arb", timeframe=Timeframe.INTRADAY,
    )

    _inject_signal_into_orchestrator(orch, sig1)
    _inject_signal_into_orchestrator(orch, sig2)

    await orch._run_voting_round()

    pending = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(pending) == 1, f"Expected 1 decision, got {len(pending)}"

    decision = pending[0]
    assert isinstance(decision, TradeDecision)
    assert decision.asset == "ETH"
    assert decision.action == TradeAction.OPEN_LONG
    assert decision.consensus_score > 0
    assert decision.entry_price == 3000.0
    assert decision.metadata.get("source") == "meta_orchestrator"
    assert decision.metadata.get("agent_count") == 2


# ---------------------------------------------------------------------------
# 2. Decision approval flow (RiskGuardian)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_decision_approval_flow(tracking_bus: TrackingBus):
    """A small, valid decision should pass all risk checks and be approved."""
    guardian = _make_guardian(tracking_bus)
    # Set healthy portfolio state.
    guardian._portfolio = make_portfolio(
        total_nav=100_000.0,
        gross_exposure=0.5,
        net_exposure=0.2,
    )

    decision = make_decision(
        asset="ETH",
        action=TradeAction.OPEN_LONG,
        size_pct=0.01,
        consensus_score=0.5,
        entry_price=3000.0,
    )

    await guardian._evaluate(decision)

    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    risk_checks = tracking_bus.get_published(STREAM_RISK_CHECKS)

    assert len(approved) == 1, f"Expected 1 approval, got {len(approved)}"
    assert len(rejected) == 0, f"Expected 0 rejections, got {len(rejected)}"
    assert len(risk_checks) == 1

    approved_decision = approved[0]
    assert approved_decision.risk_approved is True
    assert approved_decision.asset == "ETH"


# ---------------------------------------------------------------------------
# 3. Decision rejection — gross exposure breach
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_decision_rejection_gross_exposure(tracking_bus: TrackingBus):
    """Portfolio near gross exposure limit (2.95) + decision (0.10) > 3.0 -> reject."""
    guardian = _make_guardian(tracking_bus)
    guardian._portfolio = make_portfolio(
        total_nav=100_000.0,
        gross_exposure=2.95,
        net_exposure=0.5,
    )

    decision = make_decision(
        asset="SOL",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,  # capped to max_position_pct=0.02, still 2.95+0.02=2.97 < 3.0? No, use 0.10 pre-cap
        consensus_score=0.6,
        entry_price=100.0,
    )
    # Override size to exceed cap so the guardian's gross check triggers.
    # The guardian will first cap to 0.02, then check gross: 2.95 + 0.02 = 2.97 < 3.0 => approved.
    # We need gross to fail: set size higher.  But size_pct is capped at max_position_pct (0.02).
    # So to actually trigger gross rejection, we need gross_exposure very close:
    # gross=2.99, size_pct=0.02 -> capped to 0.02 -> 2.99+0.02=3.01 > 3.0 => rejected.
    guardian._portfolio = make_portfolio(
        total_nav=100_000.0,
        gross_exposure=2.99,
        net_exposure=0.5,
    )

    decision = make_decision(
        asset="SOL",
        action=TradeAction.OPEN_LONG,
        size_pct=0.02,
        consensus_score=0.6,
        entry_price=100.0,
    )

    await guardian._evaluate(decision)

    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)

    assert len(rejected) == 1, f"Expected 1 rejection, got {len(rejected)}"
    assert len(approved) == 0, f"Expected 0 approvals, got {len(approved)}"

    risk_checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(risk_checks) == 1
    check = risk_checks[0]
    assert check.approved is False
    assert "Gross exposure" in check.veto_reason


# ---------------------------------------------------------------------------
# 4. Full legacy pipeline end-to-end
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_legacy_pipeline(tracking_bus: TrackingBus):
    """End-to-end: signals -> orchestrator -> guardian -> engine -> executed trade."""
    # --- Set up agents ---
    orch = _make_orchestrator(tracking_bus)
    guardian = _make_guardian(tracking_bus)
    engine = _make_engine(tracking_bus)

    # Orchestrator state.
    orch._mid_prices["BTC"] = 50000.0

    # Guardian state — healthy portfolio, plenty of room.
    guardian._portfolio = make_portfolio(
        total_nav=100_000.0,
        gross_exposure=0.1,
        net_exposure=0.05,
    )

    # Engine state — needs equity to compute size.
    engine._account_cache = AccountHealth(equity=100_000.0, margin_available=80_000.0)

    # --- Step 1: Inject signals into orchestrator ---
    sig1 = make_signal(
        asset="BTC", direction=0.9, conviction=0.85,
        agent_id="technical_analyst", timeframe=Timeframe.INTRADAY,
    )
    sig2 = make_signal(
        asset="BTC", direction=0.7, conviction=0.75,
        agent_id="fundamental_valuation", timeframe=Timeframe.SWING,
    )
    _inject_signal_into_orchestrator(orch, sig1)
    _inject_signal_into_orchestrator(orch, sig2)

    # --- Step 2: Run voting round ---
    await orch._run_voting_round()

    pending = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(pending) == 1, "Orchestrator should produce exactly 1 decision"
    decision = pending[0]
    assert decision.asset == "BTC"
    assert decision.action == TradeAction.OPEN_LONG

    # --- Step 3: Guardian evaluates ---
    await guardian._evaluate(decision)

    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)
    assert len(approved) == 1, "Decision should be approved"
    assert len(rejected) == 0

    approved_dec = approved[0]
    assert approved_dec.risk_approved is True

    # --- Step 4: Engine executes (paper mode) ---
    await engine._execute_decision(approved_dec)

    executed = tracking_bus.get_published(STREAM_TRADES_EXECUTED)
    assert len(executed) == 1, "Engine should produce 1 executed trade"

    trade = executed[0]
    assert isinstance(trade, ExecutedTrade)
    assert trade.asset == "BTC"
    assert trade.side == "buy"
    assert trade.paper_trade is True
    assert trade.size > 0
    assert trade.price == 50000.0


# ---------------------------------------------------------------------------
# 5. Position size capping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_position_size_capping(tracking_bus: TrackingBus):
    """Decision with size_pct=0.05 (> max_position_pct=0.02) gets capped but approved."""
    guardian = _make_guardian(tracking_bus)
    guardian._portfolio = make_portfolio(
        total_nav=100_000.0,
        gross_exposure=0.5,
        net_exposure=0.2,
    )

    decision = make_decision(
        asset="ETH",
        action=TradeAction.OPEN_LONG,
        size_pct=0.05,  # Exceeds max_position_pct (0.02)
        consensus_score=0.7,
        entry_price=3000.0,
    )

    await guardian._evaluate(decision)

    approved = tracking_bus.get_published(STREAM_DECISIONS_APPROVED)
    rejected = tracking_bus.get_published(STREAM_DECISIONS_REJECTED)

    assert len(approved) == 1, "Oversized decision should be capped, not rejected"
    assert len(rejected) == 0

    capped_decision = approved[0]
    assert capped_decision.size_pct == settings.max_position_pct  # 0.02
    assert capped_decision.risk_approved is True

    # Verify risk check records the adjustment.
    risk_checks = tracking_bus.get_published(STREAM_RISK_CHECKS)
    assert len(risk_checks) == 1
    check = risk_checks[0]
    assert check.approved is True
    assert "size_capped_to" in check.adjustments


# ---------------------------------------------------------------------------
# 6. Quorum not met — no decision
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quorum_not_met(tracking_bus: TrackingBus):
    """Only 1 signal (below orch_min_quorum=2) -> no decision produced."""
    orch = _make_orchestrator(tracking_bus)
    orch._mid_prices["DOGE"] = 0.10

    sig = make_signal(
        asset="DOGE", direction=0.9, conviction=0.9,
        agent_id="technical_analyst", timeframe=Timeframe.INTRADAY,
    )
    _inject_signal_into_orchestrator(orch, sig)

    await orch._run_voting_round()

    pending = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(pending) == 0, (
        f"With only 1 signal (quorum={settings.orch_min_quorum}), "
        f"no decision should be produced. Got {len(pending)}."
    )


# ---------------------------------------------------------------------------
# 7. Consensus below threshold — no decision
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consensus_below_threshold(tracking_bus: TrackingBus):
    """Signals that cancel out (one bullish, one bearish, equal weight) -> no decision."""
    orch = _make_orchestrator(tracking_bus)
    orch._mid_prices["LINK"] = 15.0

    # Two agents with equal registry weight, opposite directions, same conviction.
    # technical_analyst: weight 0.20, direction +0.5, conviction 0.8
    # funding_arb: weight 0.10, direction -0.5, conviction 0.8
    # But these weights differ, so let's pick agents more carefully.
    # We want |consensus| < 0.30.
    #
    # consensus = weighted_dir / total_weight
    # For near-cancellation with different weights, use directions/convictions
    # that make the weighted sum small.
    #
    # technical_analyst (w=0.20): direction=+0.3, conviction=0.5 -> eff=0.10, contrib=+0.03
    # funding_arb (w=0.10): direction=-0.3, conviction=0.5 -> eff=0.05, contrib=-0.015
    # consensus = 0.015 / 0.15 = 0.10 < 0.30 threshold
    sig_bull = make_signal(
        asset="LINK", direction=0.3, conviction=0.5,
        agent_id="technical_analyst", timeframe=Timeframe.INTRADAY,
    )
    sig_bear = make_signal(
        asset="LINK", direction=-0.3, conviction=0.5,
        agent_id="funding_arb", timeframe=Timeframe.INTRADAY,
    )

    _inject_signal_into_orchestrator(orch, sig_bull)
    _inject_signal_into_orchestrator(orch, sig_bear)

    await orch._run_voting_round()

    pending = tracking_bus.get_published(STREAM_DECISIONS_PENDING)
    assert len(pending) == 0, (
        f"Opposing signals with low consensus should not produce a decision. "
        f"Got {len(pending)}."
    )
