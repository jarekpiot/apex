#!/usr/bin/env python3
"""
APEX Signal Tracer — follow a signal through the full decision pipeline.

Creates a synthetic AgentSignal and traces its journey:
  1. Signal published to apex:signals
  2. MetaOrchestrator buffers it in the signal matrix
  3. Consensus voting (if quorum met) → TradeDecision on decisions:pending
  4. RiskGuardian evaluation → decisions:approved or decisions:rejected
  5. ExecutionEngine fill → trades:executed (paper mode)

Usage:
    python scripts/trace_signal.py [--asset BTC] [--direction 0.8] [--conviction 0.9]

Requires: No external services (uses TrackingBus + direct method calls)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
os.chdir(_project_root)

# Patch DB before any agent imports
import core.database as _db

class _FakeSession:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    def add(self, o): pass
    def add_all(self, o): pass
    async def commit(self): pass
    async def rollback(self): pass
    async def execute(self, s):
        from unittest.mock import MagicMock
        return MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[]))))

_db.async_session_factory = lambda: _FakeSession()


from core.message_bus import (
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_PENDING,
    STREAM_DECISIONS_REJECTED,
    STREAM_RISK_CHECKS,
    STREAM_SIGNALS,
    STREAM_TRADES_EXECUTED,
)
from core.models import AgentSignal, Timeframe, TradeDecision


# ---------------------------------------------------------------------------
# Inline TrackingBus (standalone — no pytest dependency)
# ---------------------------------------------------------------------------

import json
from typing import Any


class _TraceBus:
    """Minimal bus that captures publishes for tracing."""

    def __init__(self) -> None:
        self.published: dict[str, list[Any]] = {}
        self._queues: dict[str, list[asyncio.Queue]] = {}

    async def connect(self) -> None: pass
    async def close(self) -> None: pass
    async def health_check(self) -> bool: return True
    async def ensure_group(self, stream: str, group: str) -> None: pass

    async def publish_to(self, stream: str, model: Any) -> str:
        self.published.setdefault(stream, []).append(model)
        if hasattr(model, "model_dump_json"):
            payload = json.loads(model.model_dump_json())
        else:
            payload = model
        for q in self._queues.get(stream, []):
            await q.put(("trace-id", payload))
        return "trace-id"

    async def publish_signal(self, signal: Any) -> str:
        return await self.publish_to(STREAM_SIGNALS, signal)

    async def publish_decision(self, decision: Any) -> str:
        return await self.publish_to("apex:decisions", decision)

    async def publish_risk_check(self, check: Any) -> str:
        return await self.publish_to("apex:risk_checks", check)

    async def publish_red_team(self, challenge: Any) -> str:
        return await self.publish_to("apex:red_team", challenge)

    def subscribe_to(self, stream: str, group: str, consumer: str, **kw: Any):
        q: asyncio.Queue = asyncio.Queue()
        self._queues.setdefault(stream, []).append(q)
        return self._iter(q)

    def subscribe_signals(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to(STREAM_SIGNALS, group, consumer)

    def subscribe_decisions(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:decisions", group, consumer)

    def subscribe_risk_checks(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:risk_checks", group, consumer)

    def subscribe_red_team(self, group: str, consumer: str, **kw: Any):
        return self.subscribe_to("apex:red_team", group, consumer)

    async def _iter(self, q: asyncio.Queue):
        while True:
            item = await asyncio.wait_for(q.get(), timeout=60)
            yield item

    def get(self, stream: str) -> list[Any]:
        return self.published.get(stream, [])


# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------

G = "\033[32m"
Y = "\033[33m"
R = "\033[31m"
B = "\033[34m"
C = "\033[36m"
N = "\033[0m"


def step(n: int, msg: str) -> None:
    print(f"\n{C}[Step {n}]{N} {msg}")


def ok(msg: str) -> None:
    print(f"  {G}OK{N} {msg}")


def fail(msg: str) -> None:
    print(f"  {R}FAIL{N} {msg}")


def info(msg: str) -> None:
    print(f"  {Y}INFO{N} {msg}")


# ---------------------------------------------------------------------------
# Main trace
# ---------------------------------------------------------------------------

async def trace(asset: str, direction: float, conviction: float) -> int:
    bus = _TraceBus()

    print(f"\n{'='*60}")
    print(f"{B}  APEX Signal Tracer{N}")
    print(f"  Asset: {asset}  Direction: {direction}  Conviction: {conviction}")
    print(f"{'='*60}")

    # -----------------------------------------------------------------------
    # Step 1: Create and publish signals
    # -----------------------------------------------------------------------
    step(1, "Publishing synthetic signals to apex:signals")

    # We need ≥2 signals from voting agents to meet quorum
    signal_a = AgentSignal(
        agent_id="technical_analyst",
        asset=asset,
        direction=direction,
        conviction=conviction,
        timeframe=Timeframe.INTRADAY,
        reasoning=f"Trace test: bullish {asset}",
    )
    signal_b = AgentSignal(
        agent_id="funding_arb",
        asset=asset,
        direction=direction * 0.8,
        conviction=conviction * 0.9,
        timeframe=Timeframe.SWING,
        reasoning=f"Trace test: funding arb {asset}",
    )

    await bus.publish_signal(signal_a)
    await bus.publish_signal(signal_b)

    signals = bus.get(STREAM_SIGNALS)
    ok(f"Published {len(signals)} signals to {STREAM_SIGNALS}")
    for s in signals:
        info(f"  {s.agent_id}: dir={s.direction:.2f} conv={s.conviction:.2f}")

    # -----------------------------------------------------------------------
    # Step 2: MetaOrchestrator consensus voting
    # -----------------------------------------------------------------------
    step(2, "MetaOrchestrator consensus voting")

    from agents.meta.orchestrator import MetaOrchestrator
    orch = MetaOrchestrator(bus=bus)

    # Feed signals directly into the buffer
    orch._buffers[asset].add(signal_a)
    orch._buffers[asset].add(signal_b)
    orch._mid_prices[asset] = 50000.0 if asset == "BTC" else 3000.0

    # Run a voting round
    await orch._run_voting_round()

    decisions = bus.get(STREAM_DECISIONS_PENDING)
    if decisions:
        decision = decisions[0]
        ok(f"Decision produced: {decision.action} {decision.asset} "
           f"size={decision.size_pct:.4f} consensus={decision.consensus_score:.3f}")
    else:
        fail("No decision produced (consensus or quorum not met)")
        info("Signals may not have enough weight or agreement")
        return 1

    # -----------------------------------------------------------------------
    # Step 3: RiskGuardian evaluation
    # -----------------------------------------------------------------------
    step(3, "RiskGuardian risk evaluation")

    from agents.risk.guardian import RiskGuardian
    from core.models import PortfolioState

    guardian = RiskGuardian(bus=bus)
    guardian._portfolio = PortfolioState(total_nav=100_000.0)

    await guardian._evaluate(decision)

    approved = bus.get(STREAM_DECISIONS_APPROVED)
    rejected = bus.get(STREAM_DECISIONS_REJECTED)
    risk_checks = bus.get(STREAM_RISK_CHECKS)

    if approved:
        ok(f"Decision APPROVED (risk_approved={approved[0].risk_approved})")
        if risk_checks:
            rc = risk_checks[0]
            if rc.adjustments:
                info(f"  Adjustments: {rc.adjustments}")
            info(f"  Portfolio impact: {rc.portfolio_impact}")
    elif rejected:
        fail(f"Decision REJECTED: {risk_checks[0].veto_reason if risk_checks else 'unknown'}")
        return 1
    else:
        fail("No risk check output")
        return 1

    # -----------------------------------------------------------------------
    # Step 4: ExecutionEngine fill
    # -----------------------------------------------------------------------
    step(4, "ExecutionEngine paper-trade execution")

    from agents.execution.engine import ExecutionEngine
    from core.models import AccountHealth

    engine = ExecutionEngine(bus=bus, platform_specialist=None)
    # Set account equity so _compute_size can work
    engine._account_cache = AccountHealth(equity=100_000.0, margin_available=50_000.0)

    # Call the internal execute method (paper mode)
    approved_decision = approved[0]
    await engine._execute_decision(approved_decision)

    trades = bus.get(STREAM_TRADES_EXECUTED)
    if trades:
        trade = trades[0]
        ok(f"Trade executed: {trade.side} {trade.asset} "
           f"size={trade.size:.6f} price={trade.price:.2f} "
           f"fee={trade.fee:.4f} paper={trade.paper_trade}")
    else:
        fail("No trade executed")
        return 1

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"{G}  Signal successfully traced through full pipeline!{N}")
    print(f"  {STREAM_SIGNALS} -> {STREAM_DECISIONS_PENDING} -> "
          f"{STREAM_DECISIONS_APPROVED} -> {STREAM_TRADES_EXECUTED}")
    print(f"{'='*60}\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="APEX Signal Tracer")
    parser.add_argument("--asset", default="BTC", help="Asset to trade (default: BTC)")
    parser.add_argument("--direction", type=float, default=0.8, help="Signal direction -1..+1")
    parser.add_argument("--conviction", type=float, default=0.9, help="Signal conviction 0..1")
    args = parser.parse_args()

    return asyncio.run(trace(args.asset, args.direction, args.conviction))


if __name__ == "__main__":
    sys.exit(main())
