#!/usr/bin/env python3
"""
APEX Smoke Test — quick sanity check that runs without Docker.

Validates:
  1. All modules import successfully
  2. All 26 agents instantiate with a mock bus
  3. Agent registry has 26 entries, all ACTIVE
  4. All stream constants are defined
  5. All core models instantiate
  6. Agent health() returns valid dicts

Usage:
    python scripts/smoke_test.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

# Ensure the project root is on sys.path so `core.*` / `config.*` resolve.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
os.chdir(_project_root)

# ---------------------------------------------------------------------------
# Minimal mock bus (no pytest dependency)
# ---------------------------------------------------------------------------

class _SmokeBus:
    """Bare-minimum bus mock for smoke testing."""

    async def connect(self) -> None: pass
    async def close(self) -> None: pass
    async def health_check(self) -> bool: return True
    async def ensure_group(self, stream: str, group: str) -> None: pass
    async def publish_to(self, stream: str, model: Any) -> str: return "smoke-id"
    async def publish_signal(self, signal: Any) -> str: return "smoke-id"
    async def publish_decision(self, decision: Any) -> str: return "smoke-id"
    async def publish_risk_check(self, check: Any) -> str: return "smoke-id"
    async def publish_red_team(self, challenge: Any) -> str: return "smoke-id"

    def subscribe_to(self, stream: str, group: str, consumer: str, **kw: Any):
        return self._noop()

    def subscribe_signals(self, group: str, consumer: str, **kw: Any):
        return self._noop()

    def subscribe_decisions(self, group: str, consumer: str, **kw: Any):
        return self._noop()

    def subscribe_risk_checks(self, group: str, consumer: str, **kw: Any):
        return self._noop()

    def subscribe_red_team(self, group: str, consumer: str, **kw: Any):
        return self._noop()

    async def _noop(self):
        import asyncio
        while True:
            await asyncio.sleep(3600)
            yield  # pragma: no cover


passed = 0
failed = 0
errors: list[str] = []


def check(label: str, fn):
    """Run a check, print result, track pass/fail."""
    global passed, failed
    try:
        fn()
        print(f"  \033[32mPASS\033[0m  {label}")
        passed += 1
    except Exception as e:
        print(f"  \033[31mFAIL\033[0m  {label}: {e}")
        failed += 1
        errors.append(f"{label}: {e}")


def main() -> int:
    global passed, failed
    start = time.monotonic()
    bus = _SmokeBus()

    print("\n=== APEX Smoke Test ===\n")

    # -----------------------------------------------------------------------
    # 1. Core imports
    # -----------------------------------------------------------------------
    print("[1] Core imports")

    check("core.models", lambda: __import__("core.models"))
    check("core.message_bus", lambda: __import__("core.message_bus"))
    check("core.database", lambda: __import__("core.database"))
    check("core.logger", lambda: __import__("core.logger"))
    check("config.settings", lambda: __import__("config.settings"))
    check("config.agent_registry", lambda: __import__("config.agent_registry"))
    check("core.gamification", lambda: __import__("core.gamification"))
    check("core.decision_journal", lambda: __import__("core.decision_journal"))

    # -----------------------------------------------------------------------
    # 2. Stream constants
    # -----------------------------------------------------------------------
    print("\n[2] Stream constants")

    from core import message_bus as mb

    EXPECTED_STREAMS = [
        "STREAM_SIGNALS", "STREAM_DECISIONS", "STREAM_RISK_CHECKS", "STREAM_RED_TEAM",
        "STREAM_MARKET_PRICES", "STREAM_PLATFORM_STATE", "STREAM_NEW_LISTING",
        "STREAM_ACCOUNT_HEALTH", "STREAM_EXECUTION_ADVISORY",
        "STREAM_DECISIONS_PENDING", "STREAM_DECISIONS_APPROVED", "STREAM_DECISIONS_REJECTED",
        "STREAM_TRADES_EXECUTED", "STREAM_PORTFOLIO_STATE", "STREAM_RISK_ANOMALY",
        "STREAM_SIGNALS_TECHNICAL", "STREAM_SIGNALS_FUNDING_ARB",
        "STREAM_DATA_ONCHAIN", "STREAM_DATA_MACRO", "STREAM_DATA_SENTIMENT", "STREAM_DATA_REGIME",
        "STREAM_CIO_RESEARCH_TASKS", "STREAM_CIO_RESEARCH_RESULTS",
        "STREAM_CIO_SIGNAL_MATRIX", "STREAM_CIO_PRIORITIES",
        "STREAM_PERFORMANCE_REPORTS", "STREAM_WEIGHT_UPDATES", "STREAM_SHADOW_RESULTS",
        "STREAM_AGENT_RANKINGS",
    ]

    for name in EXPECTED_STREAMS:
        check(
            f"stream {name}",
            lambda n=name: (
                getattr(mb, n),
                assert_(isinstance(getattr(mb, n), str), f"{n} is not a string"),
            ),
        )

    # -----------------------------------------------------------------------
    # 3. Agent registry
    # -----------------------------------------------------------------------
    print("\n[3] Agent registry")

    from config.agent_registry import AGENT_REGISTRY, AgentStatus

    check(
        f"registry has 26 agents (got {len(AGENT_REGISTRY)})",
        lambda: assert_(len(AGENT_REGISTRY) == 26, f"Expected 26, got {len(AGENT_REGISTRY)}"),
    )

    active = [a for a in AGENT_REGISTRY.values() if a.status == AgentStatus.ACTIVE]
    check(
        f"all 26 agents ACTIVE (got {len(active)})",
        lambda: assert_(len(active) == 26, f"Expected 26 active, got {len(active)}"),
    )

    # -----------------------------------------------------------------------
    # 4. Agent imports + instantiation
    # -----------------------------------------------------------------------
    print("\n[4] Agent instantiation (26 agents)")

    AGENT_CLASSES = [
        ("agents.ingestion.market_data", "MarketDataCollector", {}),
        ("agents.ingestion.platform_specialist", "PlatformSpecialist", {}),
        ("agents.ingestion.onchain_intel", "OnChainIntelligence", {}),
        ("agents.ingestion.macro_feed", "MacroFeed", {}),
        ("agents.ingestion.sentiment", "SentimentScraper", {}),
        ("agents.execution.engine", "ExecutionEngine", {"platform_specialist": None}),
        ("agents.execution.position_manager", "PositionManager", {"platform_specialist": None}),
        ("agents.risk.guardian", "RiskGuardian", {}),
        ("agents.risk.anomaly", "AnomalyDetector", {}),
        ("agents.risk.hedging", "HedgingEngine", {}),
        ("agents.analysis.technical", "TechnicalAnalyst", {}),
        ("agents.analysis.funding_arb", "FundingRateArb", {}),
        ("agents.fundamental.valuation", "FundamentalValuation", {}),
        ("agents.on_chain.flow", "OnChainFlow", {}),
        ("agents.sentiment.social", "SentimentSocial", {}),
        ("agents.sentiment.funding", "SentimentFunding", {}),
        ("agents.macro.regime", "MacroRegime", {}),
        ("agents.red_team.challenger", "RedTeamChallenger", {}),
        ("agents.decision.cio", "ChiefInvestmentOfficer", {}),
        ("agents.decision.signal_aggregator", "SignalAggregator", {}),
        ("agents.decision.red_team", "RedTeamStrategist", {}),
        ("agents.decision.portfolio_allocator", "PortfolioAllocator", {}),
        ("agents.decision.regime_classifier", "RegimeClassifier", {}),
        ("agents.meta.orchestrator", "MetaOrchestrator", {}),
        ("agents.meta.performance", "PerformanceAuditor", {}),
        ("agents.meta.strategy_lab", "StrategyLab", {}),
    ]

    agents = []
    for module_path, class_name, kwargs in AGENT_CLASSES:
        def _check(mp=module_path, cn=class_name, kw=kwargs):
            import importlib
            mod = importlib.import_module(mp)
            cls = getattr(mod, cn)
            agent = cls(bus=bus, **kw)
            agents.append(agent)
            assert_(agent.agent_id, f"{cn} has no agent_id")
            assert_(agent.agent_type, f"{cn} has no agent_type")

        check(f"{class_name}", _check)

    # -----------------------------------------------------------------------
    # 5. Agent health checks
    # -----------------------------------------------------------------------
    print("\n[5] Agent health() checks")

    for agent in agents:
        def _check_health(a=agent):
            h = a.health()
            assert_(isinstance(h, dict), "health() must return dict")
            assert_("agent_id" in h, "missing agent_id in health")
            assert_("running" in h, "missing running in health")
            assert_(h["running"] is False, "agent should not be running")

        check(f"{agent.__class__.__name__}.health()", _check_health)

    # -----------------------------------------------------------------------
    # 6. Core models
    # -----------------------------------------------------------------------
    print("\n[6] Core model instantiation")

    from core.models import (
        AgentSignal, TradeDecision, RiskCheck, PortfolioState, Position,
        PriceUpdate, ExecutedTrade, AnomalyAlert, HedgeSuggestion,
        AccountHealth, ExecutionAdvisory, AssetInfo,
        AgentPerformanceReport, WeightUpdate, ShadowTradeResult,
    )

    check("AgentSignal", lambda: AgentSignal(
        agent_id="test", asset="BTC", direction=0.5, conviction=0.8,
        timeframe="intraday",
    ))
    check("TradeDecision", lambda: TradeDecision(
        asset="BTC", action="open_long", size_pct=0.01, consensus_score=0.5,
    ))
    check("RiskCheck", lambda: RiskCheck(decision_id="x", approved=True))
    check("PortfolioState", lambda: PortfolioState())
    check("PriceUpdate", lambda: PriceUpdate(prices={"BTC": 50000}))
    check("ExecutedTrade", lambda: ExecutedTrade(
        decision_id="x", asset="BTC", side="buy", size=0.1, price=50000,
    ))
    check("AnomalyAlert", lambda: AnomalyAlert(
        anomaly_type="price_deviation", severity="critical", asset="BTC",
    ))
    check("HedgeSuggestion", lambda: HedgeSuggestion(
        reason="test", hedge_asset="ETH", hedge_action="open_short",
        hedge_size_pct=0.05,
    ))
    check("AccountHealth", lambda: AccountHealth())
    check("ExecutionAdvisory", lambda: ExecutionAdvisory(asset="BTC"))
    check("AssetInfo", lambda: AssetInfo(
        asset="BTC", sz_decimals=5, max_leverage=50,
    ))
    check("AgentPerformanceReport", lambda: AgentPerformanceReport(
        agent_id="test", window_days=30, total_signals=10,
        signals_traded=5, signal_to_trade_conversion=0.5,
        win_rate=0.6, avg_return_bps=10.0, sharpe_ratio=1.5,
        max_drawdown_contribution=0.01, total_pnl=1000.0,
        recommended_weight=0.1,
    ))
    check("WeightUpdate", lambda: WeightUpdate(
        weights={"test": 0.1}, reason="smoke",
    ))
    check("ShadowTradeResult", lambda: ShadowTradeResult(
        signal_id="x", agent_id="test", asset="BTC",
        direction=0.5, entry_price=50000, exit_price=51000,
        holding_period_hours=24, return_bps=200,
    ))

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    elapsed = time.monotonic() - start
    total = passed + failed
    print(f"\n{'='*50}")
    print(f"  {passed}/{total} checks passed in {elapsed:.1f}s")
    if failed:
        print(f"  \033[31m{failed} FAILED:\033[0m")
        for e in errors:
            print(f"    - {e}")
        print(f"{'='*50}\n")
        return 1
    else:
        print(f"  \033[32mAll checks passed.\033[0m")
        print(f"{'='*50}\n")
        return 0


def assert_(condition: bool, msg: str = "") -> None:
    if not condition:
        raise AssertionError(msg)


if __name__ == "__main__":
    sys.exit(main())
