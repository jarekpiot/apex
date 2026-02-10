"""
Central registry describing every agent in the APEX system.

Each entry declares the agent's identity, type, default consensus weight,
and operational status so the orchestrator can decide which agents
participate in signal aggregation.
"""

from __future__ import annotations

from enum import StrEnum
from typing import NamedTuple


class AgentType(StrEnum):
    """Broad classification of what an agent does."""

    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    SENTIMENT = "sentiment"
    MACRO = "macro"
    ON_CHAIN = "on_chain"
    RISK = "risk"
    RED_TEAM = "red_team"
    EXECUTION = "execution"
    INGESTION = "ingestion"
    META = "meta"


class AgentStatus(StrEnum):
    """Operational mode for an agent."""

    ACTIVE = "active"       # Signals feed into live trading decisions.
    PAPER = "paper"         # Signals recorded but not acted on.
    DISABLED = "disabled"   # Agent is not started.


class AgentEntry(NamedTuple):
    """Immutable descriptor for a registered agent."""

    agent_id: str
    agent_type: AgentType
    description: str
    default_weight: float   # 0.0–1.0 weight in consensus aggregation.
    status: AgentStatus


# ---------------------------------------------------------------------------
# Master list — add new agents here.
# ---------------------------------------------------------------------------
AGENT_REGISTRY: dict[str, AgentEntry] = {
    e.agent_id: e
    for e in [
        # ---- Technical analysis ----
        AgentEntry(
            agent_id="technical_analyst",
            agent_type=AgentType.TECHNICAL,
            description="Multi-TF TA (RSI, MACD, BB, ADX, EMAs, volume profile, S/R, StochRSI).",
            default_weight=0.20,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="funding_arb",
            agent_type=AgentType.TECHNICAL,
            description="Cross-exchange funding rate arbitrage scanner (HL vs Binance/Bybit).",
            default_weight=0.10,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Fundamental / on-chain ----
        AgentEntry(
            agent_id="fundamental_valuation",
            agent_type=AgentType.FUNDAMENTAL,
            description="Token valuation models (NVT, MVRV, fees-to-mcap).",
            default_weight=0.10,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="on_chain_flow",
            agent_type=AgentType.ON_CHAIN,
            description="Exchange flows, whale wallet tracking, supply distribution.",
            default_weight=0.10,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Sentiment ----
        AgentEntry(
            agent_id="sentiment_social",
            agent_type=AgentType.SENTIMENT,
            description="Social-media sentiment from Reddit, Twitter, and news headlines.",
            default_weight=0.08,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="sentiment_funding",
            agent_type=AgentType.SENTIMENT,
            description="Funding-rate and open-interest skew as sentiment proxy.",
            default_weight=0.07,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Macro ----
        AgentEntry(
            agent_id="macro_regime",
            agent_type=AgentType.MACRO,
            description="Macro regime detection (DXY, yields, FRED data, risk-on/off).",
            default_weight=0.10,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Risk & red-team ----
        AgentEntry(
            agent_id="risk_guardian",
            agent_type=AgentType.RISK,
            description="Portfolio-level risk gating: drawdown, exposure, correlation, asset-class concentration.",
            default_weight=0.0,   # Risk doesn't vote; it vetoes.
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="anomaly_detector",
            agent_type=AgentType.RISK,
            description="Real-time market anomaly detection: volume spikes, flash moves, book imbalance.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="hedging_engine",
            agent_type=AgentType.RISK,
            description="Automatic portfolio hedging when net exposure exceeds threshold; tail-risk protection.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="red_team",
            agent_type=AgentType.RED_TEAM,
            description="Devil's advocate — challenges every proposed trade with counter-thesis.",
            default_weight=0.0,   # Red-team adjusts conviction, not direction.
            status=AgentStatus.ACTIVE,
        ),

        # ---- Ingestion / platform ----
        AgentEntry(
            agent_id="market_data",
            agent_type=AgentType.INGESTION,
            description="WebSocket market data collector — OHLCV candles, L2 book, trades.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="platform_specialist",
            agent_type=AgentType.INGESTION,
            description="Hyperliquid platform intelligence — asset registry, fees, account health, bridge.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Execution ----
        AgentEntry(
            agent_id="execution_engine",
            agent_type=AgentType.EXECUTION,
            description="Smart order routing and execution on Hyperliquid (MARKET/LIMIT/TWAP/ICEBERG).",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),
        AgentEntry(
            agent_id="position_manager",
            agent_type=AgentType.EXECUTION,
            description="Position tracking, stop/TP management, portfolio state, circuit breaker.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),

        # ---- Meta / ensemble ----
        AgentEntry(
            agent_id="meta_orchestrator",
            agent_type=AgentType.META,
            description="Aggregates signals, runs consensus, dispatches to risk + red-team.",
            default_weight=0.0,
            status=AgentStatus.ACTIVE,
        ),
    ]
}


def active_agents() -> list[AgentEntry]:
    """Return only agents whose status is ACTIVE."""
    return [a for a in AGENT_REGISTRY.values() if a.status == AgentStatus.ACTIVE]


def paper_agents() -> list[AgentEntry]:
    """Return agents in PAPER (shadow) mode."""
    return [a for a in AGENT_REGISTRY.values() if a.status == AgentStatus.PAPER]


def voting_agents() -> list[AgentEntry]:
    """Return active agents that participate in consensus (weight > 0)."""
    return [a for a in active_agents() if a.default_weight > 0]
