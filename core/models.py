"""
Canonical Pydantic v2 domain models shared across every APEX component.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class TradeAction(StrEnum):
    OPEN_LONG = "open_long"
    OPEN_SHORT = "open_short"
    CLOSE = "close"
    REDUCE = "reduce"
    FLIP = "flip"


class ChallengeType(StrEnum):
    """Categories of red-team challenges."""
    CONTRARIAN_DATA = "contrarian_data"
    REGIME_MISMATCH = "regime_mismatch"
    LIQUIDITY_CONCERN = "liquidity_concern"
    CORRELATION_RISK = "correlation_risk"
    TIMING_RISK = "timing_risk"
    BLACK_SWAN = "black_swan"


class RedTeamRecommendation(StrEnum):
    PROCEED = "proceed"
    REDUCE_CONVICTION = "reduce_conviction"
    PAUSE_AND_REVIEW = "pause_and_review"
    VETO = "veto"


class Timeframe(StrEnum):
    SCALP = "scalp"         # minutes
    INTRADAY = "intraday"   # hours
    SWING = "swing"         # days
    POSITION = "position"   # weeks+


class ExecutionMode(StrEnum):
    MARKET = "market"
    LIMIT = "limit"
    TWAP = "twap"
    ICEBERG = "iceberg"


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AssetClass(StrEnum):
    """Broad asset classification for concentration checks."""
    CRYPTO_MAJOR = "crypto_major"     # BTC, ETH
    CRYPTO_ALT = "crypto_alt"         # SOL, AVAX, DOGE, …
    EQUITY = "equity"                 # SPY, QQQ, AAPL, …
    COMMODITY = "commodity"           # GOLD, SILVER, OIL, …
    FX = "fx"                         # EUR, GBP, JPY, …
    UNKNOWN = "unknown"


class AnomalyType(StrEnum):
    VOLUME_SPIKE = "volume_spike"
    PRICE_DEVIATION = "price_deviation"
    BOOK_IMBALANCE = "book_imbalance"
    FUNDING_EXTREME = "funding_extreme"
    CORRELATION_SPIKE = "correlation_spike"
    API_LATENCY = "api_latency"


class MarketRegime(StrEnum):
    """Market regime classifications from the RegimeClassifier."""
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    MEAN_REVERTING = "mean_reverting"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"


class ICDecision(StrEnum):
    """Investment Committee final decision on a thesis."""
    APPROVE = "approve"
    REJECT = "reject"
    REDUCE = "reduce"
    DEFER = "defer"


class ChallengeCategory(StrEnum):
    """Red-team challenge categories (v2 — LLM-powered)."""
    COUNTER_EVIDENCE = "counter_evidence"
    ASSUMPTION_FLAW = "assumption_flaw"
    MISSING_CONTEXT = "missing_context"
    ADVERSE_SCENARIO = "adverse_scenario"
    TIMING_RISK = "timing_risk"
    SURVIVORSHIP_BIAS = "survivorship_bias"
    CROWDED_TRADE = "crowded_trade"


class PositionAction(StrEnum):
    """CIO portfolio review action for open positions."""
    HOLD = "hold"
    TIGHTEN_STOP = "tighten_stop"
    REDUCE_SIZE = "reduce_size"
    ADD_TO_POSITION = "add_to_position"
    CLOSE_POSITION = "close_position"


# ---------------------------------------------------------------------------
# Core domain models
# ---------------------------------------------------------------------------

class AgentSignal(BaseModel):
    """A directional opinion emitted by a single agent."""

    signal_id: str = Field(default_factory=_new_id)
    agent_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str = Field(description="Ticker symbol, e.g. 'BTC', 'ETH', 'SPY'.")
    direction: float = Field(
        ge=-1.0,
        le=1.0,
        description="-1 = max short conviction, +1 = max long conviction.",
    )
    conviction: float = Field(
        ge=0.0,
        le=1.0,
        description="Strength of belief in the signal (0 = uncertain, 1 = very high).",
    )
    timeframe: Timeframe
    reasoning: str = Field(default="", description="Human-readable rationale.")
    data_sources: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime | None = Field(
        default=None,
        description="Signal is stale after this time.",
    )


class TradeDecision(BaseModel):
    """An aggregated, risk-checked decision ready for execution."""

    decision_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    action: TradeAction
    size_pct: float = Field(
        ge=0.0,
        le=1.0,
        description="Position size as fraction of NAV.",
    )
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    contributing_signals: list[str] = Field(
        default_factory=list,
        description="signal_ids that contributed to this decision.",
    )
    consensus_score: float = Field(
        ge=-1.0,
        le=1.0,
        description="Weighted aggregate direction from voting agents.",
    )
    risk_approved: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class RedTeamChallenge(BaseModel):
    """A devil's-advocate challenge against a proposed trade."""

    challenge_id: str = Field(default_factory=_new_id)
    decision_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    challenge_type: ChallengeType
    severity: float = Field(
        ge=0.0,
        le=1.0,
        description="0 = minor concern, 1 = critical objection.",
    )
    counter_thesis: str
    evidence: list[str] = Field(default_factory=list)
    recommendation: RedTeamRecommendation
    conviction_adjustment: float = Field(
        default=0.0,
        ge=-1.0,
        le=0.0,
        description="Suggested reduction to conviction (always <= 0).",
    )


class RiskCheck(BaseModel):
    """Risk manager's verdict on a proposed trade."""

    check_id: str = Field(default_factory=_new_id)
    decision_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    approved: bool
    adjustments: dict[str, Any] = Field(
        default_factory=dict,
        description="Any modifications risk wants (e.g. reduced size).",
    )
    portfolio_impact: dict[str, Any] = Field(
        default_factory=dict,
        description="Projected exposure / drawdown after the trade.",
    )
    veto_reason: str | None = None


class Position(BaseModel):
    """A single open position in the portfolio."""

    asset: str
    direction: Direction
    size_pct: float
    entry_price: float
    current_price: float
    unrealised_pnl: float
    opened_at: datetime


class PortfolioState(BaseModel):
    """Real-time snapshot of portfolio health."""

    timestamp: datetime = Field(default_factory=_utcnow)
    positions: list[Position] = Field(default_factory=list)
    total_nav: float = 0.0
    daily_pnl: float = 0.0
    drawdown: float = Field(
        default=0.0,
        ge=0.0,
        description="Current drawdown from HWM as a positive fraction.",
    )
    gross_exposure: float = 0.0
    net_exposure: float = 0.0


# ---------------------------------------------------------------------------
# Sprint 2 — Market data & platform models
# ---------------------------------------------------------------------------

class PriceUpdate(BaseModel):
    """Tick-level mid-price update published to market:prices."""

    timestamp: datetime = Field(default_factory=_utcnow)
    prices: dict[str, float] = Field(
        default_factory=dict,
        description="Asset -> mid-price mapping.",
    )


class AssetInfo(BaseModel):
    """Metadata for a single tradeable perp on Hyperliquid."""

    asset: str
    sz_decimals: int
    max_leverage: int
    margin_requirement: float = 0.0
    tick_size: float = 0.0
    lot_size: float = 0.0
    open_interest: float = 0.0
    volume_24h: float = 0.0
    funding_rate: float = 0.0
    mark_price: float = 0.0
    is_new: bool = False


class AccountHealth(BaseModel):
    """Wallet and margin health snapshot published to platform:account_health."""

    timestamp: datetime = Field(default_factory=_utcnow)
    equity: float = 0.0
    margin_used: float = 0.0
    margin_available: float = 0.0
    margin_utilisation: float = 0.0
    withdrawable: float = 0.0
    cross_margin_ratio: float = 0.0
    distance_to_liquidation: float = 1.0
    severity: AlertSeverity = AlertSeverity.INFO


class ExecutionAdvisory(BaseModel):
    """Platform-specific execution guidance published to platform:execution_advisory."""

    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    bid_depth_usd: float = 0.0
    ask_depth_usd: float = 0.0
    spread_bps: float = 0.0
    recommended_order_type: str = "limit"
    max_size_no_impact: float = 0.0
    maker_fee_bps: float = 0.0
    taker_fee_bps: float = 0.0
    notes: str = ""


class NewListingAlert(BaseModel):
    """Alert when a new perp is detected on Hyperliquid."""

    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    max_leverage: int = 1
    sz_decimals: int = 0
    detected_at: datetime = Field(default_factory=_utcnow)


class ExecutedTrade(BaseModel):
    """Confirmation of a filled trade published to trades:executed."""

    trade_id: str = Field(default_factory=_new_id)
    decision_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    side: str
    size: float
    price: float
    fee: float = 0.0
    slippage_bps: float = 0.0
    exchange_order_id: str = ""
    execution_mode: ExecutionMode = ExecutionMode.MARKET
    paper_trade: bool = False


class StopLevel(BaseModel):
    """Stop-loss configuration for a position."""

    price: float
    trailing: bool = False
    trail_pct: float = 0.0
    highest_price: float = 0.0


class TakeProfitLevel(BaseModel):
    """A single take-profit target."""

    price: float
    close_pct: float = Field(
        description="Fraction of position to close at this TP level.",
    )
    triggered: bool = False


# ---------------------------------------------------------------------------
# Sprint 3 — Risk layer models
# ---------------------------------------------------------------------------

class AnomalyAlert(BaseModel):
    """Market anomaly detected by the AnomalyDetector."""

    alert_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    anomaly_type: AnomalyType
    severity: AlertSeverity
    asset: str = ""
    description: str = ""
    value: float = 0.0
    threshold: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class HedgeSuggestion(BaseModel):
    """A hedge trade proposed by the HedgingEngine."""

    suggestion_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    reason: str
    hedge_asset: str
    hedge_action: TradeAction
    hedge_size_pct: float = Field(ge=0.0, le=1.0)
    current_net_exposure: float = 0.0
    target_net_exposure: float = 0.0
    correlation_to_portfolio: float = 0.0


# ---------------------------------------------------------------------------
# Data ingestion models
# ---------------------------------------------------------------------------

class OnChainDataPoint(BaseModel):
    """Aggregated on-chain metrics from multiple providers."""

    timestamp: datetime = Field(default_factory=_utcnow)
    source: str = "onchain_intel"
    protocol_tvls: dict[str, float] = Field(default_factory=dict)
    chain_tvls: dict[str, float] = Field(default_factory=dict)
    total_stablecoin_mcap: float = 0.0
    stablecoin_breakdown: dict[str, float] = Field(default_factory=dict)
    dex_volumes_24h: dict[str, float] = Field(default_factory=dict)
    total_dex_volume_24h: float = 0.0
    bridge_volumes_24h: dict[str, float] = Field(default_factory=dict)
    trending_tokens: list[dict[str, Any]] = Field(default_factory=list)
    market_cap_changes: dict[str, float] = Field(default_factory=dict)
    volume_spikes: dict[str, float] = Field(default_factory=dict)
    fear_greed_index: int = 0
    fear_greed_label: str = ""
    upcoming_unlocks: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MacroDataPoint(BaseModel):
    """Aggregated macro-economic indicators."""

    timestamp: datetime = Field(default_factory=_utcnow)
    source: str = "macro_feed"
    fed_funds_rate: float | None = None
    cpi_yoy: float | None = None
    m2_money_supply: float | None = None
    us10y_yield: float | None = None
    us2y_yield: float | None = None
    yield_curve_spread: float | None = None
    dxy_index: float | None = None
    spy_price: float | None = None
    qqq_price: float | None = None
    gld_price: float | None = None
    uso_price: float | None = None
    dxy_price: float | None = None
    vix_price: float | None = None
    crypto_fear_greed: int = 0
    upcoming_events: list[dict[str, Any]] = Field(default_factory=list)
    us_market_open: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class SentimentDataPoint(BaseModel):
    """Per-asset sentiment aggregation from multiple sources."""

    timestamp: datetime = Field(default_factory=_utcnow)
    source: str = "sentiment_scraper"
    asset: str = ""
    reddit_post_count: int = 0
    reddit_sentiment_score: float = 0.0
    reddit_bullish_pct: float = 0.0
    news_count: int = 0
    news_sentiment_score: float = 0.0
    news_positive_pct: float = 0.0
    news_negative_pct: float = 0.0
    google_trends_interest: float = 0.0
    google_trends_change_pct: float = 0.0
    galaxy_score: float = 0.0
    social_volume: float = 0.0
    social_sentiment: float = 0.0
    composite_sentiment: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# CIO / Decision layer models
# ---------------------------------------------------------------------------

class RegimeState(BaseModel):
    """Market regime classification from the RegimeClassifier."""

    timestamp: datetime = Field(default_factory=_utcnow)
    regime: MarketRegime = MarketRegime.LOW_VOLATILITY
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    previous_regime: MarketRegime | None = None
    regime_duration_hours: float = 0.0
    features: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MarketBrief(BaseModel):
    """CIO's synthesised market overview produced every briefing cycle."""

    brief_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    regime: MarketRegime = MarketRegime.LOW_VOLATILITY
    regime_confidence: float = 0.0
    themes: list[str] = Field(default_factory=list)
    opportunities: list[dict[str, Any]] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    information_gaps: list[str] = Field(default_factory=list)
    summary: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchTask(BaseModel):
    """A research assignment dispatched by the CIO to a specific agent."""

    task_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    target_agent: str = Field(description="agent_id of the researcher")
    question: str
    context: str = ""
    asset: str = ""
    deadline_seconds: int = Field(default=120)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchResult(BaseModel):
    """Response from an agent to a CIO-assigned research task."""

    result_id: str = Field(default_factory=_new_id)
    task_id: str
    agent_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    findings: str
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    supporting_data: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class InvestmentThesis(BaseModel):
    """A fully-formed trade thesis generated by the CIO."""

    thesis_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    direction: Direction
    edge_description: str = ""
    catalyst: str = ""
    supporting_signals: list[str] = Field(default_factory=list)
    risk_factors: list[str] = Field(default_factory=list)
    invalidation_criteria: list[str] = Field(default_factory=list)
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    timeframe: Timeframe = Timeframe.INTRADAY
    conviction: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExpertOpinion(BaseModel):
    """An expert opinion submitted during the IC review phase."""

    agent_id: str
    role: str = ""
    assessment: str = ""
    conviction: float = Field(default=0.5, ge=0.0, le=1.0)
    concerns: list[str] = Field(default_factory=list)
    supports_thesis: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class RedTeamChallengeV2(BaseModel):
    """LLM-powered adversarial challenge (v2 — used by RedTeamStrategist)."""

    challenge_id: str = Field(default_factory=_new_id)
    thesis_id: str = ""
    timestamp: datetime = Field(default_factory=_utcnow)
    category: ChallengeCategory = ChallengeCategory.COUNTER_EVIDENCE
    severity: float = Field(default=0.5, ge=0.0, le=1.0)
    challenge_text: str = ""
    counter_evidence: list[str] = Field(default_factory=list)
    recommendation: RedTeamRecommendation = RedTeamRecommendation.PROCEED
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TradeProposal(BaseModel):
    """A sized trade proposal output by the PortfolioAllocator."""

    proposal_id: str = Field(default_factory=_new_id)
    thesis_id: str = ""
    timestamp: datetime = Field(default_factory=_utcnow)
    asset: str
    action: TradeAction
    size_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    kelly_fraction: float = 0.0
    conviction_adjusted_size: float = 0.0
    reasoning: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class InvestmentCommitteeRecord(BaseModel):
    """Master record of a full Investment Committee debate."""

    record_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    thesis: InvestmentThesis | None = None
    expert_opinions: list[ExpertOpinion] = Field(default_factory=list)
    red_team_challenges: list[RedTeamChallengeV2] = Field(default_factory=list)
    cio_decision: ICDecision = ICDecision.DEFER
    cio_reasoning: str = ""
    final_proposal: TradeProposal | None = None
    debate_duration_seconds: float = 0.0
    consecutive_red_team_overrides: int = 0
    human_review_required: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class StrategyPriority(BaseModel):
    """CIO's current strategic priorities broadcast to all agents."""

    timestamp: datetime = Field(default_factory=_utcnow)
    priority_level: int = Field(default=1, ge=1, le=5)
    focus_assets: list[str] = Field(default_factory=list)
    regime_context: MarketRegime = MarketRegime.LOW_VOLATILITY
    risk_budget_pct: float = Field(default=1.0, ge=0.0, le=1.0)
    notes: str = ""
    avoid_assets: list[str] = Field(default_factory=list)
    preferred_timeframes: list[Timeframe] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DailySummary(BaseModel):
    """End-of-day performance summary generated by the CIO."""

    summary_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    date: str = ""
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    trades_executed: int = 0
    trades_won: int = 0
    trades_lost: int = 0
    best_trade: str = ""
    worst_trade: str = ""
    agent_grades: dict[str, str] = Field(default_factory=dict)
    lessons_learned: list[str] = Field(default_factory=list)
    regime_summary: str = ""
    tomorrow_priorities: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class PositionReview(BaseModel):
    """CIO's review verdict for a single open position."""

    asset: str
    action: PositionAction = PositionAction.HOLD
    reasoning: str = ""
    new_stop_loss: float | None = None
    size_adjustment_pct: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)
