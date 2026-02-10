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
