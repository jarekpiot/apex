"""
APEX global configuration loaded from environment / .env file.

All secrets and tunables live here so the rest of the codebase never
touches ``os.environ`` directly.
"""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for every configurable knob in APEX."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -- Exchange: Hyperliquid ------------------------------------------------
    hyperliquid_api_key: SecretStr = SecretStr("")
    hyperliquid_api_secret: SecretStr = SecretStr("")
    hyperliquid_vault_address: str = ""
    hyperliquid_testnet: bool = True

    # -- AI / LLM -------------------------------------------------------------
    anthropic_api_key: SecretStr = SecretStr("")

    # -- Market-data providers ------------------------------------------------
    fred_api_key: SecretStr = SecretStr("")
    coinglass_api_key: SecretStr = SecretStr("")
    coingecko_api_key: SecretStr = SecretStr("")
    defillama_base_url: str = "https://api.llama.fi"
    newsapi_key: SecretStr = SecretStr("")
    dune_api_key: SecretStr = SecretStr("")

    # -- Social feeds ---------------------------------------------------------
    reddit_client_id: SecretStr = SecretStr("")
    reddit_client_secret: SecretStr = SecretStr("")
    twitter_bearer_token: SecretStr = SecretStr("")

    # -- Infrastructure -------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://apex:apex@localhost:5432/apex"

    # -- Logging --------------------------------------------------------------
    log_level: str = "INFO"

    # -- Risk / Trading limits ------------------------------------------------
    max_position_pct: float = Field(
        default=0.02,
        description="Maximum single-position size as fraction of NAV.",
    )
    max_daily_drawdown: float = Field(
        default=0.10,
        description="Halt new entries if daily drawdown exceeds this fraction.",
    )
    circuit_breaker_drawdown: float = Field(
        default=0.15,
        description="Emergency flatten threshold as fraction of NAV.",
    )
    max_gross_exposure: float = Field(
        default=3.0,
        description="Maximum gross leverage (sum of abs positions / NAV).",
    )

    # -- Execution ------------------------------------------------------------
    paper_trade_mode: bool = Field(
        default=True,
        description="When True, orders are simulated locally instead of sent to exchange.",
    )
    slippage_threshold_bps: float = Field(
        default=50.0,
        description="Reject orders if estimated slippage exceeds this (basis points).",
    )
    twap_default_splits: int = Field(
        default=5,
        description="Default number of child orders for TWAP execution.",
    )
    twap_default_duration_secs: int = Field(
        default=300,
        description="Default duration for TWAP execution in seconds.",
    )
    iceberg_show_pct: float = Field(
        default=0.2,
        description="Fraction of total size shown on book for iceberg orders.",
    )
    margin_utilisation_warn: float = Field(
        default=0.80,
        description="Warn when margin utilisation exceeds this fraction.",
    )
    liquidation_distance_warn: float = Field(
        default=0.20,
        description="Alert when distance-to-liquidation drops below this fraction.",
    )

    # -- Risk Guardian ---------------------------------------------------------
    max_net_exposure: float = Field(
        default=1.0,
        description="Maximum net exposure as fraction of NAV.",
    )
    max_correlation_threshold: float = Field(
        default=0.80,
        description="Reject if 14d Pearson correlation with existing positions exceeds this.",
    )
    max_single_asset_class_pct: float = Field(
        default=0.40,
        description="Max single asset class exposure as fraction of NAV.",
    )
    weekly_drawdown_pause: float = Field(
        default=0.20,
        description="Pause trading 24h if weekly drawdown exceeds this.",
    )
    monthly_drawdown_review: float = Field(
        default=0.25,
        description="Enter review mode if monthly drawdown exceeds this.",
    )
    drawdown_size_reduction: float = Field(
        default=0.50,
        description="Reduce new position sizes by this fraction when daily dd > soft limit.",
    )
    defensive_position_reduction: float = Field(
        default=0.30,
        description="Reduce all positions by this fraction in defensive mode.",
    )
    defensive_pause_minutes: float = Field(
        default=15.0,
        description="Pause new entries for this many minutes in defensive mode.",
    )
    correlation_lookback_days: int = Field(
        default=14,
        description="Rolling window in days for Pearson correlation calculation.",
    )

    # -- Anomaly Detector -----------------------------------------------------
    volume_spike_multiplier: float = Field(
        default=5.0,
        description="Volume spike detection: trigger when volume > N * rolling avg.",
    )
    price_zscore_threshold: float = Field(
        default=3.0,
        description="Price anomaly: trigger when z-score exceeds this in 5 min.",
    )
    book_imbalance_threshold: float = Field(
        default=10.0,
        description="Order book imbalance ratio to trigger anomaly.",
    )
    funding_rate_extreme: float = Field(
        default=0.001,
        description="Funding rate absolute threshold per 8h period.",
    )
    api_latency_threshold_ms: float = Field(
        default=2000.0,
        description="HL API latency threshold in milliseconds.",
    )

    # -- Hedging Engine -------------------------------------------------------
    hedge_net_exposure_threshold: float = Field(
        default=0.50,
        description="Trigger hedge suggestions when net exposure exceeds this.",
    )
    hedge_tail_risk_pct: float = Field(
        default=0.20,
        description="Fraction of portfolio to hedge in tail-risk mode.",
    )

    # -- Technical Analysis ---------------------------------------------------
    ta_watchlist_size: int = Field(
        default=30,
        description="Top N assets by volume/liquidity to analyse.",
    )
    ta_short_tf_interval: int = Field(
        default=60,
        description="Analysis interval for 5m/15m timeframes (seconds).",
    )
    ta_long_tf_interval: int = Field(
        default=300,
        description="Analysis interval for 1h/4h/1d timeframes (seconds).",
    )
    ta_min_candles: int = Field(
        default=200,
        description="Minimum candles needed before running indicators.",
    )
    ta_adx_threshold: float = Field(
        default=25.0,
        description="ADX above this indicates a trending market.",
    )
    ta_rsi_overbought: float = Field(
        default=70.0,
        description="RSI overbought threshold.",
    )
    ta_rsi_oversold: float = Field(
        default=30.0,
        description="RSI oversold threshold.",
    )

    # -- Funding Rate Arbitrage -----------------------------------------------
    funding_arb_min_rate_diff: float = Field(
        default=0.0001,
        description="Min funding rate diff per 8h for arb signal (0.01%).",
    )
    funding_arb_scan_interval: int = Field(
        default=300,
        description="Funding arb scan interval in seconds.",
    )

    # -- Meta Orchestrator ----------------------------------------------------
    orch_voting_interval: int = Field(
        default=30,
        description="Seconds between consensus voting rounds.",
    )
    orch_min_consensus: float = Field(
        default=0.30,
        description="Minimum |consensus| score to generate a trade decision.",
    )
    orch_min_quorum: int = Field(
        default=2,
        description="Minimum distinct agents that must vote on an asset.",
    )
    orch_decision_cooldown: int = Field(
        default=300,
        description="Seconds between decisions for the same asset.",
    )
    orch_max_signal_age: int = Field(
        default=3600,
        description="Hard cap on signal age (seconds) even without expires_at.",
    )
    orch_base_size_pct: float = Field(
        default=0.01,
        description="Base position size (scaled by consensus × conviction).",
    )
    orch_flip_consensus: float = Field(
        default=0.60,
        description="Minimum |consensus| to FLIP an opposing position (vs CLOSE).",
    )

    # -- Fundamental Valuation ------------------------------------------------
    fundamental_scan_interval: int = Field(
        default=900,
        description="Fundamental valuation scan interval in seconds (15 min).",
    )

    # -- On-Chain Flow --------------------------------------------------------
    on_chain_scan_interval: int = Field(
        default=600,
        description="On-chain TVL / stablecoin scan interval in seconds (10 min).",
    )

    # -- Sentiment ------------------------------------------------------------
    sentiment_scan_interval: int = Field(
        default=300,
        description="Sentiment (social + funding) scan interval in seconds (5 min).",
    )

    # -- Macro Regime ---------------------------------------------------------
    macro_scan_interval: int = Field(
        default=1800,
        description="Macro regime scan interval in seconds (30 min).",
    )

    # -- Data ingestion: On-Chain Intelligence --------------------------------
    onchain_tvl_interval: int = Field(default=900, description="TVL poll interval (seconds).")
    onchain_dex_interval: int = Field(default=600, description="DEX volume poll interval (seconds).")
    onchain_stablecoin_interval: int = Field(default=3600, description="Stablecoin poll interval (seconds).")
    onchain_trending_interval: int = Field(default=300, description="Trending tokens poll interval (seconds).")
    onchain_fear_greed_interval: int = Field(default=3600, description="Fear & Greed poll interval (seconds).")
    onchain_unlocks_interval: int = Field(default=3600, description="Token unlocks poll interval (seconds).")

    # -- Data ingestion: Macro Feed -------------------------------------------
    macro_feed_fred_interval: int = Field(default=3600, description="FRED data poll interval (seconds).")
    macro_feed_yfinance_interval: int = Field(default=60, description="Yahoo Finance poll interval (seconds).")
    macro_feed_calendar_interval: int = Field(default=3600, description="Economic calendar poll interval (seconds).")

    # -- Data ingestion: Sentiment Scraper ------------------------------------
    sentiment_reddit_interval: int = Field(default=300, description="Reddit scrape interval (seconds).")
    sentiment_cryptopanic_interval: int = Field(default=300, description="CryptoPanic poll interval (seconds).")
    sentiment_trends_interval: int = Field(default=1800, description="Google Trends poll interval (seconds).")
    sentiment_lunarcrush_interval: int = Field(default=600, description="LunarCrush poll interval (seconds).")

    # -- Data ingestion: API keys ---------------------------------------------
    cryptopanic_api_key: SecretStr = SecretStr("")
    lunarcrush_api_key: SecretStr = SecretStr("")

    # -- Platform polling intervals (seconds) ---------------------------------
    account_health_interval: float = 10.0
    asset_registry_interval: float = 300.0
    portfolio_state_interval: float = 10.0


# Module-level singleton — import ``settings`` everywhere.
settings = Settings()
