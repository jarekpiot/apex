-- ==========================================================================
-- APEX — TimescaleDB schema
-- Run once against a freshly-created database:
--   psql -U apex -d apex -f data/schema.sql
-- ==========================================================================

-- Enable TimescaleDB extension (idempotent).
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- --------------------------------------------------------------------------
-- OHLCV market data (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv_data (
    id          BIGSERIAL       NOT NULL,
    ts          TIMESTAMPTZ     NOT NULL,
    asset       VARCHAR(32)     NOT NULL,
    timeframe   VARCHAR(8)      NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('ohlcv_data', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_ohlcv_asset_ts
    ON ohlcv_data (asset, ts DESC);

-- --------------------------------------------------------------------------
-- Agent signals
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_signals (
    id              BIGSERIAL       NOT NULL,
    signal_id       VARCHAR(64)     NOT NULL UNIQUE,
    agent_id        VARCHAR(64)     NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    asset           VARCHAR(32)     NOT NULL,
    direction       DOUBLE PRECISION NOT NULL,
    conviction      DOUBLE PRECISION NOT NULL,
    timeframe       VARCHAR(16)     NOT NULL,
    reasoning       TEXT            DEFAULT '',
    data_sources    JSONB           DEFAULT '[]'::jsonb,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    expires_at      TIMESTAMPTZ,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('agent_signals', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_signals_agent_ts
    ON agent_signals (agent_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_signals_asset_ts
    ON agent_signals (asset, ts DESC);

-- --------------------------------------------------------------------------
-- Trade decisions
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trade_decisions (
    id                      BIGSERIAL       NOT NULL,
    decision_id             VARCHAR(64)     NOT NULL UNIQUE,
    ts                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    asset                   VARCHAR(32)     NOT NULL,
    action                  VARCHAR(16)     NOT NULL,
    size_pct                DOUBLE PRECISION NOT NULL,
    entry_price             DOUBLE PRECISION,
    stop_loss               DOUBLE PRECISION,
    take_profit             DOUBLE PRECISION,
    contributing_signals    JSONB           DEFAULT '[]'::jsonb,
    consensus_score         DOUBLE PRECISION NOT NULL,
    risk_approved           BOOLEAN         DEFAULT FALSE,
    metadata                JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('trade_decisions', 'ts', if_not_exists => TRUE);

-- --------------------------------------------------------------------------
-- Executed trades
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS executed_trades (
    id                  BIGSERIAL       NOT NULL,
    trade_id            VARCHAR(64)     NOT NULL UNIQUE,
    decision_id         VARCHAR(64)     NOT NULL,
    ts                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    asset               VARCHAR(32)     NOT NULL,
    side                VARCHAR(8)      NOT NULL,
    size                DOUBLE PRECISION NOT NULL,
    price               DOUBLE PRECISION NOT NULL,
    fee                 DOUBLE PRECISION DEFAULT 0.0,
    slippage_bps        DOUBLE PRECISION DEFAULT 0.0,
    exchange_order_id   VARCHAR(128)    DEFAULT '',
    metadata            JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('executed_trades', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_decision
    ON executed_trades (decision_id, ts DESC);

-- --------------------------------------------------------------------------
-- Portfolio snapshots (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    total_nav       DOUBLE PRECISION NOT NULL,
    daily_pnl       DOUBLE PRECISION DEFAULT 0.0,
    drawdown        DOUBLE PRECISION DEFAULT 0.0,
    gross_exposure  DOUBLE PRECISION DEFAULT 0.0,
    net_exposure    DOUBLE PRECISION DEFAULT 0.0,
    positions       JSONB           DEFAULT '[]'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('portfolio_snapshots', 'ts', if_not_exists => TRUE);

-- --------------------------------------------------------------------------
-- Performance metrics (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS performance_metrics (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    metric_name     VARCHAR(64)     NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('performance_metrics', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_perf_metric_ts
    ON performance_metrics (metric_name, ts DESC);

-- --------------------------------------------------------------------------
-- On-chain metrics (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onchain_metrics (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    source          VARCHAR(64)     NOT NULL,
    metric_name     VARCHAR(128)    NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    asset           VARCHAR(64)     DEFAULT '',
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('onchain_metrics', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_onchain_source_ts
    ON onchain_metrics (source, ts DESC);

CREATE INDEX IF NOT EXISTS idx_onchain_metric_ts
    ON onchain_metrics (metric_name, ts DESC);

-- --------------------------------------------------------------------------
-- Macro indicators (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_indicators (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    source          VARCHAR(64)     NOT NULL,
    indicator_name  VARCHAR(128)    NOT NULL,
    indicator_value DOUBLE PRECISION NOT NULL,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('macro_indicators', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_macro_indicator_ts
    ON macro_indicators (indicator_name, ts DESC);

-- --------------------------------------------------------------------------
-- Sentiment scores (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    source          VARCHAR(64)     NOT NULL,
    asset           VARCHAR(32)     NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    post_count      INTEGER         DEFAULT 0,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('sentiment_scores', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sentiment_source_ts
    ON sentiment_scores (source, ts DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_asset_ts
    ON sentiment_scores (asset, ts DESC);

-- --------------------------------------------------------------------------
-- Investment Committee records (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ic_records (
    id              BIGSERIAL       NOT NULL,
    record_id       VARCHAR(64)     NOT NULL UNIQUE,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    asset           VARCHAR(32)     NOT NULL,
    decision        VARCHAR(16)     NOT NULL,
    thesis          JSONB           DEFAULT '{}'::jsonb,
    debate          JSONB           DEFAULT '{}'::jsonb,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('ic_records', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_ic_asset_ts
    ON ic_records (asset, ts DESC);

-- --------------------------------------------------------------------------
-- Daily summaries (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_summaries (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    date            VARCHAR(10)     NOT NULL UNIQUE,
    total_pnl       DOUBLE PRECISION DEFAULT 0.0,
    trades_executed INTEGER         DEFAULT 0,
    summary         JSONB           DEFAULT '{}'::jsonb,
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('daily_summaries', 'ts', if_not_exists => TRUE);

-- --------------------------------------------------------------------------
-- Agent dynamic weights (hypertable)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_weights (
    id              BIGSERIAL       NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    agent_id        VARCHAR(64)     NOT NULL,
    weight          DOUBLE PRECISION NOT NULL,
    previous_weight DOUBLE PRECISION DEFAULT 0.0,
    reason          TEXT            DEFAULT '',
    metadata        JSONB           DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('agent_weights', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_agent_weights_agent_ts
    ON agent_weights (agent_id, ts DESC);

-- --------------------------------------------------------------------------
-- Extend performance_metrics with source/agent/asset columns (idempotent)
-- --------------------------------------------------------------------------
ALTER TABLE performance_metrics ADD COLUMN IF NOT EXISTS source VARCHAR(64) DEFAULT '';
ALTER TABLE performance_metrics ADD COLUMN IF NOT EXISTS agent_id VARCHAR(64) DEFAULT '';
ALTER TABLE performance_metrics ADD COLUMN IF NOT EXISTS asset VARCHAR(32) DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_perf_source_ts
    ON performance_metrics (source, ts DESC);
CREATE INDEX IF NOT EXISTS idx_perf_agent_ts
    ON performance_metrics (agent_id, ts DESC);
