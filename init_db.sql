-- ============================================
-- TimescaleDB Schema for Volatility Monitor
-- ============================================

-- Raw trades table (optional, for debugging/replay)
CREATE TABLE IF NOT EXISTS raw_trades (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    trade_id    BIGINT
);

SELECT create_hypertable('raw_trades', 'time', if_not_exists => TRUE);

-- Windowed aggregation table (10-second tumbling windows)
CREATE TABLE IF NOT EXISTS trade_windows (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    total_volume    DOUBLE PRECISION NOT NULL,
    trade_count     INTEGER NOT NULL,
    avg_price       DOUBLE PRECISION NOT NULL,
    min_price       DOUBLE PRECISION NOT NULL,
    max_price       DOUBLE PRECISION NOT NULL,
    vwap            DOUBLE PRECISION NOT NULL,  -- Volume-weighted average price
    volatility      DOUBLE PRECISION NOT NULL   -- (max - min) / avg
);

SELECT create_hypertable('trade_windows', 'window_start', if_not_exists => TRUE);

-- Alerts history table
CREATE TABLE IF NOT EXISTS alerts (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol          TEXT NOT NULL,
    alert_type      TEXT NOT NULL,   -- 'whale', 'volume_spike', 'volatility'
    message         TEXT NOT NULL,
    price           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    threshold       DOUBLE PRECISION
);

SELECT create_hypertable('alerts', 'time', if_not_exists => TRUE);

-- Data retention policy: keep 30 days of raw trades, 90 days for windows
SELECT add_retention_policy('raw_trades', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('trade_windows', INTERVAL '90 days', if_not_exists => TRUE);

-- Useful continuous aggregate for Grafana dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS trade_1min_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', window_start) AS bucket,
    symbol,
    SUM(total_volume) AS total_volume,
    SUM(trade_count) AS total_trades,
    AVG(avg_price) AS avg_price,
    MIN(min_price) AS min_price,
    MAX(max_price) AS max_price,
    AVG(vwap) AS vwap,
    AVG(volatility) AS avg_volatility
FROM trade_windows
GROUP BY bucket, symbol
WITH NO DATA;
