-- CryptoPulse ClickHouse Database Initialization
-- Creates database and tables for raw trades and 1-minute candles

CREATE DATABASE IF NOT EXISTS cryptopulse;

USE cryptopulse;

-- Raw trades table
-- Stores individual trade events from exchanges
-- ORDER BY (symbol, ts, trade_id): efficient time-range + symbol lookups
CREATE TABLE IF NOT EXISTS raw_trades
(
    exchange String,
    symbol String,
    trade_id String,
    price Float64,
    qty Float64,
    ts DateTime64(3, 'UTC'),
    ingestion_ts DateTime DEFAULT now(),
    INDEX idx_ts_minmax ts TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY (symbol, ts, trade_id)
PARTITION BY toYYYYMM(ts)
TTL ts + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 1-minute candles table
-- Stores aggregated OHLCV data per symbol per minute
-- ORDER BY (symbol, timestamp): efficient time-range + symbol lookups
CREATE TABLE IF NOT EXISTS candles_1m
(
    symbol String,
    timestamp DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trades UInt32,
    ingestion_ts DateTime DEFAULT now(),
    INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Create materialized view for real-time candle aggregation (optional, for future use)
-- This can be used to automatically aggregate trades into candles
-- For now, we'll do this in the processor service
