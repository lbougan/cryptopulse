#!/bin/bash
# Initialize ClickHouse database and tables
# Run this script after starting the docker-compose services

echo "Initializing ClickHouse database and tables..."

docker compose exec -T clickhouse clickhouse-client <<EOF
CREATE DATABASE IF NOT EXISTS cryptopulse;

USE cryptopulse;

-- Raw trades table
CREATE TABLE IF NOT EXISTS raw_trades
(
    exchange String,
    symbol String,
    trade_id String,
    price Float64,
    qty Float64,
    ts DateTime64(3, 'UTC'),
    ingestion_ts DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, ts, trade_id)
PARTITION BY toYYYYMM(ts)
TTL ts + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 1-minute candles table
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
    ingestion_ts DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
EOF

echo "ClickHouse initialization complete!"
echo "Verifying tables..."
docker compose exec clickhouse clickhouse-client --query "USE cryptopulse; SHOW TABLES"
