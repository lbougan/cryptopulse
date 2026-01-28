# CryptoPulse - Real-time crypto trade data pipeline

A real-time cryptocurrency trade data pipeline built with Redpanda, ClickHouse, and FastAPI.

## Architecture

![CryptoPulse Architecture](architecture-diagram.png)

**Data Flow**: `[Binance WS] -> [collector] -> [Redpanda] -> [processor] -> [ClickHouse] -> [FastAPI]`

For a detailed architecture diagram and component descriptions, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- `wget` or `curl` for health checks (included in containers)

### Step 1: Start Services

```bash
docker compose up -d --build
```

This will start:
- **Redpanda** (Kafka-compatible streaming) on ports 19092, 18081, 18082
- **ClickHouse** (OLAP database) on ports 8123, 9000
- **Collector** service (Binance WebSocket collector) with health check on port 8080
- **Processor** service (OHLCV aggregation) with metrics/health on port 8080
- **API** service (FastAPI) on port 8000

### Step 2: Initialize ClickHouse

After services are running, initialize the database and tables:

```bash
./init-clickhouse.sh
```

This creates:
- `cryptopulse` database
- `raw_trades` table (for individual trade events)
- `candles_1m` table (for 1-minute OHLCV candles)

**Note**: ClickHouse initialization happens automatically via the init script, but you can run it manually if needed.

### Step 3: Verify Services

```bash
# Check all services are running and healthy
docker compose ps

# Check service health endpoints
curl http://localhost:8000/health          # API service
curl http://localhost:8080/health          # Collector service (from within container)
curl http://localhost:8080/health          # Processor service (from within container)

# Test ClickHouse
curl http://localhost:8123/ping

# View service logs
docker compose logs -f collector
docker compose logs -f processor
docker compose logs -f api
```

## Project Structure

```
cryptopulse/
├── docker-compose.yml       # Service orchestration
├── collector/               # Binance WebSocket collector (Step 2)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
├── processor/               # OHLCV aggregation processor (Step 3)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
├── api/                     # FastAPI query layer (Step 5)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
├── clickhouse/
│   └── init.sql            # Database schema
└── init-clickhouse.sh      # Initialization script
```

## Development Status

- ✅ **Step 1**: Infrastructure setup (COMPLETE)
- ✅ **Step 2**: Binance WebSocket Collector (COMPLETE)
- ✅ **Step 3**: Stream Processor (OHLCV Aggregation) (COMPLETE)
- ✅ **Step 4**: ClickHouse Storage Layer (COMPLETE)
- ✅ **Step 5**: FastAPI Query Layer (COMPLETE)
- ✅ **Step 6**: Observability & Polish (COMPLETE)

## Services

### Redpanda
- Internal Kafka endpoint: `redpanda:9092`
- External endpoint: `localhost:19092`
- Schema Registry: `localhost:18081`
- Proxy: `localhost:18082`

### ClickHouse
- HTTP interface: `localhost:8123`
- Native protocol: `localhost:9000`
- Database: `cryptopulse`

### Collector Service
- Connects to Binance WebSocket API (`btcusdt@trade` stream)
- Publishes trades to Redpanda `trades` topic
- Health check: `http://localhost:8080/health` (internal)
- Logs: `docker compose logs -f collector`

### Processor Service
- Consumes trades from Redpanda `trades` topic
- Aggregates into 1-minute OHLCV candles
- Writes candles and raw trades to ClickHouse
- Metrics endpoint: `http://localhost:8080/metrics` (internal)
- Health check: `http://localhost:8080/health` (internal)
- Logs: `docker compose logs -f processor`

### API Service
- Base URL: `http://localhost:8000`
- Health: `http://localhost:8000/health`
- Endpoints:
  - `GET /` - API information
  - `GET /health` - Health check
  - `GET /symbols` - List available symbols
  - `GET /candles?symbol=BTCUSDT&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z` - Query candles
- API docs: `http://localhost:8000/docs` (Swagger UI)
- Logs: `docker compose logs -f api`

## Testing Guide

### End-to-End Flow Test

1. **Start all services**:
   ```bash
   docker compose up -d --build
   ```

2. **Wait for services to be healthy** (about 30 seconds):
   ```bash
   docker compose ps
   ```
   All services should show "healthy" status.

3. **Check that data is flowing**:
   ```bash
   # Check collector logs (should see trades being collected)
   docker compose logs collector | tail -20
   
   # Check processor logs (should see trades being processed and candles generated)
   docker compose logs processor | tail -20
   ```

4. **Query ClickHouse directly** (optional):
   ```bash
   docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM cryptopulse.raw_trades"
   docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM cryptopulse.candles_1m"
   ```

5. **Query via API** (wait 2-3 minutes for candles to accumulate):
   ```bash
   # Get available symbols
   curl http://localhost:8000/symbols
   
   # Get candles for a time range (adjust dates to current time)
   curl "http://localhost:8000/candles?symbol=BTCUSDT&from=2024-01-28T00:00:00Z&to=2024-01-28T23:59:59Z"
   ```

### Health Checks

```bash
# API health
curl http://localhost:8000/health

# Collector health (from host)
docker compose exec collector wget -qO- http://localhost:8080/health

# Processor health and metrics (from host)
docker compose exec processor wget -qO- http://localhost:8080/health
docker compose exec processor wget -qO- http://localhost:8080/metrics
```

### Monitoring

- **View real-time logs**: `docker compose logs -f`
- **View specific service logs**: `docker compose logs -f collector|processor|api`
- **Check service status**: `docker compose ps`
- **View processor metrics**: `docker compose exec processor wget -qO- http://localhost:8080/metrics | jq`

## Configuration

### Environment Variables

**Collector**:
- `REDPANDA_BROKERS`: Comma-separated list of Redpanda brokers (default: `redpanda:9092`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `HEALTH_PORT`: Port for health check endpoint (default: `8080`)

**Processor**:
- `REDPANDA_BROKERS`: Comma-separated list of Redpanda brokers (default: `redpanda:9092`)
- `CLICKHOUSE_HOST`: ClickHouse host (default: `clickhouse`)
- `CLICKHOUSE_PORT`: ClickHouse port (default: `9000`)
- `CLICKHOUSE_DB`: ClickHouse database (default: `cryptopulse`)
- `CLICKHOUSE_PASSWORD`: ClickHouse password (default: `default`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `METRICS_PORT`: Port for metrics/health endpoints (default: `8080`)

**API**:
- `CLICKHOUSE_HOST`: ClickHouse host (default: `clickhouse`)
- `CLICKHOUSE_PORT`: ClickHouse port (default: `9000`)
- `CLICKHOUSE_DB`: ClickHouse database (default: `cryptopulse`)
- `CLICKHOUSE_PASSWORD`: ClickHouse password (default: `default`)
- `LOG_LEVEL`: Logging level (default: `INFO`)

## Troubleshooting

### Services not starting
- Check Docker logs: `docker compose logs`
- Ensure ports are not already in use
- Check disk space: `docker system df`

### No data flowing
- Verify Binance WebSocket is accessible (check collector logs)
- Check Redpanda topic: `docker compose exec redpanda rpk topic describe trades`
- Verify ClickHouse tables exist: `docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM cryptopulse"`

### API returns empty results
- Wait a few minutes for candles to accumulate (1-minute intervals)
- Check processor logs for errors
- Verify data in ClickHouse directly
