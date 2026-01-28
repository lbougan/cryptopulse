"""
CryptoPulse API Service
FastAPI service to query stored data from ClickHouse
"""
import os
import logging
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from clickhouse_driver import Client
from dateutil import parser as date_parser

# Configure structured logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ClickHouse configuration
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "cryptopulse")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")


class ClickHouseClient:
    """ClickHouse client wrapper"""
    
    def __init__(self):
        self.client = None
        self._connect()
    
    def _connect(self):
        """Create ClickHouse connection"""
        try:
            self.client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DB,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            # Test connection
            self.client.execute("SELECT 1")
            logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def execute(self, query: str, params: Optional[dict] = None):
        """Execute a query and return results"""
        try:
            if params:
                return self.client.execute(query, params)
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"ClickHouse query error: {e}, query: {query}")
            raise


# Global ClickHouse client
clickhouse_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    global clickhouse_client
    # Startup
    logger.info("Starting CryptoPulse API Service...")
    try:
        clickhouse_client = ClickHouseClient()
        logger.info("API service started successfully")
    except Exception as e:
        logger.error(f"Failed to start API service: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down API service...")
    if clickhouse_client and clickhouse_client.client:
        clickhouse_client.client.disconnect()
    logger.info("API service stopped")


app = FastAPI(
    title="CryptoPulse API",
    version="0.1.0",
    description="REST API to query cryptocurrency trade and candle data",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Test ClickHouse connection
        if clickhouse_client:
            clickhouse_client.execute("SELECT 1")
            return {
                "status": "healthy",
                "service": "api",
                "clickhouse": "connected"
            }
        return {
            "status": "degraded",
            "service": "api",
            "clickhouse": "disconnected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "CryptoPulse API",
        "version": "0.1.0",
        "endpoints": {
            "health": "/health",
            "symbols": "/symbols",
            "candles": "/candles?symbol=BTCUSDT&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z"
        }
    }


@app.get("/symbols")
async def get_symbols():
    """
    List all available symbols from the candles table
    
    Returns a list of unique symbols that have candle data
    """
    try:
        query = """
            SELECT DISTINCT symbol
            FROM candles_1m
            ORDER BY symbol
        """
        results = clickhouse_client.execute(query)
        symbols = [row[0] for row in results]
        
        return {
            "symbols": symbols,
            "count": len(symbols)
        }
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")


@app.get("/candles")
async def get_candles(
    symbol: str = Query(..., description="Trading symbol (e.g., BTCUSDT)", min_length=1),
    from_time: str = Query(..., alias="from", description="Start time (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
    to_time: str = Query(..., alias="to", description="End time (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
    limit: Optional[int] = Query(1000, ge=1, le=10000, description="Maximum number of candles to return")
):
    """
    Query candles for a specific symbol within a time range
    
    - **symbol**: Trading symbol (e.g., BTCUSDT)
    - **from**: Start time in ISO 8601 format (e.g., 2024-01-01T00:00:00Z)
    - **to**: End time in ISO 8601 format (e.g., 2024-01-02T00:00:00Z)
    - **limit**: Maximum number of candles to return (default: 1000, max: 10000)
    
    Returns a list of candles with OHLCV data
    """
    try:
        # Parse and validate timestamps
        try:
            from_dt = date_parser.parse(from_time)
            to_dt = date_parser.parse(to_time)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format. Use ISO 8601 format (e.g., 2024-01-01T00:00:00Z). Error: {e}"
            )
        
        if from_dt >= to_dt:
            raise HTTPException(
                status_code=400,
                detail="'from' time must be before 'to' time"
            )
        
        # Convert to ClickHouse DateTime format (YYYY-MM-DD HH:MM:SS)
        from_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Query candles
        query = """
            SELECT 
                symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                trades
            FROM candles_1m
            WHERE symbol = %(symbol)s
                AND timestamp >= %(from_time)s
                AND timestamp <= %(to_time)s
            ORDER BY timestamp ASC
            LIMIT %(limit)s
        """
        
        params = {
            "symbol": symbol.upper(),
            "from_time": from_str,
            "to_time": to_str,
            "limit": limit
        }
        
        results = clickhouse_client.execute(query, params)
        
        # Format results
        candles = []
        for row in results:
            candles.append({
                "symbol": row[0],
                "timestamp": row[1].isoformat() + "Z" if isinstance(row[1], datetime) else str(row[1]),
                "open": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "close": float(row[5]),
                "volume": float(row[6]),
                "trades": int(row[7])
            })
        
        return {
            "symbol": symbol.upper(),
            "from": from_time,
            "to": to_time,
            "candles": candles,
            "count": len(candles)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching candles: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch candles: {str(e)}")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
