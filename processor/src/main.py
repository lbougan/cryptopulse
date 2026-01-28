"""
CryptoPulse Processor Service
Consumes trades from Redpanda and aggregates into 1-minute OHLCV candles
"""
import json
import logging
import os
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from kafka import KafkaConsumer
from clickhouse_driver import Client
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# Load environment variables
load_dotenv()

# Configure structured logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092").split(",")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "cryptopulse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")
TRADES_TOPIC = "trades"
CANDLE_INTERVAL_SECONDS = 60  # 1 minute
RAW_TRADES_BATCH_SIZE = 500  # Flush raw_trades to ClickHouse when buffer reaches this size
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))


class CandleWindow:
    """Represents a 1-minute candle window for a symbol"""
    
    def __init__(self, symbol: str, window_start: datetime):
        self.symbol = symbol
        self.window_start = window_start
        self.window_end = window_start + timedelta(seconds=CANDLE_INTERVAL_SECONDS)
        self.prices = []
        self.volumes = []
        self.trade_count = 0
        self.first_trade_ts = None
        self.last_trade_ts = None
    
    def add_trade(self, price: float, qty: float, trade_ts: int):
        """Add a trade to this candle window"""
        self.prices.append(price)
        self.volumes.append(qty)
        self.trade_count += 1
        
        # Track first and last trade timestamps
        trade_dt = datetime.fromtimestamp(trade_ts / 1000, tz=timezone.utc)
        if self.first_trade_ts is None:
            self.first_trade_ts = trade_dt
        self.last_trade_ts = trade_dt
    
    def to_candle_dict(self) -> Optional[Dict[str, Any]]:
        """Convert window to candle dictionary for ClickHouse"""
        if not self.prices:
            return None
        
        return {
            "symbol": self.symbol,
            "timestamp": self.window_start.replace(tzinfo=None),  # ClickHouse expects naive datetime
            "open": self.prices[0],
            "high": max(self.prices),
            "low": min(self.prices),
            "close": self.prices[-1],
            "volume": sum(self.volumes),
            "trades": self.trade_count,
        }
    
    def is_expired(self, current_time: datetime) -> bool:
        """Check if this window has expired (past the end time)"""
        return current_time >= self.window_end


class OHLCVProcessor:
    """Processes trades and aggregates into 1-minute candles"""
    
    def __init__(self):
        self.consumer = None
        self.clickhouse_client = None
        self.windows: Dict[str, CandleWindow] = {}  # symbol -> current window
        self.raw_trades_buffer: list = []
        self.running = False
        self.stats = {
            "trades_processed": 0,
            "candles_generated": 0,
            "raw_trades_inserted": 0,
            "errors": 0,
            "start_time": datetime.now(timezone.utc).isoformat(),
        }
        self.metrics_app = None
        self.metrics_thread = None
    
    def create_consumer(self):
        """Create Kafka consumer for Redpanda"""
        try:
            consumer = KafkaConsumer(
                TRADES_TOPIC,
                bootstrap_servers=REDPANDA_BROKERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                group_id='cryptopulse-processor',
                consumer_timeout_ms=1000,  # Timeout for polling
            )
            logger.info(f"Connected to Redpanda brokers: {REDPANDA_BROKERS}")
            logger.info(f"Subscribed to topic: {TRADES_TOPIC}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            raise
    
    def create_clickhouse_client(self):
        """Create ClickHouse client"""
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DB,
                password=CLICKHOUSE_PASSWORD,
            )
            logger.info(f"Connected to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
            return client
        except Exception as e:
            logger.error(f"Failed to create ClickHouse client: {e}")
            raise
    
    def get_window_key(self, symbol: str, trade_ts: int) -> str:
        """Get the window start time for a trade timestamp"""
        # Convert milliseconds to datetime
        trade_dt = datetime.fromtimestamp(trade_ts / 1000, tz=timezone.utc)
        # Round down to the nearest minute
        window_start = trade_dt.replace(second=0, microsecond=0)
        return f"{symbol}_{window_start.isoformat()}"
    
    def get_or_create_window(self, symbol: str, trade_ts: int) -> CandleWindow:
        """Get or create a candle window for a symbol and timestamp"""
        trade_dt = datetime.fromtimestamp(trade_ts / 1000, tz=timezone.utc)
        window_start = trade_dt.replace(second=0, microsecond=0)
        
        window_key = f"{symbol}_{window_start.isoformat()}"
        
        if window_key not in self.windows:
            self.windows[window_key] = CandleWindow(symbol, window_start)
            logger.debug(f"Created new window for {symbol} at {window_start}")
        
        return self.windows[window_key]
    
    def flush_expired_windows(self, current_time: Optional[datetime] = None):
        """Flush expired windows to ClickHouse"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        expired_keys = []
        candles_to_insert = []
        
        for window_key, window in self.windows.items():
            if window.is_expired(current_time):
                candle = window.to_candle_dict()
                if candle:
                    candles_to_insert.append(candle)
                    self.stats["candles_generated"] += 1
                expired_keys.append(window_key)
        
        # Remove expired windows
        for key in expired_keys:
            del self.windows[key]
        
        # Insert candles to ClickHouse
        if candles_to_insert:
            self.insert_candles(candles_to_insert)
            logger.info(f"Flushed {len(candles_to_insert)} expired candles to ClickHouse")
    
    def insert_candles(self, candles: list):
        """Insert candles into ClickHouse"""
        if not candles:
            return
        
        try:
            # Convert dictionaries to tuples in the correct column order
            # Column order: symbol, timestamp, open, high, low, close, volume, trades
            data_tuples = [
                (
                    candle["symbol"],
                    candle["timestamp"],
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    candle["volume"],
                    candle["trades"],
                )
                for candle in candles
            ]
            
            self.clickhouse_client.execute(
                """
                INSERT INTO candles_1m (symbol, timestamp, open, high, low, close, volume, trades)
                VALUES
                """,
                data_tuples
            )
            logger.debug(f"Inserted {len(candles)} candles into ClickHouse")
        except Exception as e:
            logger.error(f"Error inserting candles to ClickHouse: {e}")
            self.stats["errors"] += 1
    
    def insert_raw_trades(self, trades: list):
        """Insert raw trades into ClickHouse (exchange, symbol, trade_id, price, qty, ts)"""
        if not trades:
            return
        
        try:
            data_tuples = []
            for t in trades:
                ts_dt = datetime.fromtimestamp(t["ts"] / 1000.0, tz=timezone.utc)
                data_tuples.append((
                    t["exchange"],
                    t["symbol"],
                    t["trade_id"],
                    t["price"],
                    t["qty"],
                    ts_dt,
                ))
            
            self.clickhouse_client.execute(
                """
                INSERT INTO raw_trades (exchange, symbol, trade_id, price, qty, ts)
                VALUES
                """,
                data_tuples
            )
            self.stats["raw_trades_inserted"] += len(trades)
            logger.debug(f"Inserted {len(trades)} raw trades into ClickHouse")
        except Exception as e:
            logger.error(f"Error inserting raw trades to ClickHouse: {e}")
            self.stats["errors"] += 1
    
    def flush_raw_trades(self):
        """Flush buffered raw trades to ClickHouse"""
        if not self.raw_trades_buffer:
            return
        batch = self.raw_trades_buffer.copy()
        self.raw_trades_buffer.clear()
        self.insert_raw_trades(batch)
        logger.info(f"Flushed {len(batch)} raw trades to ClickHouse")
    
    def process_trade(self, trade: Dict[str, Any]):
        """Process a single trade message"""
        try:
            symbol = trade.get("symbol")
            price = trade.get("price")
            qty = trade.get("qty")
            trade_ts = trade.get("ts")
            
            # Validate trade data
            if not all([symbol, price, qty, trade_ts]):
                logger.warning(f"Invalid trade data: {trade}")
                return
            
            # Buffer raw trade for ClickHouse
            self.raw_trades_buffer.append(trade)
            if len(self.raw_trades_buffer) >= RAW_TRADES_BATCH_SIZE:
                self.flush_raw_trades()
            
            # Get or create window for this symbol and timestamp
            window = self.get_or_create_window(symbol, trade_ts)
            window.add_trade(price, qty, trade_ts)
            
            self.stats["trades_processed"] += 1
            
            # Log periodically
            if self.stats["trades_processed"] % 100 == 0:
                logger.info(
                    f"Processed {self.stats['trades_processed']} trades, "
                    f"candles={self.stats['candles_generated']}, "
                    f"raw_trades_inserted={self.stats['raw_trades_inserted']}, "
                    f"active windows: {len(self.windows)}"
                )
        
        except Exception as e:
            logger.error(f"Error processing trade: {e}, trade: {trade}")
            self.stats["errors"] += 1
    
    def create_metrics_app(self):
        """Create FastAPI app for metrics and health endpoints"""
        app = FastAPI(title="CryptoPulse Processor Metrics", version="0.1.0")
        
        @app.get("/health")
        async def health():
            """Health check endpoint"""
            try:
                # Check if consumer is connected
                consumer_healthy = self.consumer is not None
                # Check if ClickHouse is accessible
                clickhouse_healthy = False
                if self.clickhouse_client:
                    try:
                        self.clickhouse_client.execute("SELECT 1")
                        clickhouse_healthy = True
                    except:
                        pass
                
                if consumer_healthy and clickhouse_healthy:
                    return {
                        "status": "healthy",
                        "service": "processor",
                        "redpanda": "connected" if consumer_healthy else "disconnected",
                        "clickhouse": "connected" if clickhouse_healthy else "disconnected"
                    }
                else:
                    return JSONResponse(
                        status_code=503,
                        content={
                            "status": "degraded",
                            "service": "processor",
                            "redpanda": "connected" if consumer_healthy else "disconnected",
                            "clickhouse": "connected" if clickhouse_healthy else "disconnected"
                        }
                    )
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                return JSONResponse(
                    status_code=503,
                    content={"status": "unhealthy", "service": "processor", "error": str(e)}
                )
        
        @app.get("/metrics")
        async def metrics():
            """Metrics endpoint"""
            try:
                start_dt = datetime.fromisoformat(self.stats["start_time"].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                # Fallback if start_time format is unexpected
                start_dt = datetime.now(timezone.utc)
            
            uptime_seconds = (datetime.now(timezone.utc) - start_dt).total_seconds()
            
            return {
                "service": "processor",
                "uptime_seconds": int(uptime_seconds),
                "stats": {
                    "trades_processed": self.stats["trades_processed"],
                    "candles_generated": self.stats["candles_generated"],
                    "raw_trades_inserted": self.stats["raw_trades_inserted"],
                    "errors": self.stats["errors"],
                    "active_windows": len(self.windows),
                    "buffered_raw_trades": len(self.raw_trades_buffer),
                },
                "rates": {
                    "trades_per_second": round(self.stats["trades_processed"] / uptime_seconds, 2) if uptime_seconds > 0 else 0,
                    "candles_per_minute": round(self.stats["candles_generated"] / (uptime_seconds / 60), 2) if uptime_seconds > 0 else 0,
                }
            }
        
        return app
    
    def start_metrics_server(self):
        """Start metrics server in a separate thread"""
        self.metrics_app = self.create_metrics_app()
        config = uvicorn.Config(
            app=self.metrics_app,
            host="0.0.0.0",
            port=METRICS_PORT,
            log_level="warning"  # Reduce uvicorn logging noise
        )
        server = uvicorn.Server(config)
        server.run()
    
    def start(self):
        """Start the processor service"""
        logger.info("Starting CryptoPulse Processor Service...")
        self.running = True
        
        # Start metrics server in background thread
        self.metrics_thread = threading.Thread(target=self.start_metrics_server, daemon=True)
        self.metrics_thread.start()
        logger.info(f"Metrics server started on port {METRICS_PORT}")
        
        # Create consumer and ClickHouse client
        try:
            self.consumer = self.create_consumer()
            self.clickhouse_client = self.create_clickhouse_client()
        except Exception as e:
            logger.error(f"Failed to initialize. Exiting: {e}")
            return
        
        logger.info("Processor ready. Consuming trades and aggregating candles...")
        
        # Main processing loop
        last_flush_time = time.time()
        flush_interval = 10  # Flush expired windows every 10 seconds
        
        try:
            while self.running:
                # Poll for messages
                message_pack = self.consumer.poll(timeout_ms=1000)
                
                # Process messages
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        trade = message.value
                        self.process_trade(trade)
                
                # Periodically flush expired windows and buffered raw trades
                current_time = time.time()
                if current_time - last_flush_time >= flush_interval:
                    self.flush_expired_windows()
                    self.flush_raw_trades()
                    last_flush_time = current_time
        
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Unexpected error in processing loop: {e}")
        finally:
            # Flush any remaining windows and raw trades before shutdown
            logger.info("Flushing remaining windows and raw trades before shutdown...")
            self.flush_expired_windows()
            self.flush_raw_trades()
            self.stop()
    
    def stop(self):
        """Stop the processor service"""
        logger.info("Stopping processor service...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        if self.clickhouse_client:
            self.clickhouse_client.disconnect()
        
        logger.info(
            f"Processor stopped. Final stats: "
            f"trades={self.stats['trades_processed']}, "
            f"candles={self.stats['candles_generated']}, "
            f"raw_trades={self.stats['raw_trades_inserted']}, "
            f"errors={self.stats['errors']}"
        )


def main():
    """Main entry point"""
    processor = OHLCVProcessor()
    
    try:
        processor.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
