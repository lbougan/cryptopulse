"""
CryptoPulse Collector Service
Connects to Binance WebSocket API and publishes trades to Redpanda
"""
import json
import logging
import os
import time
import threading
from datetime import datetime
from typing import Dict, Any

import websocket
from kafka import KafkaProducer
from dotenv import load_dotenv
from fastapi import FastAPI
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
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
TOPIC_NAME = "trades"
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 10
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))


class BinanceCollector:
    """Collects trade data from Binance WebSocket and publishes to Redpanda"""
    
    def __init__(self):
        self.producer = None
        self.ws = None
        self.reconnect_count = 0
        self.trade_count = 0
        self.running = False
        self.start_time = datetime.now()
        self.health_app = None
        self.health_thread = None
        
    def create_producer(self):
        """Create Kafka producer for Redpanda"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=REDPANDA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info(f"Connected to Redpanda brokers: {REDPANDA_BROKERS}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create producer: {e}")
            raise
    
    def transform_trade(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Binance trade message to our data model
        
        Binance format:
        {
            "e": "trade",
            "E": 123456789,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "0.001",
            "q": "100",
            "b": 88,
            "a": 50,
            "T": 123456785,
            "m": true,
            "M": true
        }
        
        Our format:
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": 12345,
            "price": 0.001,
            "qty": 100.0,
            "ts": 123456785  # milliseconds (as provided by Binance)
        }
        """
        try:
            return {
                "exchange": "binance",
                "symbol": message.get("s", "").upper(),
                "trade_id": str(message.get("t", "")),  # Convert to string for ClickHouse
                "price": float(message.get("p", 0)),
                "qty": float(message.get("q", 0)),
                "ts": int(message.get("T", 0)),  # Keep as milliseconds
            }
        except (ValueError, KeyError) as e:
            logger.error(f"Error transforming trade message: {e}, message: {message}")
            return None
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Binance sends trade events with "e": "trade"
            if data.get("e") == "trade":
                trade = self.transform_trade(data)
                if trade:
                    self.trade_count += 1
                    # Publish to Redpanda with symbol as key for partitioning
                    future = self.producer.send(
                        TOPIC_NAME,
                        key=trade["symbol"],
                        value=trade
                    )
                    
                    # Log periodically (every 100 trades to avoid spam)
                    if self.trade_count % 100 == 0:
                        logger.debug(f"Published trade: {trade['symbol']} @ {trade['price']}")
                    
                    # Handle send errors asynchronously
                    try:
                        future.get(timeout=1)
                    except Exception as e:
                        logger.error(f"Error sending message to Redpanda: {e}")
            else:
                logger.debug(f"Received non-trade message: {data.get('e')}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {e}, message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.ws = None
        
        # Attempt reconnection if still running
        if self.running:
            self.reconnect()
    
    def on_open(self, ws):
        """Handle WebSocket open"""
        logger.info("Connected to Binance WebSocket")
        self.reconnect_count = 0  # Reset reconnect counter on successful connection
    
    def reconnect(self):
        """Reconnect to WebSocket with exponential backoff"""
        if self.reconnect_count >= MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached. Exiting.")
            self.running = False
            return
        
        self.reconnect_count += 1
        delay = min(RECONNECT_DELAY * (2 ** (self.reconnect_count - 1)), 60)
        logger.info(f"Reconnecting in {delay} seconds (attempt {self.reconnect_count}/{MAX_RECONNECT_ATTEMPTS})...")
        time.sleep(delay)
        
        if self.running:
            self.start_websocket()
    
    def create_health_app(self):
        """Create FastAPI app for health check endpoint"""
        app = FastAPI(title="CryptoPulse Collector Health", version="0.1.0")
        
        @app.get("/health")
        async def health():
            """Health check endpoint"""
            try:
                # Check if producer is connected
                producer_healthy = self.producer is not None
                # Check if WebSocket is connected
                websocket_healthy = self.ws is not None and self.ws.sock is not None
                
                if producer_healthy and websocket_healthy:
                    return {
                        "status": "healthy",
                        "service": "collector",
                        "redpanda": "connected" if producer_healthy else "disconnected",
                        "websocket": "connected" if websocket_healthy else "disconnected",
                        "trades_collected": self.trade_count,
                        "reconnect_count": self.reconnect_count
                    }
                else:
                    return JSONResponse(
                        status_code=503,
                        content={
                            "status": "degraded",
                            "service": "collector",
                            "redpanda": "connected" if producer_healthy else "disconnected",
                            "websocket": "connected" if websocket_healthy else "disconnected",
                            "trades_collected": self.trade_count,
                            "reconnect_count": self.reconnect_count
                        }
                    )
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                return JSONResponse(
                    status_code=503,
                    content={"status": "unhealthy", "service": "collector", "error": str(e)}
                )
        
        return app
    
    def start_health_server(self):
        """Start health check server in a separate thread"""
        self.health_app = self.create_health_app()
        config = uvicorn.Config(
            app=self.health_app,
            host="0.0.0.0",
            port=HEALTH_PORT,
            log_level="warning"  # Reduce uvicorn logging noise
        )
        server = uvicorn.Server(config)
        server.run()
    
    def start_websocket(self):
        """Start WebSocket connection"""
        try:
            logger.info(f"Connecting to Binance WebSocket: {BINANCE_WS_URL}")
            self.ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            # Run forever (blocks until connection closes)
            self.ws.run_forever()
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")
            if self.running:
                self.reconnect()
    
    def start(self):
        """Start the collector service"""
        logger.info("Starting CryptoPulse Collector Service...")
        self.running = True
        
        # Start health check server in background thread
        self.health_thread = threading.Thread(target=self.start_health_server, daemon=True)
        self.health_thread.start()
        logger.info(f"Health check server started on port {HEALTH_PORT}")
        
        # Create producer
        try:
            self.producer = self.create_producer()
        except Exception as e:
            logger.error(f"Failed to create producer. Exiting: {e}")
            return
        
        # Start WebSocket connection
        self.start_websocket()
    
    def stop(self):
        """Stop the collector service"""
        logger.info("Stopping collector service...")
        self.running = False
        
        if self.ws:
            self.ws.close()
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info(
            f"Collector service stopped. Final stats: "
            f"trades_collected={self.trade_count}, "
            f"reconnect_count={self.reconnect_count}"
        )


def main():
    """Main entry point"""
    collector = BinanceCollector()
    
    try:
        collector.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        collector.stop()


if __name__ == "__main__":
    main()
