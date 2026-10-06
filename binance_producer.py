"""
Binance WebSocket Producer
==========================
Connects to Binance WebSocket API and publishes real-time trade data
to Kafka topic 'trades-stream' with partition key based on symbol.

Supports multiple trading pairs via TRADING_PAIRS env var.
"""

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone

import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TRADING_PAIRS = os.environ.get('TRADING_PAIRS', 'btcusdt').lower().split(',')
BINANCE_WS_BASE = 'wss://stream.binance.com:9443/ws'
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'trades-stream')

# Graceful shutdown
shutdown_event = asyncio.Event()


def create_kafka_producer(max_retries=10, retry_delay=5):
    """Create Kafka producer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,  # Small batch window for low-latency
                compression_type='gzip',
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BOOTSTRAP} (attempt {attempt})")
            return producer
        except KafkaError as e:
            print(f"[Producer] Kafka connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                import time
                time.sleep(retry_delay)
            else:
                raise


def parse_binance_trade(raw: dict) -> dict:
    """Parse Binance trade message into standardized format.

    Binance trade stream format:
    {
        "e": "trade",       // Event type
        "E": 1672515782136, // Event time (ms)
        "s": "BTCUSDT",    // Symbol
        "t": 12345,        // Trade ID
        "p": "0.001",      // Price
        "q": "100",        // Quantity (volume)
        "T": 1672515782136, // Trade time (ms)
        "m": true,         // Is buyer the market maker?
    }
    """
    trade_time_ms = raw.get('T', raw.get('E', 0))
    trade_time = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc)

    return {
        'symbol': raw.get('s', 'UNKNOWN'),
        'price': float(raw.get('p', 0)),
        'volume': float(raw.get('q', 0)),
        'trade_id': raw.get('t', 0),
        'time': trade_time.isoformat(),
        'timestamp_ms': trade_time_ms,
        'is_buyer_maker': raw.get('m', False),
    }


async def stream_trades(producer: KafkaProducer):
    """Connect to Binance WebSocket and stream trades to Kafka."""

    # Build combined stream URL for multiple pairs
    streams = '/'.join([f"{pair}@trade" for pair in TRADING_PAIRS])
    ws_url = f"{BINANCE_WS_BASE}/{streams}" if len(TRADING_PAIRS) == 1 else \
             f"wss://stream.binance.com:9443/stream?streams={streams}"

    print(f"[Producer] Connecting to Binance WebSocket: {ws_url}")
    print(f"[Producer] Trading pairs: {[p.upper() for p in TRADING_PAIRS]}")
    print(f"[Producer] Publishing to Kafka topic: {KAFKA_TOPIC}")

    trade_count = 0
    reconnect_delay = 1

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                print(f"[Producer] WebSocket connected!")
                reconnect_delay = 1  # Reset on successful connect

                while not shutdown_event.is_set():
                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        # Send ping to keep connection alive
                        await ws.ping()
                        continue

                    data = json.loads(raw_msg)

                    # Handle combined stream format
                    if 'stream' in data:
                        data = data['data']

                    # Skip non-trade events
                    if data.get('e') != 'trade':
                        continue

                    trade = parse_binance_trade(data)
                    symbol = trade['symbol']

                    # Publish to Kafka with symbol as partition key
                    producer.send(
                        KAFKA_TOPIC,
                        key=symbol,
                        value=trade,
                    )

                    trade_count += 1
                    if trade_count % 100 == 0:
                        producer.flush()
                        print(
                            f"[Producer] Published {trade_count} trades | "
                            f"Last: {symbol} ${trade['price']:.2f} "
                            f"vol={trade['volume']:.6f}"
                        )

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[Producer] WebSocket closed: {e}. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            print(f"[Producer] Error: {e}. Reconnecting in {reconnect_delay}s...")

        if not shutdown_event.is_set():
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff, max 60s

    # Final flush
    producer.flush()
    producer.close()
    print(f"[Producer] Shutdown complete. Total trades published: {trade_count}")


def handle_signal(sig, frame):
    """Handle shutdown signals gracefully."""
    print(f"\n[Producer] Received signal {sig}, shutting down...")
    shutdown_event.set()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("=" * 60)
    print("  Binance WebSocket → Kafka Producer")
    print("=" * 60)

    producer = create_kafka_producer()
    asyncio.run(stream_trades(producer))
