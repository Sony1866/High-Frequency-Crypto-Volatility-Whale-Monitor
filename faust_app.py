"""
Faust Stream Processor — Volatility Monitor
=============================================
Consumes trades from 'trades-stream', performs:
1. Tumbling window aggregation (10 seconds): volume sum, avg/min/max price, VWAP
2. Whale transaction detection (single trade > threshold)
3. Volume spike detection (window volume > threshold)
4. Volatility detection (price range > threshold)
5. Publishes alerts to 'alerts' topic
6. Saves windowed aggregations to TimescaleDB
"""

import os
import json
import faust
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ============================================================
# Configuration
# ============================================================
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
DB_HOST = os.environ.get('DB_HOST', 'timescaledb')
DB_PORT = int(os.environ.get('DB_PORT', '5432'))
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'volatility_db')

# Alert thresholds
WHALE_VOLUME_THRESHOLD = float(os.environ.get('WHALE_VOLUME_THRESHOLD', '10.0'))  # BTC
VOLUME_SPIKE_THRESHOLD = float(os.environ.get('VOLUME_SPIKE_THRESHOLD', '50.0'))  # BTC per window
VOLATILITY_THRESHOLD = float(os.environ.get('VOLATILITY_THRESHOLD', '0.005'))  # 0.5%

WINDOW_SIZE_SECONDS = int(os.environ.get('WINDOW_SIZE_SECONDS', '10'))

# ============================================================
# Faust App
# ============================================================
app = faust.App(
    'volatility-monitor',
    broker=f'kafka://{KAFKA_BROKER}',
    value_serializer='json',
    web_host='0.0.0.0',
    web_port=6066,
    topic_partitions=3,
)

# ============================================================
# Faust Records (Schemas)
# ============================================================
class Trade(faust.Record, serializer='json'):
    symbol: str
    price: float
    volume: float
    trade_id: int = 0
    time: str = ''
    timestamp_ms: int = 0
    is_buyer_maker: bool = False


class Alert(faust.Record, serializer='json'):
    alert_type: str       # 'whale', 'volume_spike', 'volatility'
    symbol: str
    message: str
    price: float = 0.0
    volume: float = 0.0
    threshold: float = 0.0
    time: str = ''


class WindowAgg(faust.Record, serializer='json'):
    """Aggregated data for a tumbling window."""
    symbol: str
    window_start: str
    window_end: str
    total_volume: float = 0.0
    trade_count: int = 0
    price_sum: float = 0.0
    price_volume_sum: float = 0.0  # For VWAP calculation
    min_price: float = float('inf')
    max_price: float = 0.0


# ============================================================
# Topics
# ============================================================
trades_topic = app.topic('trades-stream', value_type=Trade)
alerts_topic = app.topic('alerts', value_type=Alert)

from prometheus_client import CollectorRegistry
registry = CollectorRegistry()

# ============================================================
# Prometheus Metrics
# ============================================================
trades_processed = Counter(
    'trades_processed_total', 'Total trades processed', ['symbol'], registry=registry
)
whale_alerts = Counter(
    'whale_alerts_total', 'Total whale transaction alerts', ['symbol'], registry=registry
)
volume_spike_alerts = Counter(
    'volume_spike_alerts_total', 'Total volume spike alerts', ['symbol'], registry=registry
)
volatility_alerts = Counter(
    'volatility_alerts_total', 'Total volatility alerts', ['symbol'], registry=registry
)
current_price = Gauge(
    'current_price', 'Current price', ['symbol'], registry=registry
)
window_volume = Gauge(
    'window_volume', 'Volume in current window', ['symbol'], registry=registry
)
trade_latency = Histogram(
    'trade_processing_latency_seconds',
    'Time from trade to processing',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    registry=registry
)

# ============================================================
# In-memory window state (per symbol)
# ============================================================
# We use Faust tables for windowed aggregation
window_table = app.Table(
    'window_aggs',
    default=lambda: {
        'total_volume': 0.0,
        'trade_count': 0,
        'price_sum': 0.0,
        'price_volume_sum': 0.0,
        'min_price': float('inf'),
        'max_price': 0.0,
        'window_start': None,
        'spike_alerted': False,
        'volatility_alerted': False,
    },
).tumbling(WINDOW_SIZE_SECONDS, expires=timedelta(seconds=WINDOW_SIZE_SECONDS))

last_window_agg = {}

# ============================================================
# Database connection pool
# ============================================================
db_pool = None


async def get_db_pool():
    """Lazy-init asyncpg connection pool."""
    global db_pool
    if db_pool is None:
        try:
            import asyncpg
            db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                min_size=2,
                max_size=5,
            )
            print(f"[Faust] Connected to TimescaleDB at {DB_HOST}:{DB_PORT}/{DB_NAME}")
        except Exception as e:
            print(f"[Faust] DB connection error (will retry): {e}")
    return db_pool


async def save_window_to_db(agg: dict, symbol: str, window_start: datetime, window_end: datetime):
    """Save windowed aggregation to TimescaleDB."""
    pool = await get_db_pool()
    if pool is None:
        return

    try:
        avg_price = agg['price_sum'] / max(agg['trade_count'], 1)
        vwap = agg['price_volume_sum'] / max(agg['total_volume'], 0.0001)
        volatility = (agg['max_price'] - agg['min_price']) / max(avg_price, 0.0001)

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trade_windows
                    (window_start, window_end, symbol, total_volume, trade_count,
                     avg_price, min_price, max_price, vwap, volatility)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                window_start, window_end, symbol,
                agg['total_volume'], agg['trade_count'],
                avg_price, agg['min_price'], agg['max_price'],
                vwap, volatility,
            )
    except Exception as e:
        print(f"[Faust] DB write error: {e}")


async def save_alert_to_db(alert: Alert):
    """Save alert to TimescaleDB alerts table."""
    pool = await get_db_pool()
    if pool is None:
        return

    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts (symbol, alert_type, message, price, volume, threshold)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
                alert.symbol, alert.alert_type, alert.message,
                alert.price, alert.volume, alert.threshold,
            )
    except Exception as e:
        print(f"[Faust] Alert DB write error: {e}")


# ============================================================
# Main Stream Processor
# ============================================================
@app.agent(trades_topic)
async def process_trades(stream):
    """Process incoming trades with tumbling window aggregation."""
    async for trade in stream:
        symbol = trade.symbol
        
        # 1. Update Tumbling Window
        price = trade.price
        volume = trade.volume
        now = datetime.now(timezone.utc)

        # --- Prometheus metrics ---
        trades_processed.labels(symbol=symbol).inc()
        current_price.labels(symbol=symbol).set(price)

        # Measure processing latency
        if trade.timestamp_ms > 0:
            trade_time = trade.timestamp_ms / 1000.0
            latency = now.timestamp() - trade_time
            if latency > 0:
                trade_latency.observe(latency)

        # --- 1. Whale Detection (single trade) ---
        if volume >= WHALE_VOLUME_THRESHOLD:
            alert = Alert(
                alert_type='whale',
                symbol=symbol,
                message=(
                    f"🐋 WHALE ALERT: {symbol}\n"
                    f"Volume: {volume:.4f} BTC (${volume * price:,.2f} USD)\n"
                    f"Price: ${price:,.2f}\n"
                    f"Threshold: {WHALE_VOLUME_THRESHOLD} BTC"
                ),
                price=price,
                volume=volume,
                threshold=WHALE_VOLUME_THRESHOLD,
                time=trade.time or now.isoformat(),
            )
            await alerts_topic.send(value=alert)
            await save_alert_to_db(alert)
            whale_alerts.labels(symbol=symbol).inc()
            print(f"[WHALE] {symbol} vol={volume:.4f} price=${price:,.2f}")

        # --- 2. Update tumbling window ---
        window = window_table[symbol]
        agg = window.value()

        is_new_window = (agg['trade_count'] == 0)

        # Update aggregation state
        agg['total_volume'] += volume
        agg['trade_count'] += 1
        agg['price_sum'] += price
        agg['price_volume_sum'] += price * volume
        agg['min_price'] = min(agg['min_price'], price)
        agg['max_price'] = max(agg['max_price'], price)
        if agg['window_start'] is None:
            agg['window_start'] = now.isoformat()

        if is_new_window and symbol in last_window_agg:
            prev_agg = last_window_agg[symbol]
            prev_start = datetime.fromisoformat(prev_agg['window_start'].replace('Z', '+00:00'))
            prev_end = prev_start + timedelta(seconds=WINDOW_SIZE_SECONDS)
            import asyncio
            asyncio.create_task(save_window_to_db(prev_agg, symbol, prev_start, prev_end))

        last_window_agg[symbol] = agg.copy()
        window_table[symbol] = agg

        # Update Prometheus window gauge
        window_volume.labels(symbol=symbol).set(agg['total_volume'])

        # --- Sub-second Alerts ---
        total_vol = agg['total_volume']
        avg_price = agg['price_sum'] / agg['trade_count']
        volatility = (agg['max_price'] - agg['min_price']) / max(avg_price, 0.0001)

        # --- Volume Spike Alert ---
        if total_vol >= VOLUME_SPIKE_THRESHOLD and not agg.get('spike_alerted'):
            agg['spike_alerted'] = True
            window_table[symbol] = agg
            
            alert = Alert(
                alert_type='volume_spike',
                symbol=symbol,
                message=(
                    f"📈 VOLUME SPIKE: {symbol}\n"
                    f"Window Volume: {total_vol:.4f} BTC "
                    f"(${total_vol * avg_price:,.2f} USD)\n"
                    f"Trades: {agg['trade_count']}\n"
                    f"Avg Price: ${avg_price:,.2f}\n"
                    f"Window: {WINDOW_SIZE_SECONDS}s\n"
                    f"Threshold: {VOLUME_SPIKE_THRESHOLD} BTC"
                ),
                price=avg_price,
                volume=total_vol,
                threshold=VOLUME_SPIKE_THRESHOLD,
                time=now.isoformat(),
            )
            await alerts_topic.send(value=alert)
            await save_alert_to_db(alert)
            volume_spike_alerts.labels(symbol=symbol).inc()
            print(f"[SPIKE] {symbol} vol={total_vol:.4f} avg_price=${avg_price:,.2f}", flush=True)

        # --- Volatility Alert ---
        if volatility >= VOLATILITY_THRESHOLD and not agg.get('volatility_alerted'):
            agg['volatility_alerted'] = True
            window_table[symbol] = agg
            
            alert = Alert(
                alert_type='volatility',
                symbol=symbol,
                message=(
                    f"🔥 VOLATILITY ALERT: {symbol}\n"
                    f"Price Range: ${agg['min_price']:,.2f} - ${agg['max_price']:,.2f}\n"
                    f"Volatility: {volatility:.4%}\n"
                    f"Avg Price: ${avg_price:,.2f}\n"
                    f"Window: {WINDOW_SIZE_SECONDS}s\n"
                    f"Threshold: {VOLATILITY_THRESHOLD:.4%}"
                ),
                price=avg_price,
                volume=total_vol,
                threshold=VOLATILITY_THRESHOLD,
                time=now.isoformat(),
            )
            await alerts_topic.send(value=alert)
            await save_alert_to_db(alert)
            volatility_alerts.labels(symbol=symbol).inc()
            print(f"[VOLATILITY] {symbol} vol={volatility:.4%} range=${agg['min_price']:,.2f}-${agg['max_price']:,.2f}", flush=True)



# ============================================================
# App lifecycle hooks
# ============================================================
@app.task
async def on_started():
    """Called when Faust worker has started."""
    print("=" * 60)
    print("  Faust Volatility Monitor — STARTED")
    print(f"  Broker: {KAFKA_BROKER}")
    print(f"  Window size: {WINDOW_SIZE_SECONDS}s")
    print(f"  Whale threshold: {WHALE_VOLUME_THRESHOLD} BTC")
    print(f"  Volume spike threshold: {VOLUME_SPIKE_THRESHOLD} BTC/window")
    print(f"  Volatility threshold: {VOLATILITY_THRESHOLD:.2%}")
    print("=" * 60)

    # Start Prometheus metrics HTTP server
    try:
        start_http_server(8000, registry=registry)
        print("[Faust] Prometheus metrics server started on :8000")
    except Exception as e:
        print(f"[Faust] Prometheus metrics server error: {e}")

    # Initialize DB connection
    await get_db_pool()