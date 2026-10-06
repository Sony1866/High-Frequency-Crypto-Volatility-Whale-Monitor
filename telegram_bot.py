"""
Telegram Alert Bot
==================
Consumes alerts from Kafka 'alerts' topic and sends
formatted notifications to Telegram.
"""

import asyncio
import json
import os
from kafka import KafkaConsumer
from telegram import Bot
from telegram.error import TelegramError

# Configuration
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
ALERTS_TOPIC = os.environ.get('ALERTS_TOPIC', 'alerts')

# Alert type emoji mapping
ALERT_EMOJI = {
    'whale': '🐋',
    'volume_spike': '📈',
    'volatility': '🔥',
}


def format_alert(data: dict) -> str:
    """Format alert data into a Telegram message."""
    alert_type = data.get('alert_type', 'unknown')
    emoji = ALERT_EMOJI.get(alert_type, '🚨')

    # If the alert already has a formatted message, use it
    if 'message' in data and data['message']:
        return f"{data['message']}\n⏰ {data.get('time', 'N/A')}"

    # Fallback formatting
    return (
        f"{emoji} ALERT: {alert_type.upper()}\n"
        f"Symbol: {data.get('symbol', 'N/A')}\n"
        f"Price: ${data.get('price', 0):,.2f}\n"
        f"Volume: {data.get('volume', 0):.4f}\n"
        f"Threshold: {data.get('threshold', 'N/A')}\n"
        f"Time: {data.get('time', 'N/A')}"
    )


async def send_alert(bot: Bot, message: str):
    """Send alert message to Telegram with rate limiting."""
    if not CHAT_ID:
        print(f"[Telegram] No CHAT_ID set, skipping: {message[:80]}...")
        return

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode=None,
        )
        print(f"[Telegram] Sent alert ({len(message)} chars)")
    except TelegramError as e:
        print(f"[Telegram] Error sending: {e}")
        # Rate limit handling
        if 'Retry' in str(e):
            await asyncio.sleep(5)


async def main():
    """Main consumer loop."""
    print("=" * 60)
    print("  Telegram Alert Bot")
    print(f"  Kafka: {KAFKA_BOOTSTRAP}")
    print(f"  Topic: {ALERTS_TOPIC}")
    print(f"  Chat ID: {CHAT_ID or '(not set)'}")
    print("=" * 60)

    if not TELEGRAM_TOKEN:
        print("[Telegram] WARNING: TELEGRAM_TOKEN not set!")
        print("[Telegram] Bot will log alerts to console only.")

    # Initialize Telegram bot (only if token is set)
    bot = None
    if TELEGRAM_TOKEN:
        try:
            bot = Bot(token=TELEGRAM_TOKEN)
            print(f"[Telegram] Bot initialized")
        except Exception as e:
            print(f"[Telegram] Bot init failed: {e}")

    # Connect to Kafka with retry
    consumer = None
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                ALERTS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='telegram-alert-group',
                enable_auto_commit=True,
            )
            print(f"[Kafka] Connected on attempt {attempt}")
            break
        except Exception as e:
            print(f"[Kafka] Connection attempt {attempt}/30 failed: {e}")
            if attempt < 30:
                await asyncio.sleep(5)
            else:
                raise

    print("[Bot] Listening for alerts...")
    alert_count = 0

    for msg in consumer:
        data = msg.value
        alert_count += 1

        alert_text = format_alert(data)
        print(f"\n[Alert #{alert_count}] {data.get('alert_type', '?')} — {data.get('symbol', '?')}")

        if bot:
            await send_alert(bot, alert_text)
            # Simple rate limiting: max 1 message per second
            await asyncio.sleep(1)
        else:
            print(f"[Console] {alert_text}")


if __name__ == "__main__":
    asyncio.run(main())