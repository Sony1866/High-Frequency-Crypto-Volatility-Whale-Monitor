# Portofolio - High-Frequency Volatility Monitor

## Sistem Ringkasan
Sistem ini mendeteksi whale transaction, volatility spike, dan slippage secara real-time menggunakan:
- Binance WebSocket
- Kafka + Faust
- TimescaleDB (PostgreSQL)
- Telegram Bot notifier
- Grafana + Prometheus dashboard

## Struktur Project
```
.
├── docker-compose.yml          # Full stack (Kafka, Faust, TimescaleDB, Grafana, Prometheus, Telegram)
├── terraform/
│   ├── main.tf
│   ├── docker-compose.tf
│   └── output.tf
├── faust_app.py                # Faust stream processing + alert logic
├── telegram_bot.py             # Telegram notifier
├── Dockerfile.faust
├── Dockerfile.telegram
├── prometheus.yml
└── README.md
```

## Cara Menjalankan
1. `docker compose up -d` (dari root)
2. Buka Grafana: http://localhost:3000 (admin/admin)
3. Prometheus: http://localhost:9090
4. Telegram bot akan otomatis mengirim notifikasi saat volume spike

## Database Rekomendasi
**TimescaleDB** (PostgreSQL dengan extension) adalah pilihan terbaik karena:
- Optimized untuk time-series data
- SQL compatible dengan PostgreSQL
- Support continuous aggregates (volume rolling window)
- Low latency insert/update
- Grafana native support

## Telegram Integration
Bot akan mengirim notifikasi ke Telegram saat ada volume spike > threshold.

## Terraform
Terraform deploy ke Docker swarm atau Kubernetes.