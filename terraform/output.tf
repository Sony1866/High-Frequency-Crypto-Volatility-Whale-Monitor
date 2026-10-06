output "kafka_address" {
  value = "kafka:9092"
}

output "timescaledb_connection" {
  value = "postgresql://postgres:postgres@timescaledb:5432/volatility_db"
}

output "telegram_webhook" {
  value = "https://api.telegram.org/bot${var.telegram_token}/sendMessage"
}

variable "telegram_token" {
  description = "Telegram bot token"
  type        = string
  sensitive   = true
}