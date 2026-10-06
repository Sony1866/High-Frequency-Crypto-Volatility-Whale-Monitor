terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

resource "docker_image" "kafka" {
  name = "confluentinc/cp-kafka:7.6.1"
}

resource "docker_image" "zookeeper" {
  name = "confluentinc/cp-zookeeper:7.6.1"
}

resource "docker_image" "faust" {
  name = "faust-volatility"
}

resource "docker_image" "timescaledb" {
  name = "timescale/timescaledb:latest-pg17"
}

resource "docker_image" "prometheus" {
  name = "prom/prometheus:latest"
}

resource "docker_image" "grafana" {
  name = "grafana/grafana:11.2.0"
}

resource "docker_image" "telegram" {
  name = "telegram-bot"
}