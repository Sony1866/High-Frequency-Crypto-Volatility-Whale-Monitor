resource "docker_container" "zookeeper" {
  name  = "terraform-zookeeper"
  image = docker_image.zookeeper.latest
  ports {
    internal = 2181
    external = 2181
  }
  depends_on = [docker_image.zookeeper]
}

resource "docker_container" "kafka" {
  name  = "terraform-kafka"
  image = docker_image.kafka.latest
  ports {
    internal = 9092
    external = 9092
  }
  depends_on = [docker_container.zookeeper]
}

resource "docker_container" "faust" {
  name  = "terraform-faust"
  image = docker_image.faust.latest
  depends_on = [docker_container.kafka]
}

resource "docker_container" "timescaledb" {
  name  = "terraform-timescaledb"
  image = docker_image.timescaledb.latest
  ports {
    internal = 5432
    external = 5432
  }
  depends_on = [docker_image.timescaledb]
}

resource "docker_container" "prometheus" {
  name  = "terraform-prometheus"
  image = docker_image.prometheus.latest
  ports {
    internal = 9090
    external = 9090
  }
  depends_on = [docker_image.prometheus]
}

resource "docker_container" "grafana" {
  name  = "terraform-grafana"
  image = docker_image.grafana.latest
  ports {
    internal = 3000
    external = 3030
  }
  depends_on = [docker_image.grafana]
}

resource "docker_container" "telegram" {
  name  = "terraform-telegram"
  image = docker_image.telegram.latest
  depends_on = [docker_container.faust]
}