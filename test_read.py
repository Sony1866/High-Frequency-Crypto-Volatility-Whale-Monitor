from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)
producer.send("trades-stream", {"price": 65000, "volume": 1.5, "time": "2026-09-05T14:00:00"})
producer.flush()
print("Trade sent")