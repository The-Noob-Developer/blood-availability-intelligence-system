import json
from kafka import KafkaProducer

from streaming.common.config import KAFKA_BROKER, REQUEST_TOPIC

producer = None


def get_producer() -> KafkaProducer:
    global producer

    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            linger_ms=10,
            batch_size=32768,
        )

    return producer


def publish_request(event: dict):
    kafka_producer = get_producer()
    kafka_producer.send(REQUEST_TOPIC, event)

    print(f"Published request event: {event}")