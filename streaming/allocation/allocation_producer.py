from kafka import KafkaProducer
import json
from streaming.common.config import KAFKA_BROKER, ALLOCATION_TOPIC


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def publish_allocation(event: dict):
    producer.send(ALLOCATION_TOPIC, event)
    producer.flush()

    print(f"Published allocation event: {event}")