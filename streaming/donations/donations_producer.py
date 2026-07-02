from kafka import KafkaProducer
import json

from streaming.common.config import KAFKA_BROKER, DONATION_TOPIC


def publish_donation(event: dict):

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    producer.send(DONATION_TOPIC, event)
    producer.flush()

    print(f"Published donation event: {event}")

    producer.close()