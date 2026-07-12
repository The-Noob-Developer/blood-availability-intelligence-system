from kafka import KafkaProducer
import json
from streaming.common.config import KAFKA_BROKER, DONATION_TOPIC


def publish_donation(event: dict):
    print("helooooooo")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER, # Tells the producer where Kafka is running
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
        # {k1:v1 , k2:v2} -> string i.e., '{k1:v1 , k2:v2}' done by json.dumps()
        # .encode("utf-8") : convert to bytes
        # JSON to String -> String to Encode Binary
    )

    producer.send(DONATION_TOPIC, event)
    producer.flush() # Sends all buffered messages and waits until Kafka acknowledges them

    print(f"Published donation event: {event}")

    producer.close()