from kafka import KafkaConsumer
import json

from config import KAFKA_BROKER, DONATION_TOPIC

consumer = KafkaConsumer(
    DONATION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Listening...")

for message in consumer:
    print(message.value)