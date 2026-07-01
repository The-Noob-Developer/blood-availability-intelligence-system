from kafka import KafkaProducer
import json
import time
from config import KAFKA_BROKER, DONATION_TOPIC
from events.donation_event import DonationEvent

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(10):
    event = {
        "donor_id": i + 1,
        "blood_group": "O+",
        "units": 1
    }

    producer.send("donations", event)
    producer.flush()

    print(f"Sent {i+1}")

    time.sleep(2)