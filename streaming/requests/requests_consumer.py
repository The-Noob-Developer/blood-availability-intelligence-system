from kafka import KafkaConsumer
import json
import snowflake.connector
from streaming.common.config import KAFKA_BROKER, REQUEST_TOPIC
from dotenv import load_dotenv
load_dotenv()
import os

ACCOUNT = os.getenv("SF_ACCOUNT")
USER = os.getenv("SF_USER")
PASSWORD = os.getenv("SF_PASSWORD")
DATABASE = os.getenv("SF_DATABASE")
SCHEMA = os.getenv("SF_SCHEMA")
WAREHOUSE = os.getenv("SF_WAREHOUSE")
ROLE = os.getenv("SF_ROLE")

SNOWFLAKE_CONFIG = {
    "user": USER,
    "password": PASSWORD,
    "account": ACCOUNT,
    "warehouse": WAREHOUSE,
    "database": DATABASE,
    "schema": SCHEMA,
    "role": ROLE,
}

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()


consumer = KafkaConsumer(
    REQUEST_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    # Decode Binary to string -> String to JSON
)

print("Waiting for messages...")

try:
    for message in consumer:
        data = message.value

        print(data)

        columns = ", ".join(col.upper() for col in data.keys())
        placeholders = ", ".join(["%s"] * len(data))

        query = f"""
        INSERT INTO BLOOD_REQUEST ({columns})
        VALUES ({placeholders})
        """

        cursor.execute(query, tuple(data.values()))
        conn.commit()

        print("Inserted successfully")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")