from kafka import KafkaConsumer
import json
import snowflake.connector
from streaming.common.config import KAFKA_BROKER, DONATION_TOPIC
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
    DONATION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    # Decode Binary to string -> String to JSON
)

print("Waiting for messages...")

try:
    for message in consumer:
        data = message.value

        columns = ", ".join(col.upper() for col in data.keys())
        placeholders = ", ".join(["%s"] * len(data))

        insert_query = f"""
        INSERT INTO DONATION_EVENT ({columns})
        VALUES ({placeholders})
        """

        cursor.execute(insert_query, tuple(data.values()))

        update_query = """
        UPDATE BLOOD_INVENTORY
        SET UNITS_AVAILABLE = UNITS_AVAILABLE + %s
        WHERE BLOOD_BANK_ID = %s
          AND BLOOD_GROUP = %s;
        """

        cursor.execute(
            update_query,
            (
                data["units_donated"],
                data["blood_bank_id"],
                data["blood_group"],
            ),
        )

        conn.commit()

        print(f"Processed Donation ID: {data['donor_id']}")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")