from kafka import KafkaConsumer
import json
import snowflake.connector
import time
from dotenv import load_dotenv
import os

from streaming.common.config import KAFKA_BROKER, DONATION_TOPIC

load_dotenv()

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
)

print("\nDonation Consumer Started...\n")

message_count = 0

try:
    for message in consumer:

        total_start = time.time()

        data = message.value

        kafka_delay = total_start - data["created_at"]

        db_start = time.time()

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

        db_end = time.time()

        db_time = db_end - db_start

        total_time = db_end - data["created_at"]

        message_count += 1

        print("\n" + "=" * 60)
        print(f"Message Number      : {message_count}")
        print(f"Donor ID            : {data['donor_id']}")
        print(f"Kafka Delay         : {kafka_delay:.4f} sec")
        print(f"Database Time       : {db_time:.4f} sec")
        print(f"End-to-End Time     : {total_time:.4f} sec")
        print("=" * 60)

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")