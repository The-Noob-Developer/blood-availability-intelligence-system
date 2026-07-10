from kafka import KafkaConsumer, KafkaProducer
import json
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
import os

from streaming.common.config import (
    KAFKA_BROKER,
    ALLOCATION_TOPIC,
    ALLOCATION_RESPONSE_TOPIC,
)

load_dotenv()

# -------------------------
# Snowflake Configuration
# -------------------------
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

# -------------------------
# Kafka Consumer
# -------------------------
consumer = KafkaConsumer(
    ALLOCATION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# -------------------------
# Kafka Producer
# -------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Allocation Service Started...")

try:
    for message in consumer:

        request = message.value

        print("Received:", request)

        request_id = request.get("request_id") or request["event_id"]
        units_required = request["units_required"]
        request_latitude = request["latitude"]
        request_longitude = request["longitude"]

        select_query = """
        SELECT
            bb.BLOOD_BANK_ID,
            bb."Blood Bank Name",
            bi.UNITS_AVAILABLE,
            6371 * ACOS(
                LEAST(
                    1,
                    GREATEST(
                        -1,
                        COS(RADIANS(%s)) * COS(RADIANS(bb."Latitude"))
                        * COS(RADIANS(bb."Longitude") - RADIANS(%s))
                        + SIN(RADIANS(%s)) * SIN(RADIANS(bb."Latitude"))
                    )
                )
            ) AS DISTANCE_KM
        FROM BLOOD_BANK bb
        JOIN BLOOD_INVENTORY bi
            ON bi.BLOOD_BANK_ID = bb.BLOOD_BANK_ID
        WHERE bi.BLOOD_GROUP = %s
          AND bi.UNITS_AVAILABLE >= %s
        ORDER BY DISTANCE_KM ASC, bi.UNITS_AVAILABLE DESC, bb.BLOOD_BANK_ID ASC
        LIMIT 1
        """

        cursor.execute(
            select_query,
            (
                request_latitude,
                request_longitude,
                request_latitude,
                request["blood_group"],
                units_required,
            ),
        )
        allocation_row = cursor.fetchone()

        if allocation_row is None:
            allocation_result = {
                "request_id": request_id,
                "status": "UNALLOCATED",
                "blood_bank_id": None,
                "blood_bank_name": None,
                "blood_group": request["blood_group"],
                "units_allocated": 0
            }
            producer.send(ALLOCATION_RESPONSE_TOPIC, allocation_result)
            producer.flush()
            print("No allocation available")
            continue

        blood_bank_id, blood_bank_name, _, distance_km = allocation_row
        units_allocated = units_required

        # -------------------------
        # Insert into Snowflake
        # -------------------------
        update_query = """
        UPDATE BLOOD_INVENTORY
        SET UNITS_AVAILABLE = UNITS_AVAILABLE - %s
        WHERE BLOOD_BANK_ID = %s
          AND BLOOD_GROUP = %s
          AND UNITS_AVAILABLE >= %s
        """

        insert_query = """
        INSERT INTO BLOOD_REQUEST_FULFILLMENT
        (
            FULFILLMENT_ID,
            REQUEST_ID,
            BLOOD_BANK_ID,
            BLOOD_GROUP,
            UNITS_ALLOCATED,
            FULFILLMENT_TIME
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        # Temporary fulfillment id
        fulfillment_id = request_id

        cursor.execute(
            update_query,
            (
                units_allocated,
                blood_bank_id,
                request["blood_group"],
                units_allocated
            )
        )

        cursor.execute(
            insert_query,
            (
                fulfillment_id,
                request_id,
                blood_bank_id,
                request["blood_group"],
                units_allocated,
                datetime.utcnow().isoformat()
            )
        )

        conn.commit()

        print("Inserted into BLOOD_REQUEST_FULFILLMENT")

        allocation_result = {
            "request_id": request_id,
            "status": "ALLOCATED",
            "blood_bank_id": blood_bank_id,
            "blood_bank_name": blood_bank_name,
            "blood_group": request["blood_group"],
            "units_allocated": units_allocated,
            "distance_km": distance_km
        }

        producer.send(ALLOCATION_RESPONSE_TOPIC, allocation_result)
        producer.flush()

        print("Allocation Result Published")

except KeyboardInterrupt:
    print("Stopping Allocation Service...")

finally:
    cursor.close()
    conn.close()
    consumer.close()
    producer.close()

    print("Allocation Service Stopped")
