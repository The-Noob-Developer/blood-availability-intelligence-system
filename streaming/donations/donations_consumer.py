from kafka import KafkaConsumer
import json
import snowflake.connector
from dotenv import load_dotenv
import os
from collections import defaultdict
import time

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
    group_id="donation-consumer-group",
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("\nDonation Consumer Started...\n")

message_count = 0
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))

# batching buffers
batch_rows = []
batch_columns = None
inventory_delta = defaultdict(int)  # key: (blood_bank_id, blood_group) -> units
last_flush = time.time()


def flush_batch(cursor, conn, batch_rows, batch_columns, inventory_delta):
    """Flush the current batch_rows and inventory_delta to Snowflake.
    Returns the new batch_columns value (None).
    """
    if not batch_rows:
        return None

    columns_sql = ", ".join(col.upper() for col in batch_columns)
    placeholders = ", ".join(["%s"] * len(batch_columns))
    insert_query = f"INSERT INTO DONATION_EVENT ({columns_sql}) VALUES ({placeholders})"

    try:
        cursor.executemany(insert_query, batch_rows)

        update_query = (
            "UPDATE BLOOD_INVENTORY"
            " SET UNITS_AVAILABLE = UNITS_AVAILABLE + %s"
            " WHERE BLOOD_BANK_ID = %s AND BLOOD_GROUP = %s"
        )
        for (bank_id, group), delta in inventory_delta.items():
            cursor.execute(update_query, (delta, bank_id, group))

        conn.commit()
        print(f"Committed batch of {len(batch_rows)} donation rows; updated {len(inventory_delta)} inventory keys")

    except Exception as e:
        print(f"Batch insert failed during flush: {e}; falling back to single-row inserts")
        for r in batch_rows:
            try:
                cursor.execute(insert_query, r)
            except Exception as e2:
                print(f"Failed single insert for row {r}: {e2}")
        for (bank_id, group), delta in inventory_delta.items():
            try:
                cursor.execute(update_query, (delta, bank_id, group))
            except Exception as e3:
                print(f"Failed inventory update for {(bank_id, group)}: {e3}")
        conn.commit()

    # clear buffers
    batch_rows.clear()
    inventory_delta.clear()
    return None

try:
    for message in consumer:

        data = message.value

        # initialize batch columns order from first message in batch
        if batch_columns is None:
            batch_columns = list(data.keys())

        # prepare row tuple according to batch_columns order
        row = tuple(data.get(col) for col in batch_columns)
        batch_rows.append(row)

        # aggregate inventory update per (blood_bank_id, blood_group)
        try:
            inventory_delta[(data["blood_bank_id"], data["blood_group"])] += int(data["units_donated"])
        except Exception:
            # fall back to adding as-is if casting fails
            inventory_delta[(data.get("blood_bank_id"), data.get("blood_group"))] += data.get("units_donated", 0)

        # If batch full or flush interval exceeded, flush inserts and aggregated updates
        if len(batch_rows) >= BATCH_SIZE or (batch_rows and (time.time() - last_flush) >= FLUSH_INTERVAL_SEC):
            batch_columns = flush_batch(cursor, conn, batch_rows, batch_columns, inventory_delta)
            last_flush = time.time()
            # reset batch_columns after flush
            batch_columns = None
            continue

except KeyboardInterrupt:
    print("\nStopping consumer...")
    # flush any remaining rows before exit
    if batch_rows:
        batch_columns = flush_batch(cursor, conn, batch_rows, batch_columns, inventory_delta)
        last_flush = time.time()

finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")