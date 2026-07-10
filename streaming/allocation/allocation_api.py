from fastapi import FastAPI
from dotenv import load_dotenv
import os
import snowflake.connector

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

app = FastAPI()


@app.get("/allocations/{request_id}")
def get_allocation(request_id: str):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                REQUEST_ID,
                BLOOD_BANK_ID,
                BLOOD_GROUP,
                UNITS_ALLOCATED,
                FULFILLMENT_TIME
            FROM BLOOD_REQUEST_FULFILLMENT
            WHERE REQUEST_ID = %s
            ORDER BY FULFILLMENT_TIME DESC
            LIMIT 1
            """,
            (request_id,),
        )

        row = cursor.fetchone()

        if row is None:
            return {
                "request_id": request_id,
                "status": "PENDING",
            }

        return {
            "request_id": row[0],
            "status": "ALLOCATED",
            "blood_bank_id": row[1],
            "blood_group": row[2],
            "units_allocated": row[3],
            "fulfillment_time": row[4],
        }

    finally:
        cursor.close()
        conn.close()
