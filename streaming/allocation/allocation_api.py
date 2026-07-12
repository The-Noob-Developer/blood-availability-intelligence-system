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
                f.REQUEST_ID,
                f.BLOOD_BANK_ID,
                bb."Blood Bank Name" AS BLOOD_BANK_NAME,
                f.BLOOD_GROUP,
                f.UNITS_ALLOCATED,
                f.DISTANCE_KM,
                f.FULFILLMENT_TIME
            FROM BLOOD_REQUEST_FULFILLMENT f
            LEFT JOIN BLOOD_BANK bb
                ON bb.BLOOD_BANK_ID = f.BLOOD_BANK_ID
            WHERE f.REQUEST_ID = %s
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
            "blood_bank_name": row[2],
            "blood_group": row[3],
            "units_allocated": row[4],
            "distance_km": row[5],
            "fulfillment_time": row[6],
        }

    finally:
        cursor.close()
        conn.close()
