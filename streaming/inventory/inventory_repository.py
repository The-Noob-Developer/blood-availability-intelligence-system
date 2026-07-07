import snowflake.connector
from streaming.common.snowflake import get_connection


def find_inventory(blood_group, units_required):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT BLOOD_BANK_ID,
               AVAILABLE_UNITS,
               LATITUDE,
               LONGITUDE
        FROM BLOOD_INVENTORY
        WHERE BLOOD_GROUP = %s
          AND AVAILABLE_UNITS >= %s
        ORDER BY AVAILABLE_UNITS DESC
        LIMIT 1
    """, (blood_group, units_required))

    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return row