import datetime
import os
from typing import Optional

import snowflake.connector
from dotenv import load_dotenv
from streaming.auth.auth_models import UserCreate

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": os.getenv("SF_SCHEMA"),
    "role": os.getenv("SF_ROLE"),
}


def get_connection():
    """Establishes a connection to Snowflake."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def serialize_row(row, columns):
    # Snowflake returns column names in uppercase. Convert them to lowercase for consistency.
    user = dict(zip([col.lower() for col in columns], row))
    for key, value in list(user.items()):
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            user[key] = value.isoformat()
    return user


def get_user_by_google_sub(google_sub: str) -> Optional[dict]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, full_name, given_name, family_name, picture, locale, google_sub, created_at, last_login_at FROM auth_user WHERE google_sub = %s",
            (google_sub,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return serialize_row(row, columns)
    finally:
        if "cursor" in locals():
            cursor.close()
        conn.close()


def get_user_by_email(email: str) -> Optional[dict]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, full_name, given_name, family_name, picture, locale, google_sub, created_at, last_login_at FROM auth_user WHERE email = %s",
            (email,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return serialize_row(row, columns)
    finally:
        if "cursor" in locals():
            cursor.close()
        conn.close()


def create_or_update_user(user_data: UserCreate) -> dict:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            MERGE INTO auth_user AS target
            USING (
                SELECT %s AS email, %s AS full_name, %s AS given_name, %s AS family_name,
                       %s AS picture, %s AS locale, %s AS google_sub
            ) AS source
            ON target.google_sub = source.google_sub
            WHEN MATCHED THEN UPDATE SET
                target.email = source.email,
                target.full_name = source.full_name,
                target.given_name = source.given_name,
                target.family_name = source.family_name,
                target.picture = source.picture,
                target.locale = source.locale,
                target.last_login_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                email, full_name, given_name, family_name, picture, locale, google_sub, created_at, last_login_at
            ) VALUES (
                source.email, source.full_name, source.given_name, source.family_name,
                source.picture, source.locale, source.google_sub, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            (
                user_data.email,
                user_data.full_name,
                user_data.given_name,
                user_data.family_name,
                user_data.picture,
                user_data.locale,
                user_data.google_sub,
            ),
        )
        conn.commit()

        # Read back the user to get the ID and other details
        cursor.execute(
            "SELECT id, email, full_name, given_name, family_name, picture, locale, google_sub, created_at, last_login_at FROM auth_user WHERE google_sub = %s",
            (user_data.google_sub,),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Failed to read back user after MERGE")
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return serialize_row(row, columns)
    finally:
        if "cursor" in locals():
            cursor.close()
        conn.close()
