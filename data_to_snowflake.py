import os
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
load_dotenv()
# ==========================
# Snowflake Connection
# ==========================
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
ROLE = os.getenv("SNOWFLAKE_ROLE")

CSV_FOLDER = r"D:\DOWNLOADS\FILE"

# ==========================
# Connect to Snowflake
# ==========================
engine = create_engine(URL(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    schema=SCHEMA,
    warehouse=WAREHOUSE,
    role=ROLE
))

# ==========================
# Upload every CSV
# ==========================
for file in os.listdir(CSV_FOLDER):

    if not file.endswith(".csv"):
        continue

    table_name = os.path.splitext(file)[0].upper()

    path = os.path.join(CSV_FOLDER, file)

    print(f"Loading {table_name}...")

    df = pd.read_csv(path)

    # Automatically creates the table if it doesn't exist
    df.to_sql(
        table_name,
        engine,
        if_exists="replace",   # replace existing table
        index=False,
        method="multi"
    )

    print(f"{table_name} loaded successfully.")

print("All tables uploaded successfully.")