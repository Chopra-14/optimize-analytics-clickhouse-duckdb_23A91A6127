import os
import clickhouse_connect
import pandas as pd

# Configuration
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
DATABASE = "analytics"
TABLE = "taxi_baseline"
DATA_PATH = "data/parquet"

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    database=DATABASE
)

# Idempotency: clear table before loading
client.command(f"TRUNCATE TABLE {TABLE}")

print("Baseline table truncated successfully.")

# Load and insert Parquet files
parquet_files = [
    os.path.join(DATA_PATH, f)
    for f in os.listdir(DATA_PATH)
    if f.endswith(".parquet")
]

total_rows = 0

for file in parquet_files:
    print(f"Loading file: {file}")
    df = pd.read_parquet(file)

    # Select only required columns (schema alignment)
    df = df[
        [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "payment_type",
        ]
    ]

    client.insert_df(TABLE, df)
    total_rows += len(df)

print(f"ClickHouse baseline table loaded successfully with {total_rows} rows.")
