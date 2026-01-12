import os
import clickhouse_connect
import pandas as pd

# Configuration
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
DATABASE = "analytics"
TABLE = "taxi_optimized"
DATA_PATH = "data/parquet"

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    database=DATABASE,
    session_id=None
)

# Idempotency: clear optimized table before loading
client.command(f"TRUNCATE TABLE {TABLE}")
print("Optimized table truncated successfully.")

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

    # Schema alignment
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

print(f"Optimized ClickHouse table loaded successfully with {total_rows} rows.")
client.close()
