import duckdb
import os

# Paths
DATA_PATH = "data/parquet"
DB_PATH = "duckdb_baseline.db"

# Connect to DuckDB (creates DB if it doesn't exist)
con = duckdb.connect(DB_PATH)

# Idempotency: drop table if it exists
con.execute("DROP TABLE IF EXISTS taxi_baseline")

# Create table from Parquet files
con.execute(f"""
    CREATE TABLE taxi_baseline AS
    SELECT
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        payment_type
    FROM read_parquet('{DATA_PATH}/*.parquet');
""")

# Verify row count
row_count = con.execute("SELECT COUNT(*) FROM taxi_baseline").fetchone()[0]
print(f"DuckDB baseline table recreated successfully with {row_count} rows.")

con.close()
