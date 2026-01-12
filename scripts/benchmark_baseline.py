import time
import csv
import clickhouse_connect

# Baseline analytical queries
QUERIES = {
    "Q1_Monthly_Revenue": """
        SELECT
            toYYYYMM(tpep_pickup_datetime) AS pickup_month,
            SUM(fare_amount) AS total_revenue
        FROM taxi_baseline
        GROUP BY pickup_month
        ORDER BY pickup_month
    """,
    "Q2_Avg_Trip_Distance_Per_Vendor": """
        SELECT
            VendorID,
            AVG(trip_distance) AS avg_trip_distance
        FROM taxi_baseline
        WHERE trip_distance > 0
        GROUP BY VendorID
        ORDER BY VendorID
    """,
    "Q3_Rolling_Avg_Fare": """
        SELECT
            VendorID,
            tpep_pickup_datetime,
            fare_amount,
            AVG(fare_amount) OVER (
                PARTITION BY VendorID
                ORDER BY tpep_pickup_datetime
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_avg_fare
        FROM taxi_baseline
    """,
    "Q4_High_Value_Trip_Count": """
        SELECT
            COUNT(*) AS high_value_trip_count
        FROM taxi_baseline
        WHERE fare_amount > 50
    """
}

ITERATIONS = 3
RESULTS_FILE = "results/baseline_benchmark_results.csv"

results = []

print("Starting baseline benchmarking...\n")

for query_name, query_sql in QUERIES.items():
    times = []
    print(f"Running {query_name}...")

    for i in range(ITERATIONS):
        # NEW client per iteration (prevents session locking)
        client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            database="analytics",
            session_id=None   # explicitly disable session reuse
        )

        start_time = time.time()
        client.query(query_sql)
        elapsed = time.time() - start_time
        times.append(elapsed)

        print(f"  Iteration {i + 1}: {elapsed:.4f} seconds")

        client.close()

    avg_time = sum(times) / len(times)

    results.append({
        "query": query_name,
        "iteration_1": times[0],
        "iteration_2": times[1],
        "iteration_3": times[2],
        "average_time_sec": avg_time
    })

    print(f"  Average time: {avg_time:.4f} seconds\n")

# Write results to CSV
with open(RESULTS_FILE, mode="w", newline="") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "query",
            "iteration_1",
            "iteration_2",
            "iteration_3",
            "average_time_sec"
        ]
    )
    writer.writeheader()
    writer.writerows(results)

print("Baseline benchmarking completed.")
print(f"Results saved to {RESULTS_FILE}")
