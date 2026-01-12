import clickhouse_connect
import sys

def main():
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        database='analytics'
    )

    validation_query = """
    WITH
    mv AS (
        SELECT
            pickup_month,
            SUM(total_revenue) AS mv_revenue
        FROM mv_monthly_revenue
        GROUP BY pickup_month
    ),
    raw AS (
        SELECT
            toYYYYMM(tpep_pickup_datetime) AS pickup_month,
            SUM(fare_amount) AS raw_revenue
        FROM taxi_optimized
        GROUP BY pickup_month
    )
    SELECT
        mv.pickup_month,
        abs(mv.mv_revenue - raw.raw_revenue) AS diff
    FROM mv
    INNER JOIN raw USING (pickup_month)
    WHERE abs(mv.mv_revenue - raw.raw_revenue) > 0.01
    """

    result = client.query(validation_query).result_rows

    if len(result) == 0:
        print("✅ Materialized View Validation PASSED")
        print("No mismatches found between MV and raw aggregation.")
    else:
        print("❌ Materialized View Validation FAILED")
        print("Mismatched rows:")
        for row in result:
            print(row)
        sys.exit(1)

if __name__ == "__main__":
    main()
