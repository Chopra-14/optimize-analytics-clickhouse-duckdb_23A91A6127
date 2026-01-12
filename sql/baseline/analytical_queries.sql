-- Q1: Total revenue per month
SELECT
    toYYYYMM(tpep_pickup_datetime) AS pickup_month,
    SUM(fare_amount) AS total_revenue
FROM analytics.taxi_baseline
GROUP BY pickup_month
ORDER BY pickup_month;

-- Q2: Average trip distance per vendor (filtered)
SELECT
    VendorID,
    AVG(trip_distance) AS avg_trip_distance
FROM analytics.taxi_baseline
WHERE trip_distance > 0
GROUP BY VendorID
ORDER BY VendorID;

-- Q3: Rolling average fare per vendor
SELECT
    VendorID,
    tpep_pickup_datetime,
    fare_amount,
    AVG(fare_amount) OVER (
        PARTITION BY VendorID
        ORDER BY tpep_pickup_datetime
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_fare
FROM analytics.taxi_baseline;

-- Q4: Count of high-value trips
SELECT
    COUNT(*) AS high_value_trip_count
FROM analytics.taxi_baseline
WHERE fare_amount > 50;

