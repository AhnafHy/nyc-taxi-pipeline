{{ config(schema='GOLD', alias='GOLD_TRIP_SUMMARY') }}

SELECT
    DATE(tpep_pickup_datetime) AS trip_date,
    payment_method,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_percentage), 2) AS avg_tip_pct,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{ ref('silver_taxi_trips') }}
GROUP BY trip_date, payment_method
ORDER BY total_trips DESC
