{{ config(schema='SILVER', alias='SILVER_TAXI_TRIPS') }}

SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    ROUND(tip_amount / NULLIF(fare_amount, 0) * 100, 2) AS tip_percentage,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown'
    END AS payment_method,
    ingested_at
FROM {{ ref('bronze_taxi_trips') }}
WHERE trip_distance > 0
  AND fare_amount > 0
  AND passenger_count > 0
