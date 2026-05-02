{{ config(schema='BRONZE', alias='BRONZE_TAXI_TRIPS') }}
-- Bronze layer: raw data with basic cleaning only
SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    ingested_at
FROM NYC_TAXI.BRONZE.RAW_TAXI_TRIPS
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
