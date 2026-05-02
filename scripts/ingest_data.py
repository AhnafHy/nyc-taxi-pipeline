import snowflake.connector

import pandas as pd

import requests

import os

from io import BytesIO

SNOWFLAKE_CONFIG = {

    'account': os.environ.get('SNOWFLAKE_ACCOUNT'),

    'user': os.environ.get('SNOWFLAKE_USER'),

    'password': os.environ.get('SNOWFLAKE_PASSWORD'),

    'database': 'NYC_TAXI',

    'schema': 'BRONZE',

    'warehouse': 'TAXI_WH',

    'role': 'ACCOUNTADMIN'

}

def download_taxi_data():

    print("Downloading NYC taxi data...")

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    response = requests.get(url)

    df = pd.read_parquet(BytesIO(response.content))

    # Use a sample for speed

    df = df.sample(n=50000, random_state=42)

    print(f"Downloaded {len(df)} rows")

    return df

def load_to_snowflake(df):

    print("Connecting to Snowflake...")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    cursor = conn.cursor()

    print("Creating bronze table...")

    cursor.execute("""

        CREATE OR REPLACE TABLE NYC_TAXI.BRONZE.RAW_TAXI_TRIPS (

            vendor_id INTEGER,

            tpep_pickup_datetime TIMESTAMP,

            tpep_dropoff_datetime TIMESTAMP,

            passenger_count FLOAT,

            trip_distance FLOAT,

            fare_amount FLOAT,

            tip_amount FLOAT,

            total_amount FLOAT,

            payment_type INTEGER,

            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

        )

    """)

    print("Loading data...")

    from snowflake.connector.pandas_tools import write_pandas

    df_clean = df[[

        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',

        'passenger_count', 'trip_distance', 'fare_amount',

        'tip_amount', 'total_amount', 'payment_type'

    ]].copy()

    df_clean.columns = [

        'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME',

        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'FARE_AMOUNT',

        'TIP_AMOUNT', 'TOTAL_AMOUNT', 'PAYMENT_TYPE'

    ]

    write_pandas(conn, df_clean, 'RAW_TAXI_TRIPS',

                database='NYC_TAXI', schema='BRONZE')

    print("Data loaded successfully")

    cursor.close()

    conn.close()

if __name__ == '__main__':

    df = download_taxi_data()

    load_to_snowflake(df)
