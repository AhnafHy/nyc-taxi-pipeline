import snowflake.connector
import os
import sys

SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
    'user': os.environ.get('SNOWFLAKE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),
    'database': 'NYC_TAXI',
    'warehouse': 'TAXI_WH',
    'role': 'ACCOUNTADMIN'
}

def run_check(cursor, check_name, query, expected):
    cursor.execute(query)
    result = cursor.fetchone()[0]
    status = "PASS" if result == expected else "FAIL"
    print(f"[{status}] {check_name}: got {result}, expected {expected}")
    return status == "PASS"

def validate_bronze(cursor):
    print("\n--- Bronze Layer Checks ---")
    checks = [
        ("Row count > 0", "SELECT COUNT(*) > 0 FROM NYC_TAXI.BRONZE.RAW_TAXI_TRIPS", True),
        ("No null pickup times", "SELECT COUNT(*) FROM NYC_TAXI.BRONZE.RAW_TAXI_TRIPS WHERE TPEP_PICKUP_DATETIME IS NULL", 0),
        ("No null dropoff times", "SELECT COUNT(*) FROM NYC_TAXI.BRONZE.RAW_TAXI_TRIPS WHERE TPEP_DROPOFF_DATETIME IS NULL", 0),
        ("Negative fares under 2%", "SELECT (COUNT(*) < 1000) FROM NYC_TAXI.BRONZE.RAW_TAXI_TRIPS WHERE FARE_AMOUNT < 0", True),
    ]
    return all(run_check(cursor, name, query, expected) for name, query, expected in checks)

def validate_silver(cursor):
    print("\n--- Silver Layer Checks ---")
    checks = [
        ("Row count > 0", "SELECT COUNT(*) > 0 FROM NYC_TAXI.SILVER.SILVER_TAXI_TRIPS", True),
        ("No zero distances", "SELECT COUNT(*) FROM NYC_TAXI.SILVER.SILVER_TAXI_TRIPS WHERE TRIP_DISTANCE <= 0", 0),
        ("Invalid durations under 1%", "SELECT (COUNT(*) < 500) FROM NYC_TAXI.SILVER.SILVER_TAXI_TRIPS WHERE TRIP_DURATION_MINUTES <= 0", True),
        ("Payment method populated", "SELECT COUNT(*) FROM NYC_TAXI.SILVER.SILVER_TAXI_TRIPS WHERE PAYMENT_METHOD = 'Unknown'", 0),
    ]
    return all(run_check(cursor, name, query, expected) for name, query, expected in checks)

def validate_gold(cursor):
    print("\n--- Gold Layer Checks ---")
    checks = [
        ("Row count > 0", "SELECT COUNT(*) > 0 FROM NYC_TAXI.GOLD.GOLD_TRIP_SUMMARY", True),
        ("No negative revenue", "SELECT COUNT(*) FROM NYC_TAXI.GOLD.GOLD_TRIP_SUMMARY WHERE TOTAL_REVENUE < 0", 0),
        ("Avg fare reasonable", "SELECT AVG(AVG_FARE) BETWEEN 5 AND 100 FROM NYC_TAXI.GOLD.GOLD_TRIP_SUMMARY", True),
    ]
    return all(run_check(cursor, name, query, expected) for name, query, expected in checks)

if __name__ == '__main__':
    print("Starting data quality validation...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    bronze_ok = validate_bronze(cursor)
    silver_ok = validate_silver(cursor)
    gold_ok = validate_gold(cursor)

    cursor.close()
    conn.close()

    if bronze_ok and silver_ok and gold_ok:
        print("\n✅ All quality checks passed")
        sys.exit(0)
    else:
        print("\n❌ Quality checks failed — pipeline halted")
        sys.exit(1)
