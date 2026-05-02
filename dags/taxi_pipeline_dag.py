from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_DIR = os.path.expanduser('~/nyc-taxi-pipeline')
DBT_DIR = os.path.join(PROJECT_DIR, 'dbt_project')

def run_ingestion():
    result = subprocess.run(
        [sys.executable, f'{PROJECT_DIR}/scripts/ingest_data.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")

def run_validation():
    result = subprocess.run(
        [sys.executable, f'{PROJECT_DIR}/tests/validate_data.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Quality checks failed: {result.stderr}")

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi data pipeline with bronze/silver/gold layers',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'data_engineering']
) as dag:

    ingest = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=run_ingestion
    )

    validate_bronze = PythonOperator(
        task_id='validate_bronze_layer',
        python_callable=run_validation
    )

    dbt_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=f'cd {DBT_DIR} && dbt run --select bronze --profiles-dir .'
    )

    dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_DIR} && dbt run --select silver --profiles-dir .'
    )

    dbt_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_DIR} && dbt run --select gold --profiles-dir .'
    )

    validate_all = PythonOperator(
        task_id='validate_all_layers',
        python_callable=run_validation
    )

    # Pipeline order with quality gates
    ingest >> validate_bronze >> dbt_bronze >> dbt_silver >> dbt_gold >> validate_all
