# NYC Taxi Data Pipeline

An end-to-end automated data pipeline ingesting 50,000 NYC taxi records into Snowflake across bronze, silver, and gold layers, transformed with dbt, quality-gated with Great Expectations-style validation, and orchestrated daily with Apache Airflow. A GitHub Actions CI workflow validates dbt compilation on every push.

---

## What It Does

- **Ingestion** — downloads 50,000 NYC yellow taxi trip records from a public dataset and loads them into Snowflake's bronze layer
- **Bronze layer** — raw data with minimal cleaning, preserving source fidelity
- **Silver layer** — cleaned and enriched data: zero-distance and zero-fare trips filtered, payment method decoded, tip percentage calculated
- **Gold layer** — aggregated business metrics by date and payment method: total trips, average fare, average tip percentage, total revenue
- **Quality gates** — automated data validation checks run between layers, halting the pipeline on failures
- **Orchestration** — Apache Airflow DAG runs the full pipeline daily in dependency order
- **CI/CD** — GitHub Actions validates dbt model compilation on every push to master

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              Apache Airflow DAG              │
                    │                                              │
                    │  ingest_raw_data                             │
                    │       │                                      │
                    │       ▼                                      │
                    │  validate_bronze_layer (quality gate)        │
                    │       │                                      │
                    │       ▼                                      │
                    │  dbt_run_bronze ──────────────────────────┐  │
                    │       │                                   │  │
                    │       ▼                                   │  │
                    │  dbt_run_silver ──────────────────────────┤  │
                    │       │                                   │  │
                    │       ▼                                   │  │
                    │  dbt_run_gold  ──────────────────────────►│  │
                    │       │                                   │  │
                    │       ▼                                   │  │
                    │  validate_all_layers (quality gate)       │  │
                    └───────────────────────────────────────────┘  │
                                                                    │
                    ┌───────────────────────────────────────────────┤
                    │              Snowflake (NYC_TAXI)             │
                    │                                               │
                    │  BRONZE.RAW_TAXI_TRIPS     (50,000 rows)     │
                    │  SILVER.SILVER_TAXI_TRIPS  (45,824 rows)     │
                    │  GOLD.GOLD_TRIP_SUMMARY    (45,424 rows)     │
                    └───────────────────────────────────────────────┘

CI/CD: GitHub Actions validates dbt compilation on every push
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow |
| Data Validation | Custom quality gate framework |
| Ingestion | Python, Snowflake Connector, Pandas |
| CI/CD | GitHub Actions |
| Language | Python 3.12 |
| Environment | WSL2 / Ubuntu |

---

## Project Structure

```
nyc-taxi-pipeline/
├── .github/
│   └── workflows/
│       └── pipeline.yml          # GitHub Actions CI — dbt compile check on push
├── dags/
│   └── taxi_pipeline_dag.py      # Airflow DAG — orchestrates full pipeline daily
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # Snowflake connection (gitignored)
│   └── models/
│       ├── bronze/
│       │   └── bronze_taxi_trips.sql    # Raw data from RAW_TAXI_TRIPS
│       ├── silver/
│       │   └── silver_taxi_trips.sql    # Cleaned, filtered, enriched
│       └── gold/
│           └── gold_trip_summary.sql    # Aggregated business metrics
├── macros/
│   └── generate_schema_name.sql  # dbt macro — enforces exact schema naming
├── scripts/
│   └── ingest_data.py            # Downloads NYC taxi data, loads to Snowflake bronze
├── tests/
│   └── validate_data.py          # Data quality checks across all three layers
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Data Flow

| Layer | Table | Rows | Description |
|---|---|---|---|
| Bronze | `BRONZE.RAW_TAXI_TRIPS` | 50,000 | Raw ingested data, minimal cleaning |
| Silver | `SILVER.SILVER_TAXI_TRIPS` | 45,824 | Filtered, enriched, payment decoded |
| Gold | `GOLD.GOLD_TRIP_SUMMARY` | 45,424 | Aggregated by date and payment method |

---

## Quality Gates

The validation framework runs checks at both the bronze and final layers, halting the pipeline automatically if any check fails:

| Layer | Check | Threshold |
|---|---|---|
| Bronze | Row count | > 0 |
| Bronze | Null pickup timestamps | 0 |
| Bronze | Null dropoff timestamps | 0 |
| Bronze | Negative fares | < 1,000 (< 2%) |
| Silver | Row count | > 0 |
| Silver | Zero distance trips | 0 |
| Silver | Invalid durations | < 500 (< 1%) |
| Silver | Unknown payment method | 0 |
| Gold | Row count | > 0 |
| Gold | Negative revenue | 0 |
| Gold | Average fare | Between $5–$100 |

---

## How to Deploy

### Prerequisites
- [Snowflake account](https://signup.snowflake.com) (free trial)
- [Python 3.11+](https://www.python.org/downloads/)
- [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) with Ubuntu (Windows only)
- Apache Airflow, dbt-snowflake installed in a virtual environment

### Snowflake Setup
Run the following in a Snowflake SQL worksheet:
```sql
CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.BRONZE;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.SILVER;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.GOLD;
CREATE WAREHOUSE IF NOT EXISTS TAXI_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### Local Setup

**1. Clone the repository**
```bash
git clone https://github.com/AhnafHy/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
```

**2. Create and activate virtual environment**
```bash
python3 -m venv ~/pipeline-env
source ~/pipeline-env/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Create profiles.yml**
Create `dbt_project/profiles.yml` with your Snowflake credentials:
```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_SNOWFLAKE_ACCOUNT
      user: YOUR_SNOWFLAKE_USER
      password: YOUR_SNOWFLAKE_PASSWORD
      role: ACCOUNTADMIN
      database: NYC_TAXI
      warehouse: TAXI_WH
      schema: PUBLIC
      threads: 4
```

**5. Set environment variables**
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
```

**6. Run the ingestion script**
```bash
python3 scripts/ingest_data.py
```

**7. Run dbt transformations**
```bash
cd dbt_project
dbt run --select bronze --profiles-dir .
dbt run --select silver --profiles-dir .
dbt run --select gold --profiles-dir .
```

**8. Run quality checks**
```bash
cd ..
python3 tests/validate_data.py
```

**9. Start Airflow**

Terminal 1:
```bash
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080
```

Terminal 2:
```bash
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

Copy the DAG file to Airflow:
```bash
mkdir -p ~/airflow/dags
cp dags/taxi_pipeline_dag.py ~/airflow/dags/
```

Open `http://localhost:8080`, enable the `nyc_taxi_pipeline` DAG, and trigger a manual run.

---

## Screenshots

**Quality checks — all layers passing:**

<img width="714" height="341" alt="Quality Checks" src="https://github.com/user-attachments/assets/72a39103-8b4a-4adc-bc6e-75b542d056e4" />

**Airflow DAG — all tasks green:**

<img width="1865" height="45" alt="DAG Check" src="https://github.com/user-attachments/assets/2a568d9c-6ac8-4cc3-975a-a5f8d7a64983" />


<img width="314" height="298" alt="DAG Check 2" src="https://github.com/user-attachments/assets/7c2ab0aa-75da-4073-8750-cb962dabc710" />

**Snowflake row counts across all three layers:**

Bronze
<img width="1521" height="263" alt="Bronze count" src="https://github.com/user-attachments/assets/19970c3c-d87b-43d4-9482-b3c1755ef4ce" />

Silver
<img width="1512" height="239" alt="Silver count" src="https://github.com/user-attachments/assets/ade10cef-d08c-4513-8be3-7b185b851e84" />

Gold
<img width="1511" height="227" alt="Gold count" src="https://github.com/user-attachments/assets/ed552ef2-6f82-4b15-ae07-b87a337d4a32" />

---

## Key Concepts Demonstrated

- **Medallion architecture** — bronze/silver/gold layered data model separating raw, cleaned, and aggregated data
- **dbt transformations** — SQL models with `{{ ref() }}` dependencies enforcing correct execution order
- **Custom schema macro** — overrides dbt's default schema naming to land tables in exact target schemas
- **Data quality gates** — pipeline halts automatically on validation failures, preventing bad data from propagating downstream
- **Workflow orchestration** — Airflow DAG with explicit task dependencies ensuring correct pipeline order
- **CI/CD** — GitHub Actions validates dbt model compilation on every push without requiring a live database connection
- **Credential management** — sensitive credentials stored as GitHub secrets and environment variables, never hardcoded
