"""
USD → INR Exchange Rate Pipeline

- Fetches daily exchange rates from Frankfurter API
- Stores raw API responses in Bronze (immutable)
- Curates clean daily rates into Silver using MERGE
- Runs production-grade data quality validations
- Scheduled daily at 03:00 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import json
import logging
from google.cloud import bigquery

# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------
PROJECT_ID = "ankit-data-platform"

BRONZE_DATASET = "ds_bronze"
SILVER_DATASET = "ds_silver"
ANALYTICS_DATASET = "ds_analytics"

BRONZE_TABLE = "exchange_rates_raw"
SILVER_TABLE = "usd_inr_daily_rates"

API_URL = "https://api.frankfurter.app/latest?from=USD"

BQ_LOCATION = "ASIA-SOUTH1"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1)
}

# -------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------
dag = DAG(
    dag_id="usd_to_inr_fx_pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Daily at 03:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["fx", "bigquery", "bronze_silver"]
)

# -------------------------------------------------
# TASK 1: FETCH API & LOAD BRONZE
# -------------------------------------------------
def fetch_and_load_bronze(**context):
    logging.info("Fetching FX rates from API")

    response = requests.get(API_URL, timeout=30)
    if response.status_code != 200:
        raise Exception("FX API request failed")

    payload = response.json()

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"

    rows = [{
        "ingestion_date": datetime.utcnow().date().isoformat(),
        "raw_payload": json.dumps(payload),
        "ingestion_ts": datetime.utcnow().isoformat(),
        "pipeline_run_id": context["run_id"],
        "source": "frankfurter"
    }]

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"BigQuery insert failed: {errors}")

    logging.info("Bronze table updated successfully")

fetch_bronze = PythonOperator(
    task_id="fetch_and_load_bronze",
    python_callable=fetch_and_load_bronze,
    provide_context=True,
    dag=dag
)

# -------------------------------------------------
# TASK 2: MERGE BRONZE → SILVER
# -------------------------------------------------
merge_silver_sql = f"""
MERGE `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` t
USING (
  SELECT
    DATE(raw_payload.date)                 AS rate_date,
    raw_payload.base                       AS base_currency,
    'INR'                                  AS target_currency,
    CAST(raw_payload.rates.INR AS NUMERIC) AS rate,
    pipeline_run_id
  FROM `{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}`
  WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND raw_payload.base = 'USD'
    AND raw_payload.rates.INR IS NOT NULL
) s
ON t.rate_date = s.rate_date
AND t.base_currency = s.base_currency
AND t.target_currency = s.target_currency

WHEN MATCHED THEN
  UPDATE SET
    rate = s.rate,
    ingestion_ts = CURRENT_TIMESTAMP(),
    pipeline_run_id = s.pipeline_run_id,
    source = 'frankfurter'

WHEN NOT MATCHED THEN
  INSERT (
    rate_date,
    base_currency,
    target_currency,
    rate,
    ingestion_ts,
    pipeline_run_id,
    source
  )
  VALUES (
    s.rate_date,
    s.base_currency,
    s.target_currency,
    s.rate,
    CURRENT_TIMESTAMP(),
    s.pipeline_run_id,
    'frankfurter'
  );
"""

merge_silver = BigQueryInsertJobOperator(
    task_id="merge_bronze_to_silver",
    configuration={"query": {"query": merge_silver_sql, "useLegacySql": False}},
    location=BQ_LOCATION,
    dag=dag
)

# -------------------------------------------------
# VALIDATION TASKS (FAIL DAG IF DATA IS BAD)
# -------------------------------------------------
def fail_if_rows_exist(query, error_message):
    client = bigquery.Client(project=PROJECT_ID)
    results = client.query(query).result()
    if results.total_rows > 0:
        raise Exception(error_message)

# 1️⃣ Rate Freshness
def rate_freshness_check():
    query = f"""
    SELECT 1
    FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
    HAVING MAX(rate_date) < CURRENT_DATE()
    """
    fail_if_rows_exist(query, "Rate freshness check failed")

# 2️⃣ Rate Spike (>5%)
def rate_spike_check():
    query = f"""
    WITH rates AS (
      SELECT
        rate_date,
        rate,
        LAG(rate) OVER (ORDER BY rate_date) AS prev_rate
      FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
      ORDER BY rate_date DESC
      LIMIT 2
    )
    SELECT 1
    FROM rates
    WHERE prev_rate IS NOT NULL
      AND ABS(rate - prev_rate) / prev_rate > 0.05
    """
    fail_if_rows_exist(query, "FX rate spike detected")

# 3️⃣ Duplicate Rate Check
def duplicate_rate_check():
    query = f"""
    SELECT 1
    FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
    GROUP BY rate_date, base_currency, target_currency
    HAVING COUNT(*) > 1
    """
    fail_if_rows_exist(query, "Duplicate FX rates found")

# 4️⃣ Transaction Coverage Check (Effective-Date)
def transaction_coverage_check():
    query = f"""
    SELECT 1
    FROM `{PROJECT_ID}.{ANALYTICS_DATASET}.transactions` t
    LEFT JOIN `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` r
    ON r.rate_date = (
        SELECT MAX(rate_date)
        FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` r2
        WHERE r2.rate_date <= t.txn_date
    )
    WHERE r.rate IS NULL
    """
    fail_if_rows_exist(query, "Some transactions cannot be converted to INR")

freshness = PythonOperator(
    task_id="rate_freshness_check",
    python_callable=rate_freshness_check,
    dag=dag
)

spike = PythonOperator(
    task_id="rate_spike_check",
    python_callable=rate_spike_check,
    dag=dag
)

duplicate = PythonOperator(
    task_id="duplicate_rate_check",
    python_callable=duplicate_rate_check,
    dag=dag
)

coverage = PythonOperator(
    task_id="transaction_coverage_check",
    python_callable=transaction_coverage_check,
    dag=dag
)

# -------------------------------------------------
# DAG DEPENDENCIES
# -------------------------------------------------
fetch_bronze >> merge_silver >> [freshness, spike, duplicate, coverage]
