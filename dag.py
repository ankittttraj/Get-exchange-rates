"""
- Fetches daily exchange rates from an API
- Stores raw API responses in Bronze layer
- Transforms and upserts clean rates into Silver layer
- Performs multiple production-grade validations
- Scheduled daily at 3:00 AM
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests, json
from google.cloud import bigquery
import logging

# ----------------------------
# CONFIGURATION
# ----------------------------
PROJECT_ID = "gcp-project-id"
BRONZE_DATASET = "dataset_name"
SILVER_DATASET = "dataset_name"
BRONZE_TABLE = "exchange_rates_raw"
SILVER_TABLE = "usd_to_inr_rates"
API_URL = "https://api.frankfurter.app/latest?from=USD"

# Default DAG args
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1)  # DAG starts yesterday
}

# ----------------------------
# DEFINE DAG
# ----------------------------
dag = DAG(
    "usd_to_inr_pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # daily at 3 AM
    catchup=False,
    max_active_runs=1,
    tags=["fx", "exchange_rate"]
)

# ----------------------------
# TASK 1: Fetch API & Load Bronze
# ----------------------------
def fetch_and_load_bronze():
    """
    Fetch USD->INR exchange rate from API and insert into Bronze table in BigQuery.
    This is the raw, immutable layer.
    """
    response = requests.get(API_URL)
    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()
    logging.info("API fetched successfully: %s", data)

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"

    rows = [{
        "ingestion_date": str(datetime.utcnow().date()),
        "raw_payload": json.dumps(data),
        "ingestion_ts": datetime.utcnow().isoformat()
    }]

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"BigQuery insert errors: {errors}")
    logging.info("Bronze table updated successfully.")

fetch_bronze_task = PythonOperator(
    task_id="fetch_bronze",
    python_callable=fetch_and_load_bronze,
    dag=dag
)

# ----------------------------
# TASK 2: MERGE into Silver
# ----------------------------
merge_sql = f"""
MERGE `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` t
USING (
  SELECT
    DATE(raw_payload.date) AS rate_date,
    raw_payload.base AS base_currency,
    'INR' AS target_currency,
    CAST(raw_payload.rates.INR AS NUMERIC) AS rate
  FROM `{PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}`
  WHERE ingestion_date = CURRENT_DATE()
    AND raw_payload.base = 'USD'
    AND raw_payload.rates.INR IS NOT NULL
    ) s
ON t.rate_date = s.rate_date
AND t.base_currency = s.base_currency
AND t.target_currency = s.target_currency
WHEN MATCHED THEN
  UPDATE SET rate = s.rate, ingestion_ts = CURRENT_TIMESTAMP(), source='frankfurter'
WHEN NOT MATCHED THEN
  INSERT (rate_date, base_currency, target_currency, rate, ingestion_ts, source)
  VALUES (s.rate_date, s.base_currency, s.target_currency, s.rate, CURRENT_TIMESTAMP(), 'frankfurter')
"""

merge_silver_task = BigQueryInsertJobOperator(
    task_id="merge_silver",
    configuration={"query": {"query": merge_sql, "useLegacySql": False}},
    location="ASIA-SOUTH1",
    dag=dag
)

# ----------------------------
# TASK 3a: Rate Freshness Check -- Ensure that the latest rate is present (today's date)
# ----------------------------
rate_freshness_sql = f"""
SELECT
  MAX(rate_date) AS latest_rate_date,
  CURRENT_DATE() AS today
FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
"""

rate_freshness_task = BigQueryInsertJobOperator(
    task_id="rate_freshness_check",
    configuration={"query": {"query": rate_freshness_sql, "useLegacySql": False}},
    location="ASIA-SOUTH1",
    dag=dag
)

# ----------------------------
# TASK 3b: Rate Spike / Bounds Check -- Check for abnormal daily rate changes (>5% jump)
# ----------------------------
rate_spike_sql = f"""
WITH last_two_rates AS (
  SELECT rate_date, rate
  FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
  ORDER BY rate_date DESC
  LIMIT 2
)
SELECT * FROM last_two_rates
WHERE ABS(rate - LAG(rate) OVER (ORDER BY rate_date)) / LAG(rate) OVER (ORDER BY rate_date) > 0.05
"""

rate_spike_task = BigQueryInsertJobOperator(
    task_id="rate_spike_check",
    configuration={"query": {"query": rate_spike_sql, "useLegacySql": False}},
    location="ASIA-SOUTH1",
    dag=dag
)

# ----------------------------
# TASK 3c: Duplicate Detection -- Ensure there are no duplicate rates for the same date
# ----------------------------
duplicate_sql = f"""
SELECT rate_date, COUNT(*) AS count_per_day
FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
GROUP BY rate_date
HAVING COUNT(*) > 1
"""

duplicate_rate_task = BigQueryInsertJobOperator(
    task_id="duplicate_rate_check",
    configuration={"query": {"query": duplicate_sql, "useLegacySql": False}},
    location="ASIA-SOUTH1",
    dag=dag
)

# ----------------------------
# TASK 3d: Transaction Coverage Check -- Ensure all transactions can be converted using available rates
# ----------------------------
transaction_coverage_sql = f"""
SELECT t.txn_id, t.txn_date, t.amount_usd
FROM `project.dataset.transactions` t
LEFT JOIN `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` r
ON r.rate_date = (
    SELECT MAX(rate_date)
    FROM `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` r2
    WHERE r2.rate_date <= t.txn_date
)
WHERE r.rate IS NULL
"""

transaction_coverage_task = BigQueryInsertJobOperator(
    task_id="transaction_coverage_check",
    configuration={"query": {"query": transaction_coverage_sql, "useLegacySql": False}},
    location="ASIA-SOUTH1",
    dag=dag
)

# ----------------------------
# DAG DEPENDENCIES
# ----------------------------
# fetch_bronze_task → fetch API and store raw JSON
# merge_silver_task → merge clean USD→INR rate into Silver
# All validations run in parallel after Silver is ready:

fetch_bronze_task >> merge_silver_task >> [rate_freshness_task, rate_spike_task, duplicate_rate_task, transaction_coverage_task]
