# Objective

To design and implement a production-grade,
automated data pipeline that ingests daily USD → INR foreign exchange rates from an external API,
stores raw data for auditability,
transforms and validates curated rates,
and ensures all downstream financial transactions can be accurately converted using historical exchange rates.




# Architecture

                +-----------------------+
                | External FX API       |
                | (Frankfurter)         |
                +-----------------------+
                            |
                            | 1. HTTPS (JSON)
                            v
                +-----------------------+
                | Airflow (Composer)   |
                | DAG: usd_to_inr      |
                +-----------------------+
                            |
                            | 2. Python Operator
                            v
                +----------------------------------+
                | BigQuery Dataset: ds_bronze      |
                |----------------------------------|
                | exchange_rates_raw               |
                | (raw JSON, immutable)            |
                +----------------------------------+
                            |
                            | 3. SQL MERGE
                            v
                +----------------------------------+
                | BigQuery Dataset: ds_silver      |
                |----------------------------------|
                | usd_inr_daily_rates              |
                | (1 row/day, clean FX rate)       |
                +----------------------------------+
                            |
                            | 4. Effective-date join
                            v
                +----------------------------------+
                | BigQuery Dataset: ds_analytics   |
                |----------------------------------|
                | transactions                     |
                | transactions_inr                 |
                +----------------------------------+
                            |
                            | 5. Consumption
                            v
                +-----------------------+
                | BI / Reporting / ML   |
                +-----------------------+




# Step-by-Step Process

### Step 1: Fetch API Data (Python)

Fetch daily exchange rates from an external API (Frankfurter) using a Python task in Airflow.
Log API responses and metadata for observability.
Store the entire raw JSON payload in a BigQuery Bronze table (ds_bronze.exchange_rates_raw).
The Bronze layer is append-only and immutable, enabling replay and auditability.


### Step 2: Transform to Silver Table

Extract the USD → INR exchange rate from the raw JSON payload.
Load one clean exchange rate per day into the Silver table (ds_silver.usd_inr_daily_rates).
Use MERGE in BigQuery to ensure idempotency and SCD-like behavior.


### Step 3: Convert Transaction Amounts

Join the transaction fact table (ds_analytics.transactions) with the Silver FX table.
Convert transaction amounts from USD to INR and store results in ds_analytics.transactions_inr.
Ensure all transactions are covered by available FX rates.


### Step 4: Automation & Validations

Orchestrate the entire pipeline using Apache Airflow (Cloud Composer).
Schedule the pipeline to run daily at 03:00 AM.
API failure
Rate missing for today
Duplicate or abnormal rate spikes
Transactions not covered by available rates




# Layers

| **Layer**  | **Purpose**                                                               |
| ---------- | ------------------------------------------------------------------------- |
| **Bronze** | Store raw JSON payload from API (immutable, replayable, full API data)    |
| **Silver** | Store cleaned USD→INR rate; 1 row per day; idempotent / SCD-like behavior |
| **Gold**   | Transactions converted to INR using Silver table rates                    |




# Tables

| Dataset      | Table               | Layer   | Purpose         |
| ------------ | ------------------- | ------- | --------------- |
| ds_bronze    | exchange_rates_raw  | Bronze  | Raw API JSON    |
| ds_silver    | usd_inr_daily_rates | Silver  | Clean FX rate   |
| ds_analytics | transactions        | Gold    | Source facts    |
| ds_analytics | transactions_inr    | Gold    | Converted facts |




# Sample bronze data

| ingestion_date | ingestion_ts     | raw_payload                                                                                             |
| -------------- | ---------------- | ------------------------------------------------------------------------------------------------------- |
| 2026-01-16     | 2026-01-16 08:30 | { "base": "USD", "date": "2026-01-15", "rates": {"INR":90.3,"EUR":0.86029,"GBP":0.74621,"JPY":158.56} } |
| 2026-01-17     | 2026-01-17 08:30 | { "base": "USD", "date": "2026-01-16", "rates": {"INR":91.0,"EUR":0.86500,"GBP":0.75000,"JPY":159.20} } |




# Sample silver data

| rate_date  | base_currency | target_currency | rate | ingestion_ts     | source      |
| ---------- | ------------- | --------------- | ---- | ---------------- | ----------- |
| 2026-01-15 | USD           | INR             | 90.3 | 2026-01-16 08:30 | frankfurter |
| 2026-01-16 | USD           | INR             | 91.0 | 2026-01-17 08:30 | frankfurter |
