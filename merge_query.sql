---------------------------------
--Daily Transform (Bronze → Silver)
---------------------------------
--It safely upserts (update or insert) one USD → INR exchange rate per day into a Silver BigQuery table.

MERGE `project_name.dataset_name.usd_to_inr_rates` t
USING (
  SELECT
    DATE(raw_payload.date)                 AS rate_date,
    raw_payload.base                       AS base_currency,
    'INR'                                  AS target_currency,
    CAST(raw_payload.rates.INR AS NUMERIC) AS rate
  FROM `my_project.bronze.exchange_rates_raw`
  WHERE ingestion_date = CURRENT_DATE()
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
    source = 'frankfurter'

WHEN NOT MATCHED THEN
  INSERT (
    rate_date,
    base_currency,
    target_currency,
    rate,
    ingestion_ts,
    source
  )
  VALUES (
    s.rate_date,
    s.base_currency,
    s.target_currency,
    s.rate,
    CURRENT_TIMESTAMP(),
    'frankfurter'
  )

