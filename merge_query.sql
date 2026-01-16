---------------------------------
--Daily Transform (Bronze → Silver)
---------------------------------
--It safely upserts (update or insert) one USD → INR exchange rate per day into a Silver BigQuery table.

MERGE `ankit-data-platform.ds_silver.usd_inr_daily_rates` t
USING (
  SELECT
    DATE(raw_payload.date)                 AS rate_date,
    raw_payload.base                       AS base_currency,
    'INR'                                  AS target_currency,
    CAST(raw_payload.rates.INR AS NUMERIC) AS rate
  FROM `ankit-data-platform.ds_bronze.exchange_rates_raw`
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
  );
