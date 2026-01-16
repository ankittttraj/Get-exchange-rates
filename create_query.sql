-------------------
-- Create Silver Table
-------------------
CREATE TABLE IF NOT EXISTS `ankit-data-platform.ds_silver.usd_inr_daily_rates` (
  rate_date        DATE,
  base_currency    STRING,
  target_currency  STRING,
  rate             NUMERIC,
  ingestion_ts     TIMESTAMP,
  source           STRING,
  pipeline_run_id  STRING
)
PARTITION BY rate_date;

