-------------------
--Create silver table
-------------------
CREATE TABLE IF NOT EXISTS `project_name.dataset_name.usd_to_inr_rates` (
  rate_date        DATE,
  base_currency    STRING,
  target_currency  STRING,
  rate             NUMERIC,
  ingestion_ts     TIMESTAMP,
  source           STRING
)
PARTITION BY rate_date
CLUSTER BY base_currency, target_currency
