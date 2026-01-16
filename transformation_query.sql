-----------------------------
--Use in Transaction Conversion : “ROW_NUMBER() lets us rank all valid historical rates per transaction and select the most recent one.”
-----------------------------
config {
  type: "table",
  schema: "ds_analytics",
  name: "transactions_inr",
  description: "Transactions converted from USD to INR using effective-date exchange rates",
  tags: ["transactions"]
}

WITH joined_rates AS (
  SELECT
    t.transaction_id,
    t.amount AS amount_usd,
    t.txn_date,
    r.rate,
    ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY r.rate_date DESC) AS rn
  FROM `ankit-data-platform.ds_analytics.transactions` t
  JOIN `ankit-data-platform.ds_silver.usd_inr_daily_rates` r
    ON r.rate_date <= t.txn_date
)

SELECT
  transaction_id,
  amount_usd,
  amount_usd * rate AS amount_inr,
  txn_date
FROM joined_rates
WHERE rn = 1;
