-----------------------------
--Use in Transaction Conversion
-----------------------------
config {
  type: "table",
  schema: "analytics",        -- target dataset
  name: "transactions_inr",   -- target table name
  description: "Transactions converted from USD to INR using daily exchange rates",
  tags: ["transactions"]
}

SELECT
  t.transaction_id,
  t.amount AS amount_usd,
  t.amount * r.rate AS amount_inr,
  t.txn_date
FROM `project_name.dataset_name.transactions` t
JOIN `project_name.dataset_name.usd_to_inr_rates` r
ON t.txn_date = r.rate_date
