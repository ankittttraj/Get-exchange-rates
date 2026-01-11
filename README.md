        +------------------+
        | Exchange Rate API |
        +------------------+
                  ↓
           (Python Script)
                  ↓
     +--------------------------+
     | Bronze Table: Raw JSON   |  <-- Immutable, replayable
     +--------------------------+
                  ↓
     +--------------------------+
     | Silver Table: USD→INR   |  <-- Cleaned daily rate, SCD-like
     +--------------------------+
                  ↓
     +--------------------------+
     | Fact Table: Transactions |
     +--------------------------+
                  ↓
        Amount converted to INR




| **Layer**  | **Purpose**                                                               |
| ---------- | ------------------------------------------------------------------------- |
| **Bronze** | Store raw JSON payload from API (immutable, replayable, full API data)    |
| **Silver** | Store cleaned USD→INR rate; 1 row per day; idempotent / SCD-like behavior |
| **Fact**   | Transactions converted to INR using Silver table rates                    |




## Step-by-Step Process

### Step 1: Fetch API Data (Python)

Call a free exchange rate API (e.g., Frankfurter) using requests.
Print API response for debugging.
Store the full JSON response in Bronze table in BigQuery.
Bronze layer is immutable, allowing data replay if needed.


### Step 2: Transform to Silver Table

Extract USD → INR rate from the raw JSON.
Store one row per day in Silver table.
Use MERGE in BigQuery to ensure idempotency and SCD-like behavior.


### Step 3: Convert Transaction Amounts

Join Fact Table (transactions) with Silver Table on date.
Multiply USD amounts by corresponding INR rate to store converted values.
Ensure all transactions can be converted using available rates.


### Step 4: Automation & Validations

Schedule Python scripts via Cloud Composer (Airflow) or Cloud Functions + Cloud Scheduler.
Daily MERGE ensures Silver table is up-to-date and consistent.
API failure
Rate missing for today
Duplicate or abnormal rate spikes
Transactions not covered by available rates
