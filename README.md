# OpenBreweryDB Lakehouse Pipeline  
### **Modern Data Engineering Project using Databricks, Unity Catalog, Delta Lake, Medallion Architecture & Data Quality Framework**

This project implements a complete **end-to-end Data Engineering pipeline** built entirely on **Databricks (Free Serverless Edition)**, using:

- **Medallion Architecture (Bronze → Silver → Gold)**
- **Unity Catalog (UC)** for governance
- **Delta Lake** for storage and ACID transactions
- **Volumes** for raw data storage
- **Incremental & Full refresh pipelines**
- **Data Quality framework (DQ Bronze, Silver, Gold)**
- **Observability & auditing tables**
- **Databricks Workflows orchestration**
- **Optional Docker/Kubernetes deployment patterns**

Data source:  
→ https://www.openbrewerydb.org/

---

# Architecture Overview

### Goals

- Deliver a production-grade Lakehouse pipeline  
- Use **Delta Lake** for durability  
- Use **Volumes** for raw ingestion  
- Apply **Data Quality validation at all layers**  
- Provide **incremental + full refresh** mechanisms  
- Build workflow orchestration and governance  
- Ensure observability and traceability  

---

## High-Level Architecture Diagram

```
                            ┌──────────────────────────┐
                            │   OpenBreweryDB API      │
                            └──────────────┬───────────┘
                                           │
                                 JSON from REST API
                                           ▼
                ┌─────────────────────────────────────────────┐
                │               BRONZE (RAW)                  │
                │ Delta files stored in Volumes               │
                │ Path: /Volumes/<catalog>/<schema>/delta     │
                │ Append-only ingestion                       │
                └───────────────────┬─────────────────────────┘
                                    │
                                    ▼
                ┌─────────────────────────────────────────────┐
                │              SILVER (CLEAN)                 │
                │ UC Managed Table                            │
                │ Full weekly + incremental daily transforms  │
                │ Normalized schema, cleaned fields           │
                │ Latitude/Longitude kept as STRING           │
                └───────────────────┬─────────────────────────┘
                                    │
                                    ▼
                ┌─────────────────────────────────────────────┐
                │              GOLD (AGGREGATED)              │
                │ Aggregations: breweries per city/type/state │
                │ Used for BI dashboards and analytics        │
                └─────────────────────────────────────────────┘
```

---

# Project Structure

```text
/src
  /bronze
    bronze_ingest.py
    bronze_clean_up.py
  /silver
    silver_transform_full.py
    silver_transform_incremental.py
  /gold
    gold_transform_full.py
    gold_transform_incremental.py
  /dq
    dq_bronze.py
    dq_silver.py
    dq_gold.py
    dq_runner.py
config.py
README.md
```

---

# Bronze Layer (RAW)

### Storage:
Stored as **Delta files inside a Volume**, not as a UC table.

Path example:

```text
/Volumes/brewery_prod/bronze/delta
```

### Features:
- Fetches paginated data from OpenBreweryDB API  
- Normalizes schema dynamically  
- Adds ingestion metadata: `_source`, `_ingestion_ts`, `_ingestion_date`  
- Written in append mode  
- **Not registered** as a UC table  

This preserves raw, immutable snapshots and fully decouples Bronze from UC.

---

# Silver Layer (CLEAN)

### Source:
Reads directly from **Volume Delta files**, not from a UC Bronze table.

### Features:
- Weekly **full refresh** (`silver_full`)  
- Daily **incremental** (`silver_incremental`)  
- Deduplication by `id`  
- Standardization:
  - `country` → UPPERCASE  
  - `city`, `state_province` → InitCap  
  - `brewery_type` → lowercase  
- Forces `latitude` and `longitude` to **STRING**  
  → avoids malformed values causing cast issues  
- Produces a **Unity Catalog managed table**:

```text
brewery_prod.silver.brewery_clean
```

Silver is the main curated entity layer for analytics.

---

# Gold Layer (AGGREGATED)

### Features:
- Aggregates breweries by:
  - `country`
  - `state_province`
  - `city`
  - `brewery_type`
- Supports **full** and **incremental** refresh
- Produces a UC managed table:

```text
brewery_prod.gold.brewery_agg
```

Gold is the KPI layer used for dashboards and analytical queries.

---

# Data Quality Framework

Each layer has its own DQ module:

| Layer  | File         | Purpose                                                 |
|--------|--------------|---------------------------------------------------------|
| Bronze | `dq_bronze.py` | Raw completeness, schema drift, duplicates, freshness |
| Silver | `dq_silver.py` | Uniqueness, validity, formatting, geolocation checks  |
| Gold   | `dq_gold.py`   | Valid counts, grouping keys, schema consistency       |

All DQ functions follow the same pattern:

```python
passed: bool
checks: dict[str, bool]
errors: dict[str, DataFrame]
```

- `passed` → overall result for the layer  
- `checks` → each rule’s status  
- `errors` → a DataFrame per rule holding the failing rows  

This enables detailed debugging, rich observability, and strong governance.

---

# DQ Runner

The `dq_runner.py` orchestrates all Data Quality checks:

1. Reads **Bronze** from Volume Delta (`BRONZE_PATH/delta`)
2. Reads **Silver** and **Gold** from Unity Catalog
3. Executes:
   - `dq_check_bronze`
   - `dq_check_silver`
   - `dq_check_gold`
4. Logs results to two UC tables:

### 1️⃣ `brewery_prod.quality.dq_audit`

Stores per-check results:

```text
layer | check_name | status | timestamp | details
```

### 2️⃣ `brewery_prod.quality.dq_errors`

Stores failing records as JSON strings:

```text
layer | check_name | error_record | timestamp
```
---

# Observability & Monitoring

With `dq_audit` and `dq_errors` you can build:

- Quality dashboards  
- Alerts (Slack, email)  
- Trend analysis (how checks evolve over time)  
- Root-cause investigations based on actual failing rows  

Examples:

```sql
-- Latest failures
SELECT *
FROM brewery_prod.quality.dq_audit
WHERE status = false
ORDER BY timestamp DESC;
```

```sql
-- Raw error samples
SELECT layer, check_name, error_record, timestamp
FROM brewery_prod.quality.dq_errors
ORDER BY timestamp DESC
LIMIT 100;
```

---

# Orchestration with Databricks Workflows

The solution uses **two Workflows**: **Daily Incremental** and **Weekly Full + DQ**.

---

## 🗓 Daily Workflow (Incremental)

Sequence:

```text
Bronze Ingest → Silver Incremental → Gold Incremental
```

- Updates the data incrementally  
- No DQ to keep it lightweight  
- Ideal for daily refresh with minimal overhead  

Suggested schedule:

```text
0 0 0 ? * SUN,TUE,WED,THU,FRI,SAT * (UTC+00:00 — UTC)
(run at 12 AM every day, except Monday)
```

---

## 🗓 Weekly Workflow (Full + DQ)

Sequence:

```text
Bronze Ingest → Silver Full → Gold Full → DQ Runner → Bronze Cleanup
```

- Rebuilds curated and aggregated data  
- Runs full Data Quality checks across all layers  
- Optionally cleans old Bronze snapshots  

Suggested schedule:

```text
0 0 * * MON
(run at 12 AM every Monday)
```

Each workflow task is configured as:

- **Task type:** Run a Python file  
- **Runtime:** Databricks 14.x Serverless  
- **Source:** Workspace file path, e.g.  
  `Workspace/Users/<user>/brewery/silver/silver_transform_full.py`

---

# Deploying the Project on Databricks Free Edition

This section describes how to get everything running **inside Databricks Free Edition**, considering its constraints:

- No DBFS root for general storage  
- Volumes stored under Unity Catalog  
- UC managed tables  
- Serverless jobs and workflows  

---

## 1️⃣ Create Catalog and Schemas

In a Databricks SQL query:

```sql
CREATE CATALOG brewery_prod;

CREATE SCHEMA IF NOT EXISTS brewery_prod.bronze;
CREATE SCHEMA IF NOT EXISTS brewery_prod.silver;
CREATE SCHEMA IF NOT EXISTS brewery_prod.gold;
CREATE SCHEMA IF NOT EXISTS brewery_prod.quality;

USE CATALOG brewery_prod;
```

---

## 2️⃣ Create Volume for Bronze

```sql
CREATE VOLUME IF NOT EXISTS brewery_prod.bronze;
```

Bronze files will be stored under:

```text
/Volumes/brewery_prod/bronze/delta
```

---

## 3️⃣ Upload Project Files to the Workspace

Navigate to:

```text
Workspace → Users → <your_user>/brewery/
```

Upload:

```text
bronze/
silver/
gold/
dq/
config.py
```

Ensure that the imports inside your Python files (e.g., `from config import ...`) match the folder structure.

---

## 4️⃣ Configure `config.py`

Example configuration:

```python
CATALOG = "brewery_prod"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
DQ_SCHEMA = "quality"

BRONZE_PATH = "/Volumes/brewery_prod/bronze"

OPENBREWERYDB_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
```

---

## 5️⃣ Create the Daily Workflow (Incremental)

1. Go to **Workflows** → **Create Job**  
2. Add tasks:

### Task 1 — Bronze Ingest
- Type: **Run a Python file**
- File: `brewery/bronze/bronze_ingest_delta.py`

### Task 2 — Silver Incremental
- Depends on: **Bronze Ingest**
- File: `brewery/silver/silver_transform_incremental.py`

### Task 3 — Gold Incremental
- Depends on: **Silver Incremental**
- File: `brewery/gold/gold_transform_incremental.py`

3. Set schedule: `0 0 0 ? * SUN,TUE,WED,THU,FRI,SAT *` (12 AM daily, exept Monday)

---

## 6️⃣ Create the Weekly Workflow (Full + DQ)

1. Create another Job in Workflows  

2. Add tasks:

### Task 1 — Bronze Full Ingest
- File: `brewery/bronze/bronze_ingest_delta.py`

### Task 2 — Silver Full
- Depends on Bronze Full
- File: `brewery/silver/silver_transform_full.py`

### Task 3 — Gold Full
- Depends on Silver Full
- File: `brewery/gold/gold_transform_full.py`

### Task 4 — DQ Runner
- Depends on Gold Full
- File: `brewery/dq/dq_runner.py`

### Task 5 — Bronze Cleanup (optional)
- Depends on DQ Runner
- File: `brewery/bronze/bronze_cleanup.py` (if implemented)

3. Set schedule: `0 0 * * MON` (12 AM every Monday)

---

## 7️⃣ Enable Workflow Notifications

In each workflow:

- Configure email, Slack or webhook notifications on:
  - Failure  
  - Timeout  
  - Retry exhaustion  

This provides operational alerting and reliability.

---

## 8️⃣ Validate End-to-End

Once everything is wired:

### Check Bronze data:

```python
spark.read.format("delta").load("/Volumes/brewery_prod/bronze/delta").show(10)
```

### Check Silver table:

```sql
SELECT * FROM brewery_prod.silver.brewery_clean LIMIT 20;
```

### Check Gold table:

```sql
SELECT * FROM brewery_prod.gold.brewery_agg LIMIT 20;
```

### Check DQ tables:

```sql
SELECT * FROM brewery_prod.quality.dq_audit ORDER BY timestamp DESC;
SELECT * FROM brewery_prod.quality.dq_errors ORDER BY timestamp DESC;
```

---

# 👤 Author

**Fernando Florencio dos Santos**  
_Data Engineering Portfolio Project_
