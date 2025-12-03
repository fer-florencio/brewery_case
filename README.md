# OpenBreweryDB Lakehouse Pipeline  
**(Databricks | Medallion Architecture | DQ | Orchestration | Observability | Deployment)**

This project implements a production-grade **Data Engineering pipeline** using:

- **Databricks Serverless (Free Edition)**
- **Unity Catalog + Volumes**
- **Delta Lake**
- **Medallion Architecture (Bronze → Silver → Gold)**
- **Incremental & Full Processing**
- **Data Quality Framework**
- **Orchestration (Workflows / Airflow-ready)**
- **Observability + Alerts**
- **Docker/Kubernetes Deployment Design**

The pipeline ingests brewery data from:

👉 https://www.openbrewerydb.org/

It demonstrates end-to-end capabilities expected in enterprise-grade data platforms, including ingestion, governance, validation, monitoring, orchestration, and deployment.

---

# Architecture

## Design Goals

- Implement a Lakehouse using **Delta Lake** for reliability and ACID guarantees  
- Use **Medallion Architecture** for layered data refinement  
- Store raw data in **Volumes** (Bronze)  
- Store curated/aggregated data in **Unity Catalog managed tables** (Silver/Gold)  
- Support **incremental** and **full-refresh** transforms  
- Integrate a **robust Data Quality framework**  
- Provide **orchestration + observability**  
- Be deployable in **Docker/Kubernetes**

---

##  Architecture Blueprint

```
                    ┌─────────────────────────┐
                    │   OpenBreweryDB API     │
                    └──────────────┬──────────┘
                                   │ REST JSON
                                   ▼
                  ┌────────────────────────────────┐
                  │          BRONZE                │
                  │ Delta Lake on Volumes          │
                  │ Raw snapshots (_ingestion_date)│
                  │ Append-only ingestion          │
                  └──────────────┬─────────────────┘
                                 │
                   Read from Volume path
                                 ▼
                   ┌───────────────────────────┐
                   │           SILVER          │
                   │ Unity Catalog Managed     │
                   │ Clean, standardized       │
                   │ FULL Weekly + Incremental |
                   └──────────────┬────────────┘
                                  │
                         MERGE INTO Silver
                                  ▼
                   ┌──────────────────────────────┐
                   │             GOLD             │
                   │ Business Aggregations        │
                   │ Breweries per city/state/type│
                   │ FULL Weekly + Incremental    │
                   └──────────────────────────────┘
```

### Storage Layout

| Layer | Storage | Description |
|-------|---------|-------------|
| Bronze | Volume Delta Lake | Raw data snapshots |
| Silver | UC Managed | Clean business entities |
| Gold | UC Managed | Aggregated KPIs |

---

# Orchestration Strategy

The pipeline is orchestrated using **Databricks Workflows**, supporting:

- Task dependencies  
- Automated retries  
- Failure handling  
- Notifications  
- Serverless execution  

The solution provides **two pipelines**:  
✔ Daily (incremental) from TUE to SUN  
✔ Weekly (full + DQ) at MON

---

## Daily Workflow (Incremental)

```
Bronze Ingest → Silver Incremental → Gold Incremental
```

Purpose:

- Fast, lightweight processing  
- Updates the curated and aggregated layers  
- No Data Quality (DQ) check to reduce operational overhead

---

## Weekly Workflow (Full + DQ)

```
Bronze Ingest (weekly)
       ↓
Silver FULL
       ↓
Gold FULL
       ↓
DQ Runner (Bronze + Silver + Gold)
       ↓
Bronze Cleanup
```

Purpose:

- Rebuild Silver and Gold entirely  
- Run full platform-wide Data Quality checks  
- Clean Bronze snapshots with retention window  

---

This demonstrates understanding of:

- Task retries  
- SLA monitoring  
- Failure callbacks  
- Distributed scheduling  

---

#  Data Quality Framework (DQ)

This solution implements Data Quality across all layers using multiple dimensions:

---

##  DQ Dimensions Implemented

| Dimension | Bronze | Silver | Gold |
|-----------|--------|--------|------|
| Freshness | ✔ | ✔ | — |
| Completeness | ✔ | ✔ | ✔ |
| Schema Drift | ✔ | — | — |
| Uniqueness | ✔ | ✔ | — |
| Validity | ✔ | ✔ | ✔ |
| Consistency | — | ✔ | ✔ |

---

## Bronze DQ

Checks include:

- Schema drift detection  
- Required fields (`id`, `name`) not null  
- Uniqueness per snapshot  
- Freshness validation  
- Volume anomaly detection (empty ingestion)  

---

## Silver DQ  

Silver validations include:

- Unique ID  
- Completeness of key business fields  
- Valid brewery type values  
- Proper formatting (country uppercase, city standardized)  
- Valid latitude/longitude range using:

```sql
try_cast(latitude AS double)
```

This avoids pipeline failures due to API inconsistencies.

---

## Gold DQ

Checks include:

- No negative aggregates  
- No null grouping keys  
- Country format consistency  
- Structural schema validation  

---

## DQ Runner (Weekly)

The *dq_runner.py* orchestrates all quality checks and stores results into:

```
<catalog>.quality.dq_audit
```

Audit fields:

| layer | check_name | status | timestamp | details |

This enables governance, alerting, trend analysis, and failure diagnostics.

---

# Observability & Alerts

Enterprise observability is achieved using:

---

## DQ Audit Table

Centralized quality tracking.

Example query:

```sql
SELECT *
FROM brewery_prod.quality.dq_audit
WHERE status = false
ORDER BY timestamp DESC;
```

---

## Workflow Monitoring

Databricks natively provides:

- Task execution logs  
- Runtime graphs  
- Failure traces  
- Retry history  
- Audit logs  

---

## Alerts (Email, Slack, Teams)

Databricks Workflow notifications:

- On failure  
- On timeout  
- On retry  

Example Slack webhook:

```json
{ "text": "🚨 Brewery Pipeline Failure: check dq_audit table." }
```

---

# Deployment (Docker / Kubernetes)

Even though Databricks handles infrastructure, the project includes a modular deployment design for environments where portability is required.

---

## Docker

Example Dockerfile:

```dockerfile
FROM python:3.10-slim

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python", "bronze/bronze_ingest_delta.py"]
```

---

## Kubernetes (CronJobs)

Daily:

```yaml
schedule: "0 2 * * *"
command: ["python", "silver/silver_transform_incremental.py"]
```

Weekly:

```yaml
schedule: "0 3 * * SUN"
command: ["python", "dq/dq_runner.py"]
```

Provides:

- Autoscaling  
- Fault tolerance  
- Centralized logging (ELK/Grafana)  
- Infrastructure as code  

---
