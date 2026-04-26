# NYC Taxi Azure Lakehouse Pipeline

![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Workflows-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8?style=flat&logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Pipeline-Passing-brightgreen?style=flat)

End-to-end Azure lakehouse pipeline processing **3.4M NYC Taxi records** across a Bronze / Silver / Gold medallion architecture — built on Azure Databricks, Delta Lake, and Azure Data Lake Gen2, orchestrated via a scheduled Databricks Workflow job with automated data quality auditing.

---

## Pipeline Results

| Layer | Rows | Detail |
|-------|------|--------|
| Bronze | 3,399,866 | Raw ingestion — full schema preserved with metadata columns |
| Silver | 2,259,893 | 66.5% pass rate across 6 quality rules — 1.14M records removed |
| Gold — daily_trip_summary | 30 | Daily revenue, fare, distance, tip rate aggregations |
| Gold — hourly_demand | 168 | Demand patterns by hour and day of week |
| Gold — payment_summary | 4 | Revenue and tip analysis by payment method |

### Quality audit — latest run (26 Apr 2026)

| Check | Result | Value | Threshold |
|-------|--------|-------|-----------|
| silver_has_rows | ✅ PASS | 2,259,893 | > 0 |
| silver_retention_above_40pct | ✅ PASS | 66.47% | >= 40% |
| gold_daily_has_rows | ✅ PASS | 30 | > 0 |
| gold_hourly_all_24_hours | ✅ PASS | 24 | == 24 |
| silver_no_nulls_on_critical_cols | ✅ PASS | 0 | == 0 |
| gold_payment_min_2_types | ✅ PASS | 4 | >= 2 |

**6 / 6 checks passing. Audit results written to `pipeline_audit` Delta table on every run.**

---

## Gold Layer — Sample Output

These tables are the analytics-ready output of the pipeline, available for BI tools and downstream dbt models.

### daily_trip_summary

Aggregated trip metrics per day — revenue, average fare, distance, duration, and tip rate.

| trip_date | trip_month | total_trips | total_revenue | avg_fare | avg_distance_miles | avg_duration_mins | tip_rate_pct |
|-----------|------------|-------------|---------------|----------|--------------------|-------------------|--------------|
| 2026-02-01 | 2026-02 | 70,627 | £1,385,469 | £19.62 | 3.63 mi | 14.9 min | 18.78% |
| 2026-02-02 | 2026-02 | 77,293 | £1,625,661 | £21.03 | 3.55 mi | 19.7 min | 18.17% |
| 2026-02-03 | 2026-02 | 86,707 | £1,772,587 | £20.44 | 3.26 mi | 20.4 min | 18.28% |
| 2026-02-04 | 2026-02 | 90,810 | £1,840,515 | £20.27 | 3.24 mi | 20.2 min | 18.54% |

> **Insight:** Weekend days (Feb 3–4) show the highest trip volumes and longest average durations, consistent with leisure travel patterns.

---

### hourly_demand

Trip volume and fare patterns broken down by hour of day and day of week (1=Sunday, 7=Saturday).

| hour_of_day | day_of_week | total_trips | avg_fare | avg_duration_mins | avg_distance_miles |
|-------------|-------------|-------------|----------|-------------------|--------------------|
| 0 (midnight) | Sunday | 14,432 | £16.81 | 13.9 min | 2.86 mi |
| 0 (midnight) | Monday | 3,557 | £27.67 | 15.6 min | 6.25 mi |
| 0 (midnight) | Tuesday | 2,894 | £29.33 | 15.8 min | 6.74 mi |
| 0 (midnight) | Wednesday | 4,202 | £22.91 | 14.7 min | 4.92 mi |
| 0 (midnight) | Thursday | 4,800 | £22.72 | 14.6 min | 4.77 mi |

> **Insight:** Midnight Sunday is the single busiest hour slot (14,432 trips) — Saturday night post-midnight demand. Weekday midnight fares average significantly higher (£27–£29) due to longer airport/outer-borough trips.

---

### payment_summary

Revenue and tipping behaviour broken down by payment method across all 2.26M clean trips.

| payment_method | total_trips | total_revenue | avg_fare | avg_tip | tip_rate_pct |
|----------------|-------------|---------------|----------|---------|--------------|
| Credit card | 1,984,197 | £38,749,019 | £19.53 | £4.14 | 21.18% |
| Cash | 251,353 | £4,984,095 | £19.83 | £0.00 | 0.00% |
| Dispute | 18,158 | £423,621 | £23.33 | £0.01 | 0.02% |
| No charge | 6,185 | £122,057 | £19.73 | £0.00 | 0.01% |

> **Insight:** Credit card dominates at 87.8% of all trips and drives 100% of tip revenue — £38.7M total. Cash trips represent 11.1% of volume but generate zero recorded tip income. Disputed fares average the highest fare (£23.33) suggesting longer trips are more likely to be contested.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Azure Data Lake Storage Gen2                   │
│                                                                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐    ┌─────────┐  │
│  │   raw    │────▶│ bronze  │────▶│ silver  │───▶│  gold   │  │
│  │          │     │          │     │          │    │         │  │
│  │          │     │          │     │          │    │  daily  │  │
│  │ Parquet  │     │  Delta   │     │  Delta   │    │  hourly │  │
│  │  files   │     │  table   │     │  table   │    │ payment │  │
│  └──────────┘     └──────────┘     └──────────┘    └─────────┘  │
└──────────────────────────────────────────────────────────────────┘
        │                 │                │
   Raw ingest        6 quality        Spark SQL
   + metadata        rules +          aggregations
   columns           MERGE            + OPTIMIZE
                     upsert
                     + Z-ORDER
```

### Databricks Workflow — task dependency graph

```
[01_bronze_ingest] ──▶ [02_silver_transform] ──▶ [03_gold_aggregate] ──▶ [04_data_quality_checks]
      Task 1                  Task 2                    Task 3                    Task 4
  Raw → Delta             Clean + MERGE              Aggregations             6 audit checks
  3,399,866 rows          + Z-ORDER                  + OPTIMIZE               → audit table
                          2,259,893 rows             3 gold tables            23s · all pass
```

**Schedule:** Daily at 06:00 AM Europe/London · Cluster: Standard_D4s_v3 single node

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Cloud platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Spark 3.5, Runtime 13.3 LTS) |
| Table format | Delta Lake 3.0 |
| Language | PySpark + Spark SQL + Python 3.11 |
| Orchestration | Databricks Workflows (daily cron) |
| Security | Service Principal · Azure Key Vault · Databricks Secret Scope |
| Version control | Git + GitHub |

---

## Project Structure

```
azure-lakehouse-pipeline/
├── notebooks/
│   ├── 01_bronze_ingest.py         # Raw Parquet → Bronze Delta table
│   ├── 02_silver_transform.py      # 6 quality rules + Delta MERGE upsert + Z-ORDER
│   ├── 03_gold_aggregate.py        # 3 Gold analytics tables + OPTIMIZE
│   └── 04_data_quality_checks.py   # 6 audit checks → pipeline_audit Delta table
├── config/
│   └── pipeline_config.json        # Paths and table names
├── docs/
│   └── architecture.md             # Design decisions
├── .github/
│   └── workflows/
│       └── ci.yml                  # Linting + syntax checks
├── requirements.txt
└── README.md
```

---

## Key Engineering Decisions

**Why Delta Lake over plain Parquet?**
Delta Lake provides ACID transactions, time travel, and schema enforcement, critical for a pipeline that runs on a schedule and must handle late-arriving or duplicate records reliably. The MERGE upsert pattern in the Silver layer ensures idempotency, running the pipeline twice produces the same result with no duplicates.

**Why medallion architecture?**
Separating raw (Bronze), clean (Silver), and aggregated (Gold) data isolates failures to a single layer. If a Silver transformation rule changes, raw data in Bronze is untouched and the pipeline re-runs from any layer without re-ingesting from source.

**Why Service Principal + Key Vault over Access Keys?**
Access keys grant full storage account access and cannot be scoped. A Service Principal with Storage Blob Data Contributor RBAC follows least-privilege, it can only read and write blobs. Credentials are stored in Azure Key Vault and retrieved via Databricks secret scope, meaning zero credentials appear in notebook code or version control.

**Why Z-ORDER on pickup datetime?**
The most common query pattern on taxi data is filtering by date range. Z-ORDER co-locates records with similar pickup datetimes in the same files, reducing data scanned per query. This compacted the Silver layer from 4 files to 1 optimised 52MB file.

**Why raise an exception on quality failure?**
The audit notebook deliberately raises a Python exception if any check breaches its threshold. This causes the Databricks job task to fail, halts downstream tasks, and triggers an email alert — the same pattern used in production pipelines to prevent bad data propagating to Gold.

---

## Data Quality Framework

Six rules applied in the Silver transformation layer:

| Rule | Description |
|------|-------------|
| `fare_amount > 0` | Remove zero or negative fares |
| `trip_distance > 0` | Remove zero distance trips |
| `passenger_count > 0` | Remove trips with no passengers |
| `pickup_datetime IS NOT NULL` | Remove records with missing pickup time |
| `dropoff > pickup` | Remove impossible time sequences |
| `trip_duration BETWEEN 1 AND 240 mins` | Remove sub-minute and 4hr+ trips |

**Total removed: 1,139,973 records (33.5%)**

Post-pipeline, 6 automated audit checks run on every job execution and write pass/fail results to a `pipeline_audit` Delta table. Any failure raises an exception, fails the job task, and sends an email alert.

---


### Prerequisites

- Azure subscription (free tier — £150 credit on signup)
- Azure Databricks workspace
- Azure Data Lake Storage Gen2 with 4 containers: `raw` `bronze` `silver` `gold`
- Azure Key Vault with 3 secrets: `client-id` `tenant-id` `client-secret`
- Service Principal with **Storage Blob Data Contributor** role on the storage account
- Databricks secret scope linked to Key Vault

```

---

## Dataset

**NYC Taxi and Limousine Commission — Yellow Trip Data**
- Source: [TLC Trip Record Data](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet)
- Format: Parquet
- Volume used: 1 month · ~45MB raw · 3,399,866 rows
- Licence: Publicly available

---

## Author

**Zoeisha Alle**
Data Engineer · Azure · Databricks · PySpark · Delta Lake · dbt

[LinkedIn](https://linkedin.com/in/zoeisha-alle/) · [GitHub](https://github.com/Zoei19)
