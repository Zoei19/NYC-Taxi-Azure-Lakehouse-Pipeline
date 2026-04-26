# NYC Taxi Azure Lakehouse Pipeline

![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Workflows-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8?style=flat&logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)

End-to-end Azure lakehouse pipeline processing **3.4M NYC Taxi records** across a Bronze / Silver / Gold medallion architecture — built on Azure Databricks, Delta Lake, and Azure Data Lake Storage, orchestrated via a scheduled Databricks Workflow job.

---

## Pipeline Results

| Layer  | Rows        | Notes                                      |
|--------|-------------|--------------------------------------------|
| Bronze | 3,399,866   | Raw ingestion — full schema preserved       |
| Silver | 2,259,893   | 66.5% pass rate across 6 quality rules     |
| Gold   | 3 tables    | daily summary · hourly demand · payment analysis |

- **1,139,973 records** removed by data quality framework (33.5%)
- **52MB** optimised Delta file after Z-ORDER on Silver layer
- **4-task** Databricks Workflow job — Every 30 minutes scheduled run
- **6 automated** quality checks written to audit Delta table on every run

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Azure Data Lake Storage Gen2              │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐   ┌────────┐ │
│  │   raw/   │───▶│ bronze/  │───▶│ silver/  │──▶│ gold/  │ │
│  │ nyc-taxi │    │  trips/  │    │  trips/  │   │        │ │
│  │          │    │          │    │          │   │ daily  │ │
│  │ Parquet  │    │  Delta   │    │  Delta   │   │ hourly │ │
│  │  files   │    │  table   │    │  table   │   │payment │ │
│  └──────────┘    └──────────┘    └──────────┘   └────────┘ │
└─────────────────────────────────────────────────────────────┘
         │                │               │
    Raw ingest       6 quality        Spark SQL
    + metadata       rules +          aggregations
    columns          MERGE            + OPTIMIZE
                     upsert
```

```
Databricks Workflow — nyc-taxi-lakehouse-pipeline
─────────────────────────────────────────────────
[01_bronze_ingest] → [02_silver_transform] → [03_gold_aggregate] → [04_data_quality_checks]
     Task 1               Task 2                  Task 3                  Task 4
  Raw → Delta          Clean + MERGE           Aggregations           6 audit checks
                        + Z-ORDER              + OPTIMIZE             → audit table
```

---

## Tech Stack

| Component         | Technology                        |
|-------------------|-----------------------------------|
| Cloud platform    | Microsoft Azure                   |
| Storage           | Azure Data Lake Storage Gen2      |
| Compute           | Azure Databricks (Spark 3.5)      |
| Table format      | Delta Lake 3.0                    |
| Language          | PySpark + Spark SQL + Python 3.11 |
| Orchestration     | Databricks Workflows (cron)       |
| Security          | Service Principal + Azure Key Vault + Databricks Secret Scope |
| Version control   | Git + GitHub                      |

---

## Project Structure

```
azure-lakehouse-pipeline/
├── notebooks/
│   ├── 01_bronze_ingest.py        # Raw Parquet → Bronze Delta table
│   ├── 02_silver_transform.py     # 6 quality rules + Delta MERGE upsert
│   ├── 03_gold_aggregate.py       # 3 Gold analytics tables + OPTIMIZE
│   └── 04_data_quality_checks.py  # Audit checks → pipeline_audit Delta table
├── config/
│   └── pipeline_config.json       # Paths and table names
├── docs/
│   └── architecture.md            # Design decisions
├── .github/
│   └── workflows/
│       └── ci.yml                 # Linting + syntax checks
├── requirements.txt
└── README.md
```

---

## Key Engineering Decisions

**Why Delta Lake over plain Parquet?**
Delta Lake provides ACID transactions, time travel, and schema enforcement — critical for a pipeline that runs on a schedule and must handle late-arriving or duplicate records reliably. The MERGE upsert pattern in the Silver layer ensures idempotency — running the pipeline twice produces the same result.

**Why medallion architecture?**
Separating raw (Bronze), clean (Silver), and aggregated (Gold) data means failures are isolated to a single layer. If a Silver transformation rule is wrong, raw data in Bronze is untouched and the pipeline can be re-run from any layer without re-ingesting from the source.

**Why Service Principal + Key Vault over Access Keys?**
Access keys grant full storage account access and cannot be scoped. A Service Principal with Storage Blob Data Contributor RBAC follows least-privilege principle — it can only read/write blobs, not manage the storage account. Credentials are stored in Azure Key Vault and retrieved via Databricks secret scope, meaning zero credentials appear in notebook code or version control.

**Why Z-ORDER on pickup datetime?**
The most common query pattern on taxi data is filtering by date range. Z-ORDER co-locates records with similar pickup datetimes in the same files, reducing the data scanned per query. This reduced the Silver layer from 4 files to 1 optimised file (51.5MB → 52.2MB compacted).

---

## Data Quality Framework

Six rules applied in the Silver transformation layer:

| Rule | Description | Records removed |
|------|-------------|-----------------|
| fare_amount > 0 | Remove zero or negative fares | — |
| trip_distance > 0 | Remove zero distance trips | — |
| passenger_count > 0 | Remove trips with no passengers | — |
| pickup_datetime not null | Remove records with no pickup time | — |
| dropoff > pickup | Remove impossible time sequences | — |
| trip_duration 1–240 mins | Remove sub-minute and 4hr+ trips | — |

**Total removed: 1,139,973 records (33.5%)**

Post-pipeline, 6 automated audit checks run on every job execution and write results to a `pipeline_audit` Delta table — raising an exception and triggering an email alert if any threshold is breached.

---

## How to Run

### Prerequisites
- Azure subscription (free tier works — £150 credit on signup)
- Azure Databricks workspace
- Azure Data Lake Storage Gen2 with 4 containers: `raw`, `bronze`, `silver`, `gold`
- Azure Key Vault with 3 secrets: `client-id`, `tenant-id`, `client-secret`
- Service Principal with Storage Blob Data Contributor role on the storage account
- Databricks secret scope linked to Key Vault


## Dataset

**NYC Taxi and Limousine Commission — Yellow Trip Data**
- Source: [TLC Trip Record Data](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet)
- Format: Parquet
- Volume used: 1 month (~45MB raw, 3.4M rows)
- Licence: Publicly available

---

## Author

Zoeisha Alle
Data Engineer | Azure · Databricks · PySpark · dbt

[LinkedIn](https://www.linkedin.com/in/zoeisha-alle/) · [GitHub](https://github.com/Zoei19)
