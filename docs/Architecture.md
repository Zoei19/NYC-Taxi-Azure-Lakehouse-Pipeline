# Architecture — NYC Taxi Azure Lakehouse Pipeline

## Overview

This pipeline follows the **medallion architecture** pattern — a widely adopted standard in Azure Databricks environments that organises data into three progressive layers of quality and aggregation: Bronze, Silver, and Gold.

The design prioritises four production engineering principles: **idempotency** (safe to re-run), **least-privilege security** (minimum required access), **observability** (every run is audited), and **failure isolation** (a bad Silver run cannot corrupt Bronze).

---

## Layer design

### Bronze — raw ingestion
The Bronze layer preserves source data exactly as received. No transformations are applied. Two metadata columns are added on ingestion:

- `_ingestion_ts` — timestamp of when the record was loaded
- `_source_file` — the originating file path in ADLS

This means Bronze is always a faithful replica of the source. If a transformation rule turns out to be wrong, the raw data is untouched and the pipeline can be re-run from Bronze without going back to the source system.

### Silver — cleansed and conformed
Six business rules are applied to remove invalid records:

- Zero or negative fares removed — these represent cancelled or erroneous transactions
- Zero distance trips removed — stationary trips are not valid fare events
- Missing pickup timestamps removed — unparseable records with no time anchor
- Trips where dropoff precedes pickup removed — physically impossible sequences
- Zero passenger count removed — no valid trip occurs with zero passengers
- Trips under 1 minute or over 240 minutes removed — outliers outside operational bounds

The Silver layer uses **Delta MERGE (upsert)** rather than overwrite. This means the pipeline is safe to re-run — duplicate records are updated in place rather than appended, preventing row inflation across scheduled runs.

**Z-ORDER** is applied on `tpep_pickup_datetime` — the column most frequently used in date-range filters. This co-locates related data in the same files and reduces scan volume on downstream queries.

### Gold — aggregated analytics
Three analytics tables are built from Silver using Spark SQL:

| Table | Grain | Primary use |
|-------|-------|-------------|
| `daily_trip_summary` | One row per day | Revenue reporting, trend analysis |
| `hourly_demand` | One row per hour + day of week | Demand forecasting, capacity planning |
| `payment_summary` | One row per payment type | Payment mix analysis, tip behaviour |

All Gold tables are optimised with `OPTIMIZE` on every run to compact small files produced by incremental writes.

---

## Security design

### Why Service Principal over Access Keys?
An Azure Storage Access Key grants unrestricted access to the entire storage account — equivalent to a root password. It cannot be scoped to specific containers or operations.

A Service Principal with **Storage Blob Data Contributor** RBAC is scoped to blob read/write only. It cannot manage the storage account, rotate keys, or access other Azure resources. This follows the principle of least privilege.

### Why Azure Key Vault + Databricks Secret Scope?
Credentials stored in notebook code or environment variables are at risk of accidental exposure through version control, logs, or screenshots. 

Azure Key Vault stores secrets encrypted at rest with hardware security modules. The Databricks secret scope acts as a proxy — notebooks call `dbutils.secrets.get()` which retrieves the value at runtime without ever exposing it in cell output, logs, or the notebook source file. The value is redacted automatically in all Databricks output.

---

## Orchestration design

The pipeline runs as a **4-task Databricks Workflow job** on a daily 6AM cron schedule. Tasks are wired in a linear dependency chain — each task only starts if the previous one succeeded.

If any task fails:
1. Downstream tasks are skipped automatically
2. The job run is marked as failed
3. An email alert is sent

This prevents partial pipeline runs from producing incomplete or misleading Gold data.

### Why a separate audit task?
Embedding quality checks inside the transformation notebooks would couple data logic with observability logic. A dedicated audit notebook runs after Gold is written and checks the final state of all three layers. This means:

- Quality checks run against committed data, not in-flight DataFrames
- The audit table provides a persistent, queryable run history
- Threshold changes can be made in one place without touching transformation code

---

## Cost management

The pipeline runs on a **single-node Standard_D4s_v3 cluster** with 30-minute auto-termination. For a dataset of this size (3.4M rows, ~52MB Silver), a distributed multi-node cluster adds startup time and cost without meaningful performance benefit. The single-node configuration processes the full pipeline in under 15 minutes at a fraction of the cost.

The schedule is set to **PAUSED** in this repository to prevent unintended Azure credit consumption. Enable it by toggling the schedule to Active in the Databricks Workflow UI.
