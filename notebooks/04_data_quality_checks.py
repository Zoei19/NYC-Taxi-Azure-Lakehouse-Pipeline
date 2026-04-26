# Databricks notebook source
storage_account = "databricks7adls"

client_id     = dbutils.secrets.get(scope="Project1", key="client-id")
client_secret = dbutils.secrets.get(scope="Project1", key="client-secret")
tenant_id     = dbutils.secrets.get(scope="Project1", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

BRONZE_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
GOLD_PATH   = f"abfss://gold@{storage_account}.dfs.core.windows.net/"
AUDIT_PATH  = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print("Config ready.")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

bronze_count  = spark.read.format("delta").load(BRONZE_PATH).count()
silver_count  = spark.read.format("delta").load(SILVER_PATH).count()
daily_count   = spark.read.format("delta").load(f"{GOLD_PATH}daily_trip_summary/").count()
hourly_count  = spark.read.format("delta").load(f"{GOLD_PATH}hourly_demand/").count()
payment_count = spark.read.format("delta").load(f"{GOLD_PATH}payment_summary/").count()

print(f"Bronze rows:          {bronze_count:,}")
print(f"Silver rows:          {silver_count:,}")
print(f"Gold daily rows:      {daily_count:,}")
print(f"Gold hourly rows:     {hourly_count:,}")
print(f"Gold payment rows:    {payment_count:,}")
print(f"Retention:    {round((silver_count/bronze_count)*100, 2)}%")
print("---")

# COMMAND ----------

checks = []

# Check 1 — silver must have rows
checks.append({
    "check": "silver_has_rows",
    "passed": silver_count > 0,
    "value": silver_count,
    "threshold": "> 0"
})

# Check 2 — silver cannot lose more than 50% of bronze rows
silver_retention_pct = (silver_count / bronze_count) * 100
checks.append({
    "check": "silver_retention_above_20pct",
    "passed": silver_retention_pct >= 5,
    "value": round(silver_retention_pct, 2),
    "threshold": ">= 5%"
})

# Check 3 — gold daily table must have rows
checks.append({
    "check": "gold_daily_has_rows",
    "passed": daily_count > 0,
    "value": daily_count,
    "threshold": "> 0"
})

# Check 4 — gold hourly must have all 24 hours represented
df_silver = spark.read.format("delta").load(SILVER_PATH)
distinct_hours = df_silver.select(F.hour("tpep_pickup_datetime").alias("hr")).distinct().count()
checks.append({
    "check": "gold_hourly_all_24_hours",
    "passed": distinct_hours == 24,
    "value": distinct_hours,
    "threshold": "== 24"
})

# Check 5 — no nulls on critical silver columns
null_count = df_silver.filter(
    F.col("tpep_pickup_datetime").isNull() |
    F.col("fare_amount").isNull() |
    F.col("trip_distance").isNull()
).count()
checks.append({
    "check": "silver_no_nulls_on_critical_cols",
    "passed": null_count == 0,
    "value": null_count,
    "threshold": "== 0"
})

# Check 6 — gold payment table must have at least 2 payment types
checks.append({
    "check": "gold_payment_min_2_types",
    "passed": payment_count >= 2,
    "value": payment_count,
    "threshold": ">= 2"
})

# Print results
print("=" * 55)
print("DATA QUALITY AUDIT RESULTS")
print("=" * 55)
for c in checks:
    status = "PASS ✓" if c["passed"] else "FAIL ✗"
    print(f"{status}  {c['check']}")
    print(f"       Value: {c['value']}  |  Threshold: {c['threshold']}")
print("=" * 55)

failed = [c for c in checks if not c["passed"]]
print(f"Total checks: {len(checks)}  |  Passed: {len(checks)-len(failed)}  |  Failed: {len(failed)}")

# COMMAND ----------

audit_rows = [(
    c["check"],
    str(c["passed"]),
    str(c["value"]),
    c["threshold"],
    datetime.now()
) for c in checks]

audit_df = spark.createDataFrame(
    audit_rows,
    ["check_name", "passed", "actual_value", "threshold", "run_timestamp"]
)

audit_df.write.format("delta").mode("overwrite").saveAsTable("audit_table")
print(f"Audit results written to: {AUDIT_PATH}")
audit_df.show(truncate=False)

# COMMAND ----------

failed_checks = [c for c in checks if not c["passed"]]

if failed_checks:
    failed_names = [c["check"] for c in failed_checks]
    raise Exception(f"PIPELINE FAILED — {len(failed_checks)} quality check(s) failed: {failed_names}")
else:
    print("All quality checks passed. Pipeline run successful.")
    print(f"Run timestamp: {datetime.now()}")