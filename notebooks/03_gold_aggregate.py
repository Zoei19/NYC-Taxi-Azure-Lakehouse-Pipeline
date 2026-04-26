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

SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
GOLD_PATH   = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print("Config ready.")

# COMMAND ----------

df_silver = spark.read.format("delta").load(SILVER_PATH)
df_silver.createOrReplaceTempView("silver_trips")
print(f"Silver rows available: {df_silver.count():,}")

# COMMAND ----------

daily_summary = spark.sql("""
    SELECT
        DATE(tpep_pickup_datetime)                              AS trip_date,
        DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM')            AS trip_month,
        COUNT(*)                                                AS total_trips,
        ROUND(SUM(fare_amount), 2)                              AS total_revenue,
        ROUND(AVG(fare_amount), 2)                              AS avg_fare,
        ROUND(AVG(trip_distance), 2)                            AS avg_distance_miles,
        ROUND(AVG(trip_duration_mins), 1)                       AS avg_duration_mins,
        ROUND(SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100, 2) AS tip_rate_pct,
        COUNT(DISTINCT VendorID)                                AS active_vendors
    FROM silver_trips
    GROUP BY 1, 2
    ORDER BY 1
""")

daily_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}daily_trip_summary/")
print(f"Daily summary rows: {daily_summary.count():,}")
display(daily_summary.limit(5))

# COMMAND ----------

hourly_demand = spark.sql("""
    SELECT
        HOUR(tpep_pickup_datetime)       AS hour_of_day,
        DAYOFWEEK(tpep_pickup_datetime)  AS day_of_week,
        COUNT(*)                         AS total_trips,
        ROUND(AVG(fare_amount), 2)       AS avg_fare,
        ROUND(AVG(trip_duration_mins),1) AS avg_duration_mins,
        ROUND(AVG(trip_distance), 2)     AS avg_distance_miles
    FROM silver_trips
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

hourly_demand.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}hourly_demand/")
print(f"Hourly demand rows: {hourly_demand.count():,}")
display(hourly_demand.limit(5))

# COMMAND ----------

payment_summary = spark.sql("""
    SELECT
        CASE payment_type
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            ELSE 'Unknown'
        END                              AS payment_method,
        COUNT(*)                         AS total_trips,
        ROUND(SUM(fare_amount), 2)       AS total_revenue,
        ROUND(AVG(fare_amount), 2)       AS avg_fare,
        ROUND(AVG(tip_amount), 2)        AS avg_tip,
        ROUND(AVG(tip_amount) / NULLIF(AVG(fare_amount),0) * 100, 2) AS tip_rate_pct
    FROM silver_trips
    GROUP BY 1
    ORDER BY total_trips DESC
""")

payment_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}payment_summary/")
print(f"Payment summary rows: {payment_summary.count():,}")
display(payment_summary)

# COMMAND ----------

gold_tables = ["daily_trip_summary", "hourly_demand", "payment_summary"]

for table in gold_tables:
    path = f"{GOLD_PATH}{table}/"
    spark.sql(f"OPTIMIZE delta.`{path}`")
    print(f"Optimised: {table}")

print("\nAll gold tables optimised.")

# COMMAND ----------

from delta.tables import DeltaTable

bronze_count  = 3399866
silver_count  = 2259893

daily_count   = spark.read.format("delta").load(f"{GOLD_PATH}daily_trip_summary/").count()
hourly_count  = spark.read.format("delta").load(f"{GOLD_PATH}hourly_demand/").count()
payment_count = spark.read.format("delta").load(f"{GOLD_PATH}payment_summary/").count()

print("=" * 50)
print("PIPELINE SUMMARY — NYC TAXI LAKEHOUSE")
print("=" * 50)
print(f"Bronze  (raw):     {bronze_count:>10,} rows")
print(f"Silver  (clean):   {silver_count:>10,} rows  ({round((silver_count/bronze_count)*100,1)}% passed quality)")
print(f"Gold — daily:      {daily_count:>10,} rows")
print(f"Gold — hourly:     {hourly_count:>10,} rows")
print(f"Gold — payment:    {payment_count:>10,} rows")
print("=" * 50)
print("Layers: Bronze → Silver → Gold")
print("Format: Delta Lake throughout")
print("Auth:   Service Principal + Key Vault")
print("=" * 50)