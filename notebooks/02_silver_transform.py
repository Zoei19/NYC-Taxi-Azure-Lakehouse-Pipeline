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

print("Config ready.")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

bronze_count = df_bronze.count()
print(f"Bronze rows: {bronze_count:,}")
print(f"Columns: {len(df_bronze.columns)}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Apply all cleaning rules
df_silver = (df_bronze
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
    .withColumn("trip_duration_mins",
        (F.unix_timestamp("tpep_dropoff_datetime") - 
         F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .filter(F.col("trip_duration_mins").between(1, 240))
    .withColumn("_silver_processed_ts", F.current_timestamp())
)

silver_count = df_silver.count()
dropped      = bronze_count - silver_count
drop_pct     = round((dropped / bronze_count) * 100, 2)

print(f"Bronze rows:        {bronze_count:,}")
print(f"Silver rows:        {silver_count:,}")
print(f"Rows dropped:       {dropped:,}")
print(f"Drop percentage:    {drop_pct}%")
print(f"Data quality score: {100 - drop_pct}%")

# COMMAND ----------

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (silver_table.alias("s")
        .merge(
            df_silver.alias("n"),
            """s.VendorID = n.VendorID 
               AND s.tpep_pickup_datetime = n.tpep_pickup_datetime
               AND s.tpep_dropoff_datetime = n.tpep_dropoff_datetime"""
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGE complete — existing records updated, new records inserted.")
else:
    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH))
    print("Silver Delta table created for the first time.")

# COMMAND ----------

df_check = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver rows confirmed: {df_check.count():,}")

# Optimise with Z-ORDER on the column most used in queries
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}` 
    ZORDER BY (tpep_pickup_datetime)
""")

# Check history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, SILVER_PATH)
dt.history().select("version","timestamp","operation","operationMetrics").show(5, truncate=False)