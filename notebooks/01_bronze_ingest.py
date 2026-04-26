# Databricks notebook source
storage_account = "databricks7adls"  # replace with your actual storage account name

client_id     = dbutils.secrets.get(scope="Project1", key="client-id")
client_secret = dbutils.secrets.get(scope="Project1", key="client-secret")
tenant_id     = dbutils.secrets.get(scope="Project1", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

RAW_PATH    = f"abfss://raw@{storage_account}.dfs.core.windows.net/"
BRONZE_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

print("Config set. Paths ready.")

# COMMAND ----------

df_raw = (spark.read
    .format("parquet")
    .option("inferSchema", "true")
    .load(RAW_PATH))

print(f"Rows ingested: {df_raw.count():,}")
print(f"Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (df_raw
    .withColumn("_ingestion_ts",  F.current_timestamp())
    .withColumn("_source_file",   F.input_file_name())
    .withColumn("_ingestion_date", F.current_date()))

(df_bronze.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH))

print(f"Bronze rows written: {df_bronze.count():,}")

# COMMAND ----------

df_check = spark.read.format("delta").load(BRONZE_PATH)

print(f"Total rows in bronze: {df_check.count():,}")
print(f"Partitions: {df_check.rdd.getNumPartitions()}")

# Check Delta table history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, BRONZE_PATH)
dt.history().select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC