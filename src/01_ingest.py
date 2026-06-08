# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest
# MAGIC Reads sample NYC taxi data and writes to a Delta table in Unity Catalog.

# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
# Widget to accept catalog name from job parameter
dbutils.widgets.text("catalog_name", "xponent_databricks_workspace_dev")
catalog = dbutils.widgets.get("catalog_name")
schema = "dab_training"
print(f"Target: {catalog}.{schema}")

# COMMAND ----------
# Read sample data (NYC taxi — available in Databricks sample datasets)
df = spark.read.format("delta").load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
df = df.limit(10000) # small sample for dev
display(df)

# COMMAND ----------
# Write to Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.taxi_raw")
print(f"Written to {catalog}.{schema}.taxi_raw")