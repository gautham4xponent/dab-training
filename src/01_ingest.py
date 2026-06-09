# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest
# MAGIC Reads sample NYC taxi data and writes to a Delta table in Unity Catalog.

# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
# Widget to accept catalog name from job parameter
dbutils.widgets.text("catalog_name", "xponent_dev")
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

# COMMAND ----------
# At the end of 01_ingest.py, after writing the table:
row_count = df.count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
print(f"Set task value row_count = {row_count}")