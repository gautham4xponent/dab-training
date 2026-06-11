# Databricks notebook source
# MAGIC %md
# MAGIC ## NYC Taxi — DLT Bronze-Silver-Gold Pipeline
# MAGIC Reads from taxi_transformed (produced by the transform task) and
# MAGIC builds the Medallion Architecture in Unity Catalog.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Read catalog name from pipeline parameter (set in pipeline YAML)
catalog = spark.conf.get("catalog_name", "dev")
src_table = f"{catalog}.dab_training.taxi_transformed"

# COMMAND ----------
# ── BRONZE: Raw streaming ingest from taxi_transformed ───────────────────
@dlt.table(
  name="taxi_bronze",
  comment="Raw NYC taxi data — streaming ingest from taxi_transformed",
  table_properties={"quality": "bronze"}
)
def taxi_bronze():
  return (spark.readStream
    .format("delta")
    .table(src_table))

# COMMAND ----------
# ── SILVER: Cleansed and validated ───────────────────────────────────────
@dlt.table(
  name="taxi_silver",
  comment="Cleansed taxi trips — data quality enforced",
  table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("non_null_vendor", "vendor_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "trip_duration_min BETWEEN 1 AND 120")
@dlt.expect("positive_fare", "fare_amount > 0")
def taxi_silver():
  return (dlt.read_stream("taxi_bronze")
    .select(
      "vendor_id", "pickup_datetime", "dropoff_datetime",
      "passenger_count", "trip_distance", "fare_amount",
      "tip_amount", "total_amount", "trip_duration_min"
    ))

# COMMAND ----------
# ── GOLD: Daily aggregated metrics (complete mode) ───────────────────────
@dlt.table(
  name="taxi_gold_daily",
  comment="Daily NYC taxi trip aggregates — business metrics",
  table_properties={"quality": "gold", "pipelines.reset.allowed": "false"}
)
def taxi_gold_daily():
  return (dlt.read("taxi_silver")                           # non-streaming → complete mode
    .groupBy(F.to_date("pickup_datetime").alias("trip_date"))
    .agg(
      F.count("*").alias("trip_count"),
      F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
      F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
      F.round(F.avg("tip_amount"), 2).alias("avg_tip_usd"),
      F.round(F.sum("total_amount"), 2).alias("total_revenue_usd")
    )
    .orderBy("trip_date"))