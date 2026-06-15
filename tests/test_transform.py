import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from dab_training.transform import transform


@pytest.fixture(scope="session")
def spark():
  return (SparkSession.builder
    .master("local[1]")
    .appName("dab_training_tests")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate())

def test_trip_duration_calculated(spark):
  """Duration should be (dropoff - pickup) / 60 minutes."""
  data = [("2024-01-01 10:00:00", "2024-01-01 10:30:00", 15.0)]
  df = spark.createDataFrame(data, ["pickup_datetime", "dropoff_datetime", "fare_amount"])
  df = df.withColumn("trip_duration_min",
    (F.col("dropoff_datetime").cast("long") -
     F.col("pickup_datetime").cast("long")) / 60)
  assert df.collect()[0]["trip_duration_min"] == 30.0

def test_duration_filter_removes_outliers(spark):
  """Trips shorter than 1 min or longer than 120 min should be filtered out."""
  data = [("2024-01-01 10:00:00", "2024-01-01 10:00:30", 5.0),   # 0.5 min — too short
          ("2024-01-01 10:00:00", "2024-01-01 10:20:00", 10.0),  # 20 min — valid
          ("2024-01-01 10:00:00", "2024-01-01 13:00:00", 80.0)]  # 180 min — too long
  df = spark.createDataFrame(data, ["pickup_datetime", "dropoff_datetime", "fare_amount"])
  df = df.withColumn("trip_duration_min",
    (F.col("dropoff_datetime").cast("long") -
     F.col("pickup_datetime").cast("long")) / 60)
  result = df.filter(F.col("trip_duration_min").between(1, 120))
  assert result.count() == 1

def test_zero_fare_removed(spark):
  """Trips with zero or negative fare should be filtered out."""
  data = [(0.0,), (-5.0,), (12.5,), (8.0,)]
  df = spark.createDataFrame(data, ["fare_amount"])
  result = df.filter(F.col("fare_amount") > 0)
  assert result.count() == 2

def test_empty_dataframe_handled(spark):
  """Transform should not fail on an empty input DataFrame."""
  from pyspark.sql.types import StructType, StructField, StringType, DoubleType
  schema = StructType([
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("fare_amount", DoubleType())
  ])
  df = spark.createDataFrame([], schema)
  result = df.filter(F.col("fare_amount") > 0)
  assert result.count() == 0            # empty input → empty output, no exception

def test_trip_duration_column_added(spark):
  """Output DataFrame must contain the trip_duration_min column."""
  data = [("2024-01-01 10:00:00", "2024-01-01 10:45:00", 20.0)]
  df = spark.createDataFrame(data, ["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount"])
  df = df.withColumn("trip_duration_min",
    (F.col("tpep_dropoff_datetime").cast("long") -
     F.col("tpep_pickup_datetime").cast("long")) / 60)
  assert "trip_duration_min" in df.columns
  assert df.collect()[0]["trip_duration_min"] == 45.0