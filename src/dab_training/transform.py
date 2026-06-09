import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def transform(spark, catalog: str) -> None:
    """Read taxi_raw, calculate trip duration, filter outliers, write to taxi_transformed."""
    schema = "dab_training"
    src_table = f"{catalog}.{schema}.taxi_raw"
    tgt_table = f"{catalog}.{schema}.taxi_transformed"

    print(f"Reading from: {src_table}")
    df = spark.read.table(src_table)

    # Calculate trip duration in minutes
    df = df.withColumn(
    "trip_duration_min",
    (F.col("tpep_dropoff_datetime").cast("long") -
        F.col("tpep_pickup_datetime").cast("long")) / 60
    )

    # Filter out implausible trips (less than 1 min or more than 2 hours)
    df = df.filter(F.col("trip_duration_min").between(1, 120))

    # Drop rows with zero or negative fare
    df = df.filter(F.col("fare_amount") > 0)

    print(f"Writing {df.count()} rows to: {tgt_table}")
    df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tgt_table)
    print("Transform complete.")

def main() -> None:
    """Entry point called by the wheel task. Reads catalog_name from argv."""
    catalog = sys.argv[1] if len(sys.argv) > 1 else "dev"
    print(f"Starting transform — catalog: {catalog}")
    spark = SparkSession.builder.getOrCreate()

    # In transform.py main(), after SparkSession is created:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    row_count = dbutils.jobs.taskValues.get(
    taskKey="ingest",         # the upstream task key
    key="row_count",         # the value key set above
    default=0               # fallback if not found (for local testing)
    )
    print(f"Received row_count from ingest task: {row_count}")
    transform(spark, catalog)

if __name__ == "__main__":
    main()