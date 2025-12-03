import sys, os
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))

from pyspark.sql import SparkSession, functions as F
from config import (
    CATALOG,
    BRONZE_PATH,
    SILVER_SCHEMA,
)

def silver_incremental(spark):

    spark.sql(f"USE CATALOG {CATALOG}")

    bronze_delta_path = f"{BRONZE_PATH}/delta"
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.brewery_clean"

    print(f"[INCREMENTAL] Loading Bronze Delta from: {bronze_delta_path}")
    bronze_df = spark.read.format("delta").load(bronze_delta_path)

    print("[INCREMENTAL] Loading Silver table...")
    silver_df = spark.table(silver_table)

    max_dt = silver_df.agg(F.max("_ingestion_date")).collect()[0][0]
    print(f"[INCREMENTAL] Silver max ingestion_date = {max_dt}")

    new_data = bronze_df.filter(F.col("_ingestion_date") > max_dt)

    if new_data.count() == 0:
        print("[INCREMENTAL] No new data — nothing to merge.")
        return

    print("[INCREMENTAL] Transforming new data...")

    new_data = (
        new_data
        .dropDuplicates(["id"])
        .withColumn("country", F.upper("country"))
        .withColumn("name", F.initcap("name"))
        .withColumn("city", F.initcap("city"))
        .withColumn("state_province", F.initcap("state_province"))
        .withColumn("brewery_type", F.lower("brewery_type"))
        .withColumn("latitude", F.col("latitude").cast("string"))
        .withColumn("longitude", F.col("longitude").cast("string"))
    )

    print("[INCREMENTAL] Merging into Silver...")

    (
        new_data.alias("s")
        .merge(
            silver_df.alias("t"),
            "s.id = t.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("[INCREMENTAL] Silver Incremental completed.")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    silver_incremental(spark)
