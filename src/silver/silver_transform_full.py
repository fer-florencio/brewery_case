import sys, os
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))

from pyspark.sql import SparkSession, functions as F
from config import (
    CATALOG,
    BRONZE_PATH,
    SILVER_SCHEMA,
)

def silver_full(spark):

    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

    bronze_delta_path = f"{BRONZE_PATH}/delta"
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.brewery_clean"

    print(f"[FULL] Loading Bronze Delta from: {bronze_delta_path}")

    bronze_df = spark.read.format("delta").load(bronze_delta_path)

    print("[FULL] Applying transformations...")

    df = (
        bronze_df
        .dropDuplicates(["id"])
        .withColumn("country", F.upper("country"))
        .withColumn("name", F.initcap("name"))
        .withColumn("city", F.initcap("city"))
        .withColumn("state_province", F.initcap("state_province"))
        .withColumn("brewery_type", F.lower("brewery_type"))
        .withColumn("latitude", F.col("latitude").cast("string"))
        .withColumn("longitude", F.col("longitude").cast("string"))
    )

    print("[FULL] Writing Silver curated table...")

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(silver_table)
    )

    print("[FULL] Silver FULL transformation completed.")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    silver_full(spark)
