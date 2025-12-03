from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from config import (
    CATALOG,
    BRONZE_SCHEMA,
    BRONZE_PATH
)

RETENTION_DAYS = 60
bronze_delta_path = f"{BRONZE_PATH}/delta"

def cleanup_bronze(spark: SparkSession):

    print("=== Starting Bronze Cleanup (Delta in Volume) ===")

    bronze_df = spark.read.format("delta").load(bronze_delta_path)
    bronze_delta = DeltaTable.forPath(spark, bronze_delta_path)

    print("\n[BEFORE CLEANUP] Row count in Bronze:")
    before_count = bronze_df.count()
    print(f"Rows: {before_count}")

    print("\nDistinct ingestion dates (before):")
    bronze_df.select("_ingestion_date").distinct().orderBy("_ingestion_date").show(50, False)

    print(f"\nDeleting Bronze records older than {RETENTION_DAYS} days...")

    bronze_delta.delete(
        F.col("_ingestion_date") < F.date_sub(F.current_date(), RETENTION_DAYS)
    )

    print("Logical delete completed.")

    print("\nRunning VACUUM on Bronze Delta files...")

    spark.sql(f"""
        VACUUM delta.`{bronze_delta_path}` RETAIN 168 HOURS
    """)

    print("VACUUM completed.")

    bronze_df_after = spark.read.format("delta").load(bronze_delta_path)

    print("\n[AFTER CLEANUP] Row count in Bronze:")
    after_count = bronze_df_after.count()
    print(f"Rows: {after_count}")

    print("\nDistinct ingestion dates (after):")
    bronze_df_after.select("_ingestion_date").distinct().orderBy("_ingestion_date").show(50, False)

    print("\n=== Bronze Cleanup Completed ===")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cleanup_bronze(spark)
