import sys, os
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))

from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from config import (
    CATALOG,
    SILVER_SCHEMA,
    GOLD_SCHEMA
)

def transform_gold_incremental(spark: SparkSession):

    print("Starting INCREMENTAL Gold transformation...")

    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.brewery_clean"
    gold_table = f"{CATALOG}.{GOLD_SCHEMA}.brewery_agg"

    if not spark.catalog.tableExists(silver_table):
        raise Exception("Silver table not found. Run Silver FULL first.")

    silver_df = spark.table(silver_table)

    if not spark.catalog.tableExists(gold_table):
        raise Exception("Gold table not found. Run Gold FULL first.")

    max_date = silver_df.agg(F.max("_ingestion_date")).collect()[0][0]

    print(f"Latest Silver ingestion_date: {max_date}")

    changes_df = silver_df.filter(F.col("_ingestion_date") == F.lit(max_date))

    if not changes_df.head(1):
        print("No new changes in Silver → skipping Gold incremental.")
        return

    print(f"Silver rows to aggregate: {changes_df.count()}")

    staged_df = (
        changes_df
        .groupBy("country", "state_province", "city", "brewery_type")
        .agg(F.count("*").alias("total_breweries"))
    )

    print(f"Aggregated incremental rows: {staged_df.count()}")

    gold_delta = DeltaTable.forName(spark, gold_table)

    (
        gold_delta.alias("t")
        .merge(
            staged_df.alias("s"),
            """
            t.country = s.country AND
            t.state_province = s.state_province AND
            t.city = s.city AND
            t.brewery_type = s.brewery_type
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("INCREMENTAL Gold MERGE completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    transform_gold_incremental(spark)
