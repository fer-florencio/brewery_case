from pyspark.sql import SparkSession, functions as F
from config import (
    CATALOG,
    SILVER_SCHEMA,
    GOLD_SCHEMA
)

def transform_gold_full(spark: SparkSession):

    print("Starting FULL Gold transformation...")

    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.brewery_clean"
    gold_table = f"{CATALOG}.{GOLD_SCHEMA}.brewery_agg"

    silver_df = spark.table(silver_table)

    # Aggregation: breweries per type + location
    gold_df = (
        silver_df
        .groupBy(
            "country",
            "state_province",
            "city",
            "brewery_type"
        )
        .agg(F.count("*").alias("total_breweries"))
    )

    print(f"[FULL] Gold row count: {gold_df.count()}")

    # Overwrite Gold managed table
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table)
    )

    print(f"FULL Gold transformation completed. Table: {gold_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    transform_gold_full(spark)
