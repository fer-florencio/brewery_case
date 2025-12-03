import sys, os
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))

import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

from config import (
    OPENBREWERYDB_BASE_URL,
    PER_PAGE,
    CATALOG,
    BRONZE_SCHEMA,
    BRONZE_PATH
)

bronze_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True)
])

def fetch_page(page: int):
    """
    Fetch a single page of breweries from the OpenBreweryDB API.
    """
    resp = requests.get(
        OPENBREWERYDB_BASE_URL,
        params={"page": page, "per_page": PER_PAGE},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_breweries(max_empty_pages: int = 3):
    """
    Fetch all available pages until 'max_empty_pages' consecutive empty results appear.
    """
    all_rows = []
    empty_pages = 0
    page = 1

    print("Starting API pagination for Bronze Delta ingestion...")

    while True:
        data = fetch_page(page)

        if not data:
            empty_pages += 1
            print(f"Page {page} returned empty. ({empty_pages}/{max_empty_pages})")

            if empty_pages >= max_empty_pages:
                print("Reached maximum empty pages limit. Pagination completed.")
                break
        else:
            empty_pages = 0
            all_rows.extend(data)
            print(f"Fetched {len(data)} records from page {page}")

        page += 1

    print(f"Total records collected: {len(all_rows)}")
    return all_rows

def write_bronze_delta(spark):

    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

    print("Fetching data from API...")
    raw_rows = fetch_all_breweries()

    normalized = [
        {
            field.name: str(row.get(field.name)) if row.get(field.name) is not None else None
            for field in bronze_schema.fields
        }
        for row in raw_rows
    ]

    df = spark.createDataFrame(normalized, bronze_schema)

    df = (
        df.withColumn("_source", lit("openbrewerydb"))
          .withColumn("_ingestion_ts", current_timestamp())
          .withColumn("_ingestion_date", lit(datetime.now(timezone.utc).strftime("%Y-%m-%d")))
    )

    delta_path = f"{BRONZE_PATH}/delta"
    table_fullname = f"{CATALOG}.{BRONZE_SCHEMA}.brewery_raw"

    print(f"Appending new Bronze records to Delta path: {delta_path}")

    (
        df.write
          .format("delta")
          .mode("append")
          .save(delta_path)
    )
 

    print("Bronze ingestion completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    write_bronze_delta(spark)
