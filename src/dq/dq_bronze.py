from pyspark.sql import functions as F

def dq_check_bronze(spark, bronze_delta_path):
    """
    Performs Data Quality checks on the RAW Bronze layer stored in a Delta Volume.
    Returns:
        passed (bool),
        checks (dict),
        errors (dict of DataFrames)
    """

    bronze_df = spark.read.format("delta").load(bronze_delta_path)

    checks = {}
    errors = {}

    # 1. Completeness: id and name must exist
    missing_id = bronze_df.filter("id IS NULL OR id = ''")
    checks["completeness_id"] = (missing_id.count() == 0)
    errors["completeness_id"] = missing_id

    missing_name = bronze_df.filter("name IS NULL OR name = ''")
    checks["completeness_name"] = (missing_name.count() == 0)
    errors["completeness_name"] = missing_name

    # 2. Duplicates based on id + ingestion_date snapshot
    dup_df = (
        bronze_df
        .groupBy("id", "_ingestion_date")
        .count()
        .filter("count > 1")
    )

    checks["uniqueness_id_snapshot"] = (dup_df.count() == 0)
    errors["uniqueness_id_snapshot"] = dup_df

    # 3. Freshness: ingestion_date should not be empty
    missing_ingestion = bronze_df.filter("_ingestion_date IS NULL")
    checks["freshness_ingestion_date"] = (missing_ingestion.count() == 0)
    errors["freshness_ingestion_date"] = missing_ingestion

    # 4. Schema Drift: check unexpected columns
    expected_cols = {
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "longitude",
        "latitude", "phone", "website_url", "state", "street",
        "_source", "_ingestion_ts", "_ingestion_date"
    }

    actual_cols = set(bronze_df.columns)
    drift = actual_cols - expected_cols

    checks["schema_drift"] = (len(drift) == 0)

    # Create a DataFrame with unexpected fields if any
    errors["schema_drift"] = spark.createDataFrame(
        [(col,) for col in drift],
        ["unexpected_column"]
    ) if drift else spark.createDataFrame([], "unexpected_column STRING")

    # 5. API Anomaly: zero records in ingestion
    checks["non_empty_batch"] = (bronze_df.count() > 0)

    errors["non_empty_batch"] = (
        spark.createDataFrame([], bronze_df.schema)
        if bronze_df.count() > 0
        else bronze_df  # returns empty DataFrame for anomaly
    )

    passed = all(checks.values())
    return passed, checks, errors
