import sys, os
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from datetime import datetime

from dq_bronze import dq_check_bronze
from dq_silver import dq_check_silver
from dq_gold import dq_check_gold

from config import (
    CATALOG,
    BRONZE_PATH,
    SILVER_SCHEMA,
    GOLD_SCHEMA
)

DQ_SCHEMA = "quality"
DQ_AUDIT_TABLE = "dq_audit"
DQ_ERRORS_TABLE = "dq_errors"


def create_audit_tables_if_not_exists(spark):
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DQ_SCHEMA}")

    # audit
    full_audit = f"{CATALOG}.{DQ_SCHEMA}.{DQ_AUDIT_TABLE}"
    if not spark.catalog.tableExists(full_audit):
        spark.createDataFrame([], """
            layer STRING,
            check_name STRING,
            status BOOLEAN,
            timestamp TIMESTAMP,
            details STRING
        """).write.format("delta").mode("overwrite").saveAsTable(full_audit)

    # errors
    full_errors = f"{CATALOG}.{DQ_SCHEMA}.{DQ_ERRORS_TABLE}"
    if not spark.catalog.tableExists(full_errors):
        spark.createDataFrame([], """
            layer STRING,
            check_name STRING,
            error_record STRING,
            timestamp TIMESTAMP
        """).write.format("delta").mode("overwrite").saveAsTable(full_errors)

    return full_audit, full_errors


def log_dq_results(spark, audit_table, layer_name, checks):
    now = datetime.utcnow()

    df = spark.createDataFrame([
        Row(
            layer=layer_name,
            check_name=name,
            status=bool(status),
            timestamp=now,
            details=str(checks)
        )
        for name, status in checks.items()
    ])

    df.write.format("delta").mode("append").saveAsTable(audit_table)


def log_dq_errors(spark, errors_table, layer_name, errors):
    """
    Logs invalid records for each failed check.
    Safe for Databricks Serverless — uses no RDDs, no toJSON() driver ops.
    """

    now = datetime.utcnow()

    for check_name, df in errors.items():
        if df is None or df.count() == 0:
            continue

        # Limit error rows
        limited = df.limit(50)

        # Serverless-safe row serialization
        errors_json_df = (
            limited
            .withColumn("error_record", F.to_json(F.struct("*")))
            .select("error_record")
            .withColumn("layer", F.lit(layer_name))
            .withColumn("check_name", F.lit(check_name))
            .withColumn("timestamp", F.lit(now))
            .select("layer", "check_name", "error_record", "timestamp")
        )

        errors_json_df.write.format("delta").mode("append").saveAsTable(errors_table)


def dq_runner(spark):

    audit_table, errors_table = create_audit_tables_if_not_exists(spark)

    # ============================
    # Bronze
    # ============================
    bronze_path = f"{BRONZE_PATH}/delta"
    bronze_passed, bronze_checks, bronze_errors = dq_check_bronze(spark, bronze_path)

    log_dq_results(spark, audit_table, "bronze", bronze_checks)
    log_dq_errors(spark, errors_table, "bronze", bronze_errors)

    # ============================
    # Silver
    # ============================
    silver_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.brewery_clean")
    silver_passed, silver_checks, silver_errors = dq_check_silver(silver_df)

    log_dq_results(spark, audit_table, "silver", silver_checks)
    log_dq_errors(spark, errors_table, "silver", silver_errors)

    # ============================
    # Gold
    # ============================
    gold_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.brewery_agg")
    gold_passed, gold_checks, gold_errors = dq_check_gold(gold_df)

    log_dq_results(spark, audit_table, "gold", gold_checks)
    log_dq_errors(spark, errors_table, "gold", gold_errors)

    # Final Status
    all_passed = bronze_passed and silver_passed and gold_passed

    print("\n=== FINAL DQ STATUS ===")
    print("PASSED ✅" if all_passed else "FAILED ❌")

    return all_passed


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dq_runner(spark)
