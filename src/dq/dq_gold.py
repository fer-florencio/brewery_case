from pyspark.sql import functions as F

def dq_check_gold(gold_df):
    """
    Performs Data Quality checks on the Gold aggregated layer.
    Returns:
        passed (bool),
        checks (dict),
        errors (dict of DataFrames)
    """

    checks = {}
    errors = {}

    # 1. Aggregation validity: no negative counts
    negative_counts = gold_df.filter("total_breweries < 0")
    checks["valid_positive_counts"] = (negative_counts.count() == 0)
    errors["valid_positive_counts"] = negative_counts

    # 2. Completeness of grouping keys
    null_keys = gold_df.filter("""
        country IS NULL OR city IS NULL OR brewery_type IS NULL
    """)
    checks["completeness_grouping_keys"] = (null_keys.count() == 0)
    errors["completeness_grouping_keys"] = null_keys

    # 3. Formatting consistency
    bad_country = gold_df.filter("country != upper(country)")
    checks["standardized_country_format"] = (bad_country.count() == 0)
    errors["standardized_country_format"] = bad_country

    # 4. Structural schema
    required_cols = {
        "country", "state_province", "city",
        "brewery_type", "total_breweries"
    }

    missing = required_cols - set(gold_df.columns)

    checks["structural_keys_present"] = (len(missing) == 0)

    errors["structural_keys_present"] = (
        gold_df.sparkSession.createDataFrame(
            [(c,) for c in missing], ["missing_column"]
        ) if missing else gold_df.sparkSession.createDataFrame([], "missing_column STRING")
    )

    passed = all(checks.values())
    return passed, checks, errors
