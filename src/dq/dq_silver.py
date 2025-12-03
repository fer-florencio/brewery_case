from pyspark.sql import functions as F

def dq_check_silver(silver_df):

    checks = {}
    errors = {}

    # 1. Uniqueness: IDs must be unique
    dup_df = (
        silver_df
        .groupBy("id")
        .count()
        .filter("count > 1")
    )

    checks["uniqueness_id"] = (dup_df.count() == 0)
    errors["uniqueness_id"] = dup_df


    # 2. Completeness checks
    def add_completeness_check(colname):
        err = silver_df.filter(F.col(colname).isNull())
        checks[f"completeness_{colname}"] = (err.count() == 0)
        errors[f"completeness_{colname}"] = err

    add_completeness_check("name")
    add_completeness_check("city")
    add_completeness_check("country")


    # 3. Geolocation validity
    df_geo = (
        silver_df
        .withColumn("lat_num", F.expr("try_cast(latitude AS double)"))
        .withColumn("lon_num", F.expr("try_cast(longitude AS double)"))
    )

    invalid_cast_df = df_geo.filter("""
        (latitude IS NOT NULL AND latitude != '' AND lat_num IS NULL)
        OR
        (longitude IS NOT NULL AND longitude != '' AND lon_num IS NULL)
    """)

    checks["valid_geolocation_cast"] = (invalid_cast_df.count() == 0)
    errors["valid_geolocation_cast"] = invalid_cast_df


    # Out-of-range values
    range_err_df = df_geo.filter("""
        lat_num < -90 OR lat_num > 90
        OR
        lon_num < -180 OR lon_num > 180
    """)

    checks["valid_geolocation_range"] = (range_err_df.count() == 0)
    errors["valid_geolocation_range"] = range_err_df


    # 4. Brewery type validity
    valid_types = [
        "micro", "regional", "brewpub", "large",
        "contract", "planning", "proprietor", "closed"
    ]

    invalid_type_df = silver_df.filter(~F.col("brewery_type").isin(valid_types))

    checks["valid_brewery_type"] = (invalid_type_df.count() == 0)
    errors["valid_brewery_type"] = invalid_type_df


    # 5. Formatting consistency
    bad_country_df = silver_df.filter("country != upper(country)")

    checks["standardized_country_format"] = (bad_country_df.count() == 0)
    errors["standardized_country_format"] = bad_country_df


    passed = all(checks.values())
    return passed, checks, errors
