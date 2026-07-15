"""
Cleaning transformations for raw clickstream events.

Each function is a pure DataFrame -> DataFrame transform so it can be unit tested
in isolation with a local SparkSession and small fixture data (see tests/).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schema import VALID_EVENT_TYPES


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Parse ISO timestamp strings; malformed ones become null (caught by quality checks)."""
    return df.withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp")
    )


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Drop rows that are unusable for downstream analysis:
    - missing user_id (can't attribute the event)
    - missing/unparseable event_timestamp
    - event_type outside the known set
    """
    return (
        df.filter(F.col("user_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("event_type").isin(list(VALID_EVENT_TYPES)))
    )


def dedupe_events(df: DataFrame) -> DataFrame:
    """Remove exact-duplicate event rows using event_id as the natural key."""
    return df.dropDuplicates(["event_id"])


def fill_missing_price(df: DataFrame) -> DataFrame:
    """
    Impute missing price using the median price for that product_id (computed from
    other rows in the batch). Falls back to the global median if a product never
    has a valid price in this batch.
    """
    product_median = (
        df.filter(F.col("price").isNotNull())
        .groupBy("product_id")
        .agg(F.expr("percentile_approx(price, 0.5)").alias("product_median_price"))
    )

    global_median = (
        df.filter(F.col("price").isNotNull())
        .agg(F.expr("percentile_approx(price, 0.5)").alias("global_median_price"))
        .collect()[0]["global_median_price"]
    )

    df = df.join(product_median, on="product_id", how="left")
    df = df.withColumn(
        "price",
        F.coalesce(F.col("price"), F.col("product_median_price"), F.lit(global_median))
    ).drop("product_median_price")

    return df


def add_date_partitions(df: DataFrame) -> DataFrame:
    """Add year/month/day columns for partitioned Parquet writes."""
    return (
        df.withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("day", F.dayofmonth("event_timestamp"))
    )


def clean_events(df: DataFrame) -> DataFrame:
    """Full cleaning pipeline, composed from the individual steps above."""
    df = parse_timestamps(df)
    df = drop_invalid_rows(df)
    df = dedupe_events(df)
    df = fill_missing_price(df)
    df = add_date_partitions(df)
    return df
