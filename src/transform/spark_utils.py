"""Shared SparkSession factory so local runs, tests, and EMR jobs all configure Spark consistently."""
from pyspark.sql import SparkSession


def get_spark(app_name="ecommerce-etl", shuffle_partitions=8):
    """
    Build a SparkSession.

    shuffle_partitions is set low (8) for local/dev runs since the default of 200
    massively over-partitions small datasets and slows everything down. Override
    this at higher values (e.g. 200+) when running on EMR against real data volumes.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
