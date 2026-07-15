"""Explicit schema for raw clickstream events. Enforcing this at read time (instead of
letting Spark infer it) catches schema drift immediately rather than silently coercing
bad data to null downstream."""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),  # parsed to TimestampType in transform step
])

VALID_EVENT_TYPES = {"view", "add_to_cart", "remove_from_cart", "purchase"}
