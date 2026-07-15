"""
Sessionization and funnel analytics.

The raw data already carries a session_id from the generator (simulating an upstream
session-stitching service), but in real pipelines you often only get raw timestamped
events and have to reconstruct sessions yourself. reconstruct_sessions() below rebuilds
sessions from timestamps alone using the classic 30-minute inactivity gap rule, entirely
with window functions -- no session_id required. This is the part worth walking through
in an interview.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SESSION_GAP_MINUTES = 30


def reconstruct_sessions(df: DataFrame) -> DataFrame:
    """
    Rebuild session boundaries from (user_id, event_timestamp) alone:
    1. Order each user's events by time.
    2. Compute the gap since their previous event.
    3. Flag a new session whenever the gap exceeds SESSION_GAP_MINUTES.
    4. Cumulative sum of that flag gives a monotonically increasing session index per user.
    """
    user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

    df = df.withColumn(
        "prev_event_ts", F.lag("event_timestamp").over(user_window)
    )
    df = df.withColumn(
        "gap_minutes",
        (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_event_ts")) / 60.0
    )
    df = df.withColumn(
        "is_new_session",
        F.when(F.col("prev_event_ts").isNull(), 1)
        .when(F.col("gap_minutes") > SESSION_GAP_MINUTES, 1)
        .otherwise(0)
    )
    df = df.withColumn(
        "session_seq",
        F.sum("is_new_session").over(
            user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    df = df.withColumn(
        "reconstructed_session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_seq"))
    )
    return df.drop("prev_event_ts", "gap_minutes", "is_new_session", "session_seq")


def session_summary(df: DataFrame, session_col: str = "session_id") -> DataFrame:
    """
    One row per session: duration, event count, whether it converted (had a purchase),
    and total revenue from that session.
    """
    window = Window.partitionBy(session_col)

    return (
        df.withColumn("session_start", F.min("event_timestamp").over(window))
        .withColumn("session_end", F.max("event_timestamp").over(window))
        .groupBy(session_col, "user_id", "session_start", "session_end")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("session_revenue"),
        )
        .withColumn(
            "session_duration_sec",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
        )
        .withColumn("converted", F.col("purchase_count") > 0)
    )


def funnel_dropoff(df: DataFrame, session_col: str = "session_id") -> DataFrame:
    """
    Classic funnel: view -> add_to_cart -> purchase, counted at the session level
    (a session "reached" a stage if it contains at least one event of that type).
    """
    stage_flags = (
        df.groupBy(session_col)
        .agg(
            F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("reached_view"),
            F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("reached_cart"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("reached_purchase"),
        )
    )

    totals = stage_flags.agg(
        F.sum("reached_view").alias("view_sessions"),
        F.sum("reached_cart").alias("cart_sessions"),
        F.sum("reached_purchase").alias("purchase_sessions"),
    ).collect()[0]

    return totals  # simple Row of funnel totals; caller formats/displays as needed


def daily_active_users(df: DataFrame) -> DataFrame:
    """DAU and revenue trend by event_date, for a dashboard time series."""
    return (
        df.groupBy("event_date")
        .agg(
            F.countDistinct("user_id").alias("dau"),
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .orderBy("event_date")
    )


def category_performance(df: DataFrame) -> DataFrame:
    """Revenue and conversion by product category, for a leaderboard view."""
    return (
        df.groupBy("category")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .withColumn("conversion_rate", F.round(F.col("purchases") / F.col("total_events"), 4))
        .orderBy(F.desc("revenue"))
    )
