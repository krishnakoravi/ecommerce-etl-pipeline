from src.transform.sessionize import reconstruct_sessions, session_summary, daily_active_users


def test_reconstruct_sessions_splits_on_gap(spark):
    """Two events 5 min apart = same session. Two events 45 min apart = new session."""
    df = spark.createDataFrame(
        [
            ("u1", "2026-06-01T10:00:00"),
            ("u1", "2026-06-01T10:05:00"),   # +5 min -> same session
            ("u1", "2026-06-01T10:50:00"),   # +45 min -> new session
        ],
        ["user_id", "event_timestamp"],
    ).withColumn("event_timestamp", spark_ts("event_timestamp"))

    result = reconstruct_sessions(df).orderBy("event_timestamp").collect()
    assert result[0]["reconstructed_session_id"] == result[1]["reconstructed_session_id"]
    assert result[1]["reconstructed_session_id"] != result[2]["reconstructed_session_id"]


def test_reconstruct_sessions_separates_users(spark):
    """Different users should never share a session, regardless of timestamp proximity."""
    df = spark.createDataFrame(
        [
            ("u1", "2026-06-01T10:00:00"),
            ("u2", "2026-06-01T10:00:01"),
        ],
        ["user_id", "event_timestamp"],
    ).withColumn("event_timestamp", spark_ts("event_timestamp"))

    result = reconstruct_sessions(df).collect()
    session_ids = {r["reconstructed_session_id"] for r in result}
    assert len(session_ids) == 2


def test_session_summary_flags_conversion(spark):
    df = spark.createDataFrame(
        [
            ("s1", "u1", "view", 10.0, "2026-06-01T10:00:00"),
            ("s1", "u1", "purchase", 25.0, "2026-06-01T10:05:00"),
            ("s2", "u2", "view", 10.0, "2026-06-01T10:00:00"),
        ],
        ["session_id", "user_id", "event_type", "price", "event_timestamp"],
    ).withColumn("event_timestamp", spark_ts("event_timestamp"))

    result = session_summary(df, session_col="session_id").collect()
    by_session = {r["session_id"]: r for r in result}
    assert by_session["s1"]["converted"] is True
    assert by_session["s1"]["session_revenue"] == 25.0
    assert by_session["s2"]["converted"] is False


def test_daily_active_users_counts_distinct(spark):
    df = spark.createDataFrame(
        [
            ("u1", "view", 0.0, "2026-06-01"),
            ("u1", "view", 0.0, "2026-06-01"),  # same user, same day -> counted once
            ("u2", "view", 0.0, "2026-06-01"),
        ],
        ["user_id", "event_type", "price", "event_date"],
    ).withColumn("event_date", spark_date("event_date"))

    result = daily_active_users(df).collect()
    assert result[0]["dau"] == 2
    assert result[0]["event_count"] == 3


# small helpers to keep test data construction readable
from pyspark.sql import functions as F


def spark_ts(col):
    return F.to_timestamp(col)


def spark_date(col):
    return F.to_date(col)
