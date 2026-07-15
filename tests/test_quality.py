from src.quality.checks import (
    null_rate_report, duplicate_rate, invalid_event_type_rate, schema_drift_check
)


def test_null_rate_report(spark):
    df = spark.createDataFrame(
        [("u1", "p1"), (None, "p2"), (None, None)],
        ["user_id", "product_id"],
    )
    rates = null_rate_report(df, ["user_id", "product_id"])
    assert rates["user_id"] == round(2 / 3, 4)
    assert rates["product_id"] == round(1 / 3, 4)


def test_duplicate_rate(spark):
    df = spark.createDataFrame(
        [("e1",), ("e1",), ("e2",), ("e3",)],
        ["event_id"],
    )
    rate = duplicate_rate(df, key_col="event_id")
    assert rate == round(1 / 4, 4)


def test_invalid_event_type_rate(spark):
    df = spark.createDataFrame(
        [("view",), ("purchase",), ("bogus_type",)],
        ["event_type"],
    )
    rate = invalid_event_type_rate(df)
    assert rate == round(1 / 3, 4)


def test_schema_drift_check_detects_missing_column(spark):
    df = spark.createDataFrame([("u1",)], ["user_id"])
    result = schema_drift_check(df, expected_columns=["user_id", "event_id"])
    assert result["drift_detected"] is True
    assert "event_id" in result["missing_columns"]


def test_schema_drift_check_no_drift(spark):
    df = spark.createDataFrame([("u1", "e1")], ["user_id", "event_id"])
    result = schema_drift_check(df, expected_columns=["user_id", "event_id"])
    assert result["drift_detected"] is False
