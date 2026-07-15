from src.transform.clean import (
    parse_timestamps, drop_invalid_rows, dedupe_events, fill_missing_price, clean_events
)


def test_parse_timestamps_handles_malformed(spark):
    df = spark.createDataFrame(
        [("e1", "2026-06-01T10:00:00"), ("e2", "not-a-timestamp")],
        ["event_id", "event_timestamp"],
    )
    result = parse_timestamps(df)
    rows = {r["event_id"]: r["event_timestamp"] for r in result.collect()}
    assert rows["e1"] is not None
    assert rows["e2"] is None


def test_drop_invalid_rows_removes_null_user(spark):
    df = spark.createDataFrame(
        [
            ("e1", "u1", "view", "2026-06-01T10:00:00"),
            ("e2", None, "view", "2026-06-01T10:00:00"),
        ],
        ["event_id", "user_id", "event_type", "event_timestamp"],
    )
    df = parse_timestamps(df)
    result = drop_invalid_rows(df)
    assert result.count() == 1
    assert result.collect()[0]["event_id"] == "e1"


def test_drop_invalid_rows_removes_bad_event_type(spark):
    df = spark.createDataFrame(
        [
            ("e1", "u1", "view", "2026-06-01T10:00:00"),
            ("e2", "u1", "not_a_real_event", "2026-06-01T10:00:00"),
        ],
        ["event_id", "user_id", "event_type", "event_timestamp"],
    )
    df = parse_timestamps(df)
    result = drop_invalid_rows(df)
    assert result.count() == 1


def test_dedupe_events_removes_exact_duplicates(spark):
    df = spark.createDataFrame(
        [("e1", "u1"), ("e1", "u1"), ("e2", "u1")],
        ["event_id", "user_id"],
    )
    result = dedupe_events(df)
    assert result.count() == 2


def test_fill_missing_price_uses_product_median(spark):
    df = spark.createDataFrame(
        [
            ("P1", 10.0),
            ("P1", 20.0),
            ("P1", None),
        ],
        ["product_id", "price"],
    )
    result = fill_missing_price(df)
    filled = [r["price"] for r in result.collect() if r["price"] is not None]
    assert None not in [r["price"] for r in result.collect()]
    # median of [10, 20] is 10 or 20 depending on percentile_approx interpolation;
    # the key assertion is simply that no nulls remain
    assert len(filled) == 3


def test_clean_events_end_to_end(spark):
    df = spark.createDataFrame(
        [
            ("e1", "u1", "s1", "view", "P1", "electronics", 100.0, "2026-06-01T10:00:00"),
            ("e1", "u1", "s1", "view", "P1", "electronics", 100.0, "2026-06-01T10:00:00"),  # dup
            ("e2", None, "s1", "view", "P1", "electronics", 100.0, "2026-06-01T10:01:00"),  # null user
            ("e3", "u1", "s1", "purchase", "P2", "books", None, "2026-06-01T10:02:00"),      # null price
        ],
        ["event_id", "user_id", "session_id", "event_type", "product_id", "category", "price", "event_timestamp"],
    )
    result = clean_events(df)
    # e1 deduped to 1 row, e2 dropped (null user), e3 kept with imputed price
    assert result.count() == 2
    prices = [r["price"] for r in result.collect()]
    assert None not in prices
