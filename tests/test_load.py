"""
Tests for the SQL-building functions in redshift_load.py.

These test string generation only -- no live Redshift connection required -- so
they run in CI without AWS credentials. run_load() itself (which opens a real
psycopg2 connection) is intentionally not unit tested here.
"""
from src.load.redshift_load import (
    build_create_staging_table_sql, build_copy_sql, build_upsert_sql
)


def test_build_create_staging_table_sql():
    sql = build_create_staging_table_sql("fact_events", "fact_events_staging_123")
    assert "CREATE TABLE IF NOT EXISTS fact_events_staging_123" in sql
    assert "LIKE fact_events" in sql


def test_build_copy_sql_includes_s3_path_and_role():
    sql = build_copy_sql(
        "fact_events_staging_123",
        "s3://my-bucket/processed/fact_events/",
        "arn:aws:iam::123456789012:role/RedshiftLoadRole",
    )
    assert "s3://my-bucket/processed/fact_events/" in sql
    assert "arn:aws:iam::123456789012:role/RedshiftLoadRole" in sql
    assert "FORMAT AS PARQUET" in sql


def test_build_upsert_sql_deletes_then_inserts():
    sql = build_upsert_sql("fact_events", "fact_events_staging_123", "event_id")
    assert "DELETE FROM fact_events" in sql
    assert "INSERT INTO fact_events" in sql
    assert "DROP TABLE fact_events_staging_123" in sql
    # delete must happen before insert to make the load idempotent
    assert sql.index("DELETE") < sql.index("INSERT")
