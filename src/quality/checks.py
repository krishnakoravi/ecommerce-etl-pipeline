"""
Data quality checks run against raw events before/after cleaning.

Produces a JSON report per pipeline run (see run_pipeline.py) so data quality is
tracked over time rather than silently swallowed. In a production setup this report
would be pushed to CloudWatch/Datadog and alert on threshold breaches; here it's
written to disk so it's inspectable in the repo.
"""
import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transform.schema import VALID_EVENT_TYPES


def null_rate_report(df: DataFrame, columns: list) -> dict:
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in columns}
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ]
    row = df.agg(*agg_exprs).collect()[0]
    return {c: round(row[c] / total, 4) for c in columns}


def duplicate_rate(df: DataFrame, key_col: str = "event_id") -> float:
    total = df.count()
    if total == 0:
        return 0.0
    distinct = df.select(key_col).distinct().count()
    return round((total - distinct) / total, 4)


def invalid_event_type_rate(df: DataFrame) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    invalid = df.filter(~F.col("event_type").isin(list(VALID_EVENT_TYPES))).count()
    return round(invalid / total, 4)


def schema_drift_check(df: DataFrame, expected_columns: list) -> dict:
    actual = set(df.columns)
    expected = set(expected_columns)
    return {
        "missing_columns": sorted(list(expected - actual)),
        "unexpected_columns": sorted(list(actual - expected)),
        "drift_detected": actual != expected,
    }


def run_quality_report(raw_df: DataFrame, clean_df: DataFrame, expected_columns: list) -> dict:
    """
    Run the full battery of checks comparing the raw batch to the cleaned output.
    Returns a dict suitable for json.dump.
    """
    raw_count = raw_df.count()
    clean_count = clean_df.count()

    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_row_count": raw_count,
        "clean_row_count": clean_count,
        "rows_dropped": raw_count - clean_count,
        "rows_dropped_pct": round((raw_count - clean_count) / raw_count, 4) if raw_count else 0.0,
        "null_rates_raw": null_rate_report(raw_df, ["user_id", "event_timestamp", "price", "product_id"]),
        "duplicate_rate_raw": duplicate_rate(raw_df),
        "invalid_event_type_rate_raw": invalid_event_type_rate(raw_df),
        "schema_drift": schema_drift_check(raw_df, expected_columns),
        "null_rates_clean": null_rate_report(clean_df, ["user_id", "event_timestamp", "price"]),
    }

    # simple pass/fail gate: fail the run if more than 25% of rows were dropped,
    # or if any null/invalid rate in the CLEANED data exceeds 0
    report["passed"] = (
        report["rows_dropped_pct"] <= 0.25
        and all(v == 0.0 for v in report["null_rates_clean"].values())
    )

    return report


def write_report(report: dict, output_path: str):
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Data quality report written to {output_path} | passed={report['passed']}")
