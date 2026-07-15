"""
End-to-end local pipeline runner.

Reads raw JSON events -> cleans -> runs data quality checks -> sessionizes ->
computes aggregates -> writes partitioned Parquet output + JSON reports.

This is the "S3 raw zone -> PySpark on EMR -> S3 processed zone" portion of the
architecture, runnable entirely locally against the data/sample directory (no AWS
account required). The Glue/Redshift/Lambda pieces are represented by the code in
src/load/ and lambda/, which are written against real AWS APIs but not invoked here.

Usage:
    python run_pipeline.py --input data/sample/clickstream_events.json --output data/processed
"""
import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from src.transform.spark_utils import get_spark
from src.transform.schema import RAW_EVENT_SCHEMA
from src.transform.clean import clean_events
from src.transform.sessionize import (
    session_summary, funnel_dropoff, daily_active_users, category_performance
)
from src.quality.checks import run_quality_report, write_report


def main(input_path: str, output_dir: str):
    spark = get_spark()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading raw events from {input_path} ...")
    raw_df = spark.read.schema(RAW_EVENT_SCHEMA).json(input_path)
    raw_count = raw_df.count()
    print(f"  {raw_count} raw rows")

    print("Cleaning events (dedup, null handling, price imputation) ...")
    clean_df = clean_events(raw_df)
    clean_df.cache()
    clean_count = clean_df.count()
    print(f"  {clean_count} clean rows ({raw_count - clean_count} dropped)")

    print("Running data quality checks ...")
    report = run_quality_report(raw_df, clean_df, expected_columns=RAW_EVENT_SCHEMA.fieldNames())
    write_report(report, output_dir / "data_quality_report.json")

    print("Writing cleaned, partitioned Parquet ...")
    (
        clean_df.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(output_dir / "fact_events"))
    )

    print("Computing session summaries ...")
    sessions_df = session_summary(clean_df, session_col="session_id")
    sessions_df.write.mode("overwrite").parquet(str(output_dir / "session_summary"))

    print("Computing funnel drop-off ...")
    funnel = funnel_dropoff(clean_df, session_col="session_id")
    funnel_dict = funnel.asDict()
    with open(output_dir / "funnel_report.json", "w") as f:
        json.dump(funnel_dict, f, indent=2)
    print(f"  Funnel: {funnel_dict}")

    print("Computing DAU trend ...")
    dau_df = daily_active_users(clean_df)
    dau_df.write.mode("overwrite").parquet(str(output_dir / "daily_active_users"))
    dau_df.show(10, truncate=False)

    print("Computing category performance ...")
    cat_df = category_performance(clean_df)
    cat_df.write.mode("overwrite").parquet(str(output_dir / "category_performance"))
    cat_df.show(10, truncate=False)

    print(f"\nPipeline complete. Outputs written to {output_dir}/")
    print(f"Data quality: passed={report['passed']}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/sample/clickstream_events.json")
    parser.add_argument("--output", default="data/processed")
    args = parser.parse_args()

    main(args.input, args.output)
