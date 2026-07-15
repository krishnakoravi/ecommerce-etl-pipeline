"""
Load cleaned, aggregated data into Redshift using the COPY-from-S3 pattern with a
staging-table swap for idempotent incremental loads.

This module is written against a real Redshift cluster (via psycopg2) but is not
executed in CI, since that would require live AWS credentials. tests/test_load.py
exercises the SQL-building functions directly (pure string generation, no DB call)
so the logic is still verified without needing a live cluster.

To actually run this against AWS:
    1. Set REDSHIFT_* env vars (host, port, db, user, password) and IAM role ARN.
    2. python -m src.load.redshift_load --s3-path s3://bucket/processed/fact_events/
"""
import argparse
import os


def build_create_staging_table_sql(target_table: str, staging_table: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {staging_table} (LIKE {target_table});"


def build_copy_sql(staging_table: str, s3_path: str, iam_role_arn: str) -> str:
    """
    COPY loads Parquet directly from S3 into the staging table. Using staging + swap
    (rather than COPY directly into the target table) means a failed/partial load
    never corrupts the production table -- the swap only happens after COPY succeeds.
    """
    return f"""
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS PARQUET;
    """.strip()


def build_upsert_sql(target_table: str, staging_table: str, primary_key: str) -> str:
    """
    Delete-then-insert upsert pattern (Redshift has no native MERGE prior to newer
    versions): remove rows in target whose key exists in staging, then insert
    everything from staging. This makes reruns of the same batch idempotent.
    """
    return f"""
        BEGIN;

        DELETE FROM {target_table}
        USING {staging_table}
        WHERE {target_table}.{primary_key} = {staging_table}.{primary_key};

        INSERT INTO {target_table}
        SELECT * FROM {staging_table};

        DROP TABLE {staging_table};

        COMMIT;
    """.strip()


def run_load(s3_path: str, target_table: str = "fact_events", primary_key: str = "event_id"):
    """Execute the full staging-swap load against a live Redshift cluster."""
    import psycopg2  # imported lazily so this module can be imported without the driver installed

    staging_table = f"{target_table}_staging_{os.getpid()}"
    iam_role_arn = os.environ["REDSHIFT_IAM_ROLE_ARN"]

    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=os.environ.get("REDSHIFT_PORT", 5439),
        dbname=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(build_create_staging_table_sql(target_table, staging_table))
            cur.execute(build_copy_sql(staging_table, s3_path, iam_role_arn))
            cur.execute(build_upsert_sql(target_table, staging_table, primary_key))
        conn.commit()
        print(f"Loaded {s3_path} into {target_table} via staging swap.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-path", required=True)
    parser.add_argument("--target-table", default="fact_events")
    parser.add_argument("--primary-key", default="event_id")
    args = parser.parse_args()

    run_load(args.s3_path, args.target_table, args.primary_key)
