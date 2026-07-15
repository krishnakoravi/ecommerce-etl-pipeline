# E-Commerce Clickstream ETL Pipeline

A batch + incremental data pipeline that ingests raw e-commerce clickstream events,
cleans and sessionizes them at scale with PySpark, runs automated data quality checks,
and loads aggregates into Redshift for analytics.

Built to mirror a real production data engineering workflow rather than a tutorial:
messy input data, an idempotent load pattern, a CI pipeline that actually runs Spark,
and a data quality gate that can fail a run.

## Architecture

```
Raw clickstream events (JSON)
        │
        ▼
   S3 raw zone  ──────────────────────────────────┐
        │                                          │
        │ ObjectCreated:Put                        │
        ▼                                          │
   Lambda (lambda/trigger_pipeline.py)              │
        │ submits EMR step                          │
        ▼                                          │
   PySpark on EMR (run_pipeline.py)                 │
   ├─ schema validation (explicit StructType)       │
   ├─ dedup + null handling + price imputation       │
   ├─ sessionization (window functions, 30-min gap)  │
   ├─ funnel / DAU / category aggregates             │
   └─ data quality report (JSON, pass/fail gate)      │
        │                                          │
        ▼                                          │
   S3 processed zone (Parquet, partitioned by date) ◄┘
        │
        ▼
   Glue Crawler → Glue Data Catalog
        │
        ▼
   Redshift (staging-table swap, idempotent upsert)
   fact_events / session_summary / daily_active_users
```

## What's actually interesting here

**Sessionization from raw timestamps, not a pre-existing session_id.**
`src/transform/sessionize.py::reconstruct_sessions` rebuilds session boundaries using
only `(user_id, event_timestamp)` — a `lag()` window function computes the gap since
each user's previous event, and any gap over 30 minutes starts a new session. This is
the standard approach to a common interview question, implemented for real:

```python
user_window = Window.partitionBy("user_id").orderBy("event_timestamp")
df = df.withColumn("prev_event_ts", F.lag("event_timestamp").over(user_window))
df = df.withColumn("gap_minutes", (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_event_ts")) / 60.0)
df = df.withColumn("is_new_session", F.when(F.col("gap_minutes") > 30, 1).otherwise(0))
df = df.withColumn("session_seq", F.sum("is_new_session").over(user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
```

**A data quality gate that can actually fail a run**, not just log warnings.
`src/quality/checks.py::run_quality_report` computes null rates, duplicate rates,
and schema drift before/after cleaning, and sets `passed=False` if more than 25% of
rows were dropped or any null rate survives the clean step. On a real 20,156-row
sample batch (with ~5% intentionally injected bad data), a run looks like:

```json
{
  "raw_row_count": 20156,
  "clean_row_count": 19391,
  "rows_dropped": 765,
  "rows_dropped_pct": 0.038,
  "null_rates_raw": { "user_id": 0.015, "price": 0.0102, "event_timestamp": 0.0 },
  "duplicate_rate_raw": 0.0077,
  "null_rates_clean": { "user_id": 0.0, "price": 0.0, "event_timestamp": 0.0 },
  "passed": true
}
```

**Idempotent Redshift loads via staging-table swap.**
`src/load/redshift_load.py` loads into a staging table via `COPY`, then does a
`DELETE ... USING staging` + `INSERT INTO ... SELECT * FROM staging` inside a
transaction. Rerunning the same batch twice doesn't double-count anything — a real
requirement once you have retry logic anywhere in a pipeline.

## Repo structure

```
ecommerce-etl-pipeline/
├── src/
│   ├── ingestion/generate_events.py   # synthetic clickstream generator (+ injected bad data)
│   ├── transform/
│   │   ├── schema.py                  # explicit StructType, not inferred
│   │   ├── clean.py                   # dedup, null handling, price imputation
│   │   ├── sessionize.py              # window-function sessionization + aggregates
│   │   └── spark_utils.py
│   ├── quality/checks.py              # null/duplicate/drift checks + pass/fail gate
│   └── load/redshift_load.py          # staging-swap upsert (SQL-builders are unit tested)
├── lambda/trigger_pipeline.py         # S3 ObjectCreated -> EMR step submission
├── tests/                             # pytest + local SparkSession, 20 tests
├── docker/Dockerfile
├── .github/workflows/ci.yml           # runs pytest + full pipeline + docker build on every push
├── setup_aws.py                       # boto3 script to provision S3 buckets + Glue crawler
├── run_pipeline.py                    # main entrypoint, runs everything locally end-to-end
└── data/sample/                       # generated locally, gitignored
```

## Running it locally

Requires Python 3.11+ and Java 17 (for PySpark). No AWS account needed for the local run.

```bash
pip install -r requirements.txt

# generate a synthetic dataset (~20k events, ~5% intentionally dirty)
python src/ingestion/generate_events.py --num-events 20000 --num-users 800 --output-dir data/sample

# run the full pipeline: clean -> quality checks -> sessionize -> aggregate -> write Parquet
python run_pipeline.py --input data/sample/clickstream_events.json --output data/processed
```

Sample output from the DAU aggregate:

```
+----------+---+-----------+---------+
|event_date|dau|event_count|revenue  |
+----------+---+-----------+---------+
|2026-06-01|104|743        |9276.69  |
|2026-06-02|92 |631        |10055.34 |
|2026-06-03|93 |519        |4088.90  |
+----------+---+-----------+---------+
```

Funnel drop-off (session-level: view → add_to_cart → purchase):

```json
{"view_sessions": 2988, "cart_sessions": 2287, "purchase_sessions": 533}
```

## Running the tests

```bash
pytest tests/ -v
```

20 tests covering cleaning logic, sessionization (including the gap-based session
reconstruction), data quality checks, Redshift SQL generation, and the Lambda
handler's event parsing — all against a local SparkSession, no cluster required.

## Running with Docker

```bash
docker build -f docker/Dockerfile -t ecommerce-etl-pipeline .
docker run ecommerce-etl-pipeline                    # generates data + runs the pipeline
docker run ecommerce-etl-pipeline pytest tests/ -v   # runs the test suite instead
```

## Deploying the AWS pieces

```bash
python setup_aws.py --bucket-name your-unique-bucket-name --region us-east-1
aws s3 cp data/sample/clickstream_events.json s3://your-bucket/raw/
aws s3 cp run_pipeline.py s3://your-bucket/scripts/
```

Then wire the S3 raw-zone bucket's `ObjectCreated:Put` event (filtered to `raw/*.json`)
to the `lambda/trigger_pipeline.py` handler, which submits the corresponding EMR step.

## Tech stack

Python, PySpark, AWS (S3, EMR, Glue, Lambda, Redshift), Docker, GitHub Actions, Pytest

## Possible extensions

- Great Expectations instead of the hand-rolled quality checks, for a declarative rule set
- dbt on top of the Redshift tables for the transformation-in-warehouse layer
- Backfill CLI that replays a date range through the pipeline
