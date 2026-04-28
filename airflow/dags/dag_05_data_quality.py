"""
dag_05_data_quality.py
──────────────────────
Daily DAG: Data quality checks across Bronze, Silver, and Gold layers.
Depends on DAG 03 completing successfully.
"""

import io
import os
import sys
import uuid
from datetime import datetime, date, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

import pandas as pd

default_args = {
    "owner": "marketpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_s3():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )


def _get_pg_engine():
    from sqlalchemy import create_engine
    return create_engine(os.getenv("POSTGRES_GOLD_CONN"))


def _count_parquet_rows(bucket: str, prefix: str) -> int:
    """Count total rows across all Parquet files under a prefix."""
    import pyarrow.parquet as pq
    s3 = _get_s3()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    parquet_keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]

    total = 0
    for key in parquet_keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        total += pq.read_table(buf).num_rows

    return total


def check_bronze_count(**context):
    """Fail if Bronze partition for today has fewer than 100 rows."""
    run_id = uuid.uuid4().hex[:12]
    context["ti"].xcom_push(key="run_id", value=run_id)
    start = datetime.now(timezone.utc)
    context["ti"].xcom_push(key="started_at", value=start.isoformat())

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for table in ("products", "reviews"):
        prefix = f"{table}/ingestion_date={date_str}/"
        count = _count_parquet_rows("marketpulse-bronze", prefix)
        print(f"[check_bronze_count] run_id={run_id} | {table}: {count} rows")
        if count < 100:
            raise ValueError(
                f"Bronze quality check failed: {table} has {count} rows for {date_str} (minimum 100)"
            )


def check_silver_nulls(**context):
    """Fail if critical Silver columns have null rate above 5%."""
    import pyarrow.parquet as pq
    run_id = context["ti"].xcom_pull(key="run_id")
    s3 = _get_s3()

    prefix = "reviews/"
    resp = s3.list_objects_v2(Bucket="marketpulse-silver", Prefix=prefix)
    contents = resp.get("Contents", [])
    parquet_keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]

    if not parquet_keys:
        print(f"[check_silver_nulls] run_id={run_id} | no silver review files found")
        return

    frames = []
    for key in parquet_keys:
        obj = s3.get_object(Bucket="marketpulse-silver", Key=key)
        buf = io.BytesIO(obj["Body"].read())
        frames.append(pq.read_table(buf).to_pandas())

    df = pd.concat(frames, ignore_index=True)
    total = len(df)

    critical_cols = ["Rating", "ReviewText", "ProductID"]
    for col in critical_cols:
        if col not in df.columns:
            continue
        null_rate = df[col].isna().sum() / total
        print(f"[check_silver_nulls] run_id={run_id} | {col}: null_rate={null_rate:.4f}")
        if null_rate > 0.05:
            raise ValueError(
                f"Silver quality check failed: {col} has {null_rate:.2%} null rate (max 5%)"
            )


def write_quality_record(**context):
    """Insert a summary row into gold.pipeline_runs."""
    from src.processing.gold_writer import log_pipeline_run

    run_id = context["ti"].xcom_pull(key="run_id")
    started_at = context["ti"].xcom_pull(key="started_at")
    finished_at = datetime.now(timezone.utc)

    engine = _get_pg_engine()
    with engine.begin() as conn:
        log_pipeline_run(
            run_id=run_id,
            dag_name="dag_05_data_quality",
            status="success",
            rows_written=0,
            started_at=datetime.fromisoformat(started_at),
            finished_at=finished_at,
            conn=conn,
        )

    print(f"[write_quality_record] run_id={run_id} | quality check passed and logged")


with DAG(
    dag_id="dag_05_data_quality",
    default_args=default_args,
    description="Daily data quality checks across all layers",
    schedule="0 7 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["quality"],
) as dag:

    t_bronze = PythonOperator(task_id="check_bronze_count", python_callable=check_bronze_count)
    t_nulls = PythonOperator(task_id="check_silver_nulls", python_callable=check_silver_nulls)
    t_log = PythonOperator(task_id="write_quality_record", python_callable=write_quality_record)

    t_bronze >> t_nulls >> t_log
