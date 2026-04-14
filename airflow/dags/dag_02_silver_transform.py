"""
dag_02_silver_transform.py
──────────────────────────
Daily DAG: Bronze → Silver NLP enrichment.
Depends on DAG 01 completing successfully.
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

import boto3

default_args = {
    "owner": "marketpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )


def detect_latest_bronze(**context):
    """List objects in marketpulse-bronze and find today's partition."""
    run_id = uuid.uuid4().hex[:12]
    context["ti"].xcom_push(key="run_id", value=run_id)
    start = datetime.now(timezone.utc)
    context["ti"].xcom_push(key="started_at", value=start.isoformat())

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = _get_s3()

    total_files = 0
    for table in ("products", "reviews"):
        prefix = f"{table}/ingestion_date={date_str}/"
        resp = s3.list_objects_v2(Bucket="marketpulse-bronze", Prefix=prefix)
        count = len(resp.get("Contents", []))
        total_files += count
        print(f"[detect_latest_bronze] run_id={run_id} | {table}: {count} files for {date_str}")

    if total_files == 0:
        raise RuntimeError(
            f"[detect_latest_bronze] run_id={run_id} | No bronze files found for {date_str}. "
            "DAG 01 (ingestion) may have failed or not run yet."
        )

    context["ti"].xcom_push(key="bronze_date", value=date_str)


def write_silver(**context):
    """Execute the full Silver transform and write to MinIO."""
    from src.ingestion.scraper.silver_transform import transform

    run_id = context["ti"].xcom_pull(key="run_id")
    bronze_date = context["ti"].xcom_pull(key="bronze_date")
    started_at = context["ti"].xcom_pull(key="started_at")

    print(f"[write_silver] run_id={run_id} | transforming bronze_date={bronze_date}")
    result = transform(bronze_date=bronze_date)

    if not result.get("products") and not result.get("reviews"):
        raise RuntimeError(
            f"[write_silver] run_id={run_id} | Silver transform produced no output. "
            f"Check that Bronze data exists for date={bronze_date} in MinIO."
        )

    rows_written = 0
    for key, val in result.items():
        if isinstance(val, list):
            rows_written += len(val)
        elif val:
            rows_written += 1

    print(f"[write_silver] run_id={run_id} | outputs={result} | rows_written={rows_written}")

    # Log pipeline run
    from src.processing.gold_writer import log_pipeline_run
    from sqlalchemy import create_engine
    conn_str = os.getenv("POSTGRES_GOLD_CONN")
    if conn_str:
        engine = create_engine(conn_str)
        with engine.begin() as conn:
            log_pipeline_run(
                run_id=run_id, dag_name="dag_02_silver_transform",
                status="success", rows_written=rows_written,
                started_at=datetime.fromisoformat(started_at),
                finished_at=datetime.now(timezone.utc),
                conn=conn,
            )


with DAG(
    dag_id="dag_02_silver_transform",
    default_args=default_args,
    description="Daily Bronze → Silver NLP transform",
    schedule="0 4 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transform", "silver"],
) as dag:

    t_detect = PythonOperator(task_id="detect_latest_bronze", python_callable=detect_latest_bronze)
    t_write = PythonOperator(task_id="write_silver", python_callable=write_silver)

    t_detect >> t_write
