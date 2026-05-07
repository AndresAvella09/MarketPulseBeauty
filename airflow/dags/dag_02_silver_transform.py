"""
dag_02_silver_transform.py
──────────────────────────
Daily DAG: Bronze → Silver NLP enrichment.

Tasks
─────
  detect_latest_bronze   → find today's bronze partition, mint silver_run_id
  contracts_gate         → enforce data contracts on bronze reviews
  silver_products        → enrich products with NLP, write silver/products parquet
  silver_reviews         → dedup + enrich reviews with NLP, write silver/reviews parquet

Each task is its own Airflow worker process so spaCy / GPU memory is freed
between stages.
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
    silver_run_id = uuid.uuid4().hex[:12]
    context["ti"].xcom_push(key="silver_run_id", value=silver_run_id)
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
        print(f"[detect_latest_bronze] silver_run_id={silver_run_id} | {table}: {count} files for {date_str}")

    if total_files == 0:
        raise RuntimeError(
            f"[detect_latest_bronze] silver_run_id={silver_run_id} | "
            f"No bronze files found for {date_str}. DAG 01 may have failed or not run yet."
        )

    context["ti"].xcom_push(key="bronze_date", value=date_str)


def contracts_gate(**context):
    from src.ingestion.scraper.silver_transform import stage_contracts

    silver_run_id = context["ti"].xcom_pull(key="silver_run_id", task_ids="detect_latest_bronze")
    bronze_date = context["ti"].xcom_pull(key="bronze_date", task_ids="detect_latest_bronze")
    stage_contracts(silver_run_id=silver_run_id, bronze_date=bronze_date)


def silver_products_task(**context):
    from src.ingestion.scraper.silver_transform import stage_silver_products

    silver_run_id = context["ti"].xcom_pull(key="silver_run_id", task_ids="detect_latest_bronze")
    bronze_date = context["ti"].xcom_pull(key="bronze_date", task_ids="detect_latest_bronze")
    out = stage_silver_products(silver_run_id=silver_run_id, bronze_date=bronze_date)
    print(f"[silver_products] wrote: {out}")


def silver_reviews_task(**context):
    from src.ingestion.scraper.silver_transform import stage_silver_reviews

    silver_run_id = context["ti"].xcom_pull(key="silver_run_id", task_ids="detect_latest_bronze")
    bronze_date = context["ti"].xcom_pull(key="bronze_date", task_ids="detect_latest_bronze")
    started_at = context["ti"].xcom_pull(key="started_at", task_ids="detect_latest_bronze")
    outs = stage_silver_reviews(silver_run_id=silver_run_id, bronze_date=bronze_date)
    print(f"[silver_reviews] wrote {len(outs)} file(s)")

    # Log pipeline run on the last task (only when the Postgres conn is configured)
    from src.processing.gold_writer import log_pipeline_run
    from sqlalchemy import create_engine
    conn_str = os.getenv("POSTGRES_GOLD_CONN")
    if conn_str:
        engine = create_engine(conn_str)
        with engine.begin() as conn:
            log_pipeline_run(
                run_id=silver_run_id, dag_name="dag_02_silver_transform",
                status="success", rows_written=len(outs),
                started_at=datetime.fromisoformat(started_at),
                finished_at=datetime.now(timezone.utc),
                conn=conn,
            )


with DAG(
    dag_id="dag_02_silver_transform",
    default_args=default_args,
    description="Daily Bronze → Silver NLP transform (split into stages)",
    schedule="0 4 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transform", "silver"],
) as dag:

    t_detect = PythonOperator(task_id="detect_latest_bronze", python_callable=detect_latest_bronze)
    t_contracts = PythonOperator(task_id="contracts_gate", python_callable=contracts_gate)
    t_products = PythonOperator(task_id="silver_products", python_callable=silver_products_task)
    t_reviews = PythonOperator(task_id="silver_reviews", python_callable=silver_reviews_task)

    t_detect >> t_contracts >> t_products >> t_reviews
