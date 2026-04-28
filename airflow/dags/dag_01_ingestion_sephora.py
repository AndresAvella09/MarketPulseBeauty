"""
dag_01_ingestion_sephora.py
───────────────────────────
Daily DAG: scrape Sephora → CSV backup → Bronze ingest → quality validation.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Allow imports from /opt/airflow/src
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


def health_check(**context):
    """Verify Sephora / Bazaarvoice API is reachable."""
    import requests
    run_id = uuid.uuid4().hex[:12]
    context["ti"].xcom_push(key="run_id", value=run_id)
    start = datetime.now(timezone.utc)
    context["ti"].xcom_push(key="started_at", value=start.isoformat())

    resp = requests.get("https://api.bazaarvoice.com/data/reviews.json", timeout=15)
    print(f"[health_check] run_id={run_id} | Bazaarvoice status={resp.status_code}")


def run_scraper(**context):
    """Execute the Sephora scraper and push results to XCom."""
    from src.ingestion.scraper.scraper import SephoraScraper

    run_id = context["ti"].xcom_pull(key="run_id")
    print(f"[run_scraper] run_id={run_id} | starting scrape ...")

    products_dict, reviews_list = SephoraScraper().run()
    print(f"[run_scraper] scraped {len(products_dict)} products, {len(reviews_list)} reviews")

    if not products_dict and not reviews_list:
        raise RuntimeError(
            f"[run_scraper] run_id={run_id} | Scraper returned NO data. "
            "Check BAZAARVOICE_PASSKEY env var and sitemap XML availability."
        )

    # Push as serialisable dicts
    context["ti"].xcom_push(key="products", value=list(products_dict.values()))
    context["ti"].xcom_push(key="reviews", value=reviews_list)


def write_raw_backup(**context):
    """Write CSV backup to MinIO marketpulse-raw."""
    import csv
    import io

    run_id = context["ti"].xcom_pull(key="run_id")
    products = context["ti"].xcom_pull(key="products", task_ids="run_scraper")
    reviews = context["ti"].xcom_pull(key="reviews", task_ids="run_scraper")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = _get_s3()

    for name, rows in [("products", products), ("reviews", reviews)]:
        if not rows:
            continue
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        key = f"backups/{date_str}/sephora_{name}_{run_id}.csv"
        s3.put_object(Bucket="marketpulse-raw", Key=key, Body=buf.getvalue().encode("utf-8"))
        print(f"[write_raw_backup] run_id={run_id} | {len(rows)} rows → s3://marketpulse-raw/{key}")


def bronze_ingest(**context):
    """Run bronze ingestion, writing Parquet to MinIO."""
    from src.ingestion.scraper.bronze_ingestion import ingest

    run_id = context["ti"].xcom_pull(key="run_id")
    products = context["ti"].xcom_pull(key="products", task_ids="run_scraper")
    reviews = context["ti"].xcom_pull(key="reviews", task_ids="run_scraper")

    if not products and not reviews:
        raise RuntimeError(
            f"[bronze_ingest] run_id={run_id} | No data received from scraper via XCom"
        )

    print(f"[bronze_ingest] run_id={run_id} | {len(products)} products, {len(reviews)} reviews")
    result = ingest(
        products=products, reviews=reviews,
        run_id=run_id, source="airflow_dag_01",
        fail_on_quality=True,
    )

    if not result.get("products") and not result.get("reviews"):
        raise RuntimeError(
            f"[bronze_ingest] run_id={run_id} | ingest() produced no output files"
        )

    context["ti"].xcom_push(key="bronze_result", value=result)
    print(f"[bronze_ingest] run_id={run_id} | rows processed={len(products) + len(reviews)}")


def validate_quality(**context):
    """Read quality report from MinIO and fail if thresholds are breached."""
    run_id = context["ti"].xcom_pull(key="run_id")
    started_at = context["ti"].xcom_pull(key="started_at")
    s3 = _get_s3()

    key = f"_quality_reports/report_{run_id}.json"
    try:
        obj = s3.get_object(Bucket="marketpulse-bronze", Key=key)
        report = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"[validate_quality] run_id={run_id} | Quality report not found at "
            f"s3://marketpulse-bronze/{key}. Bronze ingest may have failed silently: {exc}"
        )

    print(f"[validate_quality] run_id={run_id} | overall_pass={report.get('overall_pass')}")

    for r in report.get("reports", []):
        total = r.get("total_rows", 0)
        if total < 100:
            raise ValueError(f"Quality check failed: {r['table']} has only {total} rows (min 100)")
        for check in r.get("checks", []):
            if not check["passed"] and check.get("critical"):
                raise ValueError(f"Critical quality check failed: {check['check']} in {r['table']}")

    # Log pipeline run
    from src.processing.gold_writer import log_pipeline_run
    from sqlalchemy import create_engine
    conn_str = os.getenv("POSTGRES_GOLD_CONN")
    if conn_str:
        engine = create_engine(conn_str)
        finished_at = datetime.now(timezone.utc)
        with engine.begin() as conn:
            log_pipeline_run(
                run_id=run_id, dag_name="dag_01_ingestion_sephora",
                status="success", rows_written=0,
                started_at=datetime.fromisoformat(started_at), finished_at=finished_at,
                conn=conn,
            )


with DAG(
    dag_id="dag_01_ingestion_sephora",
    default_args=default_args,
    description="Daily Sephora scrape → Bronze ingest",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ingestion", "sephora"],
) as dag:

    t_health = PythonOperator(task_id="health_check", python_callable=health_check)
    t_scrape = PythonOperator(task_id="run_scraper", python_callable=run_scraper)
    t_backup = PythonOperator(task_id="write_raw_backup", python_callable=write_raw_backup)
    t_bronze = PythonOperator(task_id="bronze_ingest", python_callable=bronze_ingest)
    t_validate = PythonOperator(task_id="validate_quality", python_callable=validate_quality)

    t_health >> t_scrape >> t_backup >> t_bronze >> t_validate
