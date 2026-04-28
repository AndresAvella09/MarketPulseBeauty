"""
dag_04_google_trends.py
───────────────────────
Weekly DAG: Fetch Google Trends data → write to Bronze in MinIO.
"""

import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def load_keywords(**context):
    """Read keyword list from config/trends_keywords.json."""
    run_id = uuid.uuid4().hex[:12]
    context["ti"].xcom_push(key="run_id", value=run_id)
    start = datetime.now(timezone.utc)
    context["ti"].xcom_push(key="started_at", value=start.isoformat())

    config_path = "/opt/airflow/src/../config/trends_keywords.json"
    # Try mounted path first, fallback to DAG-relative
    for path in [config_path, "config/trends_keywords.json", "/opt/airflow/dags/../config/trends_keywords.json"]:
        try:
            with open(path) as f:
                config = json.load(f)
            break
        except FileNotFoundError:
            continue
    else:
        raise FileNotFoundError("Cannot find config/trends_keywords.json")

    keywords = config["keywords"]
    print(f"[load_keywords] run_id={run_id} | {len(keywords)} keywords loaded: {keywords}")
    context["ti"].xcom_push(key="config", value=config)


def fetch_trends(**context):
    """Fetch Google Trends data for each keyword with rate limiting."""
    from src.ingestion.fetch_google_trends import TrendsConfig, fetch_interest_over_time, interest_to_long_format

    run_id = context["ti"].xcom_pull(key="run_id")
    config = context["ti"].xcom_pull(key="config", task_ids="load_keywords")
    keywords = config["keywords"]

    all_frames = []
    # Process in batches of 5 (pytrends limit)
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i + batch_size]
        print(f"[fetch_trends] run_id={run_id} | fetching batch {i // batch_size + 1}: {batch}")

        cfg = TrendsConfig(
            keywords=batch,
            geo=config.get("geo", "CO"),
            timeframe=config.get("timeframe", "today 12-m"),
            hl=config.get("hl", "es-CO"),
            tz=config.get("tz", 300),
            category=config.get("category", 0),
            gprop=config.get("gprop", ""),
        )

        raw_df = fetch_interest_over_time(cfg)
        long_df = interest_to_long_format(
            raw_df, keywords=batch, geo=cfg.geo,
            timeframe=cfg.timeframe, category=cfg.category, gprop=cfg.gprop,
        )
        all_frames.append(long_df)

        # Rate limiting: minimum 5 seconds between requests
        if i + batch_size < len(keywords):
            print(f"[fetch_trends] sleeping 5s for rate limiting ...")
            time.sleep(5)

    combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    print(f"[fetch_trends] run_id={run_id} | total rows fetched: {len(combined)}")

    # Serialize to JSON for XCom
    context["ti"].xcom_push(key="trends_data", value=combined.to_dict(orient="records"))


def validate_response(**context):
    """Check that each keyword has data returned."""
    run_id = context["ti"].xcom_pull(key="run_id")
    config = context["ti"].xcom_pull(key="config", task_ids="load_keywords")
    data = context["ti"].xcom_pull(key="trends_data", task_ids="fetch_trends")

    df = pd.DataFrame(data) if data else pd.DataFrame()
    keywords = config["keywords"]

    if df.empty:
        print(f"[validate_response] run_id={run_id} | WARNING: no data returned")
        return

    present = set(df["keyword"].unique()) if "keyword" in df.columns else set()
    missing = set(keywords) - present

    for kw in keywords:
        if kw in present:
            count = len(df[df["keyword"] == kw])
            print(f"[validate_response] run_id={run_id} | {kw}: {count} rows")
        else:
            print(f"[validate_response] run_id={run_id} | {kw}: NO DATA")

    if missing:
        print(f"[validate_response] run_id={run_id} | missing keywords: {missing}")


def write_bronze(**context):
    """Write combined Parquet to MinIO marketpulse-bronze at trends/ingestion_date=YYYY-MM-DD/."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    run_id = context["ti"].xcom_pull(key="run_id")
    data = context["ti"].xcom_pull(key="trends_data", task_ids="fetch_trends")

    df = pd.DataFrame(data) if data else pd.DataFrame()
    if df.empty:
        print(f"[write_bronze] run_id={run_id} | no data to write")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = f"trends/ingestion_date={date_str}/trends_{run_id}.parquet"
    s3 = _get_s3()
    s3.put_object(Bucket="marketpulse-bronze", Key=key, Body=buf.getvalue())
    print(f"[write_bronze] run_id={run_id} | {len(df)} rows → s3://marketpulse-bronze/{key}")


def split_by_keyword(**context):
    """Write one Parquet file per keyword to MinIO."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    run_id = context["ti"].xcom_pull(key="run_id")
    started_at = context["ti"].xcom_pull(key="started_at")
    data = context["ti"].xcom_pull(key="trends_data", task_ids="fetch_trends")

    df = pd.DataFrame(data) if data else pd.DataFrame()
    if df.empty or "keyword" not in df.columns:
        print(f"[split_by_keyword] run_id={run_id} | no data to split")
        return

    s3 = _get_s3()
    total_rows = 0
    for keyword, group in df.groupby("keyword"):
        safe_name = keyword.replace(" ", "_").replace("/", "-")
        table = pa.Table.from_pandas(group)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        key = f"trends/by_keyword/{safe_name}/trends_{run_id}.parquet"
        s3.put_object(Bucket="marketpulse-bronze", Key=key, Body=buf.getvalue())
        total_rows += len(group)
        print(f"[split_by_keyword] run_id={run_id} | {keyword}: {len(group)} rows → {key}")

    # Log pipeline run
    from src.processing.gold_writer import log_pipeline_run
    from sqlalchemy import create_engine
    conn_str = os.getenv("POSTGRES_GOLD_CONN")
    if conn_str:
        engine = create_engine(conn_str)
        with engine.begin() as conn:
            log_pipeline_run(
                run_id=run_id, dag_name="dag_04_google_trends",
                status="success", rows_written=total_rows,
                started_at=datetime.fromisoformat(started_at),
                finished_at=datetime.now(timezone.utc),
                conn=conn,
            )


with DAG(
    dag_id="dag_04_google_trends",
    default_args=default_args,
    description="Weekly Google Trends fetch → Bronze",
    schedule="0 1 * * 1",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ingestion", "trends"],
) as dag:

    t_load = PythonOperator(task_id="load_keywords", python_callable=load_keywords)
    t_fetch = PythonOperator(task_id="fetch_trends", python_callable=fetch_trends)
    t_validate = PythonOperator(task_id="validate_response", python_callable=validate_response)
    t_write = PythonOperator(task_id="write_bronze", python_callable=write_bronze)
    t_split = PythonOperator(task_id="split_by_keyword", python_callable=split_by_keyword)

    t_load >> t_fetch >> t_validate >> t_write >> t_split
