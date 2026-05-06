"""
dag_03_gold_build.py
────────────────────
Daily DAG: Silver → Gold (insights + embeddings) → PostgreSQL + Parquet.

Tasks
─────
  read_silver_and_embed         → load model, read silver, embed reviews + products
  build_reviews_slim            → sentiment + slim reviews → parquet (topics null)
  cluster_topics                → BERTopic over cached embeddings, patch reviews parquet
  build_products_monthly_themes → products_slim + monthly + themes parquets
  upsert_postgres               → upsert all 4 gold tables to PostgreSQL

Each task is a fresh worker process so GPU/RAM is freed between stages
(BGE-M3 + sentiment on 190k+ reviews used to OOM the single-task DAG).
"""

import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "marketpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


SILVER_DIR = "/opt/airflow/data/processed/silver"
GOLD_DIR = "/tmp/gold"  # ephemeral handoff between tasks; PG is the real sink


def _xcom_ids(context) -> tuple[str, date]:
    gold_run_id = context["ti"].xcom_pull(key="gold_run_id", task_ids="read_silver_and_embed")
    revision_str = context["ti"].xcom_pull(key="revision_date", task_ids="read_silver_and_embed")
    return gold_run_id, date.fromisoformat(revision_str)


def read_silver_and_embed(**context):
    from src.ingestion.scraper.gold_transform import stage_embed

    gold_run_id = uuid.uuid4().hex[:12]
    revision_dt = date.today()
    context["ti"].xcom_push(key="gold_run_id", value=gold_run_id)
    context["ti"].xcom_push(key="revision_date", value=revision_dt.isoformat())
    context["ti"].xcom_push(key="started_at", value=datetime.now(timezone.utc).isoformat())

    stage_embed(
        silver_dir=SILVER_DIR,
        gold_run_id=gold_run_id,
        revision_dt=revision_dt,
        model_name=os.getenv("GOLD_EMBED_MODEL", "BAAI/bge-m3"),
        batch_size=int(os.getenv("GOLD_BATCH_SIZE", "256")),
        gold_dir=GOLD_DIR,
    )


def build_reviews_slim(**context):
    from src.ingestion.scraper.gold_transform import stage_reviews_slim

    gold_run_id, revision_dt = _xcom_ids(context)
    stage_reviews_slim(
        silver_dir=SILVER_DIR,
        gold_run_id=gold_run_id,
        revision_dt=revision_dt,
        gold_dir=GOLD_DIR,
    )


def cluster_topics_task(**context):
    from src.ingestion.scraper.gold_transform import stage_topics

    gold_run_id, revision_dt = _xcom_ids(context)
    stage_topics(
        gold_run_id=gold_run_id,
        revision_dt=revision_dt,
        gold_dir=GOLD_DIR,
    )


def build_products_monthly_themes(**context):
    from src.ingestion.scraper.gold_transform import stage_aggregates

    gold_run_id, revision_dt = _xcom_ids(context)
    written = stage_aggregates(
        silver_dir=SILVER_DIR,
        gold_run_id=gold_run_id,
        revision_dt=revision_dt,
        gold_dir=GOLD_DIR,
    )
    print(f"[build_products_monthly_themes] outputs: {list(written.keys())}")


def upsert_postgres(**context):
    from src.ingestion.scraper.gold_transform import stage_postgres

    gold_run_id, revision_dt = _xcom_ids(context)
    rows = stage_postgres(
        gold_run_id=gold_run_id,
        revision_dt=revision_dt,
        gold_dir=GOLD_DIR,
    )
    print(f"[upsert_postgres] rows upserted: {rows}")


with DAG(
    dag_id="dag_03_gold_build",
    default_args=default_args,
    description="Daily Silver → Gold (embeddings + insights) → PostgreSQL + Parquet",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transform", "gold"],
) as dag:

    t_embed = PythonOperator(task_id="read_silver_and_embed", python_callable=read_silver_and_embed)
    t_reviews = PythonOperator(task_id="build_reviews_slim", python_callable=build_reviews_slim)
    t_topics = PythonOperator(task_id="cluster_topics", python_callable=cluster_topics_task)
    t_aggs = PythonOperator(task_id="build_products_monthly_themes", python_callable=build_products_monthly_themes)
    t_pg = PythonOperator(task_id="upsert_postgres", python_callable=upsert_postgres)

    t_embed >> t_reviews >> t_topics >> t_aggs >> t_pg
