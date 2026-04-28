"""
dag_03_gold_build.py
────────────────────
Daily DAG: Silver → Gold (silver + embeddings) → PostgreSQL + Parquet.
Depends on DAG 02 completing successfully.

Calls build_gold() from gold_transform.py which:
  1. Reads silver products/reviews from MinIO
  2. Generates embeddings (sentence-transformers or spaCy fallback)
  3. Fills nulls so gold has no null values
  4. Writes gold parquet
  5. Upserts full gold.products and gold.reviews to PostgreSQL
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "marketpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_gold_build(**context):
    """Run the full gold build: silver + embeddings → parquet + PostgreSQL."""
    from src.ingestion.scraper.gold_transform import build_gold

    # Gold parquet written to /tmp (ephemeral); the real output is PostgreSQL.
    result = build_gold(
        silver_dir="/opt/airflow/data/processed/silver",
        gold_dir="/tmp/gold",
        model_name=os.getenv("GOLD_EMBED_MODEL", "BAAI/bge-m3"),
        batch_size=int(os.getenv("GOLD_BATCH_SIZE", "256")),
    )

    if not result:
        raise RuntimeError("[run_gold_build] build_gold() returned empty — no silver data?")

    print(f"[run_gold_build] Gold build complete: {result}")


with DAG(
    dag_id="dag_03_gold_build",
    default_args=default_args,
    description="Daily Silver → Gold (embeddings) → PostgreSQL + Parquet",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transform", "gold"],
) as dag:

    t_gold = PythonOperator(
        task_id="run_gold_build",
        python_callable=run_gold_build,
    )
