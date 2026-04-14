"""
pipeline.py
───────────
Orchestrates the full DataOps pipeline end-to-end:

  1. Scrape  ->  SephoraScraper.run()          ->  (products dict, reviews list)
  2. Backup  ->  (optional) raw CSVs to ./data/raw/backups/
  3. Bronze  ->  bronze_ingestion.ingest()      ->  typed Parquet + quality report
  4. Silver  ->  silver_transform.transform()   ->  NLP-enriched Parquet
  5. Gold    ->  gold_transform.build_gold()    ->  4 analytics tables

Usage
─────
  python pipeline.py                         # full run, all layers
  python pipeline.py --no-backup             # skip CSV backup
  python pipeline.py --skip-silver-gold      # bronze only (fast debug)
  python pipeline.py --silver-only           # skip scrape; re-run silver+gold
  python pipeline.py --gold-only             # skip scrape+silver; re-run gold
  python pipeline.py --no-fail-on-quality    # warn instead of abort on bad data

Recovery (if scraper already ran and CSVs exist)
────────────────────────────────────────────────
  python pipeline.py --from-csv \
    --products data/raw/backups/sephora_products.csv \
    --reviews  data/raw/backups/sephora_reviews.csv
"""

import argparse
import csv
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import config as cfg
from .scraper import SephoraScraper
from .bronze_ingestion import ingest, load_products_csv, load_reviews_csv, _use_minio, _write_csv_to_minio
from .silver_transform import transform as silver_transform
from .gold_transform import build_gold


# ── CSV backup ─────────────────────────────────────────────────────────────────

def _write_csv(rows: list[dict], path: str):
    if not rows:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [backup] {len(rows):,} rows -> {path}")


# ── Full pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(
    backup=True, backup_dir="./data/raw/backups",
    bronze_dir="./data/raw/bronze",
    silver_dir="./data/processed/silver",
    gold_dir="./data/processed/gold",
    run_silver_gold=True, fail_on_quality=True,
):
    run_id = uuid.uuid4().hex[:12]
    ts     = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Sephora Pipeline  |  run_id={run_id}")
    print(f"  Started {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}\n")

    result = {"run_id": run_id}

    # Step 1: Scrape
    print("== Step 1/5  SCRAPE ==")
    products_dict, reviews_list = SephoraScraper().run()
    if not products_dict and not reviews_list:
        print("[!] Scraper returned no data -- pipeline aborted.")
        return {}
    print(f"\n  Scraped: {len(products_dict):,} products | {len(reviews_list):,} reviews")

    # Step 2: Backup
    print("\n== Step 2/5  BACKUP ==")
    if backup:
        date_str  = ts.strftime("%Y-%m-%d")
        if _use_minio():
            prod_key = f"backups/{date_str}/sephora_products_{run_id}.csv"
            rev_key  = f"backups/{date_str}/sephora_reviews_{run_id}.csv"
            _write_csv_to_minio(list(products_dict.values()), "marketpulse-raw", prod_key)
            _write_csv_to_minio(reviews_list, "marketpulse-raw", rev_key)
            result["backup"] = {
                "products": f"s3://marketpulse-raw/{prod_key}",
                "reviews":  f"s3://marketpulse-raw/{rev_key}",
            }
        else:
            prod_path = f"{backup_dir}/{date_str}/sephora_products_{run_id}.csv"
            rev_path  = f"{backup_dir}/{date_str}/sephora_reviews_{run_id}.csv"
            _write_csv(list(products_dict.values()), prod_path)
            _write_csv(reviews_list, rev_path)
            result["backup"] = {"products": prod_path, "reviews": rev_path}
    else:
        print("  [skip] --no-backup flag set")

    # Step 3: Bronze
    print("\n== Step 3/5  BRONZE ==")
    result["bronze"] = ingest(
        products=list(products_dict.values()), reviews=reviews_list,
        bronze_dir=bronze_dir, run_id=run_id,
        source="scraper", fail_on_quality=fail_on_quality,
    )

    if not run_silver_gold:
        print("\n  [skip] --skip-silver-gold flag -- stopping after Bronze.")
        _print_summary(run_id, result)
        return result

    # Step 4: Silver
    print("\n== Step 4/5  SILVER ==")
    result["silver"] = silver_transform(bronze_dir=bronze_dir, silver_dir=silver_dir)

    # Step 5: Gold
    print("\n== Step 5/5  GOLD ==")
    result["gold"] = build_gold(silver_dir=silver_dir, gold_dir=gold_dir)

    _print_summary(run_id, result)
    return result


# ── Recovery paths ─────────────────────────────────────────────────────────────

def run_from_csv(
    products_csv, reviews_csv,
    bronze_dir="./data/raw/bronze",
    silver_dir="./data/processed/silver",
    gold_dir="./data/processed/gold",
    run_silver_gold=True, fail_on_quality=True,
):
    """Re-ingest from existing CSVs without re-scraping."""
    run_id = uuid.uuid4().hex[:12]
    ts     = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Recovery Ingest  |  run_id={run_id}")
    print(f"{'='*60}\n")

    products = load_products_csv(products_csv, run_id, ts) if products_csv else []
    reviews  = load_reviews_csv(reviews_csv,   run_id, ts) if reviews_csv  else []

    result = {"run_id": run_id}
    result["bronze"] = ingest(
        products=products, reviews=reviews,
        bronze_dir=bronze_dir, run_id=run_id,
        source="csv_recovery", fail_on_quality=fail_on_quality,
    )

    if run_silver_gold:
        result["silver"] = silver_transform(bronze_dir=bronze_dir, silver_dir=silver_dir)
        result["gold"]   = build_gold(silver_dir=silver_dir, gold_dir=gold_dir)

    _print_summary(run_id, result)
    return result


def run_silver_gold_only(
    bronze_dir="./data/raw/bronze",
    silver_dir="./data/processed/silver",
    gold_dir="./data/processed/gold",
):
    result = {}
    result["silver"] = silver_transform(bronze_dir=bronze_dir, silver_dir=silver_dir)
    result["gold"]   = build_gold(silver_dir=silver_dir, gold_dir=gold_dir)
    return result


def run_gold_only(
    silver_dir="./data/processed/silver",
    gold_dir="./data/processed/gold",
):
    return build_gold(silver_dir=silver_dir, gold_dir=gold_dir)


# ── Summary ────────────────────────────────────────────────────────────────────

def _print_summary(run_id, result):
    print(f"\n{'='*60}")
    print(f"  Pipeline complete  |  run_id={run_id}")
    print(f"\n  Outputs:")
    for layer, paths in result.items():
        if layer == "run_id":
            continue
        if isinstance(paths, dict):
            for k, v in paths.items():
                for p in (v if isinstance(v, list) else [v]):
                    print(f"    [{layer}/{k}]  {p}")
        elif isinstance(paths, list):
            for p in paths:
                print(f"    [{layer}]  {p}")
        elif isinstance(paths, str):
            print(f"    [{layer}]  {paths}")
    print(f"{'='*60}\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sephora end-to-end: scrape -> bronze -> silver -> gold."
    )
    parser.add_argument("--from-csv",         action="store_true")
    parser.add_argument("--silver-only",      action="store_true")
    parser.add_argument("--gold-only",        action="store_true")
    parser.add_argument("--skip-silver-gold", action="store_true")
    parser.add_argument("--products",   default=None)
    parser.add_argument("--reviews",    default=None)
    parser.add_argument("--backup-dir", default="./data/raw/backups")
    parser.add_argument("--bronze-dir", default="./data/raw/bronze")
    parser.add_argument("--silver-dir", default="./data/processed/silver")
    parser.add_argument("--gold-dir",   default="./data/processed/gold")
    parser.add_argument("--no-backup",           action="store_true")
    parser.add_argument("--no-fail-on-quality",  action="store_true")
    args = parser.parse_args()
    fail = not args.no_fail_on_quality

    if args.gold_only:
        result = run_gold_only(silver_dir=args.silver_dir, gold_dir=args.gold_dir)
    elif args.silver_only:
        result = run_silver_gold_only(
            bronze_dir=args.bronze_dir, silver_dir=args.silver_dir, gold_dir=args.gold_dir)
    elif args.from_csv:
        result = run_from_csv(
            products_csv=args.products, reviews_csv=args.reviews,
            bronze_dir=args.bronze_dir, silver_dir=args.silver_dir, gold_dir=args.gold_dir,
            run_silver_gold=not args.skip_silver_gold, fail_on_quality=fail,
        )
    else:
        result = run_pipeline(
            backup=not args.no_backup, backup_dir=args.backup_dir,
            bronze_dir=args.bronze_dir, silver_dir=args.silver_dir, gold_dir=args.gold_dir,
            run_silver_gold=not args.skip_silver_gold, fail_on_quality=fail,
        )

    sys.exit(0 if result else 1)
