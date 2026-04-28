"""
silver_transform.py
───────────────────
Bronze → Silver transformation layer.

What it does
────────────
  * Reads all Parquet files from bronze/ (products + reviews)
  * Applies the full NLP pipeline via cleaning.py to text fields:
      - ReviewText  ->  ReviewText_clean, ReviewText_tokens, ReviewText_lemmas,
                       ReviewText_wordcount
      - Title       ->  Title_clean,      Title_tokens,      Title_lemmas
      - ProductName ->  ProductName_clean, ProductName_tokens, ProductName_lemmas
  * Adds revision_date and _silver_run_id audit columns
  * Enforces SILVER_PRODUCTS_SCHEMA / SILVER_REVIEWS_SCHEMA from schema.py
  * Writes partitioned Parquet to silver/

Output layout
─────────────
  data/processed/silver/
    products/
      revision_date=YYYY-MM-DD/
        products_<silver_run_id>.parquet
    reviews/
      revision_date=YYYY-MM-DD/
        category_id=<cat>/
          reviews_<silver_run_id>.parquet

Usage
─────
  python silver_transform.py
  python silver_transform.py --bronze ./data/raw/bronze --silver ./data/processed/silver
  python silver_transform.py --bronze-date 2025-06-01   # process one date only
"""

import argparse
import io
import os
import re
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .cleaning import load_nlp, clean_batch
from .schema import SILVER_PRODUCTS_SCHEMA, SILVER_REVIEWS_SCHEMA

# ── MinIO toggle ──────────────────────────────────────────────────────────────

def _use_minio() -> bool:
    """Read USE_MINIO at call time so the env var is always current."""
    return os.getenv("USE_MINIO", "false").lower() in ("true", "1", "yes")


def _get_s3_client():
    """Create a boto3 S3 client pointing at MinIO."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    )


def _read_parquet_from_minio(bucket: str, prefix: str, latest_only: bool = False) -> pa.Table | None:
    """Read Parquet files under a prefix in MinIO and return as a table.

    If *latest_only* is True, only read the most recently modified file
    from the most recent date partition (``ingestion_date=`` or
    ``revision_date=``, Hive-style).
    """
    s3 = _get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    parquet_objs = [obj for obj in contents if obj["Key"].endswith(".parquet")]

    if not parquet_objs:
        return None

    if latest_only:
        date_re = re.compile(r"(?:ingestion_date|revision_date)=(\d{4}-\d{2}-\d{2})")
        dated_objs = {}
        for obj in parquet_objs:
            m = date_re.search(obj["Key"])
            if m:
                dated_objs.setdefault(m.group(1), []).append(obj)
        if dated_objs:
            latest_date = max(dated_objs.keys())
            partition_objs = dated_objs[latest_date]
            newest = max(partition_objs, key=lambda o: o["LastModified"])
            parquet_objs = [newest]
            print(f"  [minio] latest partition: {latest_date}  file={newest['Key']}")

    tables = []
    for obj in parquet_objs:
        resp = s3.get_object(Bucket=bucket, Key=obj["Key"])
        buf = io.BytesIO(resp["Body"].read())
        tables.append(pq.read_table(buf))

    return pa.concat_tables(tables) if tables else None


def _write_parquet_to_minio(table: pa.Table, bucket: str, key: str):
    """Write a PyArrow table as Parquet bytes to a MinIO bucket."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", write_statistics=True)
    buf.seek(0)
    s3 = _get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  [+] s3://{bucket}/{key}")

# ── Data contracts (mandatory preliminary gate) ────────────────────────────────
try:
    from src.processing.data_contracts import enforce_contracts, ContractViolationError
    _CONTRACTS_AVAILABLE = True
except ImportError:
    # Graceful fallback if module path differs
    import warnings
    warnings.warn(
        "data_contracts module not found at src/processing/data_contracts.py. "
        "Contract validation will be SKIPPED. Add the module to enable the gate.",
        stacklevel=2,
    )
    _CONTRACTS_AVAILABLE = False


# ── GPU setup ──────────────────────────────────────────────────────────────────

def _setup_gpu() -> bool:
    """
    Attempt to enable GPU for spaCy via cupy.
    Returns True if a GPU was successfully activated, False otherwise.
    """
    try:
        import spacy
        used = spacy.prefer_gpu()
        if used:
            import cupy
            dev = cupy.cuda.Device(0)
            props = cupy.cuda.runtime.getDeviceProperties(dev.id)
            name = props["name"].decode() if isinstance(props["name"], bytes) else props["name"]
            print(f"  [GPU] spaCy running on: {name}")
        else:
            print("  [GPU] No CUDA-capable GPU found -- falling back to CPU.")
        return used
    except ImportError:
        print("  [GPU] cupy not installed -- running on CPU.")
        print("        To enable GPU: pip install cupy-cuda11x  (or cupy-cuda12x)")
        return False

_GPU_AVAILABLE: bool = False  # set once in transform()


# ── Bronze readers ─────────────────────────────────────────────────────────────

_BRONZE_PRODUCTS_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("ingestion_date", pa.string())]),
    flavor="hive",
)
_BRONZE_REVIEWS_PARTITIONING = ds.partitioning(
    pa.schema([
        pa.field("ingestion_date", pa.string()),
        pa.field("category_id",    pa.string()),
    ]),
    flavor="hive",
)

def _read_bronze(bronze_dir: str, table_name: str, date_filter: str | None) -> pa.Table:
    """
    Read all Parquet files under bronze/<table_name>/ into a single table.
    Optionally filter to a single ingestion_date partition.
    Supports both local filesystem and MinIO.
    """
    if _use_minio():
        prefix = f"{table_name}/"
        if date_filter:
            prefix = f"{table_name}/ingestion_date={date_filter}/"
        table = _read_parquet_from_minio("marketpulse-bronze", prefix, latest_only=True)
        if table is None:
            print(f"  [!] No Bronze data found in MinIO for {table_name}")
            return None
        print(f"  [bronze/{table_name}] {len(table):,} rows loaded (MinIO)")
        return table

    base = Path(bronze_dir) / table_name
    if not base.exists():
        print(f"  [!] Bronze path not found: {base}")
        return None

    partitioning = (
        _BRONZE_PRODUCTS_PARTITIONING
        if table_name == "products"
        else _BRONZE_REVIEWS_PARTITIONING
    )
    dataset = ds.dataset(str(base), format="parquet", partitioning=partitioning)

    if date_filter:
        filt  = ds.field("ingestion_date") == date_filter
        # Filter to the date partition, then pick only the newest file
        fragments = [
            f for f in dataset.get_fragments(filter=filt)
        ]
        if fragments:
            newest_frag = max(fragments, key=lambda f: os.path.getmtime(f.path))
            table = newest_frag.to_table()
            print(f"  [bronze/{table_name}] latest file: {Path(newest_frag.path).name}")
        else:
            table = dataset.to_table(filter=filt)
    else:
        # No date filter: find latest partition, then newest file
        fragments = list(dataset.get_fragments())
        if fragments:
            latest_date = None
            for frag in fragments:
                m = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", str(frag.path))
                if m:
                    d = m.group(1)
                    if latest_date is None or d > latest_date:
                        latest_date = d
            if latest_date:
                partition_frags = [
                    f for f in fragments
                    if f"ingestion_date={latest_date}" in str(f.path)
                ]
                newest_frag = max(partition_frags, key=lambda f: os.path.getmtime(f.path))
                table = newest_frag.to_table()
                print(f"  [bronze/{table_name}] latest partition: ingestion_date={latest_date}  file={Path(newest_frag.path).name}")
            else:
                table = dataset.to_table()
        else:
            table = dataset.to_table()

    # Drop partition-injected columns to avoid duplicates with Parquet file cols
    drop = [c for c in ("ingestion_date", "category_id")
            if table.schema.get_field_index(c) != -1]
    if drop:
        table = table.drop(drop)

    print(f"  [bronze/{table_name}] {len(table):,} rows loaded")
    return table


# ── Deduplication ─────────────────────────────────────────────────────────────

def _dedup_reviews(table: pa.Table) -> pa.Table:
    """
    Deduplicate reviews by ReviewID, keeping the row with the latest
    _ingestion_ts.  Nulls in ReviewID are preserved as-is.
    """
    import pyarrow.compute as pc

    n_before = len(table)
    if "ReviewID" not in table.schema.names or "_ingestion_ts" not in table.schema.names:
        return table

    row_idx = pa.array(range(n_before), pa.int64())
    table   = table.append_column("_row_idx", row_idx)

    review_ids = table["ReviewID"].to_pylist()
    ing_ts     = table["_ingestion_ts"].to_pylist()

    best: dict[str, int] = {}
    for i, (rid, ts) in enumerate(zip(review_ids, ing_ts)):
        if rid is None:
            continue
        if rid not in best or (ts is not None and ts > ing_ts[best[rid]]):
            best[rid] = i

    null_indices = [i for i, rid in enumerate(review_ids) if rid is None]
    keep_indices = sorted(set(best.values()) | set(null_indices))

    kept  = table.take(keep_indices).remove_column(table.schema.get_field_index("_row_idx"))
    n_after = len(kept)

    if n_before != n_after:
        print(f"  [dedup] {n_before:,} -> {n_after:,} rows  ({n_before - n_after:,} duplicates removed)")
    else:
        print(f"  [dedup] No duplicates found ({n_after:,} rows)")

    return kept


# ── NLP enrichment ─────────────────────────────────────────────────────────────

def _enrich_reviews(nlp, table: pa.Table) -> pa.Table:
    """Add NLP columns to the reviews table."""
    n = len(table)
    revision_dt = date.today()

    batch_size = 512 if _GPU_AVAILABLE else 64

    review_texts = table["ReviewText"].to_pylist()
    title_texts  = table["Title"].to_pylist()

    print(f"    cleaning ReviewText ({n:,} rows) ... [batch_size={batch_size}]")
    rt_results = clean_batch(nlp, review_texts, batch_size=batch_size)

    print(f"    cleaning Title ({n:,} rows) ... [batch_size={batch_size}]")
    ti_results = clean_batch(nlp, title_texts, batch_size=batch_size)

    extras = {
        "ReviewText_clean":     pa.array([r.clean     for r in rt_results], pa.string()),
        "ReviewText_tokens":    pa.array([r.tokens    for r in rt_results], pa.string()),
        "ReviewText_lemmas":    pa.array([r.lemmas    for r in rt_results], pa.string()),
        "ReviewText_wordcount": pa.array([r.wordcount for r in rt_results], pa.int32()),
        "Title_clean":          pa.array([r.clean     for r in ti_results], pa.string()),
        "Title_tokens":         pa.array([r.tokens    for r in ti_results], pa.string()),
        "Title_lemmas":         pa.array([r.lemmas    for r in ti_results], pa.string()),
        "revision_date":        pa.array([revision_dt] * n,                pa.date32()),
        "_silver_run_id":       pa.array([_SILVER_RUN_ID] * n,             pa.string()),
    }

    combined = {col: table[col] for col in table.schema.names}
    combined.update(extras)

    ordered = {
        f.name: combined[f.name]
        for f in SILVER_REVIEWS_SCHEMA
        if f.name in combined
    }

    return pa.table(ordered, schema=SILVER_REVIEWS_SCHEMA)


def _enrich_products(nlp, table: pa.Table) -> pa.Table:
    n = len(table)
    revision_dt = date.today()

    batch_size = 512 if _GPU_AVAILABLE else 64

    name_texts = table["ProductName"].to_pylist()
    urls = table["ProductPageUrl"].to_pylist()

    print(f"    cleaning ProductName ({n:,} rows) ... [batch_size={batch_size}]")
    pn_results = clean_batch(nlp, name_texts, batch_size=batch_size)

    # Tag the product category
    target_keywords = ["hyaluronic", "niacinamide", "sunscreen"]
    categories = []
    for url in urls:
        url_lower = str(url).lower() if url else ""
        found_cat = next((kw for kw in target_keywords if kw in url_lower), "other")
        categories.append(found_cat)

    extras = {
        "ProductCategory":    pa.array(categories, pa.string()),
        "ProductName_clean":  pa.array([r.clean  for r in pn_results], pa.string()),
        "ProductName_tokens": pa.array([r.tokens for r in pn_results], pa.string()),
        "ProductName_lemmas": pa.array([r.lemmas for r in pn_results], pa.string()),
        "revision_date":      pa.array([revision_dt] * n,              pa.date32()),
        "_silver_run_id":     pa.array([_SILVER_RUN_ID] * n,           pa.string()),
    }

    combined = {col: table[col] for col in table.schema.names}
    combined.update(extras)

    ordered = {
        f.name: combined[f.name]
        for f in SILVER_PRODUCTS_SCHEMA
        if f.name in combined
    }

    return pa.table(ordered, schema=SILVER_PRODUCTS_SCHEMA)


# ── Writers ────────────────────────────────────────────────────────────────────

def _write_silver_products(table: pa.Table, silver_dir: str) -> str:
    date_str = str(date.today())
    key = f"products/revision_date={date_str}/products_{_SILVER_RUN_ID}.parquet"

    if _use_minio():
        _write_parquet_to_minio(table, "marketpulse-silver", key)
        print(f"  [+] silver/products ({len(table):,} rows) -> MinIO")
        return f"s3://marketpulse-silver/{key}"

    dest = Path(silver_dir) / "products" / f"revision_date={date_str}"
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"products_{_SILVER_RUN_ID}.parquet"
    pq.write_table(table, out, compression="snappy", write_statistics=True)
    print(f"  [+] silver/products -> {out}  ({len(table):,} rows)")
    return str(out)


def _write_silver_reviews(table: pa.Table, silver_dir: str) -> list[str]:
    date_str = str(date.today())
    written  = []

    if _use_minio():
        key = f"reviews/revision_date={date_str}/reviews_{_SILVER_RUN_ID}.parquet"
        _write_parquet_to_minio(table, "marketpulse-silver", key)
        print(f"  [+] silver/reviews ({len(table):,} rows) -> MinIO")
        written.append(f"s3://marketpulse-silver/{key}")
        return written

    if "CategoryId" in table.schema.names:
        cats = {c or "_unknown" for c in table["CategoryId"].to_pylist()}
        for cat in cats:
            mask = pc.equal(
                pc.if_else(pc.is_null(table["CategoryId"]), "_unknown", table["CategoryId"]),
                cat,
            )
            subset = table.filter(mask)
            dest = (Path(silver_dir) / "reviews"
                    / f"revision_date={date_str}"
                    / f"category_id={cat}")
            dest.mkdir(parents=True, exist_ok=True)
            out = dest / f"reviews_{_SILVER_RUN_ID}.parquet"
            pq.write_table(subset, out, compression="snappy", write_statistics=True)
            print(f"  [+] silver/reviews [{cat}] -> {out}  ({len(subset):,} rows)")
            written.append(str(out))
    else:
        dest = (Path(silver_dir) / "reviews" / f"revision_date={date_str}")
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / f"reviews_{_SILVER_RUN_ID}.parquet"
        pq.write_table(table, out, compression="snappy", write_statistics=True)
        print(f"  [+] silver/reviews -> {out}  ({len(table):,} rows)")
        written.append(str(out))

    return written


# ── Orchestrator ───────────────────────────────────────────────────────────────

_SILVER_RUN_ID: str = ""   # set at the start of transform()


def transform(
    bronze_dir:  str = "./data/raw/bronze",
    silver_dir:  str = "./data/processed/silver",
    bronze_date: str | None = None,
    fail_on_contract_violation: bool = True,
) -> dict:
    """
    Run the Bronze -> Silver transformation.

    Parameters
    ----------
    bronze_dir                 : root of the bronze lake
    silver_dir                 : root of the silver lake
    bronze_date                : optional YYYY-MM-DD to process one ingestion_date partition
    fail_on_contract_violation : if True (default), abort when critical contracts fail

    Returns
    -------
    dict with keys: products (path), reviews (list of paths)
    """
    global _SILVER_RUN_ID, _GPU_AVAILABLE
    _SILVER_RUN_ID = uuid.uuid4().hex[:12]

    print(f"\n{'='*60}")
    print(f"  Silver Transform  |  silver_run_id={_SILVER_RUN_ID}")
    print(f"{'='*60}\n")

    print("[0/4] Detecting GPU ...")
    _GPU_AVAILABLE = _setup_gpu()

    print("[1/4] Loading spaCy model ...")
    nlp = load_nlp()

    written = {}

    # ── Step 2: Data Contract Gate (mandatory preliminary step) ────────────────
    print("\n[2/4] Running data contract gate ...")
    _pre_reviews = _read_bronze(bronze_dir, "reviews", bronze_date)

    if _CONTRACTS_AVAILABLE and _pre_reviews is not None:
        if _use_minio():
            import tempfile
            report_path = str(Path(tempfile.gettempdir()) / f"quality_report_{_SILVER_RUN_ID}.json")
        else:
            report_path = str(Path(silver_dir).parent / "quality_report.json")
        try:
            enforce_contracts(
                reviews=_pre_reviews,
                report_path=report_path,
                run_id=_SILVER_RUN_ID,
            )
            print("[contract] All critical contracts passed -- proceeding to NLP.")
        except ContractViolationError as exc:
            if fail_on_contract_violation:
                print(f"\n[contract] Pipeline aborted by contract violation.")
                raise
            print(f"[contract] Violations detected (fail_on_contract_violation=False), continuing ...\n{exc}")
    elif not _CONTRACTS_AVAILABLE:
        print("[contract] data_contracts module not available -- gate skipped.")
    else:
        print("[contract] No review data found -- gate skipped.")

    # ── Products ───────────────────────────────────────────────────────────────
    print("\n[3/4] Processing products ...")
    prod_table = _read_bronze(bronze_dir, "products", bronze_date)
    if prod_table is not None and len(prod_table) > 0:
        enriched = _enrich_products(nlp, prod_table)
        written["products"] = _write_silver_products(enriched, silver_dir)
    else:
        print("  [skip] no product rows found in bronze")

    # ── Reviews ────────────────────────────────────────────────────────────────
    print("\n[4/4] Processing reviews ...")
    rev_table = _pre_reviews if _pre_reviews is not None else _read_bronze(bronze_dir, "reviews", bronze_date)
    if rev_table is not None and len(rev_table) > 0:
        print(f"  [dedup] Deduplicating {len(rev_table):,} Bronze reviews by ReviewID ...")
        rev_table = _dedup_reviews(rev_table)
        enriched = _enrich_reviews(nlp, rev_table)
        written["reviews"] = _write_silver_reviews(enriched, silver_dir)
    else:
        print("  [skip] no review rows found in bronze")

    print(f"\n{'='*60}")
    print(f"  Silver complete  |  silver_run_id={_SILVER_RUN_ID}\n")
    return written


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze -> Silver NLP transform.")
    parser.add_argument("--bronze",      default="./data/raw/bronze")
    parser.add_argument("--silver",      default="./data/processed/silver")
    parser.add_argument("--bronze-date", default=None,
                        help="Process one ingestion_date only (YYYY-MM-DD)")
    args = parser.parse_args()

    result = transform(
        bronze_dir  = args.bronze,
        silver_dir  = args.silver,
        bronze_date = args.bronze_date,
    )
    sys.exit(0 if result else 1)
