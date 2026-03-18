"""
silver_transform.py
───────────────────
Bronze → Silver transformation layer.

What it does
────────────
  • Reads all Parquet files from bronze/ (products + reviews)
  • Applies the full NLP pipeline via cleaning.py to text fields:
      – ReviewText  →  ReviewText_clean, ReviewText_tokens, ReviewText_lemmas,
                       ReviewText_wordcount
      – Title       →  Title_clean,      Title_tokens,      Title_lemmas
      – ProductName →  ProductName_clean, ProductName_tokens, ProductName_lemmas
  • Adds revision_date and _silver_run_id audit columns
  • Enforces SILVER_PRODUCTS_SCHEMA / SILVER_REVIEWS_SCHEMA from schema.py
  • Writes partitioned Parquet to silver/

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
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from cleaning import load_nlp, clean_batch
from schema import SILVER_PRODUCTS_SCHEMA, SILVER_REVIEWS_SCHEMA


# ── GPU setup ──────────────────────────────────────────────────────────────────

def _setup_gpu() -> bool:
    """
    Attempt to enable GPU for spaCy via cupy.
    Returns True if a GPU was successfully activated, False otherwise.

    Requirements (install ONE of these depending on your CUDA version):
      CUDA 11.x:  pip install cupy-cuda11x
      CUDA 12.x:  pip install cupy-cuda12x
    """
    try:
        import spacy
        used = spacy.prefer_gpu()       # returns True if GPU was activated
        if used:
            import cupy
            dev = cupy.cuda.Device(0)
            props = cupy.cuda.runtime.getDeviceProperties(dev.id)
            name = props["name"].decode() if isinstance(props["name"], bytes) else props["name"]
            print(f"  [GPU] spaCy running on: {name}")
        else:
            print("  [GPU] No CUDA-capable GPU found — falling back to CPU.")
        return used
    except ImportError:
        print("  [GPU] cupy not installed — running on CPU.")
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
    """
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
        table = dataset.to_table(filter=filt)
    else:
        table = dataset.to_table()

    # Drop partition-injected columns to avoid duplicates with Parquet file cols
    drop = [c for c in ("ingestion_date", "category_id")
            if table.schema.get_field_index(c) != -1]
    if drop:
        table = table.drop(drop)

    print(f"  [bronze/{table_name}] {len(table):,} rows loaded")
    return table


# ── NLP enrichment ─────────────────────────────────────────────────────────────

def _enrich_reviews(nlp, table: pa.Table) -> pa.Table:
    """
    Add NLP columns to the reviews table.
    Returns a new PyArrow table with all Silver review columns.
    """
    n = len(table)
    revision_dt = date.today()

    # GPU processes larger batches efficiently; CPU benefits from smaller ones
    # to avoid memory pressure. Tune NLP_BATCH_SIZE in your cleaning.py if needed.
    batch_size = 512 if _GPU_AVAILABLE else 64

    review_texts = table["ReviewText"].to_pylist()
    title_texts  = table["Title"].to_pylist()

    print(f"    cleaning ReviewText ({n:,} rows) … [batch_size={batch_size}]")
    rt_results = clean_batch(nlp, review_texts, batch_size=batch_size)

    print(f"    cleaning Title ({n:,} rows) … [batch_size={batch_size}]")
    ti_results = clean_batch(nlp, title_texts, batch_size=batch_size)

    # Build extra columns
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

    # Merge: original columns + NLP columns, cast to Silver schema
    combined = {col: table[col] for col in table.schema.names}
    combined.update(extras)

    # Select and reorder to match SILVER_REVIEWS_SCHEMA
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

    print(f"    cleaning ProductName ({n:,} rows) … [batch_size={batch_size}]")
    pn_results = clean_batch(nlp, name_texts, batch_size=batch_size)

    # NEW: Tag the product category
    target_keywords = ["hyaluronic", "niacinamide", "sunscreen"]
    categories = []
    for url in urls:
        url_lower = str(url).lower() if url else ""
        # Find the first matching keyword, otherwise label as 'other'
        found_cat = next((kw for kw in target_keywords if kw in url_lower), "other")
        categories.append(found_cat)

    extras = {
        "ProductCategory":    pa.array(categories, pa.string()), # NEW COLUMN
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
    dest = Path(silver_dir) / "products" / f"revision_date={date_str}"
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"products_{_SILVER_RUN_ID}.parquet"
    pq.write_table(table, out, compression="snappy", write_statistics=True)
    print(f"  [+] silver/products → {out}  ({len(table):,} rows)")
    return str(out)


def _write_silver_reviews(table: pa.Table, silver_dir: str) -> list[str]:
    date_str = str(date.today())
    written  = []

    # Partition by CategoryId if present via product join
    # (reviews table from bronze doesn't carry CategoryId —
    #  if you run after joining, it will be available)
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
            print(f"  [+] silver/reviews [{cat}] → {out}  ({len(subset):,} rows)")
            written.append(str(out))
    else:
        dest = (Path(silver_dir) / "reviews" / f"revision_date={date_str}")
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / f"reviews_{_SILVER_RUN_ID}.parquet"
        pq.write_table(table, out, compression="snappy", write_statistics=True)
        print(f"  [+] silver/reviews → {out}  ({len(table):,} rows)")
        written.append(str(out))

    return written


# ── Orchestrator ───────────────────────────────────────────────────────────────

_SILVER_RUN_ID: str = ""   # set at the start of transform()


def transform(
    bronze_dir:  str = "./data/raw/bronze",
    silver_dir:  str = "./data/processed/silver",
    bronze_date: str | None = None,
) -> dict:
    """
    Run the Bronze → Silver transformation.

    Parameters
    ----------
    bronze_dir  : root of the bronze lake
    silver_dir  : root of the silver lake
    bronze_date : optional YYYY-MM-DD to process a single ingestion_date partition

    Returns
    -------
    dict with keys: products (path), reviews (list of paths)
    """
    global _SILVER_RUN_ID, _GPU_AVAILABLE
    _SILVER_RUN_ID = uuid.uuid4().hex[:12]

    print(f"\n{'─'*60}")
    print(f"  Silver Transform  |  silver_run_id={_SILVER_RUN_ID}")
    print(f"{'─'*60}\n")

    print("[0/3] Detecting GPU …")
    _GPU_AVAILABLE = _setup_gpu()

    print("[1/3] Loading spaCy model …")
    nlp = load_nlp()

    written = {}

    # ── Products ───────────────────────────────────────────────────────────────
    print("\n[2/3] Processing products …")
    prod_table = _read_bronze(bronze_dir, "products", bronze_date)
    if prod_table is not None and len(prod_table) > 0:
        enriched = _enrich_products(nlp, prod_table)
        written["products"] = _write_silver_products(enriched, silver_dir)
    else:
        print("  [skip] no product rows found in bronze")

    # ── Reviews ────────────────────────────────────────────────────────────────
    print("\n[3/3] Processing reviews …")
    rev_table = _read_bronze(bronze_dir, "reviews", bronze_date)
    if rev_table is not None and len(rev_table) > 0:
        enriched = _enrich_reviews(nlp, rev_table)
        written["reviews"] = _write_silver_reviews(enriched, silver_dir)
    else:
        print("  [skip] no review rows found in bronze")

    print(f"\n{'─'*60}")
    print(f"  Silver complete  |  silver_run_id={_SILVER_RUN_ID}\n")
    return written


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver NLP transform.")
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