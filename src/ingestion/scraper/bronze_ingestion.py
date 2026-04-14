"""
bronze_ingestion.py
───────────────────
Converts raw data (dicts from scraper OR CSVs on disk) into immutable,
partitioned, schema-enforced Parquet files.

Schemas live in schema.py — this file never redefines column types.

Bronze layer rules
──────────────────
  • No business logic — only type casting + null normalisation
  • Every run is append-only (never overwrites existing partitions)
  • Adds _ingestion_ts, _source_file, _run_id audit columns to every row
  • Validates data and writes a sidecar quality-report JSON

Output layout
─────────────
  data/raw/bronze/
    products/
      ingestion_date=YYYY-MM-DD/
        products_<run_id>.parquet
    reviews/
      ingestion_date=YYYY-MM-DD/
        category_id=<cat>/
          reviews_<run_id>.parquet
    _quality_reports/
      report_<run_id>.json
"""

import csv
import io
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from .schema import PRODUCTS_SCHEMA, REVIEWS_SCHEMA

# ── MinIO toggle ──────────────────────────────────────────────────────────────

def _use_minio() -> bool:
    """Read USE_MINIO at call time so the env var is always current."""
    return os.getenv("USE_MINIO", "false").lower() in ("true", "1", "yes")

# Backwards-compatible alias used by pipeline.py
USE_MINIO = _use_minio


def _get_s3_client():
    """Create a boto3 S3 client pointing at MinIO."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    )


def _write_parquet_to_minio(table: pa.Table, bucket: str, key: str):
    """Write a PyArrow table as Parquet bytes to a MinIO bucket."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", write_statistics=True, coerce_timestamps="ms")
    buf.seek(0)
    s3 = _get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  [+] s3://{bucket}/{key}")


def _write_csv_to_minio(rows: list[dict], bucket: str, key: str):
    """Write a list of dicts as CSV bytes to a MinIO bucket."""
    if not rows:
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    s3 = _get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
    print(f"  [backup] {len(rows):,} rows → s3://{bucket}/{key}")


# ── Type coercions ─────────────────────────────────────────────────────────────

def _clean(v):
    return None if v in (None, "", "None", "none", "NULL", "null") else v

def _to_int(v):
    try:
        return int(float(v)) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

def _to_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

def _to_bool(v):
    if isinstance(v, bool):
        return v
    if v in (None, "", "None"):
        return None
    return str(v).strip().lower() in ("true", "1", "yes")

def _to_ts(v):
    """ISO-8601 string or datetime → UTC datetime. Returns None on failure."""
    if isinstance(v, datetime):
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
    if v in (None, "", "None"):
        return None

    # Python 3.11+ native ISO parsing (handles Bazaarvoice offsets perfectly)
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    # Fallback for weirdly formatted dates
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f+0000",
        "%Y-%m-%dT%H:%M:%S+0000",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ── Row normalisers ────────────────────────────────────────────────────────────

def _normalise_product(raw: dict, run_id: str, ts: datetime, source: str) -> dict:
    return {
        "ProductID":        _clean(raw.get("ProductID")),
        "Brand":            _clean(raw.get("Brand")),
        "ProductName":      _clean(raw.get("ProductName")),
        "CategoryId":       _clean(raw.get("CategoryId")),
        "ProductPageUrl":   _clean(raw.get("ProductPageUrl")),
        "AvgRating":        _to_float(raw.get("AvgRating")),
        "TotalReviewCount": _to_int(raw.get("TotalReviewCount")),
        "RecommendedCount": _to_int(raw.get("RecommendedCount")),
        "TotalPhotoCount":  _to_int(raw.get("TotalPhotoCount")),
        "RatingDist_1":     _to_int(raw.get("RatingDist_1")),
        "RatingDist_2":     _to_int(raw.get("RatingDist_2")),
        "RatingDist_3":     _to_int(raw.get("RatingDist_3")),
        "RatingDist_4":     _to_int(raw.get("RatingDist_4")),
        "RatingDist_5":     _to_int(raw.get("RatingDist_5")),
        "_ingestion_ts":    ts,
        "_source_file":     source,
        "_run_id":          run_id,
    }

def _normalise_review(raw: dict, run_id: str, ts: datetime, source: str) -> dict:
    return {
        "ProductID":        _clean(raw.get("ProductID")),
        "ReviewID":         _clean(raw.get("ReviewID")),
        "Rating":           _to_int(raw.get("Rating")),
        "Title":            _clean(raw.get("Title")),
        "ReviewText":       _clean(raw.get("ReviewText")),
        "SubmissionTime":   _to_ts(raw.get("SubmissionTime")),
        "LastModTime":      _to_ts(raw.get("LastModTime")),
        "IsRecommended":    _to_bool(raw.get("IsRecommended")),
        "HelpfulCount":     _to_int(raw.get("HelpfulCount")),
        "NotHelpfulCount":  _to_int(raw.get("NotHelpfulCount")),
        "IsFeatured":       _to_bool(raw.get("IsFeatured")),
        "IsIncentivized":   _clean(raw.get("IsIncentivized")),
        "IsStaffReview":    _clean(raw.get("IsStaffReview")),
        "UserLocation":     _clean(raw.get("UserLocation")),
        "skinTone":         _clean(raw.get("skinTone")),
        "skinType":         _clean(raw.get("skinType")),
        "eyeColor":         _clean(raw.get("eyeColor")),
        "hairColor":        _clean(raw.get("hairColor")),
        "hairType":         _clean(raw.get("hairType")),
        "hairConcerns":     _clean(raw.get("hairConcerns")),
        "skinConcerns":     _clean(raw.get("skinConcerns")),
        "ageRange":         _clean(raw.get("ageRange")),
        "ReviewPhotoCount": _to_int(raw.get("ReviewPhotoCount")),
        "_ingestion_ts":    ts,
        "_source_file":     source,
        "_run_id":          run_id,
    }


# ── Dict-list → PyArrow table ──────────────────────────────────────────────────

def _build_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Transpose a list of dicts into columnar arrays matching the schema."""
    columns = {f.name: [] for f in schema}
    for row in rows:
        for name in columns:
            columns[name].append(row.get(name))
    return pa.table(
        {name: pa.array(columns[name], type=schema.field(name).type)
         for name in columns},
        schema=schema,
    )


# ── CSV loaders (for standalone / recovery runs) ──────────────────────────────

def load_products_csv(path: str, run_id: str, ts: datetime) -> list[dict]:
    source = os.path.basename(path)
    with open(path, newline="", encoding="utf-8") as f:
        return [_normalise_product(row, run_id, ts, source)
                for row in csv.DictReader(f)]

def load_reviews_csv(path: str, run_id: str, ts: datetime) -> list[dict]:
    source = os.path.basename(path)
    with open(path, newline="", encoding="utf-8") as f:
        return [_normalise_review(row, run_id, ts, source)
                for row in csv.DictReader(f)]


# ── Validation ─────────────────────────────────────────────────────────────────

def validate(table: pa.Table, name: str) -> dict:
    total = len(table)
    report = {"table": name, "total_rows": total, "checks": [], "passed": True}

    def check(label: str, ok: bool, critical: bool = False):
        report["checks"].append({"check": label, "passed": ok, "critical": critical})
        if not ok and critical:
            report["passed"] = False

    check("has_rows", total > 0, critical=True)

    if name == "products":
        null_pid = pc.sum(pc.is_null(table["ProductID"])).as_py()
        check("ProductID_no_nulls", null_pid == 0, critical=True)
        check("ProductID_unique",
              total - len(set(table["ProductID"].to_pylist())) == 0)
        null_name = pc.sum(pc.is_null(table["ProductName"])).as_py()
        check("ProductName_fill>=95%", (total - null_name) / total >= 0.95)

    elif name == "reviews":
        null_rid = pc.sum(pc.is_null(table["ReviewID"])).as_py()
        check("ReviewID_no_nulls", null_rid == 0, critical=True)
        check("ReviewID_unique",
              total - len(set(table["ReviewID"].to_pylist())) == 0)
        bad = sum(
            1 for r in table["Rating"].to_pylist()
            if r is not None and not (1 <= r <= 5)
        )
        check("Rating_in_1_to_5", bad == 0)
        null_text = pc.sum(pc.is_null(table["ReviewText"])).as_py()
        check("ReviewText_fill>=80%", (total - null_text) / total >= 0.80)

    return report


# ── Writers ────────────────────────────────────────────────────────────────────

def _write_parquet(table: pa.Table, dest: Path):
    pq.write_table(
        table, dest,
        compression="snappy",
        write_statistics=True,
        coerce_timestamps="ms",
    )

def write_products(table: pa.Table, base_dir: str, date_str: str, run_id: str) -> str:
    key = f"products/ingestion_date={date_str}/products_{run_id}.parquet"

    if _use_minio():
        _write_parquet_to_minio(table, "marketpulse-bronze", key)
        return f"s3://marketpulse-bronze/{key}"

    dest = Path(base_dir) / "products" / f"ingestion_date={date_str}"
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"products_{run_id}.parquet"
    _write_parquet(table, out)
    print(f"  [+] products -> {out}  ({len(table):,} rows)")
    return str(out)

def write_reviews(table: pa.Table, base_dir: str, date_str: str, run_id: str) -> list[str]:
    written = []

    if _use_minio():
        key = f"reviews/ingestion_date={date_str}/reviews_{run_id}.parquet"
        _write_parquet_to_minio(table, "marketpulse-bronze", key)
        written.append(f"s3://marketpulse-bronze/{key}")
        return written

    # Partition by CategoryId when available (enriched via JOIN with products)
    if "CategoryId" in table.schema.names:
        raw_cats = set(table["CategoryId"].to_pylist())
        for cat in {c or "_unknown" for c in raw_cats}:
            mask = pc.equal(
                pc.if_else(pc.is_null(table["CategoryId"]), "_unknown", table["CategoryId"]),
                cat,
            )
            dest = (Path(base_dir) / "reviews"
                    / f"ingestion_date={date_str}"
                    / f"category_id={cat}")
            dest.mkdir(parents=True, exist_ok=True)
            out = dest / f"reviews_{run_id}.parquet"
            _write_parquet(table.filter(mask), out)
            print(f"  [+] reviews [{cat}] -> {out}  ({table.filter(mask).num_rows:,} rows)")
            written.append(str(out))
    else:
        dest = Path(base_dir) / "reviews" / f"ingestion_date={date_str}"
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / f"reviews_{run_id}.parquet"
        _write_parquet(table, out)
        print(f"  [+] reviews -> {out}  ({len(table):,} rows)")
        written.append(str(out))

    return written

def write_quality_report(reports: list[dict], base_dir: str, run_id: str) -> str:
    payload = {
        "run_id":        run_id,
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "reports":       reports,
        "overall_pass":  all(r["passed"] for r in reports),
    }
    status = "PASSED" if payload["overall_pass"] else "FAILED"

    if _use_minio():
        key = f"_quality_reports/report_{run_id}.json"
        s3 = _get_s3_client()
        s3.put_object(
            Bucket="marketpulse-bronze",
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
        )
        print(f"  [{status}] quality report -> s3://marketpulse-bronze/{key}")
        return f"s3://marketpulse-bronze/{key}"

    dest = Path(base_dir) / "_quality_reports"
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"report_{run_id}.json"
    out.write_text(json.dumps(payload, indent=2))
    print(f"  [{status}] quality report -> {out}")
    return str(out)


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest(
    products: list[dict],
    reviews:  list[dict],
    bronze_dir: str = "./data/raw/bronze",
    run_id:     str | None = None,
    source:     str = "scraper",
    fail_on_quality: bool = True,
) -> dict:
    """
    Normalise, validate, and write products + reviews to the Bronze layer.

    Parameters
    ----------
    products        : list of product-level dicts (keys = PRODUCT_FIELDS)
    reviews         : list of review-level dicts  (keys = REVIEW_FIELDS)
    bronze_dir      : root directory for the medallion lake
    run_id          : optional -- supplied by pipeline.py for traceability
    source          : label stamped in _source_file audit column
    fail_on_quality : raise RuntimeError on critical quality failures

    Returns
    -------
    dict with keys: products, reviews (list of paths), quality_report
    """
    run_id      = run_id or uuid.uuid4().hex[:12]
    ts          = datetime.now(timezone.utc)
    date_str    = ts.strftime("%Y-%m-%d")
    quality_rps = []
    written     = {}

    print(f"\n{'='*60}")
    print(f"  Bronze Ingest  |  run_id={run_id}  |  {date_str}  |  src={source}")
    print(f"{'='*60}")

    # ── Products ───────────────────────────────────────────────────────────────
    if products:
        print(f"\n[products] normalising {len(products):,} rows ...")
        norm = [_normalise_product(p, run_id, ts, source) for p in products]
        table = _build_table(norm, PRODUCTS_SCHEMA)

        report = validate(table, "products")
        quality_rps.append(report)
        for c in report["checks"]:
            tag = "+" if c["passed"] else ("!" if c["critical"] else "?")
            print(f"  [{tag}] {c['check']}")

        if fail_on_quality and not report["passed"]:
            raise RuntimeError("Products failed critical quality checks.")

        written["products"] = write_products(table, bronze_dir, date_str, run_id)

    # ── Reviews ────────────────────────────────────────────────────────────────
    if reviews:
        print(f"\n[reviews] normalising {len(reviews):,} rows ...")
        norm = [_normalise_review(r, run_id, ts, source) for r in reviews]
        table = _build_table(norm, REVIEWS_SCHEMA)

        report = validate(table, "reviews")
        quality_rps.append(report)
        for c in report["checks"]:
            tag = "+" if c["passed"] else ("!" if c["critical"] else "?")
            print(f"  [{tag}] {c['check']}")

        if fail_on_quality and not report["passed"]:
            raise RuntimeError("Reviews failed critical quality checks.")

        written["reviews"] = write_reviews(table, bronze_dir, date_str, run_id)

    # ── Quality sidecar ────────────────────────────────────────────────────────
    if quality_rps:
        written["quality_report"] = write_quality_report(quality_rps, bronze_dir, run_id)

    print(f"\n{'='*60}\n  run {run_id} complete.\n")
    return written
