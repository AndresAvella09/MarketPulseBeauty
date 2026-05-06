"""
gold_transform.py
─────────────────
Silver -> Gold transformation layer (slim, insights-only).

Postgres `gold` schema (small, indexed, query-ready):
  • gold.products                  — product master + family + health_score
  • gold.reviews                   — per-review insights (sentiment, topic, …)
  • gold.product_insights_monthly  — per-product monthly snapshots
  • gold.review_themes             — theme rollup per product (BERTopic)

MinIO `marketpulse-gold/embeddings/` (heavy artifacts, kept out of Postgres):
  • embeddings/products/…parquet   — product_name embeddings
  • embeddings/reviews/…parquet    — review_text + title embeddings

Embedding model (used for BERTopic and the future 2D map):
  Default: sentence-transformers BAAI/bge-m3
  Fallback: spaCy en_core_web_md
"""

from __future__ import annotations

import argparse
import gc
import io
import math
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .schema import (
    GOLD_BRANDS_SCHEMA,
    GOLD_DEMOGRAPHIC_INSIGHTS_SCHEMA,
    GOLD_FAMILY_DEMAND_SUPPLY_SCHEMA,
    GOLD_PRODUCT_EMBEDDINGS_SCHEMA,
    GOLD_PRODUCT_FAMILIES_SCHEMA,
    GOLD_PRODUCT_INSIGHTS_DAILY_SCHEMA,
    GOLD_PRODUCT_INSIGHTS_MONTHLY_SCHEMA,
    GOLD_PRODUCTS_SCHEMA,
    GOLD_REVIEW_EMBEDDINGS_SCHEMA,
    GOLD_REVIEW_THEMES_SCHEMA,
    GOLD_REVIEWS_SCHEMA,
)


# ── MinIO / PostgreSQL toggle ─────────────────────────────────────────────────

def _use_minio() -> bool:
    return os.getenv("USE_MINIO", "false").lower() in ("true", "1", "yes")


def _get_s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    )


def _read_parquet_from_minio(bucket: str, prefix: str, latest_only: bool = False) -> pa.Table | None:
    s3 = _get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    parquet_objs = [obj for obj in contents if obj["Key"].endswith(".parquet")]
    if not parquet_objs:
        return None

    if latest_only:
        date_re = re.compile(r"revision_date=(\d{4}-\d{2}-\d{2})")
        dated = {}
        for obj in parquet_objs:
            m = date_re.search(obj["Key"])
            if m:
                dated.setdefault(m.group(1), []).append(obj)
        if dated:
            latest_date = max(dated)
            newest = max(dated[latest_date], key=lambda o: o["LastModified"])
            parquet_objs = [newest]
            print(f"  [minio] latest partition: revision_date={latest_date}  file={newest['Key']}")

    tables = []
    for obj in parquet_objs:
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables) if tables else None


def _write_parquet_to_minio(bucket: str, key: str, table: pa.Table) -> None:
    s3 = _get_s3_client()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  [minio] wrote s3://{bucket}/{key}  ({len(table):,} rows)")


def _get_pg_engine():
    from sqlalchemy import create_engine
    conn_str = os.getenv(
        "POSTGRES_GOLD_CONN",
        "postgresql+psycopg2://postgres:changeme@localhost:5433/marketpulse",
    )
    return create_engine(conn_str)


# ── CUDA / device detection ───────────────────────────────────────────────────

def _detect_device() -> str:
    try:
        import torch
        if torch.cuda.is_available():
            name = torch.cuda.get_device_name(0)
            vram = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"  [GPU] CUDA device: {name}  ({vram:.1f} GB VRAM)")
            return "cuda"
        if torch.backends.mps.is_available():
            print("  [GPU] Apple Silicon MPS detected.")
            return "mps"
    except ImportError:
        pass
    print("  [GPU] No GPU detected -- running on CPU.")
    return "cpu"


_DEVICE: str = "cpu"


# ── Embedding backend ─────────────────────────────────────────────────────────

_EMBED_MODEL = None
_EMBED_DIM = None
_EMBED_TYPE = None  # "sbert" | "spacy"


def load_embedding_model(model_name: str = "BAAI/bge-m3"):
    global _EMBED_MODEL, _EMBED_DIM, _EMBED_TYPE
    if _EMBED_MODEL is not None:
        return _EMBED_MODEL

    try:
        from sentence_transformers import SentenceTransformer
        print(f"  [embed] Loading sentence-transformers '{model_name}' on {_DEVICE} ...")
        _EMBED_MODEL = SentenceTransformer(model_name, device=_DEVICE)
        _EMBED_DIM = _EMBED_MODEL.get_sentence_embedding_dimension()
        _EMBED_TYPE = "sbert"
        if _DEVICE == "cuda":
            _EMBED_MODEL = _EMBED_MODEL.half()
            print(f"  [embed] Running fp16 on CUDA  ({_EMBED_DIM}-dim)")
        else:
            print(f"  [embed] Ready  ({_EMBED_DIM}-dim, device={_DEVICE})")
        return _EMBED_MODEL
    except ImportError:
        print("  [embed] sentence-transformers not installed - falling back to spaCy.")

    import spacy
    print("  [embed] Loading spaCy en_core_web_md ...")
    _EMBED_MODEL = spacy.load("en_core_web_md", disable=["parser", "ner"])
    _EMBED_DIM = _EMBED_MODEL.vocab.vectors_length
    _EMBED_TYPE = "spacy"
    print(f"  [embed] spaCy ready  ({_EMBED_DIM}-dim)")
    return _EMBED_MODEL


def embed_texts(texts: list, batch_size: int = 256) -> tuple[np.ndarray, np.ndarray]:
    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)
    n = len(texts)
    dim = _EMBED_DIM
    embeddings = np.zeros((n, dim), dtype=np.float32)
    norms_out = np.zeros(n, dtype=np.float32)

    real_idx = [i for i, t in enumerate(texts) if t and str(t).strip()]
    real_text = [texts[i] for i in real_idx]
    if not real_text:
        return embeddings, norms_out

    if _EMBED_TYPE == "sbert":
        vecs = _EMBED_MODEL.encode(
            real_text, batch_size=batch_size, show_progress_bar=True,
            convert_to_numpy=True, normalize_embeddings=False, device=_DEVICE,
        ).astype(np.float32)
        for out_i, vec in zip(real_idx, vecs):
            embeddings[out_i] = vec
            norms_out[out_i] = float(np.linalg.norm(vec))
        del vecs
        if _DEVICE == "cuda":
            try:
                import torch
                torch.cuda.empty_cache()
            except ImportError:
                pass
    elif _EMBED_TYPE == "spacy":
        MINI = 10_000
        for s in range(0, len(real_idx), MINI):
            ci = real_idx[s:s + MINI]
            ct = real_text[s:s + MINI]
            for out_i, doc in zip(ci, _EMBED_MODEL.pipe(ct, batch_size=batch_size)):
                vec = doc.vector
                embeddings[out_i] = vec
                norms_out[out_i] = float(np.linalg.norm(vec))

    return embeddings, norms_out


def embed_with_late_chunking(texts: list, short_flags: list, batch_size: int = 256) -> tuple[np.ndarray, np.ndarray]:
    """Sentence-level chunking + mean pooling for long texts; keeps a single
    embedding per review aligned to the corpus."""
    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)
    REVIEW_MINI_BATCH = 500
    try:
        import torch
    except ImportError:
        torch = None

    n = len(texts)
    dim = _EMBED_DIM
    embeddings = np.zeros((n, dim), dtype=np.float32)
    norms_out = np.zeros(n, dtype=np.float32)

    for start in range(0, n, REVIEW_MINI_BATCH):
        end = min(start + REVIEW_MINI_BATCH, n)
        bt = texts[start:end]
        bf = short_flags[start:end]

        chunk_map: list[int] = []
        all_chunks: list[str] = []
        for li, (t, is_short) in enumerate(zip(bt, bf)):
            if not t or not str(t).strip():
                continue
            if is_short:
                all_chunks.append(t)
                chunk_map.append(li)
            else:
                raw = [c.strip() for c in t.replace("\n", ".").split(".") if len(c.strip()) > 5]
                if not raw:
                    raw = [t]
                for c in raw:
                    all_chunks.append(c)
                    chunk_map.append(li)
        if not all_chunks:
            continue

        if _EMBED_TYPE == "sbert":
            vecs = _EMBED_MODEL.encode(
                all_chunks, batch_size=batch_size, show_progress_bar=False,
                convert_to_numpy=True, device=_DEVICE,
            ).astype(np.float32)
        else:
            vecs = np.array(
                [doc.vector for doc in _EMBED_MODEL.pipe(all_chunks, batch_size=batch_size)],
                dtype=np.float32,
            )

        agg: dict[int, list[np.ndarray]] = defaultdict(list)
        for v, li in zip(vecs, chunk_map):
            agg[li].append(v)
        for li, vs in agg.items():
            final = np.mean(vs, axis=0).astype(np.float32)
            gi = start + li
            embeddings[gi] = final
            norms_out[gi] = float(np.linalg.norm(final))

        del vecs, agg, all_chunks, chunk_map
        gc.collect()
        if _DEVICE == "cuda" and torch is not None:
            try:
                torch.cuda.empty_cache()
            except Exception:
                pass
        print(f"    [{min(end, n):,}/{n:,} reviews embedded]")

    return embeddings, norms_out


# ── Silver readers ────────────────────────────────────────────────────────────

_SILVER_PRODUCTS_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("revision_date", pa.date32())]), flavor="hive",
)
_SILVER_REVIEWS_PARTITIONING = ds.partitioning(
    pa.schema([
        pa.field("revision_date", pa.date32()),
        pa.field("category_id", pa.string()),
    ]),
    flavor="hive",
)


def _read_silver(silver_dir: str, table_name: str):
    if _use_minio():
        prefix = f"{table_name}/"
        table = _read_parquet_from_minio("marketpulse-silver", prefix, latest_only=True)
        if table is None:
            print(f"  [!] No Silver data found in MinIO for {table_name}")
            return None
        print(f"  [silver/{table_name}] {len(table):,} rows loaded (MinIO)")
        return table

    base = Path(silver_dir) / table_name
    if not base.exists():
        print(f"  [!] Silver path not found: {base}")
        return None

    partitioning = (
        _SILVER_PRODUCTS_PARTITIONING if table_name == "products"
        else _SILVER_REVIEWS_PARTITIONING
    )
    dataset = ds.dataset(str(base), format="parquet", partitioning=partitioning)
    fragments = list(dataset.get_fragments())
    if not fragments:
        return None

    latest_date = None
    for frag in fragments:
        m = re.search(r"revision_date=(\d{4}-\d{2}-\d{2})", str(frag.path))
        if m:
            d = m.group(1)
            if latest_date is None or d > latest_date:
                latest_date = d
    if latest_date is not None:
        partition_frags = [f for f in fragments if f"revision_date={latest_date}" in str(f.path)]
        newest_frag = max(partition_frags, key=lambda f: os.path.getmtime(f.path))
        table = newest_frag.to_table()
        print(f"  [silver/{table_name}] revision_date={latest_date}")
    else:
        table = dataset.to_table()

    drop = [c for c in ("category_id",) if table.schema.get_field_index(c) != -1]
    if drop:
        table = table.drop(drop)
    print(f"  [silver/{table_name}] {len(table):,} rows loaded")
    return table


# ── Quality helpers ───────────────────────────────────────────────────────────

def _safe_div(a, b):
    return round(a / b, 6) if b else None


def _entropy(counts: list) -> float | None:
    total = sum(counts)
    if total == 0:
        return None
    probs = [c / total for c in counts if c > 0]
    return round(-sum(p * math.log2(p) for p in probs), 6)


def _polarization(dist: dict) -> float | None:
    total = sum(dist.values())
    if total == 0:
        return None
    return round((dist.get(1, 0) + dist.get(5, 0)) / total, 6)


def _text_quality(wordcount, has_text, has_title, has_photos) -> float:
    wc_norm = min((wordcount or 0) / 50, 1.0)
    return round(
        0.30 * float(bool(has_text)) +
        0.40 * wc_norm +
        0.15 * float(bool(has_title)) +
        0.15 * float(bool(has_photos)),
        6,
    )


def _np2d_to_arrow_list(arr_2d: np.ndarray) -> pa.Array:
    flat = arr_2d.ravel()
    dim = arr_2d.shape[1]
    values = pa.array(flat)
    offsets = pa.array(np.arange(0, len(flat) + 1, dim, dtype=np.int32))
    return pa.ListArray.from_arrays(offsets, values)


# ── Embedding cache loaders (skip recompute on retries) ───────────────────────

def _reuse_embeddings_enabled() -> bool:
    return os.getenv("GOLD_REUSE_EMBEDDINGS", "true").lower() in ("true", "1", "yes")


def _latest_embeddings_id_set(
    subdir: str, id_col: str, revision_dt: date, gold_dir: Path,
) -> set[str] | None:
    """Read ONLY the id column from the most recent embeddings parquet for the
    given revision_dt. Used as a lightweight cache-existence probe — avoids
    materializing the 1024-dim embedding arrays just to ask 'is it cached?'.
    """
    date_str = str(revision_dt)
    if _use_minio():
        s3 = _get_s3_client()
        prefix = f"embeddings/{subdir}/revision_date={date_str}/"
        resp = s3.list_objects_v2(Bucket="marketpulse-gold", Prefix=prefix)
        objs = [o for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not objs:
            return None
        newest = max(objs, key=lambda o: o["LastModified"])
        body = s3.get_object(Bucket="marketpulse-gold", Key=newest["Key"])["Body"].read()
        try:
            t = pq.read_table(io.BytesIO(body), columns=[id_col])
        except Exception as e:
            print(f"  [cache] could not read {id_col} from {newest['Key']}: {e}")
            return None
        return set(t.column(id_col).to_pylist())

    base = gold_dir / "embeddings" / subdir / f"revision_date={date_str}"
    if not base.exists():
        return None
    files = sorted(base.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    try:
        t = pq.ParquetFile(str(files[0])).read(columns=[id_col])
    except Exception as e:
        print(f"  [cache] could not read {id_col} from {files[0]}: {e}")
        return None
    return set(t.column(id_col).to_pylist())


def _review_embeddings_complete(review_ids: list[str], revision_dt: date, gold_dir: Path) -> bool:
    if not _reuse_embeddings_enabled():
        return False
    cached = _latest_embeddings_id_set("reviews", "ReviewID", revision_dt, gold_dir)
    if cached is None:
        return False
    missing = sum(1 for rid in review_ids if rid not in cached)
    if missing:
        print(f"  [cache] reviews: {missing:,}/{len(review_ids):,} ids missing — will re-embed")
        return False
    print(f"  [cache] reviews: {len(review_ids):,} ids present — skipping re-embed")
    return True


def _product_embeddings_complete(product_ids: list[str], revision_dt: date, gold_dir: Path) -> bool:
    if not _reuse_embeddings_enabled():
        return False
    cached = _latest_embeddings_id_set("products", "ProductID", revision_dt, gold_dir)
    if cached is None:
        return False
    missing = sum(1 for pid in product_ids if pid not in cached)
    if missing:
        print(f"  [cache] products: {missing:,}/{len(product_ids):,} ids missing — will re-embed")
        return False
    print(f"  [cache] products: {len(product_ids):,} ids present — skipping re-embed")
    return True


def _load_existing_embeddings_table(subdir: str, revision_dt: date, gold_dir: Path) -> pa.Table | None:
    """Read the most recent embeddings parquet for *revision_dt*, if any."""
    date_str = str(revision_dt)
    if _use_minio():
        prefix = f"embeddings/{subdir}/revision_date={date_str}/"
        try:
            # latest_only=True so previous-run leftovers in the same partition
            # don't get concatenated into one giant in-memory table.
            return _read_parquet_from_minio("marketpulse-gold", prefix, latest_only=True)
        except Exception as e:
            print(f"  [cache] MinIO read failed for {prefix}: {e}")
            return None
    base = gold_dir / "embeddings" / subdir / f"revision_date={date_str}"
    if not base.exists():
        return None
    files = sorted(base.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    try:
        return pq.ParquetFile(str(files[0])).read()
    except Exception as e:
        print(f"  [cache] local read failed for {files[0]}: {e}")
        return None


def _load_review_text_embeddings(
    review_ids: list[str], revision_dt: date, gold_dir: Path,
) -> np.ndarray | None:
    """Load only review_text_embedding (no title/norms) aligned to *review_ids*.

    Reads ONLY (ReviewID, review_text_embedding) columns from the latest
    parquet, avoiding the ~780 MB title_embedding column entirely. Stage_topics
    is the only caller and feeds rt vectors into BERTopic.
    """
    if not _reuse_embeddings_enabled():
        return None
    date_str = str(revision_dt)
    cols = ["ReviewID", "review_text_embedding"]

    if _use_minio():
        s3 = _get_s3_client()
        prefix = f"embeddings/reviews/revision_date={date_str}/"
        resp = s3.list_objects_v2(Bucket="marketpulse-gold", Prefix=prefix)
        objs = [o for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not objs:
            return None
        newest = max(objs, key=lambda o: o["LastModified"])
        print(f"  [cache] loading rt embeddings from s3://marketpulse-gold/{newest['Key']}")
        body = s3.get_object(Bucket="marketpulse-gold", Key=newest["Key"])["Body"].read()
        try:
            table = pq.ParquetFile(io.BytesIO(body)).read(columns=cols)
        except Exception as e:
            print(f"  [cache] could not read rt embeddings: {e}")
            return None
        del body; gc.collect()
    else:
        base = gold_dir / "embeddings" / "reviews" / f"revision_date={date_str}"
        if not base.exists():
            return None
        files = sorted(base.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            return None
        try:
            table = pq.ParquetFile(str(files[0])).read(columns=cols)
        except Exception as e:
            print(f"  [cache] could not read rt embeddings: {e}")
            return None

    cached_ids = table.column("ReviewID").to_pylist()
    cached_index = {rid: i for i, rid in enumerate(cached_ids)}
    if any(rid not in cached_index for rid in review_ids):
        return None

    col = table.column("review_text_embedding").combine_chunks()
    flat = col.flatten().to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    rt_full = flat.reshape(len(cached_ids), -1)
    del table, col, flat; gc.collect()

    order = np.fromiter((cached_index[rid] for rid in review_ids),
                        count=len(review_ids), dtype=np.int64)
    rt_emb = rt_full[order]
    del rt_full; gc.collect()
    print(f"  [cache] reviews: loaded {len(review_ids):,} review-text embeddings (revision_date={revision_dt})")
    return rt_emb


def _load_review_embedding_cache(
    review_ids: list[str], revision_dt: date, gold_dir: Path,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Return (rt_emb, ti_emb, rt_norms, ti_norms) aligned to *review_ids*,
    or None if cache is incomplete/missing."""
    if not _reuse_embeddings_enabled():
        return None
    table = _load_existing_embeddings_table("reviews", revision_dt, gold_dir)
    if table is None:
        return None

    cached_ids = table.column("ReviewID").to_pylist()
    cached_index = {rid: i for i, rid in enumerate(cached_ids)}
    missing = [rid for rid in review_ids if rid not in cached_index]
    if missing:
        print(f"  [cache] reviews: {len(missing):,}/{len(review_ids):,} ids missing — skipping reuse")
        return None

    # Flatten ListArrays directly to numpy (avoid materializing list-of-lists).
    def _list_col_to_2d(col_name: str) -> np.ndarray:
        col = table.column(col_name).combine_chunks()
        flat = col.flatten().to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
        return flat.reshape(len(cached_ids), -1)

    rt_full = _list_col_to_2d("review_text_embedding")
    ti_full = _list_col_to_2d("title_embedding")
    rt_norms_full = table.column("embedding_norm_review").to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    ti_norms_full = table.column("embedding_norm_title").to_numpy(zero_copy_only=False).astype(np.float32, copy=False)

    # Reorder into the requested ReviewID sequence with a single fancy index.
    order = np.fromiter((cached_index[rid] for rid in review_ids), count=len(review_ids), dtype=np.int64)
    rt_emb = rt_full[order]
    ti_emb = ti_full[order]
    rt_norms = rt_norms_full[order]
    ti_norms = ti_norms_full[order]
    del rt_full, ti_full, rt_norms_full, ti_norms_full
    gc.collect()

    print(f"  [cache] reviews: reusing {len(review_ids):,} cached embeddings (revision_date={revision_dt})")
    return rt_emb, ti_emb, rt_norms, ti_norms


def _load_product_embedding_cache(
    product_ids: list[str], revision_dt: date, gold_dir: Path,
) -> tuple[np.ndarray, np.ndarray] | None:
    if not _reuse_embeddings_enabled():
        return None
    table = _load_existing_embeddings_table("products", revision_dt, gold_dir)
    if table is None:
        return None

    cached_ids = table.column("ProductID").to_pylist()
    cached_index = {pid: i for i, pid in enumerate(cached_ids)}
    missing = [pid for pid in product_ids if pid not in cached_index]
    if missing:
        print(f"  [cache] products: {len(missing):,}/{len(product_ids):,} ids missing — skipping reuse")
        return None

    name_col = table.column("product_name_embedding").combine_chunks()
    flat = name_col.flatten().to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    name_full = flat.reshape(len(cached_ids), -1)
    norms_full = table.column("embedding_norm_name").to_numpy(zero_copy_only=False).astype(np.float32, copy=False)

    order = np.fromiter((cached_index[pid] for pid in product_ids), count=len(product_ids), dtype=np.int64)
    name_emb = name_full[order]
    name_norms = norms_full[order]
    del name_full, norms_full
    gc.collect()

    print(f"  [cache] products: reusing {len(product_ids):,} cached embeddings (revision_date={revision_dt})")
    return name_emb, name_norms


# ── Embedding writers (MinIO / local parquet, kept out of Postgres) ───────────

def _write_review_embeddings(
    review_ids: list, product_ids: list, sub_times: list,
    rt_emb: np.ndarray, ti_emb: np.ndarray,
    rt_norms: np.ndarray, ti_norms: np.ndarray,
    revision_dt: date, gold_run_id: str,
    gold_dir: Path,
):
    n = len(review_ids)
    table = pa.table({
        "ReviewID":              pa.array(review_ids, pa.string()),
        "ProductID":             pa.array(product_ids, pa.string()),
        "SubmissionTime":        pa.array(sub_times, pa.timestamp("ms", tz="UTC")),
        "review_text_embedding": _np2d_to_arrow_list(rt_emb),
        "title_embedding":       _np2d_to_arrow_list(ti_emb),
        "embedding_norm_review": pa.array(rt_norms, pa.float32()),
        "embedding_norm_title":  pa.array(ti_norms, pa.float32()),
        "revision_date":         pa.array([revision_dt] * n, pa.date32()),
        "_gold_run_id":          pa.array([gold_run_id] * n, pa.string()),
    }, schema=GOLD_REVIEW_EMBEDDINGS_SCHEMA)

    date_str = str(revision_dt)
    if _use_minio():
        key = f"embeddings/reviews/revision_date={date_str}/reviews_{gold_run_id}.parquet"
        _write_parquet_to_minio("marketpulse-gold", key, table)
    else:
        dest = gold_dir / "embeddings" / "reviews" / f"revision_date={date_str}" / f"reviews_{gold_run_id}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dest, compression="snappy")
        print(f"  [+] embeddings/reviews -> {dest}  ({n:,} rows)")


def _write_product_embeddings(
    product_ids: list, name_emb: np.ndarray, name_norms: np.ndarray,
    revision_dt: date, gold_run_id: str, gold_dir: Path,
):
    n = len(product_ids)
    table = pa.table({
        "ProductID":              pa.array(product_ids, pa.string()),
        "product_name_embedding": _np2d_to_arrow_list(name_emb),
        "embedding_norm_name":    pa.array(name_norms, pa.float32()),
        "revision_date":          pa.array([revision_dt] * n, pa.date32()),
        "_gold_run_id":           pa.array([gold_run_id] * n, pa.string()),
    }, schema=GOLD_PRODUCT_EMBEDDINGS_SCHEMA)

    date_str = str(revision_dt)
    if _use_minio():
        key = f"embeddings/products/revision_date={date_str}/products_{gold_run_id}.parquet"
        _write_parquet_to_minio("marketpulse-gold", key, table)
    else:
        dest = gold_dir / "embeddings" / "products" / f"revision_date={date_str}" / f"products_{gold_run_id}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dest, compression="snappy")
        print(f"  [+] embeddings/products -> {dest}  ({n:,} rows)")


# ── Local gold parquet helpers (handoff between stages) ───────────────────────

def _gold_local_dir(gold_dir: str | Path | None = None) -> Path:
    if gold_dir is not None:
        return Path(gold_dir)
    return Path(os.getenv("GOLD_LOCAL_DIR", "/tmp/gold"))


def _gold_local_path(gold_dir: Path, subdir: str, name: str,
                     gold_run_id: str, revision_dt: date) -> Path:
    return gold_dir / subdir / f"revision_date={revision_dt}" / f"{name}_{gold_run_id}.parquet"


def _write_gold_local(df: pd.DataFrame, schema, gold_dir: Path, subdir: str, name: str,
                      gold_run_id: str, revision_dt: date) -> str | None:
    if df is None or df.empty:
        return None
    dest = _gold_local_path(gold_dir, subdir, name, gold_run_id, revision_dt)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, KeyError):
        table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest, compression="snappy")
    print(f"  [+] gold/{subdir} -> {dest}  ({len(df):,} rows)")
    return str(dest)


def _read_gold_local(gold_dir: Path, subdir: str, name: str,
                     gold_run_id: str, revision_dt: date) -> pd.DataFrame | None:
    path = _gold_local_path(gold_dir, subdir, name, gold_run_id, revision_dt)
    if not path.exists():
        return None
    # ParquetFile.read() bypasses pyarrow's hive-partition discovery, which
    # would otherwise see "revision_date=YYYY-MM-DD" in the path and clash
    # with the same-named column already inside the file.
    return pq.ParquetFile(str(path)).read().to_pandas()


# ── Embedding stage helpers ───────────────────────────────────────────────────

def _embed_reviews(silver_reviews: pa.Table, gold_run_id: str, revision_dt: date,
                   batch_size: int, gold_dir: Path) -> None:
    """Embed review_text + title and write parquet. Uses cache when possible."""
    review_ids = silver_reviews["ReviewID"].to_pylist()
    if _review_embeddings_complete(review_ids, revision_dt, gold_dir):
        return  # cache hit; nothing to do

    df = silver_reviews.select([
        c for c in ("ReviewID", "ProductID", "SubmissionTime",
                    "ReviewText_lemmas", "Title_lemmas", "ReviewText_wordcount")
        if c in silver_reviews.schema.names
    ]).to_pandas()

    wordcounts = df["ReviewText_wordcount"].astype("Float64").fillna(0).astype(int).tolist()
    short_flags = [(wc < 5) for wc in wordcounts]

    rt_lemmas = df["ReviewText_lemmas"].fillna("").tolist()
    if _EMBED_TYPE == "sbert":
        rt_emb, rt_norms = embed_with_late_chunking(rt_lemmas, short_flags, batch_size=batch_size)
    else:
        rt_emb, rt_norms = embed_texts(rt_lemmas, batch_size=batch_size)
    del rt_lemmas; gc.collect()

    ti_lemmas = df["Title_lemmas"].fillna("").tolist()
    ti_emb, ti_norms = embed_texts(ti_lemmas, batch_size=batch_size)
    del ti_lemmas; gc.collect()

    sub_times_dt = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)
    _write_review_embeddings(
        review_ids=review_ids,
        product_ids=df["ProductID"].tolist(),
        sub_times=np.array(sub_times_dt.dt.to_pydatetime()).tolist(),
        rt_emb=rt_emb, ti_emb=ti_emb,
        rt_norms=rt_norms, ti_norms=ti_norms,
        revision_dt=revision_dt, gold_run_id=gold_run_id,
        gold_dir=gold_dir,
    )


def _embed_products(silver_products: pa.Table, gold_run_id: str, revision_dt: date,
                    batch_size: int, gold_dir: Path) -> None:
    product_ids = silver_products["ProductID"].to_pylist()
    if _product_embeddings_complete(product_ids, revision_dt, gold_dir):
        return
    name_lemmas = silver_products["ProductName_lemmas"].to_pylist()
    print(f"  [gold/products] embedding ProductName_lemmas ({len(name_lemmas):,}) ...")
    name_emb, name_norms = embed_texts(name_lemmas, batch_size=batch_size)
    _write_product_embeddings(
        product_ids=product_ids,
        name_emb=name_emb, name_norms=name_norms,
        revision_dt=revision_dt, gold_run_id=gold_run_id, gold_dir=gold_dir,
    )


# ── Build slim review DataFrame (no embeddings; topics filled later) ──────────

def build_reviews_slim_df(
    silver_reviews: pa.Table,
    gold_run_id: str,
    revision_dt: date,
) -> pd.DataFrame:
    """Sentiment + slim DataFrame. topic_id/topic_label left null."""
    from src.processing.gold_insights import compute_sentiment

    n = len(silver_reviews)
    print(f"  [gold/reviews] building {n:,} rows ...")

    needed_cols = [
        "ProductID", "ReviewID", "Rating", "Title", "ReviewText",
        "SubmissionTime", "LastModTime", "IsRecommended", "ReviewPhotoCount",
        "HelpfulCount", "NotHelpfulCount",
        "IsFeatured", "IsIncentivized", "IsStaffReview",
        "UserLocation", "skinTone", "skinType",
        "eyeColor", "hairColor",
        "hairType", "hairConcerns", "skinConcerns", "ageRange",
        "ReviewText_wordcount", "ReviewText_lemmas", "Title_lemmas",
    ]
    available = [c for c in needed_cols if c in silver_reviews.schema.names]
    df = silver_reviews.select(available).to_pandas()
    del silver_reviews
    gc.collect()

    wordcounts = df["ReviewText_wordcount"].astype("Float64").fillna(0).astype(int)
    titles = df["Title"].fillna("")
    review_texts = df["ReviewText"].fillna("")
    photo_counts = df.get("ReviewPhotoCount", pd.Series([0] * n)).fillna(0).astype(int)
    helpful = df["HelpfulCount"].astype("Float64").fillna(0).astype(int)
    not_helpful = df["NotHelpfulCount"].astype("Float64").fillna(0).astype(int)

    quality_scores = [
        _text_quality(wc, bool(t), bool(ti), bool(pc_))
        for wc, t, ti, pc_ in zip(wordcounts, review_texts, titles, photo_counts)
    ]
    helpful_ratios = [_safe_div(h, h + nh) for h, nh in zip(helpful, not_helpful)]

    sub_times_dt = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)
    age_days = [(revision_dt - ts.date()).days if pd.notna(ts) else None for ts in sub_times_dt]
    short_flags = [(wc < 5) for wc in wordcounts]

    print(f"  [gold/reviews] sentiment ({n:,}) ...")
    sent_scores, sent_labels = compute_sentiment(review_texts.tolist())

    # IsIncentivized / IsStaffReview arrive from Bronze as strings ("True"/"False"
    # or null). Coerce to a tri-state boolean so Postgres BOOLEAN columns accept them.
    def _to_bool(s):
        if s is None:
            return pd.array([None] * n, dtype="boolean")
        if pd.api.types.is_bool_dtype(s):
            return pd.array(s, dtype="boolean")
        lowered = s.astype("string").str.strip().str.lower()
        out = pd.Series([pd.NA] * len(s), dtype="boolean")
        out[lowered.isin(["true", "1", "yes", "t"])] = True
        out[lowered.isin(["false", "0", "no", "f"])] = False
        return out

    last_mod_dt = (
        pd.to_datetime(df["LastModTime"], errors="coerce", utc=True)
        if "LastModTime" in df.columns
        else pd.Series([pd.NaT] * n, dtype="datetime64[ns, UTC]")
    )

    slim = pd.DataFrame({
        "ProductID":             df["ProductID"],
        "ReviewID":              df["ReviewID"],
        "Rating":                df["Rating"],
        "Title":                 df["Title"],
        "ReviewText":            df["ReviewText"],
        "SubmissionTime":        sub_times_dt,
        "LastModTime":           last_mod_dt,
        "IsRecommended":         df["IsRecommended"],
        "ReviewPhotoCount":      df.get("ReviewPhotoCount"),
        "HelpfulCount":          helpful.astype("Int32"),
        "NotHelpfulCount":       not_helpful.astype("Int32"),
        "IsFeatured":            _to_bool(df.get("IsFeatured")),
        "IsIncentivized":        _to_bool(df.get("IsIncentivized")),
        "IsStaffReview":         _to_bool(df.get("IsStaffReview")),
        "UserLocation":          df.get("UserLocation"),
        "skinTone":              df.get("skinTone"),
        "skinType":              df.get("skinType"),
        "eyeColor":              df.get("eyeColor"),
        "hairColor":             df.get("hairColor"),
        "hairType":              df.get("hairType"),
        "hairConcerns":          df.get("hairConcerns"),
        "skinConcerns":          df.get("skinConcerns"),
        "ageRange":              df.get("ageRange"),
        "helpful_ratio":         pd.array(helpful_ratios, dtype="Float32"),
        "review_age_days":       pd.array(age_days, dtype="Int32"),
        "is_short_review":       pd.array(short_flags, dtype="boolean"),
        "text_quality_score":    pd.array(quality_scores, dtype="Float32"),
        "ReviewText_wordcount":  wordcounts.astype("Int32"),
        "ReviewText_lemmas":     df.get("ReviewText_lemmas"),
        "Title_lemmas":          df.get("Title_lemmas"),
        "sentiment_score":       pd.array(sent_scores, dtype="Float32"),
        "sentiment_label":       pd.array(sent_labels, dtype="string"),
        "topic_id":              pd.array([None] * n, dtype="Int32"),
        "topic_label":           pd.array([None] * n, dtype="string"),
        "umap_x":                pd.array([None] * n, dtype="Float32"),
        "umap_y":                pd.array([None] * n, dtype="Float32"),
        "revision_date":         pd.array([revision_dt] * n),
        "_gold_run_id":          pd.array([gold_run_id] * n, dtype="string"),
    })
    return slim


def build_products_slim_df(
    silver_products: pa.Table,
    reviews_slim: pd.DataFrame,
    gold_run_id: str,
    revision_dt: date,
) -> pd.DataFrame:
    from src.processing.gold_insights import (
        classify_focus_keyword_series, compute_health_score, derive_product_extras,
    )
    n = len(silver_products)
    df = silver_products.to_pandas()

    # Per-product aggregates from reviews
    sent_per_product = (
        reviews_slim.groupby("ProductID")["sentiment_score"]
        .mean().rename("avg_sentiment").reset_index()
    )
    df = df.merge(sent_per_product, on="ProductID", how="left")

    # Family taxonomy
    df["focus_keyword"] = classify_focus_keyword_series(df["ProductName"])

    # % recommended (from product-level totals)
    rec = df["RecommendedCount"].fillna(0).astype(float)
    tot = df["TotalReviewCount"].fillna(0).astype(float)
    df["pct_recommended"] = np.where(tot > 0, rec / tot, np.nan).astype("float32")

    # Rating distribution insights
    entropies, polarizations = [], []
    for i in range(n):
        dist = {k: int(df[f"RatingDist_{k}"].iloc[i] or 0) for k in range(1, 6)}
        entropies.append(_entropy(list(dist.values())))
        polarizations.append(_polarization(dist))
    df["rating_entropy"] = pd.array(entropies, dtype="Float32")
    df["polarization_score"] = pd.array(polarizations, dtype="Float32")

    df["health_score"] = [
        compute_health_score(r, c, pr, s)
        for r, c, pr, s in zip(
            df["AvgRating"], df["TotalReviewCount"],
            df["pct_recommended"], df["avg_sentiment"],
        )
    ]

    # Derived extras computed off the reviews_slim frame: first/last review,
    # velocity windows, photo coverage, edit rate, top quotes, top locations.
    extras = derive_product_extras(reviews_slim, revision_dt)
    if not extras.empty:
        df = df.merge(extras, on="ProductID", how="left")

    df["revision_date"] = revision_dt
    df["_gold_run_id"] = gold_run_id

    keep = [f.name for f in GOLD_PRODUCTS_SCHEMA]
    for col in keep:
        if col not in df.columns:
            df[col] = None
    return df[keep]


# ── Stage entry points (called individually by Airflow tasks) ─────────────────

def stage_embed(
    silver_dir: str,
    gold_run_id: str,
    revision_dt: date,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 256,
    gold_dir: str | Path | None = None,
) -> None:
    """Stage 1: load model, read silver, embed reviews + products → parquet."""
    global _DEVICE
    gold_path = _gold_local_dir(gold_dir)

    print(f"\n{'='*60}")
    print(f"  Gold Stage [embed]  |  gold_run_id={gold_run_id}  |  {revision_dt}")
    print(f"{'='*60}\n")

    print("[embed] Detecting compute device ...")
    _DEVICE = _detect_device()
    print("[embed] Loading embedding model ...")
    load_embedding_model(model_name)

    print("[embed] Reading Silver ...")
    silver_reviews = _read_silver(silver_dir, "reviews")
    if silver_reviews is None or len(silver_reviews) == 0:
        raise RuntimeError("[embed] No review data in Silver — aborting.")

    print("[embed] Embedding reviews ...")
    _embed_reviews(silver_reviews, gold_run_id, revision_dt, batch_size, gold_path)
    del silver_reviews; gc.collect()

    silver_products = _read_silver(silver_dir, "products")
    if silver_products is not None and len(silver_products) > 0:
        print("[embed] Embedding products ...")
        _embed_products(silver_products, gold_run_id, revision_dt, batch_size, gold_path)


def stage_reviews_slim(
    silver_dir: str,
    gold_run_id: str,
    revision_dt: date,
    gold_dir: str | Path | None = None,
) -> str:
    """Stage 2: sentiment + slim reviews DataFrame → parquet (topics still null)."""
    gold_path = _gold_local_dir(gold_dir)

    print(f"\n{'='*60}")
    print(f"  Gold Stage [reviews_slim]  |  gold_run_id={gold_run_id}")
    print(f"{'='*60}\n")

    silver_reviews = _read_silver(silver_dir, "reviews")
    if silver_reviews is None or len(silver_reviews) == 0:
        raise RuntimeError("[reviews_slim] No review data in Silver — aborting.")

    reviews_slim = build_reviews_slim_df(silver_reviews, gold_run_id, revision_dt)
    del silver_reviews; gc.collect()

    out = _write_gold_local(
        reviews_slim, GOLD_REVIEWS_SCHEMA, gold_path,
        "reviews", "reviews", gold_run_id, revision_dt,
    )
    if out is None:
        raise RuntimeError("[reviews_slim] No rows produced.")
    return out


def stage_topics(
    gold_run_id: str,
    revision_dt: date,
    gold_dir: str | Path | None = None,
) -> str:
    """Stage 3: BERTopic over cached embeddings → patch reviews parquet with topics."""
    from src.processing.gold_insights import cluster_topics, compute_2d_projection
    gold_path = _gold_local_dir(gold_dir)

    print(f"\n{'='*60}")
    print(f"  Gold Stage [topics]  |  gold_run_id={gold_run_id}")
    print(f"{'='*60}\n")

    reviews_slim = _read_gold_local(gold_path, "reviews", "reviews", gold_run_id, revision_dt)
    if reviews_slim is None or reviews_slim.empty:
        raise RuntimeError("[topics] reviews_slim parquet not found — run stage_reviews_slim first.")

    review_ids = reviews_slim["ReviewID"].tolist()
    rt_emb = _load_review_text_embeddings(review_ids, revision_dt, gold_path)
    if rt_emb is None:
        raise RuntimeError(
            "[topics] Review embeddings cache missing or incomplete — run stage_embed first."
        )

    print(f"[topics] Clustering {len(reviews_slim):,} reviews with BERTopic ...")
    topics, topic_labels = cluster_topics(
        texts=reviews_slim["ReviewText"].fillna("").tolist(),
        embeddings=rt_emb,
    )

    print(f"[topics] Projecting {len(reviews_slim):,} reviews to 2D ...")
    coords = compute_2d_projection(rt_emb)
    del rt_emb; gc.collect()

    reviews_slim["topic_id"] = pd.array(topics, dtype="Int32")
    reviews_slim["topic_label"] = reviews_slim["topic_id"].map(topic_labels).astype("string")
    reviews_slim["umap_x"] = pd.array(coords[:, 0], dtype="Float32")
    reviews_slim["umap_y"] = pd.array(coords[:, 1], dtype="Float32")

    out = _write_gold_local(
        reviews_slim, GOLD_REVIEWS_SCHEMA, gold_path,
        "reviews", "reviews", gold_run_id, revision_dt,
    )
    return out or ""


def stage_aggregates(
    silver_dir: str,
    gold_run_id: str,
    revision_dt: date,
    gold_dir: str | Path | None = None,
) -> dict:
    """Stage 4: products + monthly + themes + brands + families + demographic
    insights + daily + family_demand_supply parquets."""
    from src.processing.gold_insights import (
        build_brand_aggregates, build_daily_insights, build_demographic_insights,
        build_family_demand_supply, build_monthly_insights,
        build_product_family_aggregates, build_review_themes,
    )
    gold_path = _gold_local_dir(gold_dir)

    print(f"\n{'='*60}")
    print(f"  Gold Stage [aggregates]  |  gold_run_id={gold_run_id}")
    print(f"{'='*60}\n")

    reviews_slim = _read_gold_local(gold_path, "reviews", "reviews", gold_run_id, revision_dt)
    if reviews_slim is None or reviews_slim.empty:
        raise RuntimeError("[aggregates] reviews_slim parquet not found.")

    silver_products = _read_silver(silver_dir, "products")
    written: dict = {}

    products_slim = None
    if silver_products is not None and len(silver_products) > 0:
        products_slim = build_products_slim_df(silver_products, reviews_slim, gold_run_id, revision_dt)
        p = _write_gold_local(products_slim, GOLD_PRODUCTS_SCHEMA, gold_path,
                              "products", "products", gold_run_id, revision_dt)
        if p:
            written["products"] = p

    monthly = build_monthly_insights(reviews_slim)
    if not monthly.empty:
        monthly["revision_date"] = revision_dt
        monthly["_gold_run_id"] = gold_run_id
        m = _write_gold_local(monthly, GOLD_PRODUCT_INSIGHTS_MONTHLY_SCHEMA, gold_path,
                              "product_insights_monthly", "monthly", gold_run_id, revision_dt)
        if m:
            written["product_insights_monthly"] = m

    # Rebuild topic_labels from reviews_slim (avoids re-running BERTopic).
    topic_labels = dict(
        reviews_slim[["topic_id", "topic_label"]]
        .dropna(subset=["topic_id"])
        .drop_duplicates(subset=["topic_id"])
        .itertuples(index=False, name=None)
    )
    themes = build_review_themes(reviews_slim, topic_labels)
    if not themes.empty:
        themes["revision_date"] = revision_dt
        themes["_gold_run_id"] = gold_run_id
        t = _write_gold_local(themes, GOLD_REVIEW_THEMES_SCHEMA, gold_path,
                              "review_themes", "themes", gold_run_id, revision_dt)
        if t:
            written["review_themes"] = t

    # ── New rollups ────────────────────────────────────────────────────────────

    daily = build_daily_insights(reviews_slim, revision_dt)
    if not daily.empty:
        daily["revision_date"] = revision_dt
        daily["_gold_run_id"] = gold_run_id
        d = _write_gold_local(daily, GOLD_PRODUCT_INSIGHTS_DAILY_SCHEMA, gold_path,
                              "product_insights_daily", "daily", gold_run_id, revision_dt)
        if d:
            written["product_insights_daily"] = d

    if products_slim is not None and not products_slim.empty:
        brands = build_brand_aggregates(products_slim)
        if not brands.empty:
            brands["revision_date"] = revision_dt
            brands["_gold_run_id"] = gold_run_id
            b = _write_gold_local(brands, GOLD_BRANDS_SCHEMA, gold_path,
                                  "brands", "brands", gold_run_id, revision_dt)
            if b:
                written["brands"] = b

        families = build_product_family_aggregates(products_slim)
        if not families.empty:
            families["revision_date"] = revision_dt
            families["_gold_run_id"] = gold_run_id
            f = _write_gold_local(families, GOLD_PRODUCT_FAMILIES_SCHEMA, gold_path,
                                  "product_families", "families", gold_run_id, revision_dt)
            if f:
                written["product_families"] = f

        # demographic_insights needs focus_keyword on each review row.
        focus_map = products_slim[["ProductID", "focus_keyword"]]
        reviews_with_fk = reviews_slim.merge(focus_map, on="ProductID", how="left")
        demo = build_demographic_insights(reviews_with_fk)
        if not demo.empty:
            demo["revision_date"] = revision_dt
            demo["_gold_run_id"] = gold_run_id
            di = _write_gold_local(demo, GOLD_DEMOGRAPHIC_INSIGHTS_SCHEMA, gold_path,
                                   "demographic_insights", "demographic", gold_run_id, revision_dt)
            if di:
                written["demographic_insights"] = di

        # family_demand_supply joins monthly review counts with search_trends if available.
        trends_df = _read_gold_local(gold_path, "search_trends", "search_trends",
                                     gold_run_id, revision_dt)
        fds = build_family_demand_supply(monthly, products_slim, trends_df)
        if not fds.empty:
            fds["revision_date"] = revision_dt
            fds["_gold_run_id"] = gold_run_id
            fdsp = _write_gold_local(fds, GOLD_FAMILY_DEMAND_SUPPLY_SCHEMA, gold_path,
                                     "family_demand_supply", "fds", gold_run_id, revision_dt)
            if fdsp:
                written["family_demand_supply"] = fdsp

    return written


def stage_postgres(
    gold_run_id: str,
    revision_dt: date,
    gold_dir: str | Path | None = None,
) -> int:
    """Stage 5: read the 4 gold parquets and upsert into Postgres gold.* tables."""
    if not _use_minio():
        print("[postgres] USE_MINIO=false — skipping Postgres upsert.")
        return 0

    from src.processing.gold_writer import (
        log_pipeline_run, upsert_brands, upsert_demographic_insights,
        upsert_family_demand_supply, upsert_gold_products, upsert_gold_reviews,
        upsert_product_families, upsert_product_insights_daily,
        upsert_product_insights_monthly, upsert_review_themes,
    )

    gold_path = _gold_local_dir(gold_dir)
    print(f"\n{'='*60}")
    print(f"  Gold Stage [postgres]  |  gold_run_id={gold_run_id}")
    print(f"{'='*60}\n")

    reviews_slim = _read_gold_local(gold_path, "reviews", "reviews", gold_run_id, revision_dt)
    products_slim = _read_gold_local(gold_path, "products", "products", gold_run_id, revision_dt)
    monthly = _read_gold_local(gold_path, "product_insights_monthly", "monthly", gold_run_id, revision_dt)
    themes = _read_gold_local(gold_path, "review_themes", "themes", gold_run_id, revision_dt)
    daily = _read_gold_local(gold_path, "product_insights_daily", "daily", gold_run_id, revision_dt)
    brands = _read_gold_local(gold_path, "brands", "brands", gold_run_id, revision_dt)
    families = _read_gold_local(gold_path, "product_families", "families", gold_run_id, revision_dt)
    demographic = _read_gold_local(gold_path, "demographic_insights", "demographic", gold_run_id, revision_dt)
    fds = _read_gold_local(gold_path, "family_demand_supply", "fds", gold_run_id, revision_dt)

    engine = _get_pg_engine()
    started = datetime.now(timezone.utc)
    total = 0
    with engine.begin() as conn:
        if products_slim is not None and not products_slim.empty:
            n = upsert_gold_products(products_slim, conn); total += n
            print(f"  [pg] gold.products: {n}")
        if reviews_slim is not None and not reviews_slim.empty:
            n = upsert_gold_reviews(reviews_slim, conn); total += n
            print(f"  [pg] gold.reviews: {n}")
        if monthly is not None and not monthly.empty:
            n = upsert_product_insights_monthly(monthly, conn); total += n
            print(f"  [pg] gold.product_insights_monthly: {n}")
        if daily is not None and not daily.empty:
            n = upsert_product_insights_daily(daily, conn); total += n
            print(f"  [pg] gold.product_insights_daily: {n}")
        if themes is not None and not themes.empty:
            n = upsert_review_themes(themes, conn); total += n
            print(f"  [pg] gold.review_themes: {n}")
        if brands is not None and not brands.empty:
            n = upsert_brands(brands, conn); total += n
            print(f"  [pg] gold.brands: {n}")
        if families is not None and not families.empty:
            n = upsert_product_families(families, conn); total += n
            print(f"  [pg] gold.product_families: {n}")
        if demographic is not None and not demographic.empty:
            n = upsert_demographic_insights(demographic, conn); total += n
            print(f"  [pg] gold.demographic_insights: {n}")
        if fds is not None and not fds.empty:
            n = upsert_family_demand_supply(fds, conn); total += n
            print(f"  [pg] gold.family_demand_supply: {n}")

        log_pipeline_run(
            run_id=gold_run_id, dag_name="gold_transform", status="success",
            rows_written=total, started_at=started,
            finished_at=datetime.now(timezone.utc), conn=conn,
        )
    return total


# ── Orchestrator (CLI / single-process entry point) ───────────────────────────

def build_gold(
    silver_dir: str = "./data/processed/silver",
    gold_dir: str = "./data/processed/gold",
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 256,
) -> dict:
    """Run all gold stages in one process. Used by the CLI; the Airflow DAG
    calls each stage_* function as a separate task."""
    gold_run_id = uuid.uuid4().hex[:12]
    revision_dt = date.today()

    print(f"\n{'='*60}")
    print(f"  Gold Build  |  gold_run_id={gold_run_id}  |  {revision_dt}")
    print(f"{'='*60}\n")

    stage_embed(silver_dir, gold_run_id, revision_dt, model_name, batch_size, gold_dir)
    stage_reviews_slim(silver_dir, gold_run_id, revision_dt, gold_dir)
    stage_topics(gold_run_id, revision_dt, gold_dir)
    written = stage_aggregates(silver_dir, gold_run_id, revision_dt, gold_dir)
    stage_postgres(gold_run_id, revision_dt, gold_dir)

    # Include reviews path for parity with previous return shape.
    reviews_path = _gold_local_path(_gold_local_dir(gold_dir), "reviews", "reviews",
                                    gold_run_id, revision_dt)
    if reviews_path.exists():
        written.setdefault("reviews", str(reviews_path))

    print(f"\n{'='*60}")
    print(f"  Gold complete  |  outputs: {list(written.keys())}")
    print(f"  Embedding dim  |  {_EMBED_DIM}  ({_EMBED_TYPE})")
    print(f"  gold_run_id    |  {gold_run_id}\n")
    return written


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver -> slim Gold (insights + embeddings to MinIO).")
    parser.add_argument("--silver",     default="./data/processed/silver")
    parser.add_argument("--gold",       default="./data/processed/gold")
    parser.add_argument("--model",      default="BAAI/bge-m3")
    parser.add_argument("--batch-size", default=256, type=int)
    args = parser.parse_args()

    result = build_gold(
        silver_dir=args.silver, gold_dir=args.gold,
        model_name=args.model, batch_size=args.batch_size,
    )
    sys.exit(0 if result else 1)
