"""
gold_transform.py
─────────────────
Silver -> Gold transformation layer.

Produces two entity datasets (enriched with embeddings + quality metrics)
plus four analytics aggregation tables.

Gold entity datasets
────────────────────
  gold/reviews/
    All Silver review columns
    + review_text_embedding   (384-dim float32, sentence-transformers)
    + title_embedding         (384-dim float32)
    + text_quality_score      (composite 0-1)
    + helpful_ratio           (HelpfulCount / total votes)
    + review_age_days         (days from submission to revision_date)
    + is_short_review         (ReviewText_wordcount < 5)
    + embedding_norm_review   (L2 norm - low norm = low semantic content)
    + embedding_norm_title

  gold/products/
    All Silver product columns
    + product_name_embedding  (384-dim float32)
    + rating_entropy          (Shannon entropy of 1-5 distribution)
    + polarization_score      (fraction of extreme 1+5 ratings)
    + embedding_norm_name

Embedding model
───────────────
Default: sentence-transformers BAAI/bge-m3 (384-dim)
Fallback: spaCy en_core_web_md (300-dim) if sentence-transformers missing

Usage
─────
  python gold_transform.py
  python gold_transform.py --silver ./data/processed/silver --gold ./data/processed/gold
  python gold_transform.py --model all-mpnet-base-v2   # higher quality
  python gold_transform.py --batch-size 512            # faster on good hardware
"""

import argparse
import io
import math
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .schema import (
    GOLD_REVIEWS_SCHEMA,
    GOLD_PRODUCTS_SCHEMA,
)

# ── MinIO / PostgreSQL toggle ─────────────────────────────────────────────────

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
    """Read Parquet files under a prefix in MinIO and concatenate.

    If *latest_only* is True, only read files from the most recent
    ``revision_date=`` partition (Hive-style) to avoid re-processing
    the entire history.
    """
    s3 = _get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    parquet_objs = [obj for obj in contents if obj["Key"].endswith(".parquet")]

    if not parquet_objs:
        return None

    if latest_only:
        date_re = re.compile(r"revision_date=(\d{4}-\d{2}-\d{2})")
        dated_objs = {}
        for obj in parquet_objs:
            m = date_re.search(obj["Key"])
            if m:
                dated_objs.setdefault(m.group(1), []).append(obj)
        if dated_objs:
            latest_date = max(dated_objs.keys())
            partition_objs = dated_objs[latest_date]
            # Pick only the most recently modified file in the partition
            newest = max(partition_objs, key=lambda o: o["LastModified"])
            parquet_objs = [newest]
            print(f"  [minio] latest partition: revision_date={latest_date}  file={newest['Key']}")

    tables = []
    for obj in parquet_objs:
        resp = s3.get_object(Bucket=bucket, Key=obj["Key"])
        buf = io.BytesIO(resp["Body"].read())
        tables.append(pq.read_table(buf))

    return pa.concat_tables(tables) if tables else None


def _get_pg_engine():
    """Create a SQLAlchemy engine for the Gold PostgreSQL database."""
    from sqlalchemy import create_engine
    conn_str = os.getenv("POSTGRES_GOLD_CONN", "postgresql+psycopg2://postgres:changeme@localhost:5433/marketpulse")
    return create_engine(conn_str)

# ── CUDA / device detection ────────────────────────────────────────────────────

def _detect_device() -> str:
    """
    Return 'cuda', 'mps', or 'cpu' depending on what's available.
    """
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
    print("  [GPU] No GPU detected -- running on CPU (this will be slow).")
    print("        For CUDA:  pip install torch --index-url https://download.pytorch.org/whl/cu121")
    return "cpu"

_DEVICE: str = "cpu"   # set once in build_gold()

# ── Embedding backend ──────────────────────────────────────────────────────────

_EMBED_MODEL = None
_EMBED_DIM   = None
_EMBED_TYPE  = None   # "sbert" | "spacy"


def load_embedding_model(model_name: str = "BAAI/bge-m3"):
    """
    Load embedding model once and cache it globally.
    Automatically runs on GPU (CUDA/MPS) if available, otherwise CPU.
    Tries sentence-transformers first; falls back to spaCy en_core_web_md.
    """
    global _EMBED_MODEL, _EMBED_DIM, _EMBED_TYPE

    if _EMBED_MODEL is not None:
        return _EMBED_MODEL

    try:
        from sentence_transformers import SentenceTransformer
        print(f"  [embed] Loading sentence-transformers '{model_name}' on {_DEVICE} ...")
        _EMBED_MODEL = SentenceTransformer(model_name, device=_DEVICE)
        _EMBED_DIM   = _EMBED_MODEL.get_sentence_embedding_dimension()
        _EMBED_TYPE  = "sbert"

        if _DEVICE == "cuda":
            _EMBED_MODEL = _EMBED_MODEL.half()   # fp16: halves VRAM
            print(f"  [embed] Running fp16 on CUDA  ({_EMBED_DIM}-dim)")
        else:
            print(f"  [embed] Ready  ({_EMBED_DIM}-dim, device={_DEVICE})")

        return _EMBED_MODEL
    except ImportError:
        print("  [embed] sentence-transformers not installed - falling back to spaCy.")
        print("          For best results:  pip install sentence-transformers")

    try:
        import spacy
        print("  [embed] Loading spaCy en_core_web_md ...")
        _EMBED_MODEL = spacy.load("en_core_web_md", disable=["parser", "ner"])
        _EMBED_DIM   = _EMBED_MODEL.vocab.vectors_length
        _EMBED_TYPE  = "spacy"
        print(f"  [embed] spaCy ready  ({_EMBED_DIM}-dim)")
        return _EMBED_MODEL
    except OSError:
        raise RuntimeError(
            "No embedding model available.\n"
            "Option A:  pip install sentence-transformers\n"
            "Option B:  python -m spacy download en_core_web_md"
        )


def embed_texts(
    texts: list,
    batch_size: int = 256,
) -> tuple:
    """
    Embed a list of texts. None/empty strings get zero embedding + 0.0 norm.
    Returns (embeddings_2d_np, norms_np) as numpy arrays.
    """
    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)

    model = _EMBED_MODEL
    n     = len(texts)
    dim   = _EMBED_DIM

    embeddings = np.zeros((n, dim), dtype=np.float32)
    norms_out  = np.zeros(n, dtype=np.float32)

    real_idx  = [i for i, t in enumerate(texts) if t and str(t).strip()]
    real_text = [texts[i] for i in real_idx]

    if not real_text:
        return embeddings, norms_out

    if _EMBED_TYPE == "sbert":
        vecs = model.encode(
            real_text,
            batch_size=batch_size,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=False,
            device=_DEVICE,
        ).astype(np.float32)
        for out_idx, vec in zip(real_idx, vecs):
            embeddings[out_idx] = vec
            norms_out[out_idx]  = float(np.linalg.norm(vec))
        del vecs
        if _DEVICE == "cuda":
            try:
                import torch
                torch.cuda.empty_cache()
            except ImportError:
                pass

    elif _EMBED_TYPE == "spacy":
        MINI = 10_000
        for start in range(0, len(real_idx), MINI):
            chunk_idx  = real_idx[start:start + MINI]
            chunk_text = real_text[start:start + MINI]
            docs = model.pipe(chunk_text, batch_size=batch_size)
            for out_idx, doc in zip(chunk_idx, docs):
                vec = doc.vector
                embeddings[out_idx] = vec
                norms_out[out_idx]  = float(np.linalg.norm(vec))
            print(f"    [{min(start + MINI, len(real_idx)):,}/{len(real_idx):,} texts embedded]")

    return embeddings, norms_out

def embed_with_late_chunking(texts: list, short_flags: list, batch_size: int = 256) -> tuple:
    """
    Implements Hierarchical / Late Chunking for a unified vector space.
    Short texts: Embedded directly.
    Long texts: Chunked by sentence, embedded separately, then globally averaged.

    Processes reviews in mini-batches (REVIEW_MINI_BATCH) so that sentence
    chunks from only a subset of reviews are in VRAM at a time.
    """
    import gc

    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)

    REVIEW_MINI_BATCH = 500

    try:
        import torch
    except ImportError:
        torch = None

    model = _EMBED_MODEL
    dim   = _EMBED_DIM
    n     = len(texts)
    embeddings = np.zeros((n, dim), dtype=np.float32)
    norms_out  = np.zeros(n, dtype=np.float32)

    for start in range(0, n, REVIEW_MINI_BATCH):
        end         = min(start + REVIEW_MINI_BATCH, n)
        batch_texts = texts[start:end]
        batch_flags = short_flags[start:end]

        chunk_map  = []
        all_chunks = []

        for local_i, (text, is_short) in enumerate(zip(batch_texts, batch_flags)):
            if not text or not str(text).strip():
                continue
            if is_short:
                all_chunks.append(text)
                chunk_map.append(local_i)
            else:
                raw_chunks = [c.strip() for c in text.replace('\n', '.').split('.')
                              if len(c.strip()) > 5]
                if not raw_chunks:
                    raw_chunks = [text]
                for c in raw_chunks:
                    all_chunks.append(c)
                    chunk_map.append(local_i)

        if not all_chunks:
            continue

        if _EMBED_TYPE == "sbert":
            chunk_vecs = model.encode(
                all_chunks,
                batch_size=batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=_DEVICE,
            ).astype(np.float32)
        elif _EMBED_TYPE == "spacy":
            chunk_vecs = np.array(
                [doc.vector for doc in model.pipe(all_chunks, batch_size=batch_size)],
                dtype=np.float32,
            )

        agg_vecs = defaultdict(list)
        for vec, local_i in zip(chunk_vecs, chunk_map):
            agg_vecs[local_i].append(vec)

        for local_i, vecs in agg_vecs.items():
            final_vec = np.mean(vecs, axis=0).astype(np.float32)
            global_i  = start + local_i
            embeddings[global_i] = final_vec
            norms_out[global_i]  = float(np.linalg.norm(final_vec))

        del chunk_vecs, agg_vecs, all_chunks, chunk_map
        gc.collect()

        if _DEVICE == "cuda" and torch is not None:
            try:
                torch.cuda.empty_cache()
            except Exception:
                pass

        print(f"    [{min(end, n):,}/{n:,} reviews embedded]")

    return embeddings, norms_out


# ── Silver readers ─────────────────────────────────────────────────────────────

_SILVER_PRODUCTS_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("revision_date", pa.date32())]),
    flavor="hive",
)
_SILVER_REVIEWS_PARTITIONING = ds.partitioning(
    pa.schema([
        pa.field("revision_date", pa.date32()),
        pa.field("category_id",   pa.string()),
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
        _SILVER_PRODUCTS_PARTITIONING
        if table_name == "products"
        else _SILVER_REVIEWS_PARTITIONING
    )

    dataset = ds.dataset(
        str(base), format="parquet", partitioning=partitioning,
    )

    # Only read the latest file from the latest revision_date partition
    fragments = list(dataset.get_fragments())
    if not fragments:
        print(f"  [!] No parquet files found in {base}")
        return None

    latest_date = None
    for frag in fragments:
        m = re.search(r"revision_date=(\d{4}-\d{2}-\d{2})", str(frag.path))
        if m:
            d = m.group(1)
            if latest_date is None or d > latest_date:
                latest_date = d

    if latest_date is not None:
        # Filter to latest partition, then pick the newest file by mtime
        partition_frags = [
            f for f in fragments
            if f"revision_date={latest_date}" in str(f.path)
        ]
        newest_frag = max(partition_frags, key=lambda f: os.path.getmtime(f.path))
        table = newest_frag.to_table()
        print(f"  [silver/{table_name}] latest partition: revision_date={latest_date}  file={Path(newest_frag.path).name}")
    else:
        table = dataset.to_table()

    drop = [c for c in ("category_id",) if table.schema.get_field_index(c) != -1]
    if drop:
        table = table.drop(drop)

    print(f"  [silver/{table_name}] {len(table):,} rows loaded")
    return table


# ── Quality helpers ────────────────────────────────────────────────────────────

def _safe_div(a, b):
    return round(a / b, 6) if b else None

def _entropy(counts: list) -> float:
    total = sum(counts)
    if total == 0:
        return None
    probs = [c / total for c in counts if c > 0]
    return round(-sum(p * math.log2(p) for p in probs), 6)

def _polarization(dist: dict) -> float:
    total = sum(dist.values())
    if total == 0:
        return None
    return round((dist.get(1, 0) + dist.get(5, 0)) / total, 6)

def _text_quality(wordcount, has_text, has_title, has_photos) -> float:
    wc_norm = min((wordcount or 0) / 50, 1.0)
    return round(
        0.30 * float(bool(has_text)) +
        0.40 * wc_norm               +
        0.15 * float(bool(has_title)) +
        0.15 * float(bool(has_photos)),
        6,
    )

def _np2d_to_arrow_list(arr_2d: np.ndarray) -> pa.Array:
    """Convert a (N, dim) float32 numpy array to Arrow list<float32>."""
    flat = arr_2d.ravel()
    dim  = arr_2d.shape[1]
    values  = pa.array(flat)
    offsets = pa.array(np.arange(0, len(flat) + 1, dim, dtype=np.int32))
    return pa.ListArray.from_arrays(offsets, values)


# ── Gold entity builders ───────────────────────────────────────────────────────

_GOLD_REVIEWS_CHUNK = 20_000  # rows per write-batch — keeps peak RAM under ~200 MB


def _build_gold_reviews_chunk(chunk, gold_run_id, revision_dt, batch_size):
    """Embed and enrich a single slice of silver reviews. Returns a gold pa.Table."""
    import gc

    n = len(chunk)
    wordcounts   = chunk["ReviewText_wordcount"].to_pylist()
    review_texts = chunk["ReviewText"].to_pylist()
    titles       = chunk["Title"].to_pylist()
    photo_counts = chunk["ReviewPhotoCount"].to_pylist()
    helpful      = chunk["HelpfulCount"].to_pylist()
    not_helpful  = chunk["NotHelpfulCount"].to_pylist()
    sub_times    = chunk["SubmissionTime"].to_pylist()

    quality_scores, helpful_ratios, age_days, short_flags = [], [], [], []
    for i in range(n):
        quality_scores.append(_text_quality(
            wordcount=wordcounts[i], has_text=bool(review_texts[i]),
            has_title=bool(titles[i]), has_photos=bool(photo_counts[i]),
        ))
        h, nh = helpful[i] or 0, not_helpful[i] or 0
        helpful_ratios.append(_safe_div(h, h + nh))
        ts = sub_times[i]
        if ts is not None:
            if hasattr(ts, "as_py"):
                ts = ts.as_py()
            age_days.append((revision_dt - ts.date()).days if isinstance(ts, datetime) else None)
        else:
            age_days.append(None)
        short_flags.append((wordcounts[i] or 0) < 5)

    del review_texts, titles, photo_counts, helpful, not_helpful, sub_times, wordcounts
    gc.collect()

    rt_lemmas = chunk["ReviewText_lemmas"].to_pylist()
    if _EMBED_TYPE == "sbert":
        rt_emb, rt_norms = embed_with_late_chunking(
            texts=rt_lemmas, short_flags=short_flags, batch_size=batch_size
        )
    else:
        rt_emb, rt_norms = embed_texts(rt_lemmas, batch_size=batch_size)
    del rt_lemmas
    gc.collect()
    rt_emb_arr  = _np2d_to_arrow_list(rt_emb)
    rt_norm_arr = pa.array(rt_norms)
    del rt_emb, rt_norms
    gc.collect()

    ti_lemmas        = chunk["Title_lemmas"].to_pylist()
    ti_emb, ti_norms = embed_texts(ti_lemmas, batch_size=batch_size)
    del ti_lemmas
    ti_emb_arr  = _np2d_to_arrow_list(ti_emb)
    ti_norm_arr = pa.array(ti_norms)
    del ti_emb, ti_norms
    gc.collect()

    silver_cols = {col: chunk[col] for col in chunk.schema.names}
    extras = {
        "review_text_embedding": rt_emb_arr,
        "title_embedding":       ti_emb_arr,
        "text_quality_score":    pa.array(quality_scores, pa.float32()),
        "helpful_ratio":         pa.array(helpful_ratios, pa.float32()),
        "review_age_days":       pa.array(age_days,       pa.int32()),
        "is_short_review":       pa.array(short_flags,    pa.bool_()),
        "embedding_norm_review": rt_norm_arr,
        "embedding_norm_title":  ti_norm_arr,
        "_gold_run_id":          pa.array([gold_run_id]*n, pa.string()),
    }
    combined = {**silver_cols, **extras}
    ordered  = {f.name: combined[f.name] for f in GOLD_REVIEWS_SCHEMA if f.name in combined}
    return fill_nulls(pa.table(ordered, schema=GOLD_REVIEWS_SCHEMA))


def build_gold_reviews(silver_reviews, gold_run_id, revision_dt, batch_size=256, dest: Path = None):
    """
    Build Gold reviews.

    When *dest* is provided the table is written to a Parquet file in
    _GOLD_REVIEWS_CHUNK-row batches so that only one chunk of embeddings
    lives in RAM at a time (avoids OOM on large review sets).
    Returns the written pa.Table when dest is None, or the total row
    count (int) when writing to dest.
    """
    import gc

    n = len(silver_reviews)

    if dest is None:
        # Legacy in-memory path (fine for small datasets / unit tests)
        print(f"  [gold/reviews] building {n:,} rows (in-memory) ...")
        table = _build_gold_reviews_chunk(silver_reviews, gold_run_id, revision_dt, batch_size)
        return drop_empty_columns(table)

    # ── Chunked streaming write ────────────────────────────────────────────
    split_types = (pa.float32(), pa.float64())
    column_encodings = {
        GOLD_REVIEWS_SCHEMA.field(i).name: "BYTE_STREAM_SPLIT"
        for i in range(len(GOLD_REVIEWS_SCHEMA))
        if GOLD_REVIEWS_SCHEMA.field(i).type in split_types
    }

    dest.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(
        str(dest),
        schema=GOLD_REVIEWS_SCHEMA,
        compression="snappy",
        write_statistics=True,
        use_dictionary=False,
        column_encoding=column_encodings or None,
    )

    total_rows = 0
    try:
        for chunk_start in range(0, n, _GOLD_REVIEWS_CHUNK):
            chunk_len = min(_GOLD_REVIEWS_CHUNK, n - chunk_start)
            chunk_end = chunk_start + chunk_len
            print(f"  [gold/reviews] rows {chunk_start:,}–{chunk_end:,} / {n:,} ...")
            chunk = silver_reviews.slice(chunk_start, chunk_len)
            chunk_table = _build_gold_reviews_chunk(chunk, gold_run_id, revision_dt, batch_size)
            del chunk
            gc.collect()
            writer.write_table(chunk_table)
            total_rows += len(chunk_table)
            del chunk_table
            gc.collect()
    finally:
        writer.close()

    print(f"  [+] gold/reviews written: {total_rows:,} rows → {dest}")
    return total_rows


def build_gold_products(silver_products, gold_run_id, revision_dt, batch_size=256):
    n = len(silver_products)

    print(f"  [gold/products] embedding ProductName_lemmas ({n:,} rows) ...")
    name_lemmas          = silver_products["ProductName_lemmas"].to_pylist()
    name_emb, name_norms = embed_texts(name_lemmas, batch_size=batch_size)

    entropies, polarizations = [], []
    for i in range(n):
        dist = {k: (silver_products[f"RatingDist_{k}"][i].as_py() or 0) for k in range(1, 6)}
        entropies.append(_entropy(list(dist.values())))
        polarizations.append(_polarization(dist))

    silver_cols = {col: silver_products[col] for col in silver_products.schema.names}
    extras = {
        "product_name_embedding": _np2d_to_arrow_list(name_emb),
        "rating_entropy":         pa.array(entropies,         pa.float32()),
        "polarization_score":     pa.array(polarizations,     pa.float32()),
        "embedding_norm_name":    pa.array(name_norms),
        "_gold_run_id":           pa.array([gold_run_id]*n,   pa.string()),
    }

    combined = {**silver_cols, **extras}
    ordered  = {f.name: combined[f.name] for f in GOLD_PRODUCTS_SCHEMA if f.name in combined}
    return pa.table(ordered, schema=GOLD_PRODUCTS_SCHEMA)


# ── Writer ─────────────────────────────────────────────────────────────────────

def _pq_write(table, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    split_types = (pa.float32(), pa.float64())
    column_encodings = {
        table.schema.field(i).name: "BYTE_STREAM_SPLIT"
        for i in range(len(table.schema))
        if table.schema.field(i).type in split_types
    }
    pq.write_table(
        table, dest,
        compression="snappy",
        write_statistics=True,
        use_dictionary=False,
        column_encoding=column_encodings if column_encodings else None,
    )

def drop_empty_columns(table: pa.Table) -> pa.Table:
    """Drops columns where 100% of the values are null."""
    cols_to_drop = []
    for name in table.schema.names:
        if pc.sum(pc.is_valid(table[name])).as_py() == 0:
            cols_to_drop.append(name)

    if cols_to_drop:
        print(f"  [-] Dropping empty columns: {cols_to_drop}")
        return table.drop(cols_to_drop)
    return table


def fill_nulls(table: pa.Table) -> pa.Table:
    """Fill null values with type-appropriate defaults so gold has no nulls."""
    from datetime import datetime as _dt, timezone as _tz

    _EPOCH = _dt(1970, 1, 1, tzinfo=_tz.utc)
    arrays = {}

    for i in range(len(table.schema)):
        field = table.schema.field(i)
        col   = table.column(field.name)
        t     = field.type

        if pc.sum(pc.is_null(col)).as_py() == 0:
            arrays[field.name] = col
            continue

        if pa.types.is_string(t) or pa.types.is_large_string(t):
            arrays[field.name] = pc.if_else(pc.is_null(col), "", col)
        elif pa.types.is_integer(t):
            arrays[field.name] = pc.if_else(pc.is_null(col), 0, col)
        elif pa.types.is_floating(t):
            arrays[field.name] = pc.if_else(pc.is_null(col), 0.0, col)
        elif pa.types.is_boolean(t):
            arrays[field.name] = pc.if_else(pc.is_null(col), False, col)
        elif pa.types.is_timestamp(t):
            epoch_scalar = pa.scalar(_EPOCH, type=t)
            arrays[field.name] = pc.if_else(pc.is_null(col), epoch_scalar, col)
        elif pa.types.is_date(t):
            arrays[field.name] = pc.if_else(pc.is_null(col), date(1970, 1, 1), col)
        elif pa.types.is_list(t):
            pylist = col.to_pylist()
            filled = [v if v is not None else [] for v in pylist]
            arrays[field.name] = pa.array(filled, type=t)
        else:
            arrays[field.name] = col

    return pa.table(arrays, schema=table.schema)


# ── Orchestrator ───────────────────────────────────────────────────────────────

def build_gold(
    silver_dir: str = "./data/processed/silver",
    gold_dir:   str = "./data/processed/gold",
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 256,
) -> dict:
    global _DEVICE
    gold_run_id = uuid.uuid4().hex[:12]
    revision_dt = date.today()
    date_str    = str(revision_dt)

    print(f"\n{'='*60}")
    print(f"  Gold Build  |  gold_run_id={gold_run_id}  |  {date_str}")
    print(f"{'='*60}\n")

    print("[0/4] Detecting compute device ...")
    _DEVICE = _detect_device()

    print("[1/4] Loading embedding model ...")
    load_embedding_model(model_name)

    print("\n[2/4] Reading Silver ...")
    silver_products = _read_silver(silver_dir, "products")
    silver_reviews  = _read_silver(silver_dir, "reviews")

    if silver_reviews is None or len(silver_reviews) == 0:
        print("[!] No review data in Silver - aborting.")
        return {}

    written = {}

    print("\n[3/4] Building Gold entity datasets ...")

    import gc
    reviews_dest = Path(gold_dir) / "reviews" / f"revision_date={date_str}"
    reviews_out  = reviews_dest / f"reviews_{gold_run_id}.parquet"
    reviews_total = build_gold_reviews(silver_reviews, gold_run_id, revision_dt, batch_size, dest=reviews_out)
    del silver_reviews
    gc.collect()
    written["reviews"] = str(reviews_out)
    print(f"  [+] gold/reviews -> {reviews_out}  ({reviews_total:,} rows)")

    gold_products = None
    if silver_products is not None and len(silver_products) > 0:
        gold_products = build_gold_products(silver_products, gold_run_id, revision_dt, batch_size)
        gold_products = fill_nulls(gold_products)
        dest = Path(gold_dir) / "products" / f"revision_date={date_str}"
        out  = dest / f"products_{gold_run_id}.parquet"
        _pq_write(gold_products, out)
        print(f"  [+] gold/products -> {out}  ({len(gold_products):,} rows)")
        written["products"] = str(out)
    else:
        print("  [skip] no product rows in Silver")

    # ── PostgreSQL Gold upserts (when USE_MINIO is enabled) ───────────────────
    if _use_minio():
        import gc
        print("\n[4/4] Upserting to PostgreSQL Gold tables ...")
        import pandas as pd
        from src.processing.gold_writer import (
            upsert_gold_products, upsert_gold_reviews, log_pipeline_run,
        )
        engine = _get_pg_engine()
        total_upserted = 0
        started = datetime.now(timezone.utc)

        with engine.begin() as conn:
            if gold_products is not None:
                products_df = gold_products.to_pandas()
                del gold_products
                gc.collect()
                n = upsert_gold_products(products_df, conn)
                total_upserted += n
                del products_df
                gc.collect()
                print(f"  [pg] gold.products: {n} rows upserted")

            CHUNK = 5000
            reviews_n = 0
            pf = pq.ParquetFile(str(reviews_out))
            for batch in pf.iter_batches(batch_size=CHUNK):
                chunk_df = pa.Table.from_batches([batch]).to_pandas()
                reviews_n += upsert_gold_reviews(chunk_df, conn)
                del chunk_df
                gc.collect()
                print(f"    [pg] gold.reviews upserted {reviews_n:,} rows so far ...")
            del pf
            total_upserted += reviews_n
            print(f"  [pg] gold.reviews: {reviews_n} rows upserted")

            log_pipeline_run(
                run_id=gold_run_id,
                dag_name="gold_transform",
                status="success",
                rows_written=total_upserted,
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                conn=conn,
            )
            print(f"  [pg] gold.pipeline_runs: logged run {gold_run_id}")

    print(f"\n{'='*60}")
    print(f"  Gold complete  |  {len(written)} outputs")
    print(f"  Embedding dim  |  {_EMBED_DIM}  ({_EMBED_TYPE})")
    print(f"  gold_run_id    |  {gold_run_id}\n")
    return written


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver -> Gold (embeddings + analytics).")
    parser.add_argument("--silver",     default="./data/processed/silver")
    parser.add_argument("--gold",       default="./data/processed/gold")
    parser.add_argument("--model",      default="BAAI/bge-m3")
    parser.add_argument("--batch-size", default=256, type=int)
    args = parser.parse_args()

    result = build_gold(
        silver_dir=args.silver,
        gold_dir=args.gold,
        model_name=args.model,
        batch_size=args.batch_size,
    )
    sys.exit(0 if result else 1)
