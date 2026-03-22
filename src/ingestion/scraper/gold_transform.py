"""
gold_transform.py
─────────────────
Silver → Gold transformation layer.

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

Gold analytics tables
─────────────────────
  gold/brand_performance/
  gold/product_performance/
  gold/demographic_insights/
  gold/category_trends/

Embedding model
───────────────
Default: sentence-transformers all-MiniLM-L6-v2 (384-dim)
  pip install sentence-transformers

Fallback: spaCy en_core_web_md (300-dim) if sentence-transformers missing
  python -m spacy download en_core_web_md

Usage
─────
  python gold_transform.py
  python gold_transform.py --silver ./data/processed/silver --gold ./data/processed/gold
  python gold_transform.py --model all-mpnet-base-v2   # higher quality
  python gold_transform.py --batch-size 512            # faster on good hardware
"""

import argparse
import math
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

from schema import (
    GOLD_REVIEWS_SCHEMA,
    GOLD_PRODUCTS_SCHEMA,
)

# ── CUDA / device detection ────────────────────────────────────────────────────

def _detect_device() -> str:
    """
    Return 'cuda', 'mps', or 'cpu' depending on what's available.
      - CUDA: NVIDIA GPU  (requires: pip install torch --index-url https://download.pytorch.org/whl/cu121)
      - MPS:  Apple Silicon GPU
      - cpu:  fallback
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
    print("  [GPU] No GPU detected — running on CPU (this will be slow).")
    print("        For CUDA:  pip install torch --index-url https://download.pytorch.org/whl/cu121")
    return "cpu"

_DEVICE: str = "cpu"   # set once in build_gold()

# ── Embedding backend ──────────────────────────────────────────────────────────

_EMBED_MODEL = None
_EMBED_DIM   = None
_EMBED_TYPE  = None   # "sbert" | "spacy"


def load_embedding_model(model_name: str = "all-MiniLM-L6-v2"):
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

        # Pin model to GPU memory — avoids repeated host↔device transfers
        if _DEVICE == "cuda":
            _EMBED_MODEL = _EMBED_MODEL.half()   # fp16: halves VRAM, ~same accuracy
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
    Embed a list of texts. None/empty strings get None embedding + 0.0 norm.
    Batch size is automatically scaled up on CUDA for throughput.

    Returns (embeddings, norms) where embeddings is list of float lists.
    """
    # bge-m3 on 8 GB VRAM: keep at 32. Increase only if you have 16 GB+.
    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)

    model = _EMBED_MODEL
    real_idx  = [i for i, t in enumerate(texts) if t and str(t).strip()]
    real_text = [texts[i] for i in real_idx]

    embeddings = [None] * len(texts)
    norms      = [0.0]  * len(texts)

    if not real_text:
        return embeddings, norms

    if _EMBED_TYPE == "sbert":
        vecs = model.encode(
            real_text,
            batch_size=batch_size,
            show_progress_bar=True,     # useful to watch GPU throughput
            convert_to_numpy=True,
            normalize_embeddings=False,
            device=_DEVICE,
        )
        # Cast back to fp32 so PyArrow can handle it (model may be fp16)
        vecs = vecs.astype(np.float32)
        for out_idx, vec in zip(real_idx, vecs):
            embeddings[out_idx] = vec.tolist()
            norms[out_idx]      = float(np.linalg.norm(vec))

        # Free GPU memory after each encode call
        if _DEVICE == "cuda":
            try:
                import torch
                torch.cuda.empty_cache()
            except ImportError:
                pass

    elif _EMBED_TYPE == "spacy":
        docs = model.pipe(real_text, batch_size=batch_size)
        for out_idx, doc in zip(real_idx, docs):
            vec = doc.vector
            embeddings[out_idx] = vec.tolist()
            norms[out_idx]      = float(np.linalg.norm(vec))

    return embeddings, norms

def embed_with_late_chunking(texts: list, short_flags: list, batch_size: int = 256) -> tuple:
    """
    Implements Hierarchical / Late Chunking for a unified vector space.
    Short texts: Embedded directly.
    Long texts: Chunked by sentence, embedded separately, then globally averaged.

    Processes reviews in mini-batches (REVIEW_MINI_BATCH) so that sentence
    chunks from only a subset of reviews are in VRAM at a time — prevents OOM
    on 8 GB cards when processing 200k+ reviews with bge-m3.
    """
    # bge-m3 is ~2.3 GB fp16. Safe encode batch sizes by VRAM:
    #   8 GB → 32    16 GB → 64    24 GB → 128
    # Do NOT auto-scale upward — oversized batches caused the OOM.
    if _DEVICE == "cuda":
        batch_size = min(batch_size, 32)

    # Number of *reviews* to chunk+encode before clearing VRAM.
    # 2 000 reviews ≈ 10–40k sentence chunks — safe for 8 GB with bge-m3.
    REVIEW_MINI_BATCH = 2_000

    import torch

    model      = _EMBED_MODEL
    embeddings = [None] * len(texts)
    norms      = [0.0]  * len(texts)
    n          = len(texts)

    for start in range(0, n, REVIEW_MINI_BATCH):
        end         = min(start + REVIEW_MINI_BATCH, n)
        batch_texts = texts[start:end]
        batch_flags = short_flags[start:end]

        # 1. Build sentence chunks for this mini-batch only
        chunk_map  = []   # chunk index → local index within mini-batch
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

        # 2. Encode all chunks in this mini-batch
        if _EMBED_TYPE == "sbert":
            chunk_vecs = model.encode(
                all_chunks,
                batch_size=batch_size,
                show_progress_bar=False,   # per-mini-batch progress printed below
                convert_to_numpy=True,
                device=_DEVICE,
            ).astype(np.float32)
        elif _EMBED_TYPE == "spacy":
            chunk_vecs = np.array(
                [doc.vector for doc in model.pipe(all_chunks, batch_size=batch_size)],
                dtype=np.float32,
            )

        # 3. Average-pool chunks → one vector per review
        agg_vecs = defaultdict(list)
        for vec, local_i in zip(chunk_vecs, chunk_map):
            agg_vecs[local_i].append(vec)

        for local_i, vecs in agg_vecs.items():
            final_vec = np.mean(vecs, axis=0).astype(np.float32)
            global_i  = start + local_i
            embeddings[global_i] = final_vec.tolist()
            norms[global_i]      = float(np.linalg.norm(final_vec))

        # 4. Release VRAM before next mini-batch
        if _DEVICE == "cuda":
            try:
                torch.cuda.empty_cache()
            except Exception:
                pass

        print(f"    [{min(end, n):,}/{n:,} reviews embedded]")

    return embeddings, norms


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
    base = Path(silver_dir) / table_name
    if not base.exists():
        print(f"  [!] Silver path not found: {base}")
        return None

    partitioning = (
        _SILVER_PRODUCTS_PARTITIONING
        if table_name == "products"
        else _SILVER_REVIEWS_PARTITIONING
    )

    table = ds.dataset(
        str(base), format="parquet", partitioning=partitioning,
    ).to_table()

    # Drop partition-injected category_id to avoid column duplication
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

def _mode(values):
    counts = Counter(v for v in values if v is not None)
    return counts.most_common(1)[0][0] if counts else None

def _mean(values):
    nums = [v for v in values if v is not None]
    return round(sum(nums) / len(nums), 4) if nums else None

def _stddev(values):
    nums = [v for v in values if v is not None]
    if len(nums) < 2:
        return None
    m = sum(nums) / len(nums)
    return round((sum((x - m) ** 2 for x in nums) / len(nums)) ** 0.5, 4)


# ── Gold entity builders ───────────────────────────────────────────────────────

def build_gold_reviews(silver_reviews, gold_run_id, revision_dt, batch_size=256):
    n = len(silver_reviews)

    # ── Pull raw columns once ──────────────────────────────────────────────────
    wordcounts   = silver_reviews["ReviewText_wordcount"].to_pylist()
    review_texts = silver_reviews["ReviewText"].to_pylist()
    titles       = silver_reviews["Title"].to_pylist()
    photo_counts = silver_reviews["ReviewPhotoCount"].to_pylist()
    helpful      = silver_reviews["HelpfulCount"].to_pylist()
    not_helpful  = silver_reviews["NotHelpfulCount"].to_pylist()
    sub_times    = silver_reviews["SubmissionTime"].to_pylist()

    # ── Single metrics loop ────────────────────────────────────────────────────
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

    # ── Embeddings (each called exactly once) ─────────────────────────────────
    print(f"  [gold/reviews] hierarchical embedding for ReviewText ({n:,} rows) ...")
    rt_lemmas        = silver_reviews["ReviewText_lemmas"].to_pylist()
    rt_emb, rt_norms = embed_with_late_chunking(
        texts=rt_lemmas, short_flags=short_flags, batch_size=batch_size
    )

    print(f"  [gold/reviews] embedding Title_lemmas ...")
    ti_lemmas        = silver_reviews["Title_lemmas"].to_pylist()
    ti_emb, ti_norms = embed_texts(ti_lemmas, batch_size=batch_size)

    silver_cols = {col: silver_reviews[col] for col in silver_reviews.schema.names}
    extras = {
        "review_text_embedding": pa.array(rt_emb,         pa.list_(pa.float32())),
        "title_embedding":       pa.array(ti_emb,         pa.list_(pa.float32())),
        "text_quality_score":    pa.array(quality_scores, pa.float32()),
        "helpful_ratio":         pa.array(helpful_ratios, pa.float32()),
        "review_age_days":       pa.array(age_days,       pa.int32()),
        "is_short_review":       pa.array(short_flags,    pa.bool_()),
        "embedding_norm_review": pa.array(rt_norms,       pa.float32()),
        "embedding_norm_title":  pa.array(ti_norms,       pa.float32()),
        "_gold_run_id":          pa.array([gold_run_id]*n, pa.string()),
    }

    combined = {**silver_cols, **extras}
    ordered  = {f.name: combined[f.name] for f in GOLD_REVIEWS_SCHEMA if f.name in combined}
    return pa.table(ordered, schema=GOLD_REVIEWS_SCHEMA)


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
        "product_name_embedding": pa.array(name_emb,         pa.list_(pa.float32())),
        "rating_entropy":         pa.array(entropies,         pa.float32()),
        "polarization_score":     pa.array(polarizations,     pa.float32()),
        "embedding_norm_name":    pa.array(name_norms,        pa.float32()),
        "_gold_run_id":           pa.array([gold_run_id]*n,   pa.string()),
    }

    combined = {**silver_cols, **extras}
    ordered  = {f.name: combined[f.name] for f in GOLD_PRODUCTS_SCHEMA if f.name in combined}
    return pa.table(ordered, schema=GOLD_PRODUCTS_SCHEMA)





# ── Writer ─────────────────────────────────────────────────────────────────────

def _pq_write(table, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Apply BYTE_STREAM_SPLIT only to flat float/int columns (not list<float32>).
    # list<float32> embeddings are not supported by that encoding.
    split_types = (pa.float32(), pa.float64(), pa.int32(), pa.int64())
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
        # If the sum of valid (non-null) rows is 0, the column is empty
        if pc.sum(pc.is_valid(table[name])).as_py() == 0:
            cols_to_drop.append(name)
            
    if cols_to_drop:
        print(f"  [-] Dropping empty columns: {cols_to_drop}")
        return table.drop(cols_to_drop)
    return table


# ── Orchestrator ───────────────────────────────────────────────────────────────

def build_gold(
    silver_dir: str = "./data/processed/silver",
    gold_dir:   str = "./data/processed/gold",
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 256,
) -> dict:
    global _DEVICE
    gold_run_id = uuid.uuid4().hex[:12]
    revision_dt = date.today()
    date_str    = str(revision_dt)

    print(f"\n{'─'*60}")
    print(f"  Gold Build  |  gold_run_id={gold_run_id}  |  {date_str}")
    print(f"{'─'*60}\n")

    print("[0/4] Detecting compute device …")
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

    gold_reviews = build_gold_reviews(silver_reviews, gold_run_id, revision_dt, batch_size)
    gold_reviews = drop_empty_columns(gold_reviews) # NEW: Drop nulls
    
    dest = Path(gold_dir) / "reviews" / f"revision_date={date_str}"
    out  = dest / f"reviews_{gold_run_id}.parquet"
    
    # IMPORTANT: Remove 'schema=GOLD_REVIEWS_SCHEMA' from your pa.table() call 
    # inside build_gold_reviews if you haven't already, otherwise PyArrow will 
    # throw an error when writing a table that is missing schema columns.
    _pq_write(gold_reviews, out)
    print(f"  [+] gold/reviews -> {out}  ({len(gold_reviews):,} rows)")
    written["reviews"] = str(out)

    if silver_products is not None and len(silver_products) > 0:
        gold_products = build_gold_products(silver_products, gold_run_id, revision_dt, batch_size)
        dest = Path(gold_dir) / "products" / f"revision_date={date_str}"
        out  = dest / f"products_{gold_run_id}.parquet"
        _pq_write(gold_products, out)
        print(f"  [+] gold/products -> {out}  ({len(gold_products):,} rows)")
        written["products"] = str(out)
    else:
        print("  [skip] no product rows in Silver")

    print(f"\n{'─'*60}")
    print(f"  Gold complete  |  {len(written)} outputs")
    print(f"  Embedding dim  |  {_EMBED_DIM}  ({_EMBED_TYPE})")
    print(f"  gold_run_id    |  {gold_run_id}\n")
    return written


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver -> Gold (embeddings + analytics).")
    parser.add_argument("--silver",     default="./data/processed/silver")
    parser.add_argument("--gold",       default="./data/processed/gold")
    parser.add_argument("--model",      default="all-MiniLM-L6-v2")
    parser.add_argument("--batch-size", default=256, type=int)
    args = parser.parse_args()

    result = build_gold(
        silver_dir=args.silver,
        gold_dir=args.gold,
        model_name=args.model,
        batch_size=args.batch_size,
    )
    sys.exit(0 if result else 1)