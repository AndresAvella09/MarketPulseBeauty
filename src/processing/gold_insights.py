"""
gold_insights.py
────────────────
Insight computations layered on top of Silver during the Gold build.

Provides:
  • compute_sentiment             — VADER per review (compound + label)
  • classify_focus_keyword_series — vectorized family taxonomy
  • compute_health_score          — per-product 0-100 score
  • cluster_topics                — BERTopic over review embeddings (with fallback)
  • build_monthly_insights        — per-product monthly snapshot
  • build_review_themes           — theme rollup per product
  • detect_search_spikes          — % change vs trailing 4 weeks
"""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd

from src.dashboard.utils import classify_focus_keyword


# ── Sentiment (VADER) ─────────────────────────────────────────────────────────

_SIA = None


def _get_sia():
    global _SIA
    if _SIA is not None:
        return _SIA
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon", quiet=True)
    _SIA = SentimentIntensityAnalyzer()
    return _SIA


def compute_sentiment(texts: Iterable[str]) -> tuple[np.ndarray, np.ndarray]:
    """Return (compound scores in [-1,1], labels in {'positive','neutral','negative'})."""
    sia = _get_sia()
    if not isinstance(texts, list):
        texts = list(texts)
    n = len(texts)
    scores = np.empty(n, dtype=np.float32)
    for i in range(n):
        t = texts[i]
        scores[i] = sia.polarity_scores(t if t else "")["compound"]
        texts[i] = None
    labels = np.where(scores >= 0.05, "positive",
              np.where(scores <= -0.05, "negative", "neutral"))
    return scores, labels


def polarity_from_score(score: float) -> str:
    if score is None or (isinstance(score, float) and math.isnan(score)):
        return "neu"
    if score >= 0.05:
        return "pos"
    if score <= -0.05:
        return "neg"
    return "neu"


# ── Focus keyword (family taxonomy) ───────────────────────────────────────────

def classify_focus_keyword_series(names: pd.Series) -> pd.Series:
    return names.fillna("").apply(classify_focus_keyword)


# ── Health Score (per product, 0-100) ─────────────────────────────────────────
#
# Formula: weighted combination of normalized rating, log-volume, and average
# sentiment. Mirrors the design's threshold zones (≥70 green, 45-69 amber, <45
# red) in interpretation.

def _to_float(value) -> float | None:
    """Coerce pandas NA/NaN/None and numpy scalars to a plain float or None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_health_score(
    avg_rating: float | None,
    review_count: int | None,
    pct_recommended: float | None = None,
    avg_sentiment: float | None = None,
    *,
    log_volume_cap: float = math.log10(5000),
) -> float | None:
    rating = _to_float(avg_rating)
    count = _to_float(review_count)
    if rating is None or count is None:
        return None
    rating_norm = max(0.0, min(1.0, (rating - 1.0) / 4.0))   # 1-5 → 0-1
    vol = max(0, int(count))
    volume_norm = min(1.0, math.log10(vol + 1) / log_volume_cap) if log_volume_cap > 0 else 0.0
    pr = _to_float(pct_recommended)
    rec_norm = pr if pr is not None else rating_norm
    sent = _to_float(avg_sentiment)
    sent_norm = ((sent + 1.0) / 2.0) if sent is not None else rating_norm

    score = (
        0.45 * rating_norm
        + 0.20 * volume_norm
        + 0.15 * rec_norm
        + 0.20 * sent_norm
    )
    return round(100.0 * max(0.0, min(1.0, score)), 2)


# ── Topic clustering (BERTopic with fallback) ─────────────────────────────────

def cluster_topics(
    texts: list[str],
    embeddings: np.ndarray | None = None,
    min_topic_size: int = 15,
    language: str = "english",
) -> tuple[np.ndarray, dict[int, str]]:
    """
    Cluster reviews into topics using BERTopic when available.

    Returns (topic_id per text, {topic_id: human-readable label}).
    Topic id -1 is BERTopic's "noise" / outlier bucket.
    Falls back to a single-topic assignment when BERTopic isn't installed
    or the corpus is too small to cluster.
    """
    n = len(texts)
    if n < max(min_topic_size * 2, 30):
        labels = {0: "general"}
        return np.zeros(n, dtype=np.int32), labels

    try:
        from bertopic import BERTopic
    except ImportError:
        print("  [topics] BERTopic not installed — assigning single 'general' topic.")
        print("           Install with: pip install bertopic")
        return np.zeros(n, dtype=np.int32), {0: "general"}

    print(f"  [topics] BERTopic on {n:,} reviews ...")

    # For large corpora the default UMAP/HDBSCAN configs in BERTopic OOM the
    # worker. Plug in low-memory UMAP + an HDBSCAN that doesn't keep extra
    # prediction state. Activated above ~50k rows; smaller corpora use the
    # defaults (which give better topic quality).
    umap_model = None
    hdbscan_model = None
    if n >= 50_000:
        try:
            from umap import UMAP
            umap_model = UMAP(
                n_neighbors=10,
                n_components=5,
                min_dist=0.0,
                metric="cosine",
                low_memory=True,
                random_state=42,
            )
        except ImportError:
            pass
        try:
            from hdbscan import HDBSCAN
            hdbscan_model = HDBSCAN(
                min_cluster_size=min_topic_size,
                metric="euclidean",
                cluster_selection_method="eom",
                prediction_data=True,
                core_dist_n_jobs=1,
            )
        except ImportError:
            pass

    from sklearn.feature_extraction.text import CountVectorizer
    vectorizer_model = CountVectorizer(
        min_df=10,
        max_df=0.95,
        stop_words="english" if language == "english" else None,
        ngram_range=(1, 2),
        max_features=20_000,
    )

    bertopic_kwargs = dict(
        language=language,
        min_topic_size=min_topic_size,
        calculate_probabilities=False,
        verbose=False,
        vectorizer_model=vectorizer_model,
    )
    if umap_model is not None:
        bertopic_kwargs["umap_model"] = umap_model
    if hdbscan_model is not None:
        bertopic_kwargs["hdbscan_model"] = hdbscan_model

    model = BERTopic(**bertopic_kwargs)
    safe_texts = [t if (t and isinstance(t, str)) else " " for t in texts]
    has_embs = embeddings is not None and len(embeddings) == n

    # For very large corpora UMAP's kNN graph OOMs the worker even with
    # low_memory=True. Fit on a stratified random sample, then assign topics
    # to the full corpus in batches via approximate predict.
    fit_sample_cap = 60_000
    if has_embs and n > fit_sample_cap:
        rng = np.random.default_rng(42)
        sample_idx = rng.choice(n, size=fit_sample_cap, replace=False)
        sample_idx.sort()
        sample_texts = [safe_texts[i] for i in sample_idx]
        sample_embs = np.asarray(embeddings)[sample_idx]
        print(f"  [topics] fitting on {fit_sample_cap:,} sampled reviews, then transforming the rest ...")
        model.fit(sample_texts, embeddings=sample_embs)

        topics_arr = np.empty(n, dtype=np.int32)
        batch = 20_000
        embs_full = np.asarray(embeddings)
        for start in range(0, n, batch):
            end = min(start + batch, n)
            batch_topics, _ = model.transform(
                safe_texts[start:end], embeddings=embs_full[start:end]
            )
            topics_arr[start:end] = np.asarray(batch_topics, dtype=np.int32)
    else:
        if has_embs:
            topics, _ = model.fit_transform(safe_texts, embeddings=embeddings)
        else:
            topics, _ = model.fit_transform(safe_texts)
        topics_arr = np.asarray(topics, dtype=np.int32)
    info = model.get_topic_info()  # DataFrame with Topic, Count, Name, Representation
    labels: dict[int, str] = {}
    for _, row in info.iterrows():
        tid = int(row["Topic"])
        if tid == -1:
            labels[tid] = "outlier"
            continue
        repr_words = row.get("Representation") or []
        if isinstance(repr_words, str):
            repr_words = [w.strip() for w in repr_words.split(",") if w.strip()]
        top_words = [w for w in repr_words[:3] if w]
        labels[tid] = " · ".join(top_words) if top_words else f"topic_{tid}"
    return topics_arr, labels


# ── 2D projection for the review map (UMAP n_components=2) ───────────────────
#
# Full embeddings stay in MinIO; only (x, y) per review are promoted to Postgres
# so the dashboard can render the "Mapa de reseñas" without re-running UMAP.

def compute_2d_projection(
    embeddings: np.ndarray,
    sample_cap: int = 60_000,
    random_state: int = 42,
) -> np.ndarray:
    """Return an (n, 2) float32 array of UMAP coords aligned to *embeddings*.

    For corpora larger than *sample_cap*, fits UMAP on a random sample then
    transforms the rest in batches — same strategy as cluster_topics."""
    n = len(embeddings)
    coords = np.full((n, 2), np.nan, dtype=np.float32)
    if n == 0:
        return coords

    try:
        from umap import UMAP
    except ImportError:
        print("  [proj] umap-learn not installed — skipping 2D projection.")
        return coords

    embs = np.asarray(embeddings, dtype=np.float32)
    model = UMAP(
        n_neighbors=15,
        n_components=2,
        min_dist=0.1,
        metric="cosine",
        low_memory=True,
        random_state=random_state,
    )

    if n > sample_cap:
        rng = np.random.default_rng(random_state)
        sample_idx = rng.choice(n, size=sample_cap, replace=False)
        sample_idx.sort()
        print(f"  [proj] fitting 2D UMAP on {sample_cap:,} sampled reviews ...")
        model.fit(embs[sample_idx])
        batch = 20_000
        for start in range(0, n, batch):
            end = min(start + batch, n)
            coords[start:end] = model.transform(embs[start:end]).astype(np.float32)
    else:
        print(f"  [proj] fitting 2D UMAP on {n:,} reviews ...")
        coords[:] = model.fit_transform(embs).astype(np.float32)

    return coords


# ── Monthly insights (one row per product × month) ────────────────────────────

def build_monthly_insights(reviews_df: pd.DataFrame) -> pd.DataFrame:
    """reviews_df must have: ProductID, SubmissionTime (datetime), Rating,
    IsRecommended, sentiment_label, helpful_ratio.
    Output columns match GOLD_PRODUCT_INSIGHTS_MONTHLY_SCHEMA (minus audit)."""
    if reviews_df.empty:
        return pd.DataFrame()

    df = reviews_df.copy()
    df["SubmissionTime"] = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)
    df = df.dropna(subset=["SubmissionTime"])
    if df.empty:
        return pd.DataFrame()

    df["month"] = df["SubmissionTime"].dt.tz_convert(None).dt.to_period("M").dt.to_timestamp().dt.date

    grouped = df.groupby(["ProductID", "month"], sort=False)

    out = grouped.agg(
        reviews_count=("ReviewID", "count"),
        avg_rating=("Rating", "mean"),
        rec_count=("IsRecommended", lambda s: int(s.fillna(False).astype(bool).sum())),
        pos_count=("sentiment_label", lambda s: int((s == "positive").sum())),
        neu_count=("sentiment_label", lambda s: int((s == "neutral").sum())),
        neg_count=("sentiment_label", lambda s: int((s == "negative").sum())),
        avg_helpful_ratio=("helpful_ratio", "mean"),
        avg_sentiment=("sentiment_score", "mean"),
    ).reset_index()

    # Cast to plain numpy dtypes so arithmetic works regardless of the input
    # frame's Arrow-backed extension types.
    for c in ("reviews_count", "rec_count", "pos_count", "neu_count", "neg_count"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")
    for c in ("avg_rating", "avg_helpful_ratio", "avg_sentiment"):
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    n = out["reviews_count"].clip(lower=1).astype("float64")
    out["pct_recommended"] = (out["rec_count"].astype("float64") / n).astype("float32")
    out["pct_positive"]    = (out["pos_count"].astype("float64") / n).astype("float32")
    out["pct_neutral"]     = (out["neu_count"].astype("float64") / n).astype("float32")
    out["pct_negative"]    = (out["neg_count"].astype("float64") / n).astype("float32")

    out["health_score"] = [
        compute_health_score(r, c, pr, s)
        for r, c, pr, s in zip(
            out["avg_rating"], out["reviews_count"],
            out["pct_recommended"], out["avg_sentiment"],
        )
    ]

    return out[[
        "ProductID", "month", "reviews_count", "avg_rating",
        "pct_recommended", "pct_positive", "pct_neutral", "pct_negative",
        "avg_helpful_ratio", "health_score",
    ]]


# ── Theme rollup (one row per product × topic) ────────────────────────────────

def build_review_themes(
    reviews_df: pd.DataFrame,
    topic_labels: dict[int, str],
) -> pd.DataFrame:
    """reviews_df must have: ProductID, topic_id, sentiment_score."""
    if reviews_df.empty:
        return pd.DataFrame()

    df = reviews_df.dropna(subset=["topic_id"]).copy()
    df = df[df["topic_id"] >= 0]  # drop BERTopic noise bucket
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby(["ProductID", "topic_id"], sort=False)
    out = grouped.agg(
        count=("ReviewID", "count"),
        avg_sentiment=("sentiment_score", "mean"),
    ).reset_index()

    totals = df.groupby("ProductID")["ReviewID"].count().rename("total")
    out = out.merge(totals, left_on="ProductID", right_index=True, how="left")
    out["pct"] = (out["count"] / out["total"]).astype("float32")
    out = out.drop(columns=["total"])

    out["theme_label"] = out["topic_id"].map(topic_labels).fillna("topic")
    out["polarity"]    = out["avg_sentiment"].apply(polarity_from_score)
    out["topic_id"]    = out["topic_id"].astype("int32")
    out["count"]       = out["count"].astype("int32")
    return out[[
        "ProductID", "topic_id", "theme_label", "polarity",
        "count", "pct", "avg_sentiment",
    ]]


# ── Search spike detection (Google Trends) ────────────────────────────────────

def detect_search_spikes(
    trends_df: pd.DataFrame,
    *,
    threshold_pct: float = 0.10,
    trailing_weeks: int = 4,
) -> pd.DataFrame:
    """Identify weeks where interest jumped at least *threshold_pct* vs the
    trailing *trailing_weeks* mean, per (keyword, geo).

    trends_df columns: keyword, geo, date, interest.
    Output columns: keyword, geo, start_date, end_date, pct_change.
    """
    if trends_df.empty:
        return pd.DataFrame(columns=["keyword", "geo", "start_date", "end_date", "pct_change"])

    df = trends_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values(["keyword", "geo", "date"])

    rows = []
    for (kw, geo), sub in df.groupby(["keyword", "geo"], sort=False):
        sub = sub.reset_index(drop=True)
        if len(sub) <= trailing_weeks:
            continue
        rolling = sub["interest"].rolling(window=trailing_weeks, min_periods=trailing_weeks).mean().shift(1)
        for i in range(trailing_weeks, len(sub)):
            base = rolling.iloc[i]
            cur = sub["interest"].iloc[i]
            if base is None or pd.isna(base) or base <= 0:
                continue
            pct = (cur - base) / base
            if pct >= threshold_pct:
                rows.append({
                    "keyword":    kw,
                    "geo":        geo,
                    "start_date": sub["date"].iloc[i - trailing_weeks].date(),
                    "end_date":   sub["date"].iloc[i].date(),
                    "pct_change": float(pct),
                })

    return pd.DataFrame(rows, columns=["keyword", "geo", "start_date", "end_date", "pct_change"])


# ── Per-product derived extras (first/last review, velocity, top quotes, …) ───
#
# Computed once per gold build and merged onto gold.products so the dashboard
# reads them without scanning gold.reviews.

import json as _json

_DEMOGRAPHIC_DIMS = (
    "skinTone", "skinType", "eyeColor", "hairColor",
    "hairType", "hairConcerns", "skinConcerns", "ageRange",
)

# Map demographic_dim names to the snake_case Postgres value used downstream.
_DEMO_DIM_PG = {
    "skinTone":     "skin_tone",
    "skinType":     "skin_type",
    "eyeColor":     "eye_color",
    "hairColor":    "hair_color",
    "hairType":     "hair_type",
    "hairConcerns": "hair_concerns",
    "skinConcerns": "skin_concerns",
    "ageRange":     "age_range",
}

# Maps Google Trends keyword strings to product family codes. Trends keywords
# are configured in dag_04_google_trends.py — keep this in sync.
_TRENDS_TO_FAMILY = {
    "niacinamide":       "niacinamida",
    "niacinamida":       "niacinamida",
    "hyaluronic acid":   "acido_hialuronico",
    "hyaluronic":        "acido_hialuronico",
    "acido hialuronico": "acido_hialuronico",
    "sulfate free shampoo": "shampoo_sin_sulfatos",
    "shampoo sin sulfatos": "shampoo_sin_sulfatos",
    "sulfate-free shampoo": "shampoo_sin_sulfatos",
}


def _safe_top_quote(group: pd.DataFrame, label: str) -> str | None:
    """Pick the highest-`text_quality_score` review in a group with the given
    sentiment_label. Return a JSON string or None."""
    sub = group[group["sentiment_label"] == label]
    if sub.empty:
        return None
    sub = sub.sort_values("text_quality_score", ascending=False, na_position="last")
    row = sub.iloc[0]
    text = row.get("ReviewText")
    if not text or pd.isna(text):
        return None
    payload = {
        "review_id": str(row.get("ReviewID")) if row.get("ReviewID") is not None else None,
        "rating":    int(row.get("Rating")) if pd.notna(row.get("Rating")) else None,
        "title":     row.get("Title") if pd.notna(row.get("Title")) else None,
        "text":      str(text)[:500],
        "skin_type": row.get("skinType") if pd.notna(row.get("skinType")) else None,
        "age_range": row.get("ageRange") if pd.notna(row.get("ageRange")) else None,
    }
    return _json.dumps(payload, ensure_ascii=False)


def _top_locations_json(group: pd.DataFrame, k: int = 5) -> str | None:
    locs = group["UserLocation"].dropna()
    if locs.empty:
        return None
    counts = locs.value_counts().head(k)
    items = [{"location": str(loc), "count": int(c)} for loc, c in counts.items()]
    return _json.dumps(items, ensure_ascii=False)


def derive_product_extras(
    reviews_df: pd.DataFrame,
    revision_dt,
) -> pd.DataFrame:
    """One row per ProductID with the columns added to gold.products beyond
    the original aggregates."""
    if reviews_df.empty:
        return pd.DataFrame()

    df = reviews_df.copy()
    df["SubmissionTime"] = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)

    rev_dt_ts = pd.Timestamp(revision_dt, tz="UTC")
    cutoff_30 = rev_dt_ts - pd.Timedelta(days=30)
    cutoff_60 = rev_dt_ts - pd.Timedelta(days=60)

    rows = []
    for pid, group in df.groupby("ProductID", sort=False):
        sub_times = group["SubmissionTime"].dropna()
        first_dt = sub_times.min().date() if not sub_times.empty else None
        last_dt = sub_times.max().date() if not sub_times.empty else None

        v30 = int(((sub_times >= cutoff_30) & (sub_times <= rev_dt_ts)).sum()) if not sub_times.empty else 0
        v_prior = int(((sub_times >= cutoff_60) & (sub_times < cutoff_30)).sum()) if not sub_times.empty else 0

        photos = group["ReviewPhotoCount"].fillna(0).astype("float64")
        photo_cov = float((photos > 0).mean()) if len(photos) else None

        # edit_rate: share of reviews where LastModTime is more than 1 day after SubmissionTime
        if "LastModTime" in group.columns:
            last_mod = pd.to_datetime(group["LastModTime"], errors="coerce", utc=True)
            sub = pd.to_datetime(group["SubmissionTime"], errors="coerce", utc=True)
            edited = ((last_mod - sub) > pd.Timedelta(days=1)).fillna(False)
            edit_rate = float(edited.mean()) if len(edited) else None
        else:
            edit_rate = None

        rows.append({
            "ProductID":                 pid,
            "first_review_date":         first_dt,
            "last_review_date":          last_dt,
            "review_velocity_30d":       v30,
            "review_velocity_prior_30d": v_prior,
            "photo_coverage":            photo_cov,
            "edit_rate":                 edit_rate,
            "top_quote_positive":        _safe_top_quote(group, "positive"),
            "top_quote_negative":        _safe_top_quote(group, "negative"),
            "top_quote_neutral":         _safe_top_quote(group, "neutral"),
            "top_locations":             _top_locations_json(group),
        })

    return pd.DataFrame(rows)


# ── Brand-level rollup ────────────────────────────────────────────────────────

def build_brand_aggregates(products_df: pd.DataFrame) -> pd.DataFrame:
    """One row per brand. Reads the slim products frame after derived extras
    have been merged. Output matches GOLD_BRANDS_SCHEMA (minus audit)."""
    if products_df.empty:
        return pd.DataFrame()

    df = products_df.copy()
    df["TotalReviewCount"] = pd.to_numeric(df["TotalReviewCount"], errors="coerce").fillna(0)
    df["RecommendedCount"] = pd.to_numeric(df["RecommendedCount"], errors="coerce").fillna(0)

    rows = []
    for brand, group in df.groupby("Brand", dropna=True, sort=False):
        total_reviews = int(group["TotalReviewCount"].sum())
        rec = float(group["RecommendedCount"].sum())
        weights = group["TotalReviewCount"].astype(float)
        wsum = weights.sum()

        def _wmean(col: str) -> float | None:
            vals = pd.to_numeric(group[col], errors="coerce")
            mask = vals.notna() & (weights > 0)
            if not mask.any():
                return None
            return float((vals[mask] * weights[mask]).sum() / weights[mask].sum())

        family_share = group["focus_keyword"].value_counts(normalize=True).to_dict()
        top_pid = (
            group.sort_values("health_score", ascending=False, na_position="last")
                 .iloc[0]["ProductID"]
            if not group.empty else None
        )

        rows.append({
            "brand":                      brand,
            "products_count":             int(len(group)),
            "total_reviews":              total_reviews,
            "avg_rating":                 _wmean("AvgRating"),
            "pct_recommended":            (rec / wsum) if wsum > 0 else None,
            "avg_sentiment":              _wmean("avg_sentiment"),
            "avg_health_score":           _wmean("health_score"),
            "polarization_score":         _wmean("polarization_score"),
            "share_niacinamida":          float(family_share.get("niacinamida", 0.0)),
            "share_acido_hialuronico":    float(family_share.get("acido_hialuronico", 0.0)),
            "share_shampoo_sin_sulfatos": float(family_share.get("shampoo_sin_sulfatos", 0.0)),
            "top_product_id":             top_pid,
        })
    return pd.DataFrame(rows)


# ── Family-level rollup (one row per focus_keyword) ───────────────────────────

def build_product_family_aggregates(products_df: pd.DataFrame) -> pd.DataFrame:
    if products_df.empty:
        return pd.DataFrame()
    df = products_df.copy()
    df = df[df["focus_keyword"].notna()]
    if df.empty:
        return pd.DataFrame()

    df["TotalReviewCount"] = pd.to_numeric(df["TotalReviewCount"], errors="coerce").fillna(0)
    df["RecommendedCount"] = pd.to_numeric(df["RecommendedCount"], errors="coerce").fillna(0)

    rows = []
    for fk, group in df.groupby("focus_keyword", sort=False):
        total = int(group["TotalReviewCount"].sum())
        rec = float(group["RecommendedCount"].sum())
        weights = group["TotalReviewCount"].astype(float)
        wsum = weights.sum()

        def _wmean(col: str) -> float | None:
            vals = pd.to_numeric(group[col], errors="coerce")
            mask = vals.notna() & (weights > 0)
            if not mask.any():
                return None
            return float((vals[mask] * weights[mask]).sum() / weights[mask].sum())

        hs = pd.to_numeric(group["health_score"], errors="coerce").dropna()
        top_brand_row = group.groupby("Brand", dropna=True)["TotalReviewCount"].sum().sort_values(ascending=False)
        top_brand = top_brand_row.index[0] if not top_brand_row.empty else None
        top_pid = (
            group.sort_values("health_score", ascending=False, na_position="last")
                 .iloc[0]["ProductID"] if not group.empty else None
        )

        rows.append({
            "focus_keyword":     fk,
            "products_count":    int(len(group)),
            "brands_count":      int(group["Brand"].dropna().nunique()),
            "total_reviews":     total,
            "avg_rating":        _wmean("AvgRating"),
            "avg_sentiment":     _wmean("avg_sentiment"),
            "pct_recommended":   (rec / wsum) if wsum > 0 else None,
            "health_score_p50":  float(hs.median()) if not hs.empty else None,
            "health_score_p90":  float(hs.quantile(0.9)) if not hs.empty else None,
            "top_brand":         top_brand,
            "top_product_id":    top_pid,
        })
    return pd.DataFrame(rows)


# ── Demographic insights (family × dim × value) ───────────────────────────────

def build_demographic_insights(reviews_df: pd.DataFrame) -> pd.DataFrame:
    if reviews_df.empty:
        return pd.DataFrame()
    df = reviews_df.copy()
    if "focus_keyword" not in df.columns:
        return pd.DataFrame()
    df = df[df["focus_keyword"].notna()]
    if df.empty:
        return pd.DataFrame()

    out_rows = []
    for dim_camel in _DEMOGRAPHIC_DIMS:
        if dim_camel not in df.columns:
            continue
        dim_pg = _DEMO_DIM_PG[dim_camel]
        sub = df[df[dim_camel].notna() & (df[dim_camel].astype(str).str.len() > 0)]
        if sub.empty:
            continue
        grouped = sub.groupby(["focus_keyword", dim_camel], sort=False)
        agg = grouped.agg(
            reviews_count=("ReviewID", "count"),
            avg_rating=("Rating", "mean"),
            avg_sentiment=("sentiment_score", "mean"),
            rec_count=("IsRecommended", lambda s: int(s.fillna(False).astype(bool).sum())),
            pos_count=("sentiment_label", lambda s: int((s == "positive").sum())),
            neg_count=("sentiment_label", lambda s: int((s == "negative").sum())),
        ).reset_index()
        n = agg["reviews_count"].clip(lower=1).astype("float64")
        agg["pct_recommended"] = (agg["rec_count"].astype("float64") / n).astype("float32")
        agg["pct_positive"] = (agg["pos_count"].astype("float64") / n).astype("float32")
        agg["pct_negative"] = (agg["neg_count"].astype("float64") / n).astype("float32")
        agg["demographic_dim"] = dim_pg
        agg = agg.rename(columns={dim_camel: "demographic_value"})
        out_rows.append(agg[[
            "focus_keyword", "demographic_dim", "demographic_value",
            "reviews_count", "avg_rating", "avg_sentiment",
            "pct_recommended", "pct_positive", "pct_negative",
        ]])

    if not out_rows:
        return pd.DataFrame()
    out = pd.concat(out_rows, ignore_index=True)
    out["reviews_count"] = out["reviews_count"].astype("int32")
    return out


# ── Daily insights (last 90 days only) ────────────────────────────────────────

def build_daily_insights(reviews_df: pd.DataFrame, revision_dt, lookback_days: int = 90) -> pd.DataFrame:
    if reviews_df.empty:
        return pd.DataFrame()
    df = reviews_df.copy()
    df["SubmissionTime"] = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)
    df = df.dropna(subset=["SubmissionTime"])
    if df.empty:
        return pd.DataFrame()

    rev_ts = pd.Timestamp(revision_dt, tz="UTC")
    cutoff = rev_ts - pd.Timedelta(days=lookback_days)
    df = df[df["SubmissionTime"] >= cutoff]
    if df.empty:
        return pd.DataFrame()

    df["day"] = df["SubmissionTime"].dt.tz_convert(None).dt.date
    grouped = df.groupby(["ProductID", "day"], sort=False)
    out = grouped.agg(
        reviews_count=("ReviewID", "count"),
        avg_rating=("Rating", "mean"),
        pos_count=("sentiment_label", lambda s: int((s == "positive").sum())),
        neg_count=("sentiment_label", lambda s: int((s == "negative").sum())),
    ).reset_index()
    n = out["reviews_count"].clip(lower=1).astype("float64")
    out["pct_positive"] = (out["pos_count"].astype("float64") / n).astype("float32")
    out["pct_negative"] = (out["neg_count"].astype("float64") / n).astype("float32")
    out["reviews_count"] = out["reviews_count"].astype("int32")
    out["avg_rating"] = pd.to_numeric(out["avg_rating"], errors="coerce").astype("float32")
    return out[["ProductID", "day", "reviews_count", "avg_rating", "pct_positive", "pct_negative"]]


# ── Family demand (search) × supply (reviews) per month ───────────────────────

def build_family_demand_supply(
    monthly_df: pd.DataFrame,
    products_df: pd.DataFrame,
    trends_df: pd.DataFrame,
) -> pd.DataFrame:
    """monthly_df = output of build_monthly_insights, products_df has focus_keyword
    per ProductID, trends_df = gold.search_trends rows."""
    if monthly_df.empty:
        return pd.DataFrame()

    pf = products_df[["ProductID", "focus_keyword"]].dropna(subset=["focus_keyword"])
    m = monthly_df.merge(pf, on="ProductID", how="inner")
    if m.empty:
        return pd.DataFrame()

    m["month"] = pd.to_datetime(m["month"], errors="coerce").dt.date
    supply = (
        m.groupby(["focus_keyword", "month"], sort=False)
         .agg(
             reviews_count=("reviews_count", "sum"),
             avg_sentiment=("pct_positive", "mean"),  # placeholder; overridden below if sentiment column exists
         )
         .reset_index()
    )
    # Recompute avg_sentiment as a weighted mean if the source rows carried it.
    if "avg_sentiment" in monthly_df.columns:
        sent = (
            m.groupby(["focus_keyword", "month"], sort=False)
             .apply(lambda g: float(np.average(
                 pd.to_numeric(g.get("avg_sentiment", pd.Series(dtype=float)), errors="coerce").fillna(0),
                 weights=pd.to_numeric(g["reviews_count"], errors="coerce").fillna(0).clip(lower=0),
             )) if g["reviews_count"].sum() > 0 else None)
             .rename("avg_sentiment_w").reset_index()
        )
        supply = supply.drop(columns=["avg_sentiment"]).merge(
            sent, on=["focus_keyword", "month"], how="left"
        ).rename(columns={"avg_sentiment_w": "avg_sentiment"})

    # Demand side: aggregate search interest to monthly avg per family.
    if trends_df is None or trends_df.empty:
        supply["search_interest_avg"] = None
        return supply[["focus_keyword", "month", "search_interest_avg", "reviews_count", "avg_sentiment"]]

    t = trends_df.copy()
    t["family"] = t["keyword"].astype(str).str.lower().map(_TRENDS_TO_FAMILY)
    t = t.dropna(subset=["family"])
    if t.empty:
        supply["search_interest_avg"] = None
        return supply[["focus_keyword", "month", "search_interest_avg", "reviews_count", "avg_sentiment"]]

    t["date"] = pd.to_datetime(t["date"], errors="coerce")
    t = t.dropna(subset=["date"])
    t["month"] = t["date"].dt.to_period("M").dt.to_timestamp().dt.date
    demand = (
        t.groupby(["family", "month"], sort=False)["interest"]
         .mean().rename("search_interest_avg").reset_index()
         .rename(columns={"family": "focus_keyword"})
    )

    out = supply.merge(demand, on=["focus_keyword", "month"], how="outer")
    out["reviews_count"] = pd.to_numeric(out["reviews_count"], errors="coerce").fillna(0).astype("int32")
    out["search_interest_avg"] = pd.to_numeric(out["search_interest_avg"], errors="coerce").astype("float32")
    out["avg_sentiment"] = pd.to_numeric(out["avg_sentiment"], errors="coerce").astype("float32")
    return out[["focus_keyword", "month", "search_interest_avg", "reviews_count", "avg_sentiment"]]
