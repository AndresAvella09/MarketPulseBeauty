"""
gold_writer.py
──────────────
Bulk upserts for the slim Gold layer in PostgreSQL.

All embeddings/tokens/lemmas live in MinIO under marketpulse-gold/embeddings/.
Postgres only stores insights — small, indexed, query-ready.

Tables: products, reviews, product_insights_monthly, review_themes,
        search_trends, search_spikes, pipeline_runs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
from sqlalchemy import text

try:
    from psycopg2.extensions import register_adapter, AsIs

    def _adapt_np_float(v):
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return AsIs("NULL")
        return AsIs(repr(f))

    def _adapt_np_int(v):
        return AsIs(int(v))

    for _t in (np.float32, np.float64, np.float16):
        register_adapter(_t, _adapt_np_float)
    for _t in (np.int8, np.int16, np.int32, np.int64,
               np.uint8, np.uint16, np.uint32, np.uint64):
        register_adapter(_t, _adapt_np_int)

    def _adapt_np_bool(v):
        return AsIs("TRUE" if bool(v) else "FALSE")

    register_adapter(np.bool_, _adapt_np_bool)

    def _adapt_na(_v):
        return AsIs("NULL")

    register_adapter(type(pd.NA), _adapt_na)
except ImportError:
    pass


# ── Column maps: PyArrow (PascalCase) -> PostgreSQL (snake_case) ──────────────

PRODUCTS_COL_MAP = {
    "ProductID":                 "product_id",
    "Brand":                     "brand",
    "ProductName":               "product_name",
    "ProductCategory":           "product_category",
    "CategoryId":                "category_id",
    "ProductPageUrl":            "product_page_url",
    "focus_keyword":             "focus_keyword",
    "AvgRating":                 "avg_rating",
    "TotalReviewCount":          "total_review_count",
    "RecommendedCount":          "recommended_count",
    "TotalPhotoCount":           "total_photo_count",
    "RatingDist_1":              "rating_dist_1",
    "RatingDist_2":              "rating_dist_2",
    "RatingDist_3":              "rating_dist_3",
    "RatingDist_4":              "rating_dist_4",
    "RatingDist_5":              "rating_dist_5",
    "pct_recommended":           "pct_recommended",
    "rating_entropy":            "rating_entropy",
    "polarization_score":        "polarization_score",
    "avg_sentiment":             "avg_sentiment",
    "health_score":              "health_score",
    "first_review_date":         "first_review_date",
    "last_review_date":          "last_review_date",
    "review_velocity_30d":       "review_velocity_30d",
    "review_velocity_prior_30d": "review_velocity_prior_30d",
    "photo_coverage":            "photo_coverage",
    "edit_rate":                 "edit_rate",
    "top_quote_positive":        "top_quote_positive",
    "top_quote_negative":        "top_quote_negative",
    "top_quote_neutral":         "top_quote_neutral",
    "top_locations":             "top_locations",
    "revision_date":             "revision_date",
    "_gold_run_id":              "gold_run_id",
}

REVIEWS_COL_MAP = {
    "ProductID":             "product_id",
    "ReviewID":              "review_id",
    "Rating":                "rating",
    "Title":                 "title",
    "ReviewText":            "review_text",
    "SubmissionTime":        "submission_time",
    "LastModTime":           "last_mod_time",
    "IsRecommended":         "is_recommended",
    "ReviewPhotoCount":      "review_photo_count",
    "HelpfulCount":          "helpful_count",
    "NotHelpfulCount":       "not_helpful_count",
    "IsFeatured":            "is_featured",
    "IsIncentivized":        "is_incentivized",
    "IsStaffReview":         "is_staff_review",
    "UserLocation":          "user_location",
    "skinTone":              "skin_tone",
    "skinType":              "skin_type",
    "eyeColor":              "eye_color",
    "hairColor":             "hair_color",
    "hairType":              "hair_type",
    "hairConcerns":          "hair_concerns",
    "skinConcerns":          "skin_concerns",
    "ageRange":              "age_range",
    "helpful_ratio":         "helpful_ratio",
    "review_age_days":       "review_age_days",
    "is_short_review":       "is_short_review",
    "text_quality_score":    "text_quality_score",
    "ReviewText_wordcount":  "review_text_wordcount",
    "ReviewText_lemmas":     "review_text_lemmas",
    "Title_lemmas":          "title_lemmas",
    "sentiment_score":       "sentiment_score",
    "sentiment_label":       "sentiment_label",
    "topic_id":              "topic_id",
    "topic_label":           "topic_label",
    "umap_x":                "umap_x",
    "umap_y":                "umap_y",
    "revision_date":         "revision_date",
    "_gold_run_id":          "gold_run_id",
}

INSIGHTS_MONTHLY_COL_MAP = {
    "ProductID":            "product_id",
    "month":                "month",
    "reviews_count":        "reviews_count",
    "avg_rating":           "avg_rating",
    "pct_recommended":      "pct_recommended",
    "pct_positive":         "pct_positive",
    "pct_neutral":          "pct_neutral",
    "pct_negative":         "pct_negative",
    "avg_helpful_ratio":    "avg_helpful_ratio",
    "health_score":         "health_score",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

THEMES_COL_MAP = {
    "ProductID":            "product_id",
    "topic_id":             "topic_id",
    "theme_label":          "theme_label",
    "polarity":             "polarity",
    "count":                "count",
    "pct":                  "pct",
    "avg_sentiment":        "avg_sentiment",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

TRENDS_COL_MAP = {
    "keyword":              "keyword",
    "geo":                  "geo",
    "date":                 "date",
    "interest":             "interest",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

SPIKES_COL_MAP = {
    "keyword":              "keyword",
    "geo":                  "geo",
    "start_date":           "start_date",
    "end_date":             "end_date",
    "pct_change":           "pct_change",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

INSIGHTS_DAILY_COL_MAP = {
    "ProductID":            "product_id",
    "day":                  "day",
    "reviews_count":        "reviews_count",
    "avg_rating":           "avg_rating",
    "pct_positive":         "pct_positive",
    "pct_negative":         "pct_negative",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

BRANDS_COL_MAP = {
    "brand":                       "brand",
    "products_count":              "products_count",
    "total_reviews":               "total_reviews",
    "avg_rating":                  "avg_rating",
    "pct_recommended":             "pct_recommended",
    "avg_sentiment":               "avg_sentiment",
    "avg_health_score":            "avg_health_score",
    "polarization_score":          "polarization_score",
    "share_niacinamida":           "share_niacinamida",
    "share_acido_hialuronico":     "share_acido_hialuronico",
    "share_shampoo_sin_sulfatos":  "share_shampoo_sin_sulfatos",
    "top_product_id":              "top_product_id",
    "revision_date":               "revision_date",
    "_gold_run_id":                "gold_run_id",
}

PRODUCT_FAMILIES_COL_MAP = {
    "focus_keyword":        "focus_keyword",
    "products_count":       "products_count",
    "brands_count":         "brands_count",
    "total_reviews":        "total_reviews",
    "avg_rating":           "avg_rating",
    "avg_sentiment":        "avg_sentiment",
    "pct_recommended":      "pct_recommended",
    "health_score_p50":     "health_score_p50",
    "health_score_p90":     "health_score_p90",
    "top_brand":            "top_brand",
    "top_product_id":       "top_product_id",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

DEMOGRAPHIC_INSIGHTS_COL_MAP = {
    "focus_keyword":        "focus_keyword",
    "demographic_dim":      "demographic_dim",
    "demographic_value":    "demographic_value",
    "reviews_count":        "reviews_count",
    "avg_rating":           "avg_rating",
    "avg_sentiment":        "avg_sentiment",
    "pct_recommended":      "pct_recommended",
    "pct_positive":         "pct_positive",
    "pct_negative":         "pct_negative",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}

FAMILY_DEMAND_SUPPLY_COL_MAP = {
    "focus_keyword":        "focus_keyword",
    "month":                "month",
    "search_interest_avg":  "search_interest_avg",
    "reviews_count":        "reviews_count",
    "avg_sentiment":        "avg_sentiment",
    "revision_date":        "revision_date",
    "_gold_run_id":         "gold_run_id",
}


# ── Internals ────────────────────────────────────────────────────────────────

def _rename_df(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    available = {k: v for k, v in col_map.items() if k in df.columns}
    return df.rename(columns=available)[list(available.values())]


def _bulk_upsert(
    df: pd.DataFrame,
    table: str,
    pk_cols: Iterable[str],
    conn,
    chunk_size: int = 5000,
) -> int:
    """Bulk upsert via psycopg2 execute_values + ON CONFLICT.

    Falls back to row-by-row execute when psycopg2.extras is unavailable.
    Uses the underlying DBAPI cursor from the SQLAlchemy connection.
    """
    if df.empty:
        return 0

    df = df.astype(object).where(pd.notnull(df), None)

    pk_cols = tuple(pk_cols)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]

    col_list = ", ".join(cols)
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols) or "NULL"
    conflict_clause = (
        f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {update_clause}"
        if update_cols
        else f"ON CONFLICT ({', '.join(pk_cols)}) DO NOTHING"
    )

    try:
        from psycopg2.extras import execute_values
        raw = conn.connection.cursor() if hasattr(conn, "connection") else conn.cursor()
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES %s {conflict_clause}"
        )
        total = 0
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        for i in range(0, len(records), chunk_size):
            batch = records[i:i + chunk_size]
            execute_values(raw, sql, batch, page_size=chunk_size)
            total += len(batch)
        return total
    except ImportError:
        placeholders = ", ".join(f":{c}" for c in cols)
        stmt = text(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) {conflict_clause}"
        )
        records = df.to_dict(orient="records")
        for row in records:
            conn.execute(stmt, row)
        return len(records)


# ── Public upsert functions ──────────────────────────────────────────────────

def upsert_gold_products(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, PRODUCTS_COL_MAP)
    return _bulk_upsert(df, "gold.products", ("product_id",), conn)


def upsert_gold_reviews(df: pd.DataFrame, conn, chunk_size: int = 5000) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, REVIEWS_COL_MAP)
    return _bulk_upsert(df, "gold.reviews", ("review_id",), conn, chunk_size=chunk_size)


def upsert_product_insights_monthly(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, INSIGHTS_MONTHLY_COL_MAP)
    return _bulk_upsert(df, "gold.product_insights_monthly", ("product_id", "month"), conn)


def upsert_review_themes(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, THEMES_COL_MAP)
    return _bulk_upsert(df, "gold.review_themes", ("product_id", "topic_id"), conn)


def upsert_search_trends(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, TRENDS_COL_MAP)
    return _bulk_upsert(df, "gold.search_trends", ("keyword", "geo", "date"), conn)


def upsert_search_spikes(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, SPIKES_COL_MAP)
    return _bulk_upsert(df, "gold.search_spikes", ("keyword", "geo", "start_date"), conn)


def upsert_product_insights_daily(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, INSIGHTS_DAILY_COL_MAP)
    return _bulk_upsert(df, "gold.product_insights_daily", ("product_id", "day"), conn)


def upsert_brands(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, BRANDS_COL_MAP)
    return _bulk_upsert(df, "gold.brands", ("brand",), conn)


def upsert_product_families(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, PRODUCT_FAMILIES_COL_MAP)
    return _bulk_upsert(df, "gold.product_families", ("focus_keyword",), conn)


def upsert_demographic_insights(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, DEMOGRAPHIC_INSIGHTS_COL_MAP)
    return _bulk_upsert(
        df, "gold.demographic_insights",
        ("focus_keyword", "demographic_dim", "demographic_value"), conn,
    )


def upsert_family_demand_supply(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0
    df = _rename_df(df, FAMILY_DEMAND_SUPPLY_COL_MAP)
    return _bulk_upsert(df, "gold.family_demand_supply", ("focus_keyword", "month"), conn)


def log_pipeline_run(
    run_id: str,
    dag_name: str,
    status: str,
    rows_written: int,
    started_at: datetime,
    finished_at: datetime,
    conn,
) -> None:
    duration = (finished_at - started_at).total_seconds() if started_at and finished_at else None
    stmt = text("""
        INSERT INTO gold.pipeline_runs
            (run_id, dag_name, status, rows_written, started_at, finished_at, duration_seconds)
        VALUES (:run_id, :dag_name, :status, :rows_written, :started_at, :finished_at, :duration_seconds)
        ON CONFLICT (run_id) DO UPDATE SET
            status           = EXCLUDED.status,
            rows_written     = EXCLUDED.rows_written,
            finished_at      = EXCLUDED.finished_at,
            duration_seconds = EXCLUDED.duration_seconds
    """)
    conn.execute(stmt, {
        "run_id":           run_id,
        "dag_name":         dag_name,
        "status":           status,
        "rows_written":     rows_written,
        "started_at":       started_at,
        "finished_at":      finished_at,
        "duration_seconds": duration,
    })
