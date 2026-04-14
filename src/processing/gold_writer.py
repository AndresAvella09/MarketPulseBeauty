"""
gold_writer.py
──────────────
Centralizes all PostgreSQL Gold-layer writes using SQLAlchemy.

Upserts full gold.products and gold.reviews tables (silver + embeddings).
Every upsert function takes a pandas DataFrame and an open SQLAlchemy
connection, and performs ON CONFLICT DO UPDATE to ensure idempotent writes.
"""

from datetime import datetime
import pandas as pd
from sqlalchemy import text


# ── Column mappings: PyArrow (PascalCase) -> PostgreSQL (snake_case) ──────────

PRODUCTS_COL_MAP = {
    "ProductID":              "product_id",
    "Brand":                  "brand",
    "ProductCategory":        "product_category",
    "ProductName":            "product_name",
    "CategoryId":             "category_id",
    "ProductPageUrl":         "product_page_url",
    "AvgRating":              "avg_rating",
    "TotalReviewCount":       "total_review_count",
    "RecommendedCount":       "recommended_count",
    "TotalPhotoCount":        "total_photo_count",
    "RatingDist_1":           "rating_dist_1",
    "RatingDist_2":           "rating_dist_2",
    "RatingDist_3":           "rating_dist_3",
    "RatingDist_4":           "rating_dist_4",
    "RatingDist_5":           "rating_dist_5",
    "ProductName_clean":      "product_name_clean",
    "ProductName_tokens":     "product_name_tokens",
    "ProductName_lemmas":     "product_name_lemmas",
    "product_name_embedding": "product_name_embedding",
    "rating_entropy":         "rating_entropy",
    "polarization_score":     "polarization_score",
    "embedding_norm_name":    "embedding_norm_name",
    "revision_date":          "revision_date",
    "_ingestion_ts":          "ingestion_ts",
    "_source_file":           "source_file",
    "_run_id":                "run_id",
    "_silver_run_id":         "silver_run_id",
    "_gold_run_id":           "gold_run_id",
}

REVIEWS_COL_MAP = {
    "ProductID":              "product_id",
    "ReviewID":               "review_id",
    "Rating":                 "rating",
    "Title":                  "title",
    "ReviewText":             "review_text",
    "SubmissionTime":         "submission_time",
    "LastModTime":            "last_mod_time",
    "IsRecommended":          "is_recommended",
    "HelpfulCount":           "helpful_count",
    "NotHelpfulCount":        "not_helpful_count",
    "IsFeatured":             "is_featured",
    "IsIncentivized":         "is_incentivized",
    "IsStaffReview":          "is_staff_review",
    "UserLocation":           "user_location",
    "skinTone":               "skin_tone",
    "skinType":               "skin_type",
    "eyeColor":               "eye_color",
    "hairColor":              "hair_color",
    "hairType":               "hair_type",
    "hairConcerns":           "hair_concerns",
    "skinConcerns":           "skin_concerns",
    "ageRange":               "age_range",
    "ReviewPhotoCount":       "review_photo_count",
    "ReviewText_clean":       "review_text_clean",
    "ReviewText_tokens":      "review_text_tokens",
    "ReviewText_lemmas":      "review_text_lemmas",
    "ReviewText_wordcount":   "review_text_wordcount",
    "Title_clean":            "title_clean",
    "Title_tokens":           "title_tokens",
    "Title_lemmas":           "title_lemmas",
    "review_text_embedding":  "review_text_embedding",
    "title_embedding":        "title_embedding",
    "text_quality_score":     "text_quality_score",
    "helpful_ratio":          "helpful_ratio",
    "review_age_days":        "review_age_days",
    "is_short_review":        "is_short_review",
    "embedding_norm_review":  "embedding_norm_review",
    "embedding_norm_title":   "embedding_norm_title",
    "revision_date":          "revision_date",
    "_ingestion_ts":          "ingestion_ts",
    "_source_file":           "source_file",
    "_run_id":                "run_id",
    "_silver_run_id":         "silver_run_id",
    "_gold_run_id":           "gold_run_id",
}


def _rename_df(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """Rename DataFrame columns using the mapping, keeping only mapped cols."""
    available = {k: v for k, v in col_map.items() if k in df.columns}
    return df.rename(columns=available)[list(available.values())]


def _embedding_to_pg(val):
    """Convert embedding (list/ndarray of floats or None) to PostgreSQL REAL[] literal."""
    import numpy as np
    if val is None:
        return '{}'
    if isinstance(val, np.ndarray):
        if val.size == 0:
            return '{}'
        return '{' + ','.join(str(float(x)) for x in val.flat) + '}'
    if isinstance(val, (list, tuple)):
        if len(val) == 0:
            return '{}'
        if isinstance(val[0], (list, tuple)):
            val = val[0]
        return '{' + ','.join(str(float(x)) for x in val) + '}'
    return '{}'


def upsert_gold_products(df: pd.DataFrame, conn) -> int:
    """Upsert rows into gold.products. Returns rows affected."""
    if df.empty:
        return 0

    df = _rename_df(df, PRODUCTS_COL_MAP)

    # Convert embedding column to PG array literals
    if "product_name_embedding" in df.columns:
        df["product_name_embedding"] = df["product_name_embedding"].apply(_embedding_to_pg)

    pg_cols = list(df.columns)
    update_cols = [c for c in pg_cols if c != "product_id"]

    placeholders = ", ".join(f":{c}" for c in pg_cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    stmt = text(f"""
        INSERT INTO gold.products ({', '.join(pg_cols)})
        VALUES ({placeholders})
        ON CONFLICT (product_id) DO UPDATE SET {updates}
    """)

    rows = df.to_dict(orient="records")
    for row in rows:
        conn.execute(stmt, row)

    return len(rows)


def upsert_gold_reviews(df: pd.DataFrame, conn, chunk_size: int = 5000) -> int:
    """Upsert rows into gold.reviews in chunks. Returns rows affected."""
    if df.empty:
        return 0

    total = len(df)
    upserted = 0

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start:start + chunk_size].copy()
        chunk = _rename_df(chunk, REVIEWS_COL_MAP)

        for emb_col in ("review_text_embedding", "title_embedding"):
            if emb_col in chunk.columns:
                chunk[emb_col] = chunk[emb_col].apply(_embedding_to_pg)

        pg_cols = list(chunk.columns)
        update_cols = [c for c in pg_cols if c != "review_id"]

        placeholders = ", ".join(f":{c}" for c in pg_cols)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        stmt = text(f"""
            INSERT INTO gold.reviews ({', '.join(pg_cols)})
            VALUES ({placeholders})
            ON CONFLICT (review_id) DO UPDATE SET {updates}
        """)

        rows = chunk.to_dict(orient="records")
        for row in rows:
            conn.execute(stmt, row)

        upserted += len(rows)
        print(f"    [pg/reviews] {upserted:,}/{total:,} upserted")

    return upserted


def log_pipeline_run(
    run_id: str,
    dag_name: str,
    status: str,
    rows_written: int,
    started_at: datetime,
    finished_at: datetime,
    conn,
) -> None:
    """Insert a row into gold.pipeline_runs."""
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
