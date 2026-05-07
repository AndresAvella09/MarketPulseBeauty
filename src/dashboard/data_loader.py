"""Postgres-backed data access for the MarketPulse Beauty dashboard.

The Streamlit container reads `PG_CONN` (or falls back to a local-dev URL with
the host port `5433` mapped by docker-compose). Every helper is a cached SQL
query so pages stay declarative.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ── Connection ────────────────────────────────────────────────────────────────

DEFAULT_LOCAL_URL = "postgresql+psycopg2://postgres:postgres@localhost:5433/marketpulse"


def _conn_url() -> str:
    return (os.environ.get("PG_CONN")
            or os.environ.get("POSTGRES_GOLD_CONN")
            or DEFAULT_LOCAL_URL)


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """Create one SQLAlchemy engine per Streamlit session."""
    return create_engine(_conn_url(), pool_pre_ping=True, pool_recycle=300)


def _read_sql(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params or {})


# ── Lightweight wrapper ───────────────────────────────────────────────────────

@dataclass(slots=True)
class DashboardData:
    """A small façade so pages can read e.g. `data.products` without
    re-importing helpers. Heavy data stays in cached functions and is fetched
    on demand."""
    products: pd.DataFrame
    brands: pd.DataFrame
    families: pd.DataFrame
    last_run: dict | None
    source: str = "postgres"


@st.cache_data(ttl=300, show_spinner=False)
def load_products() -> pd.DataFrame:
    return _read_sql("SELECT * FROM gold.products")


@st.cache_data(ttl=300, show_spinner=False)
def load_brands() -> pd.DataFrame:
    return _read_sql("SELECT * FROM gold.brands ORDER BY avg_health_score DESC")


@st.cache_data(ttl=300, show_spinner=False)
def load_families() -> pd.DataFrame:
    return _read_sql("SELECT * FROM gold.product_families")


@st.cache_data(ttl=300, show_spinner=False)
def load_pipeline_runs() -> pd.DataFrame:
    return _read_sql("""
        SELECT run_id, dag_name, status, rows_written,
               started_at, finished_at, duration_seconds
        FROM gold.pipeline_runs
        ORDER BY started_at DESC
    """)


def load_dashboard_data() -> DashboardData:
    products = load_products()
    brands = load_brands()
    families = load_families()
    runs = load_pipeline_runs()
    last_run = runs.iloc[0].to_dict() if not runs.empty else None
    return DashboardData(products=products, brands=brands, families=families,
                         last_run=last_run, source="postgres")


# ── Overview helpers ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def overview_kpis() -> dict:
    df = _read_sql("""
        SELECT count(*)                       AS total_products,
               sum(total_review_count)        AS total_reviews,
               avg(avg_rating)                AS mean_rating,
               avg(health_score)              AS mean_health,
               avg(pct_recommended) * 100     AS pct_recommended
        FROM gold.products
    """)
    row = df.iloc[0]
    return {
        "total_products": int(row["total_products"] or 0),
        "total_reviews": int(row["total_reviews"] or 0),
        "mean_rating": float(row["mean_rating"]) if pd.notna(row["mean_rating"]) else None,
        "mean_health": float(row["mean_health"]) if pd.notna(row["mean_health"]) else None,
        "pct_recommended": float(row["pct_recommended"]) if pd.notna(row["pct_recommended"]) else None,
    }


@st.cache_data(ttl=300, show_spinner=False)
def sentiment_mix() -> dict:
    df = _read_sql("""
        SELECT sentiment_label AS label, count(*) AS n
        FROM gold.reviews
        WHERE sentiment_label IS NOT NULL
        GROUP BY sentiment_label
    """)
    return dict(zip(df["label"].astype(str), df["n"].astype(int)))


@st.cache_data(ttl=300, show_spinner=False)
def top_bottom_products(n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    top = _read_sql("""
        SELECT product_id, brand, product_name, focus_keyword,
               health_score, avg_rating, total_review_count
        FROM gold.products
        WHERE health_score IS NOT NULL AND total_review_count >= 30
        ORDER BY health_score DESC
        LIMIT :n
    """, {"n": n})
    bot = _read_sql("""
        SELECT product_id, brand, product_name, focus_keyword,
               health_score, avg_rating, total_review_count
        FROM gold.products
        WHERE health_score IS NOT NULL AND total_review_count >= 30
        ORDER BY health_score ASC
        LIMIT :n
    """, {"n": n})
    return top, bot


@st.cache_data(ttl=300, show_spinner=False)
def velocity_movers(min_total: int = 30) -> pd.DataFrame:
    return _read_sql("""
        SELECT product_id, brand, product_name,
               coalesce(review_velocity_prior_30d, 0) AS prior_30d,
               coalesce(review_velocity_30d, 0)       AS last_30d,
               total_review_count
        FROM gold.products
        WHERE review_velocity_30d IS NOT NULL
          AND review_velocity_prior_30d IS NOT NULL
          AND total_review_count >= :n
    """, {"n": min_total})


# ── Family explorer helpers ───────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def family_demand_supply(focus_keyword: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT month, search_interest_avg, reviews_count, avg_sentiment
        FROM gold.family_demand_supply
        WHERE focus_keyword = :fk
        ORDER BY month
    """, {"fk": focus_keyword})


@st.cache_data(ttl=300, show_spinner=False)
def family_health_distribution(focus_keyword: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT focus_keyword, health_score
        FROM gold.products
        WHERE health_score IS NOT NULL
          AND (focus_keyword = :fk OR focus_keyword IS DISTINCT FROM :fk)
    """, {"fk": focus_keyword})


@st.cache_data(ttl=300, show_spinner=False)
def family_brand_share(focus_keyword: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT p.brand, sum(p.total_review_count) AS reviews
        FROM gold.products p
        WHERE p.focus_keyword = :fk AND p.brand IS NOT NULL
        GROUP BY p.brand
        ORDER BY reviews DESC
    """, {"fk": focus_keyword})


@st.cache_data(ttl=300, show_spinner=False)
def family_top_products(focus_keyword: str, n: int = 8) -> pd.DataFrame:
    return _read_sql("""
        SELECT product_id, brand, product_name, product_category,
               avg_rating, health_score, total_review_count
        FROM gold.products
        WHERE focus_keyword = :fk
        ORDER BY health_score DESC NULLS LAST
        LIMIT :n
    """, {"fk": focus_keyword, "n": n})


# ── Product detail helpers ────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def product_row(product_id: str) -> pd.Series | None:
    df = _read_sql("SELECT * FROM gold.products WHERE product_id = :pid",
                   {"pid": product_id})
    if df.empty:
        return None
    return df.iloc[0]


@st.cache_data(ttl=300, show_spinner=False)
def product_monthly(product_id: str, months: int = 24) -> pd.DataFrame:
    return _read_sql("""
        SELECT month, reviews_count, avg_rating, pct_recommended,
               pct_positive, pct_neutral, pct_negative, health_score
        FROM gold.product_insights_monthly
        WHERE product_id = :pid
        ORDER BY month DESC
        LIMIT :m
    """, {"pid": product_id, "m": months}).iloc[::-1].reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def product_daily(product_id: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT day, reviews_count, avg_rating, pct_positive, pct_negative
        FROM gold.product_insights_daily
        WHERE product_id = :pid
        ORDER BY day
    """, {"pid": product_id})


@st.cache_data(ttl=300, show_spinner=False)
def product_themes(product_id: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT topic_id, theme_label, polarity, count, pct, avg_sentiment
        FROM gold.review_themes
        WHERE product_id = :pid
        ORDER BY count DESC
    """, {"pid": product_id})


@st.cache_data(ttl=300, show_spinner=False)
def product_rating_distribution(product_id: str) -> dict[int, int]:
    df = _read_sql("""
        SELECT rating_dist_1, rating_dist_2, rating_dist_3,
               rating_dist_4, rating_dist_5
        FROM gold.products WHERE product_id = :pid
    """, {"pid": product_id})
    if df.empty:
        return {}
    row = df.iloc[0]
    return {i: int(row[f"rating_dist_{i}"] or 0) for i in range(1, 6)}


@st.cache_data(ttl=300, show_spinner=False)
def product_top_locations(product_id: str) -> list[tuple[str, int]]:
    df = _read_sql("""
        SELECT user_location, count(*) AS n
        FROM gold.reviews
        WHERE product_id = :pid AND user_location IS NOT NULL
        GROUP BY user_location
        ORDER BY n DESC
        LIMIT 8
    """, {"pid": product_id})
    return [(str(r["user_location"]), int(r["n"])) for _, r in df.iterrows()]


def decode_quote(text_field: str | None) -> dict | None:
    if not text_field:
        return None
    try:
        return json.loads(text_field)
    except (json.JSONDecodeError, TypeError):
        return {"text": str(text_field)}


# ── Review explorer helpers ───────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def brand_family_distribution(brands: tuple[str, ...]) -> pd.DataFrame:
    """% of each brand's reviews by focus_keyword (real distribution).

    Returns a long DataFrame: brand, focus_keyword (or 'unclassified'),
    reviews, share.
    """
    if not brands:
        return pd.DataFrame(columns=["brand", "focus_keyword", "reviews", "share"])
    placeholders = ",".join(f":b{i}" for i in range(len(brands)))
    params = {f"b{i}": v for i, v in enumerate(brands)}
    sql = f"""
        WITH brand_reviews AS (
            SELECT p.brand,
                   COALESCE(p.focus_keyword, 'unclassified') AS focus_keyword,
                   sum(p.total_review_count) AS reviews
            FROM gold.products p
            WHERE p.brand IN ({placeholders})
            GROUP BY p.brand, COALESCE(p.focus_keyword, 'unclassified')
        ),
        totals AS (
            SELECT brand, sum(reviews) AS total FROM brand_reviews GROUP BY brand
        )
        SELECT br.brand, br.focus_keyword, br.reviews,
               br.reviews::float / NULLIF(t.total, 0) AS share
        FROM brand_reviews br
        JOIN totals t USING (brand)
        ORDER BY br.brand, br.reviews DESC
    """
    return _read_sql(sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def review_filters_meta() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    fams = _read_sql("SELECT DISTINCT focus_keyword FROM gold.products WHERE focus_keyword IS NOT NULL")
    out["families"] = sorted(fams["focus_keyword"].tolist())
    brands = _read_sql("SELECT DISTINCT brand FROM gold.products WHERE brand IS NOT NULL")
    out["brands"] = sorted(brands["brand"].tolist())
    return out


@st.cache_data(ttl=300, show_spinner=False)
def review_count(focus_keyword: str | None = None,
                 sentiment_labels: tuple[str, ...] | None = None) -> int:
    where, params = ["1=1"], {}
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    if sentiment_labels:
        ph = ",".join(f":s{i}" for i in range(len(sentiment_labels)))
        where.append(f"r.sentiment_label IN ({ph})")
        params.update({f"s{i}": v for i, v in enumerate(sentiment_labels)})
    sql = f"""
        SELECT count(*) AS n
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
    """
    return int(_read_sql(sql, params).iloc[0]["n"])


@st.cache_data(ttl=300, show_spinner=False)
def umap_sample(focus_keyword: str | None = None,
                sentiment_labels: tuple[str, ...] | None = None,
                limit: int = 3000,
                exclude_outlier: bool = True) -> pd.DataFrame:
    where = ["r.umap_x IS NOT NULL", "r.umap_y IS NOT NULL"]
    if exclude_outlier:
        where.append("r.topic_id IS DISTINCT FROM -1")
    params = {"limit": limit}
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    if sentiment_labels:
        ph = ",".join(f":s{i}" for i in range(len(sentiment_labels)))
        where.append(f"r.sentiment_label IN ({ph})")
        params.update({f"s{i}": v for i, v in enumerate(sentiment_labels)})
    sql = f"""
        SELECT r.umap_x, r.umap_y, r.topic_id, r.topic_label,
               r.sentiment_label, r.rating
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
        ORDER BY random()
        LIMIT :limit
    """
    return _read_sql(sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def topic_frequency(focus_keyword: str | None = None, top: int = 12) -> pd.DataFrame:
    where, params = ["r.topic_id IS NOT NULL", "r.topic_id != -1"], {"top": top}
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    sql = f"""
        SELECT r.topic_id, max(r.topic_label) AS label, count(*) AS n
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
        GROUP BY r.topic_id
        ORDER BY n DESC
        LIMIT :top
    """
    return _read_sql(sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def sentiment_timeline(focus_keyword: str | None = None, days: int = 365) -> pd.DataFrame:
    where, params = ["r.submission_time IS NOT NULL",
                     "r.submission_time >= now() - (:days || ' days')::interval"], {"days": days}
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    sql = f"""
        SELECT date_trunc('day', r.submission_time)::date AS day,
               avg(r.sentiment_score) AS sentiment,
               count(*) AS n
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
        GROUP BY 1
        ORDER BY 1
    """
    return _read_sql(sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def trust_signals(focus_keyword: str | None = None) -> dict:
    where, params = ["1=1"], {}
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    sql = f"""
        SELECT
          avg(CASE WHEN r.is_incentivized THEN 1 ELSE 0 END)::float AS pct_incentivized,
          avg(CASE WHEN r.is_staff_review THEN 1 ELSE 0 END)::float AS pct_staff,
          avg(CASE WHEN r.is_featured     THEN 1 ELSE 0 END)::float AS pct_featured
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
    """
    row = _read_sql(sql, params).iloc[0]
    return {k: float(row[k] or 0) for k in row.index}


@st.cache_data(ttl=300, show_spinner=False)
def reviews_table(focus_keyword: str | None = None,
                  sentiment_labels: tuple[str, ...] | None = None,
                  rating_min: int = 1, rating_max: int = 5,
                  limit: int = 100) -> pd.DataFrame:
    where, params = ["r.rating BETWEEN :rmin AND :rmax"], {
        "rmin": rating_min, "rmax": rating_max, "limit": limit
    }
    if focus_keyword:
        where.append("p.focus_keyword = :fk")
        params["fk"] = focus_keyword
    if sentiment_labels:
        ph = ",".join(f":s{i}" for i in range(len(sentiment_labels)))
        where.append(f"r.sentiment_label IN ({ph})")
        params.update({f"s{i}": v for i, v in enumerate(sentiment_labels)})
    sql = f"""
        SELECT r.title, r.rating, r.sentiment_label, r.helpful_ratio,
               r.skin_type, r.age_range, r.submission_time::date AS date,
               left(coalesce(r.review_text, ''), 220) AS snippet,
               p.brand, p.product_name
        FROM gold.reviews r
        LEFT JOIN gold.products p ON p.product_id = r.product_id
        WHERE {' AND '.join(where)}
        ORDER BY r.submission_time DESC NULLS LAST
        LIMIT :limit
    """
    return _read_sql(sql, params)


# ── Demographics ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def demographic_insights(focus_keyword: str, dim: str = "skin_tone") -> pd.DataFrame:
    return _read_sql("""
        SELECT demographic_value, reviews_count,
               avg_rating, avg_sentiment, pct_recommended,
               pct_positive, pct_negative
        FROM gold.demographic_insights
        WHERE focus_keyword = :fk AND demographic_dim = :dim
        ORDER BY reviews_count DESC
    """, {"fk": focus_keyword, "dim": dim})


@st.cache_data(ttl=300, show_spinner=False)
def demographic_dimensions(focus_keyword: str) -> list[str]:
    df = _read_sql("""
        SELECT DISTINCT demographic_dim
        FROM gold.demographic_insights
        WHERE focus_keyword = :fk
    """, {"fk": focus_keyword})
    return sorted(df["demographic_dim"].tolist())


# ── Tiny utility used by pages ────────────────────────────────────────────────

def has_column(df: pd.DataFrame | None, col: str) -> bool:
    return df is not None and col in df.columns and df[col].notna().any()


def kpi_format_number(n: float | int | None) -> str:
    if n is None:
        return "—"
    if isinstance(n, float) and not n.is_integer():
        return f"{n:,.2f}"
    return f"{int(n):,}"
