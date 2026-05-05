from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import json
import pandas as pd

from src.processing.health_score import compute_health_score

PRODUCT_ID_CANDIDATES = ["ProductID", "product_id", "pd_id"]
RATING_CANDIDATES = ["Rating", "rating", "AverageOverallRating"]
DATE_CANDIDATES = ["SubmissionTime", "review_date", "ReviewDate", "reviewDate"]
RECOMMENDED_CANDIDATES = ["IsRecommended", "is_recommended", "recommended"]

PRODUCT_NAME_CANDIDATES = ["ProductName", "Name", "product_name"]
BRAND_CANDIDATES = ["Brand", "brand"]

TREND_KEYWORD_CANDIDATES = ["trend_keyword", "keyword"]
TREND_DATE_CANDIDATES = ["week", "date"]
TREND_VALUE_CANDIDATES = ["interest_norm", "interest_weekly", "interest"]

DEFAULT_RULES: dict[str, dict[str, Any]] = {
    "rating_level": {"high": 4.5, "low": 3.2},
    "health_level": {"strong": 80.0, "weak": 50.0},
    "volume_level": {"high": 200, "low": 10},
    "rating_trend": {"minor": 0.10, "significant": 0.20, "type": "absolute"},
    "volume_trend": {"minor": 0.10, "significant": 0.20, "type": "relative"},
    "search_trend": {"minor": 0.10, "significant": 0.20, "type": "relative"},
}


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _ensure_datetime(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        return dt.dt.tz_convert(None)
    except Exception:
        return dt


def _week_start_monday(series: pd.Series) -> pd.Series:
    return series.dt.to_period("W-MON").apply(lambda p: p.start_time.date())


def detect_review_columns(reviews: pd.DataFrame) -> dict[str, str | None]:
    product_col = _first_column(reviews, PRODUCT_ID_CANDIDATES)
    if product_col is None:
        raise ValueError("No product id column found in reviews data")

    return {
        "product": product_col,
        "rating": _first_column(reviews, RATING_CANDIDATES),
        "date": _first_column(reviews, DATE_CANDIDATES),
        "recommended": _first_column(reviews, RECOMMENDED_CANDIDATES),
    }


def _compute_health_scores(reviews: pd.DataFrame, product_col: str, rating_col: str | None, rec_col: str | None) -> pd.Series:
    if rating_col is None:
        return pd.Series(dtype=float)

    score_df = reviews.copy()
    if rating_col != "Rating":
        score_df["Rating"] = score_df[rating_col]
    if rec_col is not None and rec_col != "IsRecommended":
        score_df["IsRecommended"] = score_df[rec_col]

    scores = score_df.groupby(product_col).apply(lambda df: compute_health_score(df, None))
    scores.name = "health_score"
    return scores


def compute_product_metrics(
    reviews: pd.DataFrame,
    product_col: str,
    rating_col: str | None,
    rec_col: str | None,
) -> pd.DataFrame:
    df = reviews.copy()
    df = df.dropna(subset=[product_col])
    df[product_col] = df[product_col].astype(str)

    metrics = pd.DataFrame({product_col: df[product_col].unique()}).set_index(product_col)
    metrics["review_volume"] = df.groupby(product_col).size().astype(int)

    if rating_col is not None:
        ratings = pd.to_numeric(df[rating_col], errors="coerce")
        metrics["avg_rating"] = ratings.groupby(df[product_col]).mean()
    else:
        metrics["avg_rating"] = None

    if rec_col is not None:
        rec_mask = df[rec_col].notna()
        rec_values = df.loc[rec_mask, rec_col].astype(bool)
        pct_rec = rec_values.groupby(df.loc[rec_mask, product_col]).mean()
        metrics["pct_recommended"] = pct_rec
    else:
        metrics["pct_recommended"] = None

    health_scores = _compute_health_scores(df, product_col, rating_col, rec_col)
    metrics = metrics.join(health_scores, how="left")

    return metrics


def _window_compare(series: pd.Series, window: int, mode: str) -> dict[str, float] | None:
    values = series.dropna().sort_index()
    if len(values) < window * 2:
        return None

    recent = values.iloc[-window:]
    previous = values.iloc[-(window * 2):-window]
    if recent.empty or previous.empty:
        return None

    if mode == "sum":
        current_value = float(recent.sum())
        previous_value = float(previous.sum())
    else:
        current_value = float(recent.mean())
        previous_value = float(previous.mean())

    delta = current_value - previous_value
    if previous_value == 0:
        delta_pct = None
    else:
        delta_pct = abs(delta) / abs(previous_value)

    return {
        "previous": previous_value,
        "current": current_value,
        "delta": delta,
        "delta_pct": delta_pct,
    }


def _classify_change(change: dict[str, float] | None, rule: dict[str, Any]) -> tuple[str, str]:
    if change is None:
        return "insufficient", "insufficient"

    delta = change["delta"]
    if rule["type"] == "relative":
        value = change["delta_pct"]
    else:
        value = abs(delta)

    if value is None:
        if delta == 0:
            return "none", "stable"
        return "significant", "up" if delta > 0 else "down"

    if value >= rule["significant"]:
        severity = "significant"
    elif value >= rule["minor"]:
        severity = "minor"
    else:
        severity = "none"

    if severity == "none":
        direction = "stable"
    else:
        direction = "up" if delta > 0 else "down"

    return severity, direction


def compute_weekly_trends(
    reviews: pd.DataFrame,
    product_col: str,
    rating_col: str | None,
    date_col: str | None,
    window_weeks: int,
    rules: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if date_col is None:
        return {}

    df = reviews.copy()
    df[date_col] = _ensure_datetime(df[date_col])
    df = df.dropna(subset=[product_col, date_col])
    df[product_col] = df[product_col].astype(str)

    df["week"] = _week_start_monday(df[date_col])
    weekly_counts = df.groupby([product_col, "week"]).size().rename("review_volume")

    trends: dict[str, dict[str, Any]] = {}
    for product_id, series in weekly_counts.groupby(level=0):
        change = _window_compare(series.droplevel(0), window_weeks, mode="sum")
        severity, direction = _classify_change(change, rules["volume_trend"])
        trends.setdefault(product_id, {})["review_volume_trend"] = {
            **(change or {}),
            "severity": severity,
            "direction": direction,
        }

    if rating_col is None:
        return trends

    ratings = pd.to_numeric(df[rating_col], errors="coerce")
    weekly_rating = (
        ratings.groupby([df[product_col], df["week"]]).mean().rename("avg_rating")
    )

    for product_id, series in weekly_rating.groupby(level=0):
        change = _window_compare(series.droplevel(0), window_weeks, mode="mean")
        severity, direction = _classify_change(change, rules["rating_trend"])
        trends.setdefault(product_id, {})["avg_rating_trend"] = {
            **(change or {}),
            "severity": severity,
            "direction": direction,
        }

    return trends


def compute_search_trends(
    trends: pd.DataFrame,
    product_keyword_map: dict[str, str],
    window_weeks: int,
    rules: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    keyword_col = _first_column(trends, TREND_KEYWORD_CANDIDATES)
    date_col = _first_column(trends, TREND_DATE_CANDIDATES)
    value_col = _first_column(trends, TREND_VALUE_CANDIDATES)

    if keyword_col is None or date_col is None or value_col is None:
        return {}

    df = trends.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[keyword_col, date_col])
    df["week"] = _week_start_monday(df[date_col])

    weekly = df.groupby([keyword_col, "week"])[value_col].mean()

    results: dict[str, dict[str, Any]] = {}
    for product_id, keyword in product_keyword_map.items():
        if keyword not in weekly.index.get_level_values(0):
            continue
        series = weekly.loc[keyword]
        change = _window_compare(series, window_weeks, mode="mean")
        severity, direction = _classify_change(change, rules["search_trend"])
        results[product_id] = {
            "keyword": keyword,
            "interest_trend": {
                **(change or {}),
                "severity": severity,
                "direction": direction,
            },
        }

    return results


def _build_product_lookup(products: pd.DataFrame) -> dict[str, dict[str, str]]:
    product_col = _first_column(products, PRODUCT_ID_CANDIDATES)
    if product_col is None:
        return {}

    name_col = _first_column(products, PRODUCT_NAME_CANDIDATES)
    brand_col = _first_column(products, BRAND_CANDIDATES)

    keep = [product_col]
    if name_col:
        keep.append(name_col)
    if brand_col:
        keep.append(brand_col)

    lookup: dict[str, dict[str, str]] = {}
    for _, row in products[keep].drop_duplicates().iterrows():
        product_id = str(row[product_col])
        entry: dict[str, str] = {}
        if name_col and pd.notna(row[name_col]):
            entry["product_name"] = str(row[name_col])
        if brand_col and pd.notna(row[brand_col]):
            entry["brand"] = str(row[brand_col])
        if entry:
            lookup[product_id] = entry
    return lookup


def _build_keyword_map(mapping: pd.DataFrame | None) -> dict[str, str]:
    if mapping is None:
        return {}

    product_col = _first_column(mapping, PRODUCT_ID_CANDIDATES)
    keyword_col = _first_column(mapping, TREND_KEYWORD_CANDIDATES)
    if product_col is None or keyword_col is None:
        return {}

    map_df = mapping.dropna(subset=[product_col, keyword_col]).copy()
    return dict(zip(map_df[product_col].astype(str), map_df[keyword_col].astype(str)))


def _rating_level(value: float | None, rules: dict[str, Any]) -> str:
    if value is None:
        return "unknown"
    if value >= rules["high"]:
        return "high"
    if value <= rules["low"]:
        return "low"
    return "medium"


def _health_level(value: float | None, rules: dict[str, Any]) -> str:
    if value is None:
        return "unknown"
    if value >= rules["strong"]:
        return "strong"
    if value <= rules["weak"]:
        return "weak"
    return "moderate"


def _volume_level(value: int | None, rules: dict[str, Any]) -> str:
    if value is None:
        return "unknown"
    if value >= rules["high"]:
        return "high"
    if value <= rules["low"]:
        return "low"
    return "medium"


def _trend_message(label: str, trend: dict[str, Any]) -> str:
    severity = trend.get("severity")
    direction = trend.get("direction")
    delta_pct = trend.get("delta_pct")

    suffix = ""
    if delta_pct is not None:
        suffix = f" ({delta_pct * 100:.1f}%)"

    if severity in {"minor", "significant"}:
        return f"{label} {direction}{suffix}"
    if severity == "none":
        return f"{label} estable"
    return f"{label} sin datos suficientes"


def build_insights(
    metrics: dict[str, Any],
    trends: dict[str, Any],
    rules: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    insights: list[dict[str, Any]] = []

    rating_value = metrics.get("avg_rating")
    rating_level = _rating_level(rating_value, rules["rating_level"])
    if rating_level == "high":
        insights.append({"severity": "positive", "signal": "avg_rating", "message": "Calificacion alta"})
    elif rating_level == "low":
        insights.append({"severity": "warning", "signal": "avg_rating", "message": "Calificacion baja"})

    volume_value = metrics.get("review_volume")
    volume_level = _volume_level(volume_value, rules["volume_level"])
    if volume_level == "high":
        insights.append({"severity": "positive", "signal": "review_volume", "message": "Alto volumen de resenas"})
    elif volume_level == "low":
        insights.append({"severity": "info", "signal": "review_volume", "message": "Volumen bajo; interpretar con cautela"})

    health_value = metrics.get("health_score")
    health_level = _health_level(health_value, rules["health_level"])
    if health_level == "strong":
        insights.append({"severity": "positive", "signal": "health_score", "message": "Health score fuerte"})
    elif health_level == "weak":
        insights.append({"severity": "warning", "signal": "health_score", "message": "Health score bajo"})

    rating_trend = trends.get("avg_rating_trend")
    if rating_trend:
        msg = _trend_message("Rating", rating_trend)
        severity = "warning" if rating_trend.get("direction") == "down" else "info"
        if rating_trend.get("severity") in {"minor", "significant", "none"}:
            insights.append({"severity": severity, "signal": "rating_trend", "message": msg})

    volume_trend = trends.get("review_volume_trend")
    if volume_trend:
        msg = _trend_message("Resenas", volume_trend)
        severity = "warning" if volume_trend.get("direction") == "down" else "info"
        if volume_trend.get("severity") in {"minor", "significant", "none"}:
            insights.append({"severity": severity, "signal": "volume_trend", "message": msg})

    search_trend = trends.get("interest_trend")
    if search_trend:
        msg = _trend_message("Busqueda", search_trend)
        severity = "warning" if search_trend.get("direction") == "down" else "info"
        if search_trend.get("severity") in {"minor", "significant", "none"}:
            insights.append({"severity": severity, "signal": "search_trend", "message": msg})

    return insights


def generate_insights_report(
    reviews: pd.DataFrame,
    products: pd.DataFrame | None = None,
    trends: pd.DataFrame | None = None,
    trend_map: pd.DataFrame | None = None,
    window_weeks: int = 4,
    rules: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    active_rules = rules or DEFAULT_RULES
    columns = detect_review_columns(reviews)

    metrics_df = compute_product_metrics(
        reviews,
        product_col=columns["product"],
        rating_col=columns["rating"],
        rec_col=columns["recommended"],
    )

    weekly_trends = compute_weekly_trends(
        reviews,
        product_col=columns["product"],
        rating_col=columns["rating"],
        date_col=columns["date"],
        window_weeks=window_weeks,
        rules=active_rules,
    )

    keyword_map = _build_keyword_map(trend_map)
    search_trends = compute_search_trends(
        trends,
        product_keyword_map=keyword_map,
        window_weeks=window_weeks,
        rules=active_rules,
    ) if trends is not None and keyword_map else {}

    lookup = _build_product_lookup(products) if products is not None else {}

    products_out: list[dict[str, Any]] = []
    severity_summary = {"positive": 0, "warning": 0, "info": 0}

    for product_id, row in metrics_df.iterrows():
        metrics = {
            "avg_rating": None if pd.isna(row.get("avg_rating")) else float(row.get("avg_rating")),
            "review_volume": int(row.get("review_volume")) if row.get("review_volume") is not None else None,
            "health_score": None if pd.isna(row.get("health_score")) else float(row.get("health_score")),
            "pct_recommended": None if pd.isna(row.get("pct_recommended")) else float(row.get("pct_recommended")),
        }

        trends_entry = weekly_trends.get(product_id, {})
        if product_id in search_trends:
            trends_entry.update(search_trends[product_id])

        insights = build_insights(metrics, trends_entry, active_rules)
        for insight in insights:
            severity_summary[insight["severity"]] += 1

        entry: dict[str, Any] = {
            "product_id": product_id,
            "metrics": metrics,
            "trends": trends_entry,
            "insights": insights,
        }

        if product_id in lookup:
            entry.update(lookup[product_id])

        products_out.append(entry)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_weeks": window_weeks,
        "rules": active_rules,
        "summary": {
            "products": len(products_out),
            "insights": severity_summary,
        },
        "products": products_out,
    }
