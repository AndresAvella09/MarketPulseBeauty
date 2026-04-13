from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import json
import pandas as pd

from src.processing.health_score import compute_health_score

DEFAULT_THRESHOLDS: dict[str, dict[str, Any]] = {
    "avg_rating": {"minor": 0.05, "significant": 0.2, "type": "absolute"},
    "review_volume": {"minor": 0.05, "significant": 0.2, "type": "relative"},
    "health_score": {"minor": 2.0, "significant": 5.0, "type": "absolute"},
    "pct_recommended": {"minor": 0.05, "significant": 0.15, "type": "absolute"},
}


def _first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def compute_metrics(reviews: pd.DataFrame) -> dict[str, float | int | None]:
    rating_col = _first_column(reviews, ["Rating", "rating"])
    rec_col = _first_column(reviews, ["IsRecommended", "is_recommended"])

    avg_rating: float | None = None
    if rating_col is not None:
        ratings = pd.to_numeric(reviews[rating_col], errors="coerce")
        if ratings.notna().any():
            avg_rating = float(ratings.mean())

    review_volume = int(len(reviews))

    pct_recommended: float | None = None
    if rec_col is not None and reviews[rec_col].notna().any():
        pct_recommended = float(reviews[rec_col].astype(bool).mean())

    health_score: float | None = None
    if rating_col is not None:
        score_frame = reviews.copy()
        if rating_col != "Rating":
            score_frame["Rating"] = score_frame[rating_col]
        if rec_col is not None and rec_col != "IsRecommended":
            score_frame["IsRecommended"] = score_frame[rec_col]
        health_score = float(compute_health_score(score_frame, None))

    return {
        "avg_rating": avg_rating,
        "review_volume": review_volume,
        "health_score": health_score,
        "pct_recommended": pct_recommended,
    }


def snapshot_metrics(metrics: dict[str, float | int | None]) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }


def _unwrap_metrics(data: dict[str, Any]) -> dict[str, float | int | None]:
    if "metrics" in data:
        return data["metrics"]
    return data  # type: ignore[return-value]


def _classify_change(prev: float | int | None, curr: float | int | None, rule: dict[str, Any]) -> dict[str, Any]:
    if prev is None or curr is None:
        return {
            "previous": prev,
            "current": curr,
            "delta": None,
            "delta_pct": None,
            "severity": "unknown",
        }

    delta = float(curr) - float(prev)
    if rule["type"] == "relative":
        if prev == 0:
            severity = "significant" if curr != 0 else "none"
            return {
                "previous": prev,
                "current": curr,
                "delta": delta,
                "delta_pct": None,
                "severity": severity,
            }
        delta_pct = abs(delta) / abs(float(prev))
        if delta_pct >= rule["significant"]:
            severity = "significant"
        elif delta_pct >= rule["minor"]:
            severity = "minor"
        else:
            severity = "none"
        return {
            "previous": prev,
            "current": curr,
            "delta": delta,
            "delta_pct": delta_pct,
            "severity": severity,
        }

    abs_delta = abs(delta)
    if abs_delta >= rule["significant"]:
        severity = "significant"
    elif abs_delta >= rule["minor"]:
        severity = "minor"
    else:
        severity = "none"

    return {
        "previous": prev,
        "current": curr,
        "delta": delta,
        "delta_pct": None,
        "severity": severity,
    }


def compare_metrics(
    previous: dict[str, Any],
    current: dict[str, Any],
    thresholds: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rules = thresholds or DEFAULT_THRESHOLDS
    prev_metrics = _unwrap_metrics(previous)
    curr_metrics = _unwrap_metrics(current)

    results: list[dict[str, Any]] = []
    summary = {"significant": 0, "minor": 0, "none": 0, "unknown": 0}

    for metric, rule in rules.items():
        change = _classify_change(prev_metrics.get(metric), curr_metrics.get(metric), rule)
        change.update({"metric": metric, "rule": rule})
        summary[change["severity"]] += 1
        results.append(change)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "changes": results,
    }


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
