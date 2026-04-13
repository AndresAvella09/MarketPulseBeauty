from __future__ import annotations

import pandas as pd


def compute_health_score(prod_reviews: pd.DataFrame, prod_row: pd.Series | None = None) -> float:
    """
    Compute a health score (0..100) using review rating and recommendation rate.
    """
    rating_part = 0.0
    if "Rating" in prod_reviews.columns and prod_reviews["Rating"].notna().any():
        rating_part = float(prod_reviews["Rating"].mean()) / 5.0

    rec_part = 0.5
    if "IsRecommended" in prod_reviews.columns and prod_reviews["IsRecommended"].notna().any():
        rec_part = float(prod_reviews["IsRecommended"].astype(bool).mean())

    score = 100.0 * (0.70 * rating_part + 0.30 * rec_part)
    return max(0.0, min(100.0, score))
