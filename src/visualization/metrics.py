import pandas as pd


def compute_health_score(prod_reviews: pd.DataFrame, prod_row: pd.Series | None) -> float:
    rating_avg_reviews = float(prod_reviews["Rating"].mean()) if prod_reviews["Rating"].notna().any() else 0.0
    avg_rating_product = (
        float(prod_row["AvgRating"])
        if prod_row is not None and pd.notna(prod_row.get("AvgRating"))
        else rating_avg_reviews
    )
    n_reviews = int(len(prod_reviews))

    volume_factor = min(n_reviews / 100.0, 1.0)
    score = (0.7 * avg_rating_product + 0.3 * rating_avg_reviews) * 20.0
    score = score * (0.6 + 0.4 * volume_factor)

    return round(float(score), 1)