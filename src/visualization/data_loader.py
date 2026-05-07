from pathlib import Path
import pandas as pd


def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def to_datetime_safe(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        return dt.dt.tz_convert(None)
    except Exception:
        return dt


def validate_inputs(products_path: Path, reviews_path: Path):
    if not products_path.exists() or not reviews_path.exists():
        raise FileNotFoundError(
            "No encuentro los parquet. Verifica rutas en el panel izquierdo."
        )


def prepare_dashboard_data(products: pd.DataFrame, reviews: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "ProductID" not in products.columns or "ProductID" not in reviews.columns:
        raise ValueError("No existe ProductID en products o reviews. No puedo unir.")

    products = products.copy()
    reviews = reviews.copy()

    if "SubmissionTime" in reviews.columns:
        reviews["SubmissionTime"] = to_datetime_safe(reviews["SubmissionTime"])

    df = reviews.merge(
        products[
            [
                "ProductID",
                "ProductName",
                "Brand",
                "AvgRating",
                "TotalReviewCount",
                "CategoryId",
                "ProductPageUrl",
            ]
        ],
        on="ProductID",
        how="left",
    )
    return products, df