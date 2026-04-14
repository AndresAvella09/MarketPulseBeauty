from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.dashboard.utils import classify_focus_keyword


PRODUCTS_PATH = Path("data/local/gold/products_gold_endo.parquet")
REVIEWS_PATH = Path("data/local/gold/reviews_gold_endo.parquet")


def validate_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de {label}: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"El archivo de {label} está vacío: {path}")


def load_gold_data(
    products_path: Path = PRODUCTS_PATH,
    reviews_path: Path = REVIEWS_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    validate_file(products_path, "productos")
    validate_file(reviews_path, "reviews")

    products = pd.read_parquet(products_path)
    reviews = pd.read_parquet(reviews_path)

    if products.empty:
        raise ValueError("El parquet de productos está vacío.")
    if reviews.empty:
        raise ValueError("El parquet de reviews está vacío.")

    return prepare_data(products, reviews)


def prepare_data(products: pd.DataFrame, reviews: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    products = products.copy()
    reviews = reviews.copy()

    if "ProductName" not in products.columns:
        raise ValueError("products parquet debe contener la columna 'ProductName'.")
    if "ProductID" not in products.columns:
        raise ValueError("products parquet debe contener la columna 'ProductID'.")
    if "ProductID" not in reviews.columns:
        raise ValueError("reviews parquet debe contener la columna 'ProductID'.")

    products["focus_keyword"] = products["ProductName"].apply(classify_focus_keyword)

    focus_products = products[products["focus_keyword"].notna()].copy()

    reviews = reviews.merge(
        focus_products[
            ["ProductID", "ProductName", "Brand", "focus_keyword", "AvgRating", "TotalReviewCount"]
        ],
        on="ProductID",
        how="inner",
    )

    if "SubmissionTime" in reviews.columns:
        reviews["SubmissionTime"] = pd.to_datetime(reviews["SubmissionTime"], errors="coerce", utc=True)
        reviews["SubmissionDate"] = reviews["SubmissionTime"].dt.tz_convert(None)
    else:
        reviews["SubmissionDate"] = pd.NaT

    if "Rating" in reviews.columns:
        reviews["Rating"] = pd.to_numeric(reviews["Rating"], errors="coerce")

    if "HelpfulCount" in reviews.columns:
        reviews["HelpfulCount"] = pd.to_numeric(reviews["HelpfulCount"], errors="coerce").fillna(0)

    if "NotHelpfulCount" in reviews.columns:
        reviews["NotHelpfulCount"] = pd.to_numeric(reviews["NotHelpfulCount"], errors="coerce").fillna(0)

    return focus_products, reviews