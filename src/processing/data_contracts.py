from __future__ import annotations

from typing import Iterable

import pandas as pd


REVIEW_REQUIRED_COLUMNS = {"product_id", "rating", "review_date", "review_text"}
TRENDS_REQUIRED_COLUMNS = {"date", "keyword", "interest"}


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    missing = sorted(set(required) - set(df.columns))
    return missing


def validate_reviews_contract(
    df: pd.DataFrame,
    min_text_length: int = 20,
    min_records_per_product: int = 1,
) -> list[str]:
    errors: list[str] = []

    missing = _missing_columns(df, REVIEW_REQUIRED_COLUMNS)
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return errors

    null_cols = sorted([col for col in REVIEW_REQUIRED_COLUMNS if df[col].isna().any()])
    if null_cols:
        errors.append(f"Nulls in critical fields: {', '.join(null_cols)}")

    rating_numeric = pd.to_numeric(df["rating"], errors="coerce")
    non_numeric = rating_numeric.isna() & df["rating"].notna()
    if non_numeric.any():
        errors.append(f"Rating has non-numeric values in {int(non_numeric.sum())} rows")

    out_of_range = (rating_numeric < 1) | (rating_numeric > 5)
    out_of_range = out_of_range & rating_numeric.notna()
    if out_of_range.any():
        errors.append(f"Rating out of range (1-5) in {int(out_of_range.sum())} rows")

    parsed_dates = pd.to_datetime(df["review_date"], errors="coerce", utc=True)
    invalid_dates = parsed_dates.isna() & df["review_date"].notna()
    if invalid_dates.any():
        errors.append(f"Review date has invalid values in {int(invalid_dates.sum())} rows")

    text_series = df["review_text"]
    short_text = text_series.notna() & (text_series.astype(str).str.len() < min_text_length)
    if short_text.any():
        errors.append(
            f"Review text shorter than {min_text_length} chars in {int(short_text.sum())} rows"
        )

    if min_records_per_product > 1:
        counts = df["product_id"].dropna().value_counts()
        too_few = counts[counts < min_records_per_product]
        if not too_few.empty:
            products = ", ".join(sorted(map(str, too_few.index)))
            errors.append(
                f"Products below minimum records ({min_records_per_product}): {products}"
            )

    return errors


def validate_trends_contract(
    df: pd.DataFrame,
    required_keywords: Iterable[str],
    date_range: tuple[str | pd.Timestamp, str | pd.Timestamp] | None = None,
) -> list[str]:
    errors: list[str] = []

    missing = _missing_columns(df, TRENDS_REQUIRED_COLUMNS)
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return errors

    parsed_dates = pd.to_datetime(df["date"], errors="coerce")
    invalid_dates = parsed_dates.isna() & df["date"].notna()
    if invalid_dates.any():
        errors.append(f"Date has invalid values in {int(invalid_dates.sum())} rows")

    if date_range is not None:
        start, end = (pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))
        out_of_range = (parsed_dates < start) | (parsed_dates > end)
        out_of_range = out_of_range & parsed_dates.notna()
        if out_of_range.any():
            errors.append(f"Dates out of expected range: {int(out_of_range.sum())} rows")

    required = set(required_keywords)
    present = set(df["keyword"].dropna().unique())
    missing_keywords = sorted(required - present)
    if missing_keywords:
        errors.append(f"Missing keywords: {', '.join(missing_keywords)}")

    empty_keywords: list[str] = []
    for kw in sorted(required & present):
        series = df.loc[df["keyword"] == kw, "interest"]
        if series.dropna().empty:
            empty_keywords.append(kw)

    if empty_keywords:
        errors.append(f"Empty series for keywords: {', '.join(empty_keywords)}")

    return errors


def raise_on_errors(errors: list[str], label: str) -> None:
    if errors:
        joined = "; ".join(errors)
        raise ValueError(f"{label} validation failed: {joined}")
