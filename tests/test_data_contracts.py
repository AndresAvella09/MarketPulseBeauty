import pandas as pd

from src.processing.data_contracts import (
    validate_reviews_contract,
    validate_trends_contract,
)


def test_reviews_contract_missing_columns():
    df = pd.DataFrame({
        "product_id": ["p1"],
        "rating": [5],
        "review_date": ["2026-01-01"],
    })
    errors = validate_reviews_contract(df)
    assert errors == ["Missing required columns: review_text"]


def test_reviews_contract_invalid_types():
    df = pd.DataFrame({
        "product_id": ["p1"],
        "rating": ["bad"],
        "review_date": ["not-a-date"],
        "review_text": ["this is a sufficiently long review text"],
    })
    errors = validate_reviews_contract(df)
    assert errors == [
        "Rating has non-numeric values in 1 rows",
        "Review date has invalid values in 1 rows",
    ]


def test_reviews_contract_out_of_range_and_short_text():
    df = pd.DataFrame({
        "product_id": ["p1", "p1"],
        "rating": [0, 6],
        "review_date": ["2026-01-01", "2026-01-02"],
        "review_text": ["ok", "no"],
    })
    errors = validate_reviews_contract(df, min_text_length=3)
    assert errors == [
        "Rating out of range (1-5) in 2 rows",
        "Review text shorter than 3 chars in 2 rows",
    ]


def test_trends_contract_missing_keywords_and_empty_series():
    df = pd.DataFrame({
        "date": ["2026-01-01"],
        "keyword": ["k1"],
        "interest": [None],
    })
    errors = validate_trends_contract(
        df,
        required_keywords={"k1", "k2"},
        date_range=("2026-01-01", "2026-12-31"),
    )
    assert errors == [
        "Missing keywords: k2",
        "Empty series for keywords: k1",
    ]
