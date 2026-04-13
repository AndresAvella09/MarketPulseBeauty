from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.processing.data_contracts import (
    validate_reviews_contract,
    validate_trends_contract,
)


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing sample file: {path}")
    return pd.read_csv(path)


def _fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    base = Path(__file__).parent / "sample_data"

    reviews_valid = _load_csv(base / "reviews_valid.csv")
    reviews_errors = validate_reviews_contract(reviews_valid, min_text_length=10)
    if reviews_errors:
        _fail(f"Valid reviews sample failed: {reviews_errors}")

    reviews_invalid = _load_csv(base / "reviews_invalid.csv")
    reviews_errors = validate_reviews_contract(reviews_invalid, min_text_length=10)
    if not reviews_errors:
        _fail("Invalid reviews sample did not fail as expected")

    trends_valid = _load_csv(base / "trends_valid.csv")
    trends_errors = validate_trends_contract(
        trends_valid,
        required_keywords={"kw1", "kw2"},
        date_range=("2026-01-01", "2026-01-31"),
    )
    if trends_errors:
        _fail(f"Valid trends sample failed: {trends_errors}")

    trends_invalid = _load_csv(base / "trends_invalid.csv")
    trends_errors = validate_trends_contract(
        trends_invalid,
        required_keywords={"kw1", "kw2"},
        date_range=("2026-01-01", "2026-01-31"),
    )
    if not trends_errors:
        _fail("Invalid trends sample did not fail as expected")

    print("[OK] Data contracts validated against sample data")


if __name__ == "__main__":
    main()
