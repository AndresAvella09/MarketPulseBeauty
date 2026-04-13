import pandas as pd
import pytest

from src.processing.health_score import compute_health_score


def test_compute_health_score_typical_inputs():
    df = pd.DataFrame({
        "Rating": [4, 4, 5],
        "IsRecommended": [True, False, True],
    })
    expected = 100.0 * (0.70 * (df["Rating"].mean() / 5.0) + 0.30 * df["IsRecommended"].mean())
    assert compute_health_score(df, None) == pytest.approx(expected)


def test_compute_health_score_missing_columns_uses_defaults():
    df = pd.DataFrame({"Other": [1, 2, 3]})
    assert compute_health_score(df, None) == pytest.approx(15.0)


def test_compute_health_score_clamps_output():
    df = pd.DataFrame({"Rating": [10, 10], "IsRecommended": [True, True]})
    assert compute_health_score(df, None) == 100.0
