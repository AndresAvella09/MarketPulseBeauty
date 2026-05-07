import numpy as np
import pandas as pd

from src.processing.health_score import (
    CANONICAL_COLUMNS,
    build_master_table_from_frames,
    calculate_health_scores,
)


def test_calculate_health_scores_weighted_average() -> None:
    master = pd.DataFrame(
        {
            CANONICAL_COLUMNS.category: ["c1", "c1", "c2"],
            CANONICAL_COLUMNS.product_id: ["p1", "p2", "p3"],
            CANONICAL_COLUMNS.sentiment_score: [0.5, -0.5, 1.0],
            CANONICAL_COLUMNS.volume: [10, 30, 5],
        }
    )

    result = calculate_health_scores(master)
    row_c1 = result.loc[result[CANONICAL_COLUMNS.category] == "c1"].iloc[0]
    row_c2 = result.loc[result[CANONICAL_COLUMNS.category] == "c2"].iloc[0]

    assert np.isclose(row_c1["health_score"], 0.375)
    assert row_c1["total_volume"] == 40
    assert np.isclose(row_c2["health_score"], 1.0)


def test_health_score_handles_zero_volume() -> None:
    master = pd.DataFrame(
        {
            CANONICAL_COLUMNS.category: ["c1"],
            CANONICAL_COLUMNS.product_id: ["p1"],
            CANONICAL_COLUMNS.sentiment_score: [0.2],
            CANONICAL_COLUMNS.volume: [0],
        }
    )

    result = calculate_health_scores(master)
    score = result.loc[result[CANONICAL_COLUMNS.category] == "c1", "health_score"].iloc[0]
    assert np.isnan(score)


def test_build_master_table_from_counts() -> None:
    sentiment = pd.DataFrame(
        {
            CANONICAL_COLUMNS.category: ["c1", "c1", "c1"],
            CANONICAL_COLUMNS.product_id: ["p1", "p1", "p2"],
            CANONICAL_COLUMNS.sentiment_score: [1.0, 0.0, -1.0],
        }
    )

    master = build_master_table_from_frames(
        sentiment_df=sentiment,
        volume_df=None,
        allow_volume_from_counts=True,
    )
    row_p1 = master.loc[master[CANONICAL_COLUMNS.product_id] == "p1"].iloc[0]
    row_p2 = master.loc[master[CANONICAL_COLUMNS.product_id] == "p2"].iloc[0]

    assert np.isclose(row_p1[CANONICAL_COLUMNS.sentiment_score], 0.5)
    assert row_p1[CANONICAL_COLUMNS.volume] == 2
    assert np.isclose(row_p2[CANONICAL_COLUMNS.sentiment_score], -1.0)
    assert row_p2[CANONICAL_COLUMNS.volume] == 1
