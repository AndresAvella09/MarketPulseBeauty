from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class DataColumns:
    """Column names used by the pipeline.

    Args:
        category: Category column name.
        product_id: Product id column name.
        sentiment_score: Sentiment score column name.
        volume: Volume or mention count column name.
    """

    category: str = "category"
    product_id: str = "product_id"
    sentiment_score: str = "sentiment_score"
    volume: str = "mention_count"


CANONICAL_COLUMNS = DataColumns()


@dataclass(slots=True)
class HealthScoreConfig:
    """Runtime configuration for health score calculation.

    Args:
        input_path: Parquet path with sentiment and volume in one table.
        sentiment_path: Parquet path with sentiment scores.
        volume_path: Parquet path with volume counts.
        output_path: Parquet output path.
        columns: Input column names.
        fallback_product_columns: Alternate product id names.
        fallback_category_columns: Alternate category names.
        fallback_volume_columns: Alternate volume names.
        fallback_sentiment_columns: Alternate sentiment names.
        allow_volume_from_counts: When True, derive volume from sentiment rows.
    """

    input_path: Path | None = None
    sentiment_path: Path | None = None
    volume_path: Path | None = None
    output_path: Path = Path("data/processed/category_health_scores.parquet")
    columns: DataColumns = field(default_factory=DataColumns)
    fallback_product_columns: tuple[str, ...] = ("pd_id", "ProductID")
    fallback_category_columns: tuple[str, ...] = (
        "ProductCategory",
        "category_id",
        "CategoryId",
    )
    fallback_volume_columns: tuple[str, ...] = (
        "mention_count",
        "review_count",
        "reviews_count",
        "volume",
    )
    fallback_sentiment_columns: tuple[str, ...] = (
        "sentiment_score",
        "sentiment",
        "compound",
    )
    allow_volume_from_counts: bool = True

    def __post_init__(self) -> None:
        self.input_path = _coerce_path(self.input_path)
        self.sentiment_path = _coerce_path(self.sentiment_path)
        self.volume_path = _coerce_path(self.volume_path)
        self.output_path = Path(self.output_path)

    def validate(self) -> None:
        """Validate configuration and input paths.

        Raises:
            FileNotFoundError: When an input path is missing.
            ValueError: When configuration is inconsistent.
        """
        if self.input_path and (self.sentiment_path or self.volume_path):
            raise ValueError(
                "Use input_path for a master table or sentiment_path/volume_path for split sources."
            )
        if self.input_path is None and self.sentiment_path is None:
            raise ValueError("Provide input_path or sentiment_path.")
        if self.sentiment_path is None and self.volume_path is not None:
            raise ValueError("volume_path requires sentiment_path.")
        if self.sentiment_path and self.volume_path is None and not self.allow_volume_from_counts:
            raise ValueError("volume_path is required when allow_volume_from_counts is False.")

        for label, path in (
            ("input_path", self.input_path),
            ("sentiment_path", self.sentiment_path),
            ("volume_path", self.volume_path),
        ):
            if path is not None and not path.exists():
                raise FileNotFoundError(f"{label} does not exist: {path}")


def _coerce_path(value: str | Path | None) -> Path | None:
    """Coerce a path-like value into a Path.

    Args:
        value: Path-like value.

    Returns:
        Path instance or None.
    """
    if value is None:
        return None
    return Path(value)


def _read_parquet_columns(path: Path) -> set[str]:
    """Read column names from a parquet file.

    Args:
        path: Parquet file path.

    Returns:
        Set of column names.
    """
    return set(pq.read_schema(path).names)


def _resolve_column(
    available: set[str],
    primary: str,
    fallbacks: tuple[str, ...],
    label: str,
) -> str:
    """Resolve a required column name from available columns.

    Args:
        available: Available column names.
        primary: Preferred column name.
        fallbacks: Alternate column names.
        label: Label used for error messages.

    Returns:
        The resolved column name.

    Raises:
        ValueError: When no candidate is found.
    """
    candidates = (primary,) + fallbacks
    for name in candidates:
        if name in available:
            return name
    raise ValueError(
        f"Missing required {label} column. Expected one of: {', '.join(candidates)}"
    )


def _resolve_optional_column(
    available: set[str],
    primary: str,
    fallbacks: tuple[str, ...],
) -> str | None:
    """Resolve an optional column name from available columns.

    Args:
        available: Available column names.
        primary: Preferred column name.
        fallbacks: Alternate column names.

    Returns:
        The resolved column name, or None when not found.
    """
    candidates = (primary,) + fallbacks
    for name in candidates:
        if name in available:
            return name
    return None


def _drop_missing_keys(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Drop rows with missing key fields.

    Args:
        df: Input DataFrame.
        key_columns: Column names required for grouping.

    Returns:
        DataFrame without rows missing keys.
    """
    mask = np.zeros(len(df), dtype=bool)
    for column in key_columns:
        series = df[column]
        empty = series.astype(str).str.strip().eq("")
        mask |= series.isna() | empty
    if mask.any():
        LOGGER.warning("Dropping %s rows with missing keys", int(mask.sum()))
    return df.loc[~mask].copy()


def _standardize_sentiment_frame(
    df: pd.DataFrame,
    category_col: str,
    product_col: str,
    sentiment_col: str,
) -> pd.DataFrame:
    """Standardize sentiment data to canonical columns.

    Args:
        df: Raw sentiment DataFrame.
        category_col: Category column name.
        product_col: Product id column name.
        sentiment_col: Sentiment score column name.

    Returns:
        Cleaned DataFrame with canonical column names.
    """
    frame = df.rename(
        columns={
            category_col: CANONICAL_COLUMNS.category,
            product_col: CANONICAL_COLUMNS.product_id,
            sentiment_col: CANONICAL_COLUMNS.sentiment_score,
        }
    )
    frame = frame[
        [
            CANONICAL_COLUMNS.category,
            CANONICAL_COLUMNS.product_id,
            CANONICAL_COLUMNS.sentiment_score,
        ]
    ].copy()
    frame = _drop_missing_keys(
        frame,
        [CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id],
    )
    frame[CANONICAL_COLUMNS.sentiment_score] = pd.to_numeric(
        frame[CANONICAL_COLUMNS.sentiment_score],
        errors="coerce",
    )
    frame = frame.dropna(subset=[CANONICAL_COLUMNS.sentiment_score])
    frame[CANONICAL_COLUMNS.sentiment_score] = frame[
        CANONICAL_COLUMNS.sentiment_score
    ].clip(-1.0, 1.0)
    return frame


def _standardize_volume_frame(
    df: pd.DataFrame,
    category_col: str,
    product_col: str,
    volume_col: str,
) -> pd.DataFrame:
    """Standardize volume data to canonical columns.

    Args:
        df: Raw volume DataFrame.
        category_col: Category column name.
        product_col: Product id column name.
        volume_col: Volume column name.

    Returns:
        Cleaned DataFrame with canonical column names.
    """
    frame = df.rename(
        columns={
            category_col: CANONICAL_COLUMNS.category,
            product_col: CANONICAL_COLUMNS.product_id,
            volume_col: CANONICAL_COLUMNS.volume,
        }
    )
    frame = frame[
        [
            CANONICAL_COLUMNS.category,
            CANONICAL_COLUMNS.product_id,
            CANONICAL_COLUMNS.volume,
        ]
    ].copy()
    frame = _drop_missing_keys(
        frame,
        [CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id],
    )
    frame[CANONICAL_COLUMNS.volume] = pd.to_numeric(
        frame[CANONICAL_COLUMNS.volume],
        errors="coerce",
    ).fillna(0)
    frame[CANONICAL_COLUMNS.volume] = frame[CANONICAL_COLUMNS.volume].clip(lower=0)
    return frame


def _aggregate_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sentiment scores to product level.

    Args:
        df: Sentiment DataFrame with canonical columns.

    Returns:
        Product-level sentiment averages.
    """
    keys = [CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id]
    return (
        df.groupby(keys, sort=False, observed=True)
        .agg(sentiment_score=(CANONICAL_COLUMNS.sentiment_score, "mean"))
        .reset_index()
    )


def _aggregate_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate volume counts to product level.

    Args:
        df: Volume DataFrame with canonical columns.

    Returns:
        Product-level volume totals.
    """
    keys = [CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id]
    return (
        df.groupby(keys, sort=False, observed=True)
        .agg(**{CANONICAL_COLUMNS.volume: (CANONICAL_COLUMNS.volume, "sum")})
        .reset_index()
    )


def _derive_volume_from_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Derive volume counts from sentiment rows.

    Args:
        df: Sentiment DataFrame with canonical columns.

    Returns:
        Product-level volume totals.
    """
    keys = [CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id]
    return (
        df.groupby(keys, sort=False, observed=True)
        .size()
        .reset_index(name=CANONICAL_COLUMNS.volume)
    )


def build_master_table_from_frames(
    sentiment_df: pd.DataFrame,
    volume_df: pd.DataFrame | None,
    allow_volume_from_counts: bool,
) -> pd.DataFrame:
    """Build a master table with sentiment and volume per product.

    Args:
        sentiment_df: Sentiment DataFrame with canonical columns.
        volume_df: Volume DataFrame with canonical columns, if available.
        allow_volume_from_counts: Whether to derive volume from sentiment rows.

    Returns:
        Master table with product-level sentiment and volume.

    Raises:
        ValueError: When volume is missing and derivation is disabled.
    """
    sentiment_product = _aggregate_sentiment(sentiment_df)
    if volume_df is None:
        if not allow_volume_from_counts:
            raise ValueError("Volume data is required when allow_volume_from_counts is False.")
        LOGGER.warning("Volume column missing; deriving volume from sentiment row counts.")
        volume_product = _derive_volume_from_counts(sentiment_df)
    else:
        volume_product = _aggregate_volume(volume_df)

    master = sentiment_product.merge(
        volume_product,
        on=[CANONICAL_COLUMNS.category, CANONICAL_COLUMNS.product_id],
        how="inner",
        validate="one_to_one",
    )
    if volume_df is not None:
        dropped = len(sentiment_product) - len(master)
        if dropped > 0:
            LOGGER.warning("Dropped %s products without matching volume data", dropped)
    return master


def calculate_health_scores(master_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate health scores per category.

    Args:
        master_df: Master table with canonical columns.

    Returns:
        DataFrame with category health scores and total volume.
    """
    work = master_df.copy()
    work["weighted_sentiment"] = (
        work[CANONICAL_COLUMNS.sentiment_score] * work[CANONICAL_COLUMNS.volume]
    )
    category_stats = (
        work.groupby(CANONICAL_COLUMNS.category, sort=False, observed=True)
        .agg(
            weighted_sentiment=("weighted_sentiment", "sum"),
            total_volume=(CANONICAL_COLUMNS.volume, "sum"),
        )
        .reset_index()
    )
    avg_sentiment = np.where(
        category_stats["total_volume"] > 0,
        category_stats["weighted_sentiment"] / category_stats["total_volume"],
        np.nan,
    )
    category_stats["health_score"] = (avg_sentiment + 1.0) / 2.0
    category_stats["health_score"] = category_stats["health_score"].clip(0.0, 1.0)
    return category_stats[[CANONICAL_COLUMNS.category, "health_score", "total_volume"]]


async def _read_parquet(path: Path, columns: list[str]) -> pd.DataFrame:
    """Read a parquet file in a background thread.

    Args:
        path: Parquet file path.
        columns: Columns to read.

    Returns:
        Loaded DataFrame.
    """
    return await asyncio.to_thread(pd.read_parquet, path, columns=columns)


async def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to parquet in a background thread.

    Args:
        df: Output DataFrame.
        path: Output path.
    """
    await asyncio.to_thread(df.to_parquet, path, index=False)


async def _get_parquet_columns(path: Path) -> set[str]:
    """Read parquet columns in a background thread.

    Args:
        path: Parquet file path.

    Returns:
        Set of column names.
    """
    return await asyncio.to_thread(_read_parquet_columns, path)


@dataclass(slots=True)
class HealthScoreCalculator:
    """Orchestrate the health score calculation pipeline."""

    config: HealthScoreConfig

    async def run(self) -> pd.DataFrame:
        """Run the pipeline and write the output.

        Returns:
            DataFrame with category health scores.
        """
        self.config.validate()
        master = await self.load_master_table()
        scores = calculate_health_scores(master)
        await self.save_results(scores)
        return scores

    async def load_master_table(self) -> pd.DataFrame:
        """Load or build the master table.

        Returns:
            Master table with sentiment and volume per product.
        """
        if self.config.input_path is not None:
            return await self._load_master_from_single(self.config.input_path)
        if self.config.sentiment_path is None:
            raise ValueError("sentiment_path is required when input_path is not provided.")
        return await self._load_master_from_sources(
            self.config.sentiment_path,
            self.config.volume_path,
        )

    async def save_results(self, result_df: pd.DataFrame) -> None:
        """Persist health score results to parquet.

        Args:
            result_df: Output DataFrame.
        """
        self.config.output_path.parent.mkdir(parents=True, exist_ok=True)
        await _write_parquet(result_df, self.config.output_path)
        LOGGER.info("Health scores written to %s", self.config.output_path)

    async def _load_master_from_single(self, path: Path) -> pd.DataFrame:
        """Load a unified sentiment and volume parquet.

        Args:
            path: Parquet file path.

        Returns:
            Master table with sentiment and volume per product.
        """
        available = await _get_parquet_columns(path)
        category_col = _resolve_column(
            available,
            self.config.columns.category,
            self.config.fallback_category_columns,
            "category",
        )
        product_col = _resolve_column(
            available,
            self.config.columns.product_id,
            self.config.fallback_product_columns,
            "product_id",
        )
        sentiment_col = _resolve_column(
            available,
            self.config.columns.sentiment_score,
            self.config.fallback_sentiment_columns,
            "sentiment_score",
        )
        volume_col = _resolve_optional_column(
            available,
            self.config.columns.volume,
            self.config.fallback_volume_columns,
        )

        columns = [category_col, product_col, sentiment_col]
        if volume_col is not None:
            columns.append(volume_col)

        raw = await _read_parquet(path, columns=columns)
        sentiment_df = _standardize_sentiment_frame(
            raw,
            category_col,
            product_col,
            sentiment_col,
        )
        volume_df = None
        if volume_col is not None:
            volume_df = _standardize_volume_frame(
                raw,
                category_col,
                product_col,
                volume_col,
            )
        return build_master_table_from_frames(
            sentiment_df,
            volume_df,
            self.config.allow_volume_from_counts,
        )

    async def _load_master_from_sources(
        self,
        sentiment_path: Path,
        volume_path: Path | None,
    ) -> pd.DataFrame:
        """Load sentiment and volume from separate sources.

        Args:
            sentiment_path: Parquet path with sentiment scores.
            volume_path: Parquet path with volume counts.

        Returns:
            Master table with sentiment and volume per product.
        """
        sentiment_available = await _get_parquet_columns(sentiment_path)
        sentiment_category = _resolve_column(
            sentiment_available,
            self.config.columns.category,
            self.config.fallback_category_columns,
            "category",
        )
        sentiment_product = _resolve_column(
            sentiment_available,
            self.config.columns.product_id,
            self.config.fallback_product_columns,
            "product_id",
        )
        sentiment_score = _resolve_column(
            sentiment_available,
            self.config.columns.sentiment_score,
            self.config.fallback_sentiment_columns,
            "sentiment_score",
        )
        sentiment_raw = await _read_parquet(
            sentiment_path,
            columns=[sentiment_category, sentiment_product, sentiment_score],
        )
        sentiment_df = _standardize_sentiment_frame(
            sentiment_raw,
            sentiment_category,
            sentiment_product,
            sentiment_score,
        )

        volume_df = None
        if volume_path is not None:
            volume_available = await _get_parquet_columns(volume_path)
            volume_category = _resolve_column(
                volume_available,
                self.config.columns.category,
                self.config.fallback_category_columns,
                "category",
            )
            volume_product = _resolve_column(
                volume_available,
                self.config.columns.product_id,
                self.config.fallback_product_columns,
                "product_id",
            )
            volume_col = _resolve_column(
                volume_available,
                self.config.columns.volume,
                self.config.fallback_volume_columns,
                "volume",
            )
            volume_raw = await _read_parquet(
                volume_path,
                columns=[volume_category, volume_product, volume_col],
            )
            volume_df = _standardize_volume_frame(
                volume_raw,
                volume_category,
                volume_product,
                volume_col,
            )

        return build_master_table_from_frames(
            sentiment_df,
            volume_df,
            self.config.allow_volume_from_counts,
        )


def _setup_logging() -> None:
    """Configure module logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> HealthScoreConfig:
    """Parse CLI arguments into a HealthScoreConfig.

    Returns:
        Parsed HealthScoreConfig.
    """
    parser = argparse.ArgumentParser(description="Compute health scores per category.")
    parser.add_argument(
        "--input",
        dest="input_path",
        default="data/processed/sentiment_results.parquet",
        help="Parquet path with sentiment and volume columns.",
    )
    parser.add_argument(
        "--sentiment",
        dest="sentiment_path",
        help="Parquet path with sentiment scores.",
    )
    parser.add_argument(
        "--volume",
        dest="volume_path",
        help="Parquet path with volume counts.",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        default="data/processed/category_health_scores.parquet",
        help="Output parquet path.",
    )
    parser.add_argument(
        "--category-col",
        default=CANONICAL_COLUMNS.category,
        help="Category column name.",
    )
    parser.add_argument(
        "--product-col",
        default=CANONICAL_COLUMNS.product_id,
        help="Product id column name.",
    )
    parser.add_argument(
        "--sentiment-col",
        default=CANONICAL_COLUMNS.sentiment_score,
        help="Sentiment column name.",
    )
    parser.add_argument(
        "--volume-col",
        default=CANONICAL_COLUMNS.volume,
        help="Volume column name.",
    )
    parser.add_argument(
        "--disallow-volume-from-counts",
        action="store_true",
        help="Require a volume column or volume file.",
    )
    args = parser.parse_args()

    input_path = args.input_path
    if args.sentiment_path or args.volume_path:
        input_path = None

    columns = DataColumns(
        category=args.category_col,
        product_id=args.product_col,
        sentiment_score=args.sentiment_col,
        volume=args.volume_col,
    )
    return HealthScoreConfig(
        input_path=input_path,
        sentiment_path=args.sentiment_path,
        volume_path=args.volume_path,
        output_path=args.output_path,
        columns=columns,
        allow_volume_from_counts=not args.disallow_volume_from_counts,
    )


async def main() -> None:
    """Entry point for CLI execution."""
    _setup_logging()
    config = parse_args()
    calculator = HealthScoreCalculator(config)
    await calculator.run()


if __name__ == "__main__":
    asyncio.run(main())