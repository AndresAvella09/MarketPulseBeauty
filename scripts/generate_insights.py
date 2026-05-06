from __future__ import annotations

import argparse
from pathlib import Path

from src.processing.insights import generate_insights_report, read_table, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate automatic insights per product.")
    parser.add_argument("--reviews", required=True, help="Path to reviews CSV or Parquet")
    parser.add_argument("--products", help="Optional products CSV or Parquet")
    parser.add_argument("--trends", help="Optional trends CSV (weekly or raw)")
    parser.add_argument("--trend-map", help="Optional mapping CSV (product_id to trend_keyword)")
    parser.add_argument(
        "--window-weeks",
        type=int,
        default=4,
        help="Number of weeks to compare for trend signals",
    )
    parser.add_argument(
        "--out",
        default="data/processed/insights/insights.json",
        help="Output JSON path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    reviews = read_table(Path(args.reviews))
    products = read_table(Path(args.products)) if args.products else None
    trends = read_table(Path(args.trends)) if args.trends else None
    trend_map = read_table(Path(args.trend_map)) if args.trend_map else None

    report = generate_insights_report(
        reviews,
        products=products,
        trends=trends,
        trend_map=trend_map,
        window_weeks=args.window_weeks,
    )

    out_path = Path(args.out)
    write_json(out_path, report)

    print("[OK] Insights saved:", out_path)
    print("Summary:", report["summary"])


if __name__ == "__main__":
    main()
