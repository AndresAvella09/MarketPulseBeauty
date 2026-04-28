from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.processing.monitoring import (
    compare_metrics,
    compute_metrics,
    read_json,
    snapshot_metrics,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect metric changes between pipeline runs.")
    parser.add_argument("--previous", required=True, help="Path to previous metrics JSON")
    parser.add_argument("--current", help="Path to current metrics JSON")
    parser.add_argument("--current-reviews", help="CSV for current reviews to compute metrics")
    parser.add_argument(
        "--current-out",
        default="data/processed/monitoring/metrics_current.json",
        help="Where to write computed current metrics",
    )
    parser.add_argument(
        "--report-out",
        default="data/processed/monitoring/monitoring_report.json",
        help="Where to write the monitoring report",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    previous_path = Path(args.previous)
    previous_metrics = read_json(previous_path)

    current_metrics: dict
    if args.current_reviews:
        reviews = pd.read_csv(args.current_reviews)
        metrics = compute_metrics(reviews)
        current_metrics = snapshot_metrics(metrics)
        write_json(Path(args.current_out), current_metrics)
    elif args.current:
        current_metrics = read_json(Path(args.current))
    else:
        raise SystemExit("Provide --current or --current-reviews to compare metrics")

    report = compare_metrics(previous_metrics, current_metrics)
    report_path = Path(args.report_out)
    write_json(report_path, report)

    print("[OK] Monitoring report saved:", report_path)
    print("Summary:", report["summary"])


if __name__ == "__main__":
    main()
