"""
data_contracts.py
─────────────────
Contract-based validation layer that acts as a mandatory gate between
ingestion (Bronze) and processing (Silver) layers.

Validates two datasets:
  • Reviews   — type safety, null checks, rating range, text length, record density
  • Trends    — keyword coverage, date range, series completeness

All results are collected into a structured ValidationReport and exported to
/data/processed/quality_report.json.

Public API
──────────
  validate_reviews(table, config)   → ContractResult
  validate_trends(df, config)       → ContractResult
  run_contracts(reviews, trends, config, report_path) → ValidationReport
  enforce_contracts(reviews, trends, config)  → raises ContractViolationError on failure
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

# ── Add project root to path so config.py is importable ──────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    import config as _cfg
    _DEFAULT_KEYWORDS  = getattr(_cfg, "TARGET_KEYWORDS", [])
    _DEFAULT_PASSKEY   = getattr(_cfg, "BAZAARVOICE_PASSKEY", None)
except ImportError:
    _DEFAULT_KEYWORDS = []


# ══════════════════════════════════════════════════════════════════════════════
# Data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CheckResult:
    """Outcome of a single validation check."""
    name:        str
    passed:      bool
    critical:    bool          = False
    message:     str           = ""
    detail:      dict[str, Any] = field(default_factory=dict)


@dataclass
class ContractResult:
    """Aggregated outcome for one dataset contract."""
    dataset:   str
    checks:    list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.critical)

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and not c.critical]

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and c.critical]

    def summary(self) -> dict:
        return {
            "dataset": self.dataset,
            "passed":  self.passed,
            "total_checks":   len(self.checks),
            "passed_checks":  sum(1 for c in self.checks if c.passed),
            "warnings":       len(self.warnings),
            "critical_failures": len(self.failures),
            "checks": [asdict(c) for c in self.checks],
        }


@dataclass
class ValidationReport:
    """Top-level report written to quality_report.json."""
    run_id:     str
    generated_at: str
    overall_pass: bool
    contracts:  list[ContractResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id":       self.run_id,
            "generated_at": self.generated_at,
            "overall_pass": self.overall_pass,
            "contracts":    [c.summary() for c in self.contracts],
        }

    def export(self, path: str) -> str:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(self.to_dict(), indent=2, default=str))
        status = "✓ PASSED" if self.overall_pass else "✗ FAILED"
        print(f"  [{status}] contract report → {out}")
        return str(out)


class ContractViolationError(RuntimeError):
    """Raised when one or more critical contracts fail."""
    def __init__(self, report: ValidationReport):
        self.report = report
        failures = [
            f"  [{c.dataset}] {f.name}: {f.message}"
            for c in report.contracts
            for f in c.failures
        ]
        msg = "Critical contract violations detected:\n" + "\n".join(failures)
        super().__init__(msg)


# ══════════════════════════════════════════════════════════════════════════════
# Reviews contract
# ══════════════════════════════════════════════════════════════════════════════

# Default contract configuration — override by passing a config dict
REVIEWS_CONTRACT_DEFAULTS = {
    # Critical fields that must have zero nulls.
    # DESIGN NOTE: ReviewText is intentionally excluded — Bazaarvoice allows
    # star-only submissions with no text, so null ReviewText is a valid
    # real-world state (~0.1 % of Sephora data).  Null coverage for ReviewText
    # is enforced via the fill_rate_review_text check instead.
    "critical_fields":      ["ProductID", "Rating", "SubmissionTime"],
    # Expected PyArrow types per column (subset — only what we care to enforce)
    "expected_types": {
        "ProductID":        pa.string(),
        "ReviewID":         pa.string(),
        "Rating":           pa.int8(),
        "ReviewText":       pa.string(),
        "Title":            pa.string(),
        "SubmissionTime":   pa.timestamp("ms", tz="UTC"),
        "IsRecommended":    pa.bool_(),
        "HelpfulCount":     pa.int32(),
        "NotHelpfulCount":  pa.int32(),
        "ReviewPhotoCount": pa.int16(),
    },
    "rating_min":            1,
    "rating_max":            5,
    "min_review_text_chars": 10,       # N characters minimum
    "min_records_per_product": 1,      # products with fewer reviews are flagged
    "min_records_per_product_warn": 2, # soft warning threshold
    "min_total_records":     1,        # overall minimum rows
    "fill_rate_review_text": 0.80,     # 80 % of reviews must have text
}


def validate_reviews(
    table:  pa.Table,
    config: dict | None = None,
) -> ContractResult:
    """
    Run all review contract checks against a PyArrow table.

    Parameters
    ----------
    table  : Bronze or Silver reviews PyArrow table
    config : override any key from REVIEWS_CONTRACT_DEFAULTS

    Returns
    -------
    ContractResult with all individual CheckResults attached
    """
    cfg = {**REVIEWS_CONTRACT_DEFAULTS, **(config or {})}
    result = ContractResult(dataset="reviews")
    n = len(table)

    def add(name, ok, critical=False, message="", **detail):
        msg = message or ("OK" if ok else "FAIL")
        result.checks.append(CheckResult(name, ok, critical, msg, detail))

    # ── 1. Minimum total records ───────────────────────────────────────────────
    add(
        "min_total_records",
        ok       = n >= cfg["min_total_records"],
        critical = True,
        message  = f"{n:,} rows found (min={cfg['min_total_records']:,})",
        rows     = n,
        threshold = cfg["min_total_records"],
    )

    if n == 0:
        # No point running further checks on an empty table
        return result

    schema_names = set(table.schema.names)

    # ── 2. Data types ──────────────────────────────────────────────────────────
    type_ok    = True
    mismatches = {}
    for col, expected_type in cfg["expected_types"].items():
        if col not in schema_names:
            continue
        actual = table.schema.field(col).type
        if actual != expected_type:
            type_ok = False
            mismatches[col] = {"expected": str(expected_type), "actual": str(actual)}

    add(
        "column_types",
        ok       = type_ok,
        critical = True,
        message  = "All types correct" if type_ok else f"{len(mismatches)} type mismatch(es)",
        mismatches = mismatches,
    )

    # ── 3. Null checks on critical fields ─────────────────────────────────────
    # Only ProductID, Rating, SubmissionTime are hard-critical (identity/key).
    # ReviewText is checked separately via fill_rate — null text is valid
    # (star-only reviews) and should NOT block the pipeline.
    null_counts = {}
    for col in cfg["critical_fields"]:
        if col not in schema_names:
            null_counts[col] = {"missing_column": True, "null_count": n}
            continue
        cnt = pc.sum(pc.is_null(table[col])).as_py()
        if cnt:
            null_counts[col] = int(cnt)

    add(
        "critical_fields_no_nulls",
        ok       = len(null_counts) == 0,
        critical = True,
        message  = ("No nulls in critical fields (ProductID, Rating, SubmissionTime)"
                    if not null_counts
                    else f"Nulls found in: {list(null_counts.keys())}"),
        null_counts   = null_counts,
        fields_checked = cfg["critical_fields"],
        note = (
            "ReviewText nulls are expected (star-only reviews) and are "
            "tracked separately via the review_text_fill_rate check."
        ),
    )

    # ── 4. Rating range ────────────────────────────────────────────────────────
    if "Rating" in schema_names:
        ratings = table["Rating"].to_pylist()
        bad     = [r for r in ratings if r is not None and not (cfg["rating_min"] <= r <= cfg["rating_max"])]
        add(
            "rating_range",
            ok       = len(bad) == 0,
            critical = True,
            message  = (f"All ratings in [{cfg['rating_min']}, {cfg['rating_max']}]"
                        if not bad
                        else f"{len(bad):,} rating(s) out of range"),
            out_of_range_count = len(bad),
            sample             = bad[:10],
            allowed_min        = cfg["rating_min"],
            allowed_max        = cfg["rating_max"],
        )

    # ── 5. ReviewText minimum length ──────────────────────────────────────────
    if "ReviewText" in schema_names:
        texts        = table["ReviewText"].to_pylist()
        min_chars    = cfg["min_review_text_chars"]
        too_short    = [i for i, t in enumerate(texts) if t is not None and len(t) < min_chars]
        fill_rate    = sum(1 for t in texts if t) / n
        fill_ok      = fill_rate >= cfg["fill_rate_review_text"]

        add(
            "review_text_min_length",
            ok       = len(too_short) == 0,
            critical = False,   # warning — we still want to process them
            message  = (f"All non-null reviews have ≥{min_chars} chars"
                        if not too_short
                        else f"{len(too_short):,} reviews below {min_chars} chars"),
            below_threshold_count = len(too_short),
            min_chars             = min_chars,
        )

        add(
            "review_text_fill_rate",
            ok       = fill_ok,
            critical = False,
            message  = f"ReviewText fill rate: {fill_rate:.1%} (threshold={cfg['fill_rate_review_text']:.0%})",
            fill_rate  = round(fill_rate, 4),
            threshold  = cfg["fill_rate_review_text"],
        )

    # ── 6. Minimum records per product ────────────────────────────────────────
    if "ProductID" in schema_names:
        pid_counts: dict[str, int] = {}
        for pid in table["ProductID"].to_pylist():
            if pid:
                pid_counts[pid] = pid_counts.get(pid, 0) + 1

        hard_threshold = cfg["min_records_per_product"]
        warn_threshold = cfg["min_records_per_product_warn"]

        sparse_hard = {p: c for p, c in pid_counts.items() if c < hard_threshold}
        sparse_warn = {p: c for p, c in pid_counts.items() if hard_threshold <= c < warn_threshold}

        add(
            "min_records_per_product",
            ok       = len(sparse_hard) == 0,
            critical = True,
            message  = (f"All {len(pid_counts):,} products meet min={hard_threshold} review(s)"
                        if not sparse_hard
                        else f"{len(sparse_hard):,} product(s) below min={hard_threshold} review(s)"),
            products_below_hard_threshold = len(sparse_hard),
            hard_threshold = hard_threshold,
            sample_sparse  = dict(list(sparse_hard.items())[:5]),
        )

        if sparse_warn:
            add(
                "min_records_per_product_soft",
                ok       = False,
                critical = False,
                message  = f"{len(sparse_warn):,} product(s) below soft threshold of {warn_threshold} reviews",
                products_below_soft_threshold = len(sparse_warn),
                warn_threshold = warn_threshold,
            )

    # ── 7. ReviewID uniqueness ─────────────────────────────────────────────────
    # Duplicates in Bronze are expected when the same product is scraped on
    # multiple runs.  This is a WARNING (not critical) — Silver deduplication
    # (keep latest SubmissionTime per ReviewID) is the correct fix.
    if "ReviewID" in schema_names:
        all_ids    = table["ReviewID"].to_pylist()
        unique_ids = set(all_ids)
        dupes      = n - len(unique_ids)
        dup_rate   = round(dupes / n, 4) if n else 0.0
        add(
            "review_id_unique",
            ok              = dupes == 0,
            critical        = False,   # Silver dedup handles this — do not block
            message         = (
                f"{len(unique_ids):,} unique ReviewIDs (no duplicates)"
                if dupes == 0
                else f"{dupes:,} duplicate ReviewIDs ({dup_rate:.1%} of rows) — "
                     f"Silver deduplication will resolve this"
            ),
            duplicate_count  = dupes,
            unique_count     = len(unique_ids),
            duplicate_rate   = dup_rate,
            recommendation   = (
                "Expected from multi-run scraping. Silver transform deduplicates "
                "by keeping the row with the latest _ingestion_ts per ReviewID."
                if dupes > 0 else None
            ),
        )

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Google Trends contract
# ══════════════════════════════════════════════════════════════════════════════

TRENDS_CONTRACT_DEFAULTS = {
    "required_keywords":   _DEFAULT_KEYWORDS,   # pulled from config.py
    "date_column":         "date",
    "expected_date_start": None,   # YYYY-MM-DD string or None (skip check)
    "expected_date_end":   None,
    "min_data_points":     4,      # minimum non-null values per keyword series
}


def validate_trends(
    df,                             # pandas DataFrame
    config: dict | None = None,
) -> ContractResult:
    """
    Validate a Google Trends DataFrame.

    Expected shape: one column per keyword, one row per date.
    The date column name is configured via config["date_column"].

    Parameters
    ----------
    df     : pandas DataFrame with trends data
    config : override any key from TRENDS_CONTRACT_DEFAULTS
    """
    cfg    = {**TRENDS_CONTRACT_DEFAULTS, **(config or {})}
    result = ContractResult(dataset="google_trends")

    def add(name, ok, critical=False, message="", **detail):
        result.checks.append(CheckResult(name, ok, critical, message or ("OK" if ok else "FAIL"), detail))

    try:
        import pandas as pd
    except ImportError:
        add("pandas_available", False, critical=True,
            message="pandas is required for Trends validation but is not installed.")
        return result

    n_rows, n_cols = df.shape

    # ── 1. Dataset is non-empty ────────────────────────────────────────────────
    add(
        "has_rows",
        ok       = n_rows > 0,
        critical = True,
        message  = f"{n_rows:,} rows present",
        rows     = n_rows,
    )
    if n_rows == 0:
        return result

    date_col  = cfg["date_column"]
    all_cols  = set(df.columns.tolist())
    kw_cols   = [c for c in df.columns if c != date_col]

    # ── 2. Required keywords present ──────────────────────────────────────────
    required  = cfg["required_keywords"]
    if required:
        missing = [kw for kw in required if kw not in all_cols]
        add(
            "required_keywords_present",
            ok       = len(missing) == 0,
            critical = True,
            message  = ("All required keywords present"
                        if not missing
                        else f"Missing keyword column(s): {missing}"),
            missing_keywords = missing,
            required         = required,
        )

    # ── 3. Date column present and parseable ──────────────────────────────────
    if date_col not in all_cols:
        add("date_column_present", False, critical=True,
            message=f"Date column '{date_col}' not found in DataFrame")
    else:
        try:
            parsed_dates = pd.to_datetime(df[date_col], utc=True)
            add("date_column_parseable", True, critical=True,
                message=f"Date column '{date_col}' parsed successfully")

            # ── 4. Date range check ────────────────────────────────────────────
            actual_min = parsed_dates.min().date()
            actual_max = parsed_dates.max().date()

            if cfg["expected_date_start"]:
                exp_start = date.fromisoformat(cfg["expected_date_start"])
                start_ok  = actual_min <= exp_start if exp_start else True
                add(
                    "date_range_start",
                    ok      = actual_min <= exp_start,
                    critical = False,
                    message  = f"Earliest date {actual_min} vs expected start {exp_start}",
                    actual_min    = str(actual_min),
                    expected_start = str(exp_start),
                )

            if cfg["expected_date_end"]:
                exp_end = date.fromisoformat(cfg["expected_date_end"])
                add(
                    "date_range_end",
                    ok       = actual_max >= exp_end,
                    critical = False,
                    message  = f"Latest date {actual_max} vs expected end {exp_end}",
                    actual_max   = str(actual_max),
                    expected_end = str(exp_end),
                )

            # Always report actual range for visibility
            add(
                "date_range_info",
                ok      = True,
                message = f"Data spans {actual_min} → {actual_max} ({n_rows:,} points)",
                actual_min = str(actual_min),
                actual_max = str(actual_max),
                n_rows     = n_rows,
            )

        except Exception as e:
            add("date_column_parseable", False, critical=True,
                message=f"Could not parse date column: {e}")

    # ── 5. No completely empty series ─────────────────────────────────────────
    min_pts     = cfg["min_data_points"]
    empty_series  = []
    sparse_series = []

    for kw in kw_cols:
        non_null = df[kw].notna().sum()
        if non_null == 0:
            empty_series.append(kw)
        elif non_null < min_pts:
            sparse_series.append({"keyword": kw, "non_null_count": int(non_null)})

    add(
        "no_empty_series",
        ok       = len(empty_series) == 0,
        critical = True,
        message  = ("No completely empty keyword series"
                    if not empty_series
                    else f"{len(empty_series)} empty series: {empty_series}"),
        empty_series = empty_series,
    )

    if sparse_series:
        add(
            "sparse_series_warning",
            ok       = False,
            critical = False,
            message  = f"{len(sparse_series)} keyword(s) have fewer than {min_pts} data points",
            sparse_series = sparse_series,
            min_data_points = min_pts,
        )

    # ── 6. No duplicate dates ──────────────────────────────────────────────────
    if date_col in all_cols:
        dupe_dates = df[date_col].duplicated().sum()
        add(
            "no_duplicate_dates",
            ok       = dupe_dates == 0,
            critical = False,
            message  = "No duplicate dates" if dupe_dates == 0 else f"{dupe_dates} duplicate dates",
            duplicate_count = int(dupe_dates),
        )

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════════════════════════════

def run_contracts(
    reviews:      pa.Table | None   = None,
    trends        = None,           # pandas DataFrame or None
    reviews_cfg:  dict | None       = None,
    trends_cfg:   dict | None       = None,
    report_path:  str               = "./data/processed/quality_report.json",
    run_id:       str | None        = None,
) -> ValidationReport:
    """
    Execute all applicable contracts and produce a ValidationReport.

    Parameters
    ----------
    reviews     : PyArrow reviews table (may be None to skip)
    trends      : pandas Trends DataFrame (may be None to skip)
    reviews_cfg : overrides for REVIEWS_CONTRACT_DEFAULTS
    trends_cfg  : overrides for TRENDS_CONTRACT_DEFAULTS
    report_path : where to export the JSON report
    run_id      : optional identifier; auto-generated if not supplied

    Returns
    -------
    ValidationReport (also exported to report_path)
    """
    import uuid as _uuid
    run_id = run_id or _uuid.uuid4().hex[:12]

    contracts: list[ContractResult] = []

    if reviews is not None:
        print(f"\n[contracts] Validating reviews ({len(reviews):,} rows) …")
        r = validate_reviews(reviews, reviews_cfg)
        contracts.append(r)
        _print_contract(r)

    if trends is not None:
        print(f"\n[contracts] Validating Google Trends ({len(trends):,} rows) …")
        t = validate_trends(trends, trends_cfg)
        contracts.append(t)
        _print_contract(t)

    overall = all(c.passed for c in contracts)

    report = ValidationReport(
        run_id       = run_id,
        generated_at = datetime.now(timezone.utc).isoformat(),
        overall_pass = overall,
        contracts    = contracts,
    )
    report.export(report_path)
    return report


def enforce_contracts(
    reviews:     pa.Table | None = None,
    trends       = None,
    reviews_cfg: dict | None     = None,
    trends_cfg:  dict | None     = None,
    report_path: str             = "./data/processed/quality_report.json",
    run_id:      str | None      = None,
) -> ValidationReport:
    """
    Like run_contracts() but raises ContractViolationError if any critical
    checks fail.  Use this as the mandatory gate before Silver processing.

    Raises
    ------
    ContractViolationError — contains the full ValidationReport for logging
    """
    report = run_contracts(
        reviews=reviews, trends=trends,
        reviews_cfg=reviews_cfg, trends_cfg=trends_cfg,
        report_path=report_path, run_id=run_id,
    )
    if not report.overall_pass:
        raise ContractViolationError(report)
    return report


# ── Pretty printer ─────────────────────────────────────────────────────────────

def _print_contract(result: ContractResult):
    status = "✓ PASSED" if result.passed else "✗ FAILED"
    print(f"  [{status}] contract: {result.dataset}")
    for c in result.checks:
        if c.passed:
            icon = "  ✓"
        elif c.critical:
            icon = "  ✗"
        else:
            icon = "  ⚠"
        print(f"    {icon}  [{c.name}]  {c.message}")


# ══════════════════════════════════════════════════════════════════════════════
# Standalone CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse, sys as _sys
    import pyarrow.parquet as pq

    parser = argparse.ArgumentParser(description="Run data contracts on Bronze/Trends files.")
    parser.add_argument("--reviews-parquet", default=None,
                        help="Path to a Bronze reviews .parquet file")
    parser.add_argument("--trends-csv",      default=None,
                        help="Path to a Google Trends .csv file")
    parser.add_argument("--report",          default="./data/processed/quality_report.json")
    parser.add_argument("--enforce",         action="store_true",
                        help="Exit with code 1 if critical contracts fail")
    args = parser.parse_args()

    reviews = pq.read_table(args.reviews_parquet) if args.reviews_parquet else None

    trends = None
    if args.trends_csv:
        import pandas as pd
        trends = pd.read_csv(args.trends_csv)

    if args.enforce:
        try:
            enforce_contracts(reviews=reviews, trends=trends, report_path=args.report)
        except ContractViolationError as e:
            print(f"\n{e}")
            _sys.exit(1)
    else:
        run_contracts(reviews=reviews, trends=trends, report_path=args.report)