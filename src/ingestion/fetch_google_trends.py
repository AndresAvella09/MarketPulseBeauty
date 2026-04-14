import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from pytrends.request import TrendReq


@dataclass
class TrendsConfig:
    keywords: List[str]
    geo: str
    timeframe: str
    hl: str
    tz: int
    category: int
    gprop: str


def interest_to_long_format(
    df: pd.DataFrame,
    *,
    keywords: List[str],
    geo: str,
    timeframe: str,
    category: int,
    gprop: str,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["date", "keyword", "interest", "isPartial", "geo", "timeframe", "category", "gprop"]
        )

    out = df.reset_index()
    is_partial = out["isPartial"] if "isPartial" in out.columns else False

    present_keywords = [k for k in keywords if k in out.columns]
    if not present_keywords:
        raise ValueError("Ninguna keyword aparece en el dataframe devuelto por pytrends.")

    long_df = out.melt(
        id_vars=["date"],
        value_vars=present_keywords,
        var_name="keyword",
        value_name="interest",
    )

    long_df["isPartial"] = (
        bool(is_partial.iloc[0]) if isinstance(is_partial, pd.Series) and len(is_partial) > 0 else False
    )
    long_df["geo"] = geo
    long_df["timeframe"] = timeframe
    long_df["category"] = category
    long_df["gprop"] = gprop
    return long_df


def fetch_interest_over_time(cfg: TrendsConfig) -> pd.DataFrame:
    pytrends = TrendReq(
        hl=cfg.hl,
        tz=cfg.tz,
        timeout=(10, 25),
        retries=3,
        backoff_factor=0.3,
    )
    pytrends.build_payload(cfg.keywords, cat=cfg.category, timeframe=cfg.timeframe, geo=cfg.geo, gprop=cfg.gprop)
    return pytrends.interest_over_time()


def main():
    parser = argparse.ArgumentParser(description="Descarga interes en el tiempo desde Google Trends (pytrends).")
    parser.add_argument(
        "--keywords",
        default="niacinamida,acido hialuronico,shampoo sin sulfatos",
        help="Lista separada por comas.",
    )
    parser.add_argument("--geo", default="CO", help="Ej: CO, US, o '' para global.")
    parser.add_argument("--timeframe", default="today 12-m", help="Ej: 'today 12-m', 'today 5-y'")
    parser.add_argument("--hl", default="es-CO", help="Ej: es-CO")
    parser.add_argument("--tz", type=int, default=300, help="Colombia UTC-5 => 300.")
    parser.add_argument("--category", type=int, default=0, help="0 = todas.")
    parser.add_argument("--gprop", default="", help="'' web, 'news', 'youtube', etc.")
    parser.add_argument("--out", default="data/raw/google_trends", help="Carpeta de salida.")
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    if len(keywords) < 3:
        raise ValueError("Debes probar al menos 3 palabras clave (Issue #8).")

    cfg = TrendsConfig(
        keywords=keywords,
        geo=args.geo,
        timeframe=args.timeframe,
        hl=args.hl,
        tz=args.tz,
        category=args.category,
        gprop=args.gprop,
    )

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_df = fetch_interest_over_time(cfg)
    long_df = interest_to_long_format(
        raw_df,
        keywords=cfg.keywords,
        geo=cfg.geo,
        timeframe=cfg.timeframe,
        category=cfg.category,
        gprop=cfg.gprop,
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    geo_tag = cfg.geo if cfg.geo else "GLOBAL"
    out_path = out_dir / f"google_trends_interest_over_time_{geo_tag}_{ts}.csv"

    long_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[OK] Archivo guardado en: {out_path}")
    print(f"[OK] Filas: {len(long_df)} | Keywords: {cfg.keywords} | Timeframe: {cfg.timeframe} | Geo: {geo_tag}")


if __name__ == "__main__":
    main()
