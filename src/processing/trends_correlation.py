from pathlib import Path
import pandas as pd
import unicodedata

def normalize_key(text: str) -> str:
    s = str(text).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.replace("-", "_").replace(" ", "_")

def week_start_monday(dt: pd.Series) -> pd.Series:
    return dt.dt.to_period("W-MON").apply(lambda p: p.start_time.date())

def minmax(x: pd.Series) -> pd.Series:
    rng = x.max() - x.min()
    if pd.isna(rng) or rng == 0:
        return pd.Series([0.0] * len(x), index=x.index)
    return (x - x.min()) / rng

def latest_csv(folder: Path) -> Path:
    files = list(folder.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay CSVs en {folder}")
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]

def assign_trend_keyword(text: str) -> str | None:
    """
    Clasifica un producto a 1 de los 3 'keywords' principales usados en Google Trends.
    Ajusta reglas si tu pd_info tiene otras variantes.
    """
    s = str(text).lower()

    # Niacinamida
    if "niacin" in s:
        return "niacinamida"

    # Ácido hialurónico (es/en)
    if "hialur" in s or "hyalur" in s:
        return "acido_hialuronico"

    # Shampoo sin sulfatos (es/en)
    if "shampoo" in s and ("sulfat" in s or "sulfate" in s):
        return "shampoo_sin_sulfatos"
    if "sulfat" in s or "sulfate" in s:
        return "shampoo_sin_sulfatos"

    return None

def main():
    trends_dir = Path("data/raw/google_trends")
    pdinfo_path = Path("data/raw/csv/pd_info.csv")
    reviews_path = Path("data/raw/csv/review_data.csv")

    if not pdinfo_path.exists():
        raise FileNotFoundError("Falta data/raw/csv/pd_info.csv")
    if not reviews_path.exists():
        raise FileNotFoundError("Falta data/raw/csv/review_data.csv")

    trends_csv = latest_csv(trends_dir)

    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- 1) Cargar Trends (formato largo: date, keyword, interest) ---
    trends = pd.read_csv(trends_csv)
    required_tr = {"date", "keyword", "interest"}
    missing_tr = required_tr - set(trends.columns)
    if missing_tr:
        raise ValueError(f"El CSV de Trends no tiene columnas esperadas {required_tr}. Faltan: {missing_tr}")

    trends["date"] = pd.to_datetime(trends["date"], errors="coerce")
    trends = trends.dropna(subset=["date"]).copy()

    # normalizar keyword de trends (para que quede como niacinamida / acido_hialuronico / shampoo_sin_sulfatos)
    trends["trend_keyword"] = trends["keyword"].apply(normalize_key)
    trends["week"] = week_start_monday(trends["date"])

    trends_weekly = (trends
        .groupby(["trend_keyword", "week"], as_index=False)
        .agg(interest_weekly=("interest", "mean"))
        .sort_values(["trend_keyword", "week"])
    )
    trends_weekly["interest_norm"] = trends_weekly.groupby("trend_keyword")["interest_weekly"].transform(minmax)

    # --- 2) Cargar pd_info y clasificar productos a los 3 trend_keywords ---
    pdinfo = pd.read_csv(pdinfo_path)
    if "pd_id" not in pdinfo.columns:
        raise ValueError("pd_info.csv debe tener columna pd_id.")

    # Construir texto “rico” para clasificar
    text_cols = [c for c in ["keyword", "Name", "Description", "category"] if c in pdinfo.columns]
    if not text_cols:
        raise ValueError("pd_info.csv no tiene columnas de texto (keyword/Name/Description/category) para clasificar.")

    pdinfo["_text"] = pdinfo[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
    pdinfo["trend_keyword"] = pdinfo["_text"].apply(assign_trend_keyword)
    pdinfo = pdinfo.drop(columns=["_text"])

    # solo productos que logramos mapear a los 3 keywords
    pdinfo_mapped = pdinfo.dropna(subset=["trend_keyword"])[["pd_id", "trend_keyword"]].copy()

    # --- 3) Cargar reviews y crear volumen semanal por trend_keyword ---
    reviews = pd.read_csv(reviews_path)
    required_rv = {"pd_id", "SubmissionTime"}
    missing_rv = required_rv - set(reviews.columns)
    if missing_rv:
        raise ValueError(f"review_data.csv no tiene columnas necesarias {required_rv}. Faltan: {missing_rv}")

    reviews["SubmissionTime"] = pd.to_datetime(reviews["SubmissionTime"], errors="coerce", utc=True)
    reviews = reviews.dropna(subset=["SubmissionTime"]).copy()
    reviews["week"] = week_start_monday(reviews["SubmissionTime"].dt.tz_convert(None))

    # unir para obtener trend_keyword por producto
    rv = reviews.merge(pdinfo_mapped, on="pd_id", how="left")
    rv = rv.dropna(subset=["trend_keyword"]).copy()

    reviews_weekly = (rv
        .groupby(["trend_keyword", "week"], as_index=False)
        .size()
        .rename(columns={"size": "n_reviews_weekly"})
        .sort_values(["trend_keyword", "week"])
    )
    reviews_weekly["n_reviews_norm"] = reviews_weekly.groupby("trend_keyword")["n_reviews_weekly"].transform(minmax)

    # --- 4) Merge semanal: Trends vs Volumen ---
    merged = trends_weekly.merge(reviews_weekly, on=["trend_keyword", "week"], how="inner")

    print("[INFO] trends_weekly rows:", len(trends_weekly))
    print("[INFO] reviews_weekly rows:", len(reviews_weekly))
    print("[INFO] merged rows:", len(merged))
    print("[INFO] trend keywords in trends:", trends_weekly["trend_keyword"].unique().tolist())
    print("[INFO] trend keywords in reviews:", reviews_weekly["trend_keyword"].unique().tolist())

    # --- 5) Correlaciones por keyword ---
    rows = []
    if len(merged) > 0:
        for k, sub in merged.groupby("trend_keyword"):
            sub = sub.dropna(subset=["interest_norm", "n_reviews_norm"])
            n = len(sub)
            pearson = sub["interest_norm"].corr(sub["n_reviews_norm"], method="pearson") if n >= 3 else None
            spearman = sub["interest_norm"].corr(sub["n_reviews_norm"], method="spearman") if n >= 3 else None
            rows.append({"trend_keyword": k, "n_weeks": n, "pearson": pearson, "spearman": spearman})

    corr = pd.DataFrame(rows)
    if not corr.empty:
        corr = corr.sort_values("trend_keyword")
    else:
        print("[WARN] No se pudo calcular correlación: merge Trends vs Reviews quedó vacío.")
        print("       Revisa si pd_info permite mapear productos a las 3 keywords (niacinamida/hialuronico/sin_sulfatos).")

    # --- 6) Guardar outputs (locales) ---
    trends_weekly.to_csv(out_dir / "trends_weekly.csv", index=False, encoding="utf-8")
    reviews_weekly.to_csv(out_dir / "reviews_weekly_volume.csv", index=False, encoding="utf-8")
    merged.to_csv(out_dir / "trends_reviews_weekly_merged.csv", index=False, encoding="utf-8")
    corr.to_csv(out_dir / "trends_reviews_correlations.csv", index=False, encoding="utf-8")

    print("[OK] escritos en data/processed:")
    print(" - trends_weekly.csv")
    print(" - reviews_weekly_volume.csv")
    print(" - trends_reviews_weekly_merged.csv")
    print(" - trends_reviews_correlations.csv")
    print("\nCorrelaciones:")
    print(corr)

if __name__ == "__main__":
    main()