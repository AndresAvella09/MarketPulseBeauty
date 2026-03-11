from pathlib import Path
import pandas as pd

def main():
    in_path = Path("data/raw/csv/review_data.csv")
    if not in_path.exists():
        raise FileNotFoundError("No existe data/raw/csv/review_data.csv")

    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "reviews_with_sentiment.csv"

    df = pd.read_csv(in_path)

    # Columnas esperadas según scraping_quality.ipynb
    required = {"pd_id", "Rating", "ReviewText", "SubmissionTime"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en review_data.csv: {missing}")

    # Baseline: VADER (NLTK)
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer

    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon")

    sia = SentimentIntensityAnalyzer()

    df["ReviewText"] = df["ReviewText"].fillna("").astype(str)
    df["sentiment_score"] = df["ReviewText"].apply(lambda t: sia.polarity_scores(t)["compound"])

    # Etiqueta simple (útil para análisis / dashboard)
    df["sentiment_label"] = pd.cut(
        df["sentiment_score"],
        bins=[-1.0, -0.05, 0.05, 1.0],
        labels=["negative", "neutral", "positive"],
        include_lowest=True,
    )

    # Parseo de tiempo (útil para análisis temporal si lo necesitas después)
    df["SubmissionTime"] = pd.to_datetime(df["SubmissionTime"], errors="coerce", utc=True)

    df.to_csv(out_path, index=False, encoding="utf-8")
    print("[OK] output:", out_path, "| rows:", len(df))
    print(df[["pd_id", "Rating", "sentiment_score", "sentiment_label"]].head(5))

if __name__ == "__main__":
    main()