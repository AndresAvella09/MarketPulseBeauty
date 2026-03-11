from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

def clean_text_basic(text: str) -> str:
    t = str(text).lower()
    t = re.sub(r"http\S+|www\.\S+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def main(n_topics: int = 6, top_words: int = 10):
    reviews_path = Path("data/raw/csv/review_data.csv")
    pdinfo_path = Path("data/raw/csv/pd_info.csv")
    if not reviews_path.exists():
        raise FileNotFoundError("No existe data/raw/csv/review_data.csv")

    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_reviews = out_dir / "reviews_with_topics.csv"
    out_topics = out_dir / "topics_summary.csv"

    reviews = pd.read_csv(reviews_path)

    required = {"pd_id", "ReviewText", "SubmissionTime"}
    missing = required - set(reviews.columns)
    if missing:
        raise ValueError(f"Faltan columnas en review_data.csv: {missing}")

    reviews["ReviewText"] = reviews["ReviewText"].fillna("").astype(str)
    reviews["clean_text"] = reviews["ReviewText"].apply(clean_text_basic)

    # Modelo NMF sobre TF-IDF (baseline rápido y coherente)
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import NMF

    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words="english",   # tus reviews están en inglés en el ejemplo del notebook
        max_features=8000,
        ngram_range=(1, 2),
        min_df=5
    )
    X = vectorizer.fit_transform(reviews["clean_text"])

    model = NMF(
        n_components=n_topics,
        random_state=42,
        init="nndsvda",
        max_iter=400
    )
    W = model.fit_transform(X)      # doc-topic
    H = model.components_           # topic-word

    reviews["topic_id"] = W.argmax(axis=1)

    feature_names = vectorizer.get_feature_names_out()
    rows = []
    for t in range(n_topics):
        top_idx = H[t].argsort()[::-1][:top_words]
        rows.append({
            "topic_id": t,
            "top_terms": ", ".join(feature_names[i] for i in top_idx)
        })
    topics_df = pd.DataFrame(rows)

    # (Opcional útil) unir keyword/brand si pd_info existe
    if pdinfo_path.exists():
        pdinfo = pd.read_csv(pdinfo_path)
        if "pd_id" in pdinfo.columns:
            keep_cols = [c for c in ["pd_id", "keyword", "brand", "category", "Name"] if c in pdinfo.columns]
            reviews = reviews.merge(pdinfo[keep_cols], on="pd_id", how="left")

    reviews.to_csv(out_reviews, index=False, encoding="utf-8")
    topics_df.to_csv(out_topics, index=False, encoding="utf-8")

    print("[OK] reviews_with_topics:", out_reviews, "| rows:", len(reviews))
    print("[OK] topics_summary:", out_topics)
    print(topics_df)

if __name__ == "__main__":
    main()