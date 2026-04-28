from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

DEFAULT_STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "to",
    "of",
    "in",
    "on",
    "for",
    "with",
    "y",
    "el",
    "la",
    "los",
    "las",
    "de",
    "que",
    "un",
    "una",
}


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_text(text: str | None, stopwords: Iterable[str] | None = None) -> str:
    if text is None or pd.isna(text):
        return ""

    t = str(text).lower()
    t = re.sub(r"http\S+|www\.\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = _normalize_whitespace(t)

    if not t:
        return ""

    sw = set(stopwords) if stopwords is not None else DEFAULT_STOPWORDS
    tokens = [tok for tok in t.split() if tok and tok not in sw]
    tokens = [tok[:-1] if tok.endswith("s") and len(tok) > 3 else tok for tok in tokens]
    return " ".join(tokens)


def clean_text_series(series: pd.Series, stopwords: Iterable[str] | None = None) -> pd.Series:
    return series.apply(lambda s: clean_text(s, stopwords=stopwords))
