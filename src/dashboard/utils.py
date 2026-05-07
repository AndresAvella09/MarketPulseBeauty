from __future__ import annotations

import re
import unicodedata


FOCUS_LABELS = {
    "niacinamida": "Niacinamida",
    "acido_hialuronico": "Ácido hialurónico",
    "shampoo_sin_sulfatos": "Shampoo sin sulfatos",
}

FOCUS_PATTERNS = {
    "niacinamida": [
        "niacinamide",
        "niacinamida",
    ],
    "acido_hialuronico": [
        "hyaluronic",
        "hyaluronic acid",
        "hialuronic",
        "hialuronico",
        "hialurónico",
    ],
    "shampoo_sin_sulfatos": [
        "shampoo sulfate free",
        "sulfate free shampoo",
        "sulfate-free shampoo",
        "sin sulfatos",
        "sulfate free",
        "sulfate-free"
        "hydrating shampoo"
        "moisture shampoo"
        "shampoo",
    ],
}


def normalize_text(text: str) -> str:
    text = "" if text is None else str(text)
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def classify_focus_keyword(product_name: str) -> str | None:
    text = normalize_text(product_name)

    for key, patterns in FOCUS_PATTERNS.items():
        for pattern in patterns:
            if pattern in text:
                return key

    return None


def pretty_keyword(value: str) -> str:
    return FOCUS_LABELS.get(value, value)