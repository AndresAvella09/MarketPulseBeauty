"""
cleaning.py
───────────
NLP text-cleaning utilities for the Sephora Silver layer.

Pipeline per text field (in order)
────────────────────────────────────
  1. Lowercase
  2. Punctuation removal
  3. Stop-word removal       (spaCy's built-in English stop-word list)
  4. Tokenization            (spaCy tokenizer -- handles contractions, etc.)
  5. Lemmatization           (spaCy lemmatizer -- "running" -> "run")

Outputs per field
─────────────────
  <field>_clean   : str   - lowercased, no punct, no stopwords, space-joined
  <field>_tokens  : str   - pipe-joined raw tokens (before stopword removal)
  <field>_lemmas  : str   - pipe-joined lemmas     (after stopword removal)

Public API
──────────
  load_nlp()                -> spacy.Language
  clean_field(nlp, text)    -> CleanResult (named tuple)
  clean_batch(nlp, texts)   -> list[CleanResult]   (uses nlp.pipe -- fast)
"""

import re
import string
from typing import NamedTuple

# ── spaCy model loading ────────────────────────────────────────────────────────

_NLP_SINGLETON = None

def load_nlp():
    """
    Load (and cache) the spaCy English model.

    Only the tokenizer, tagger, and lemmatizer are enabled -- the parser
    and NER are disabled for speed since we don't need them here.

    First run:  python -m spacy download en_core_web_sm
    """
    global _NLP_SINGLETON
    if _NLP_SINGLETON is None:
        try:
            import spacy
            _NLP_SINGLETON = spacy.load(
                "en_core_web_sm",
                disable=["parser", "ner"],
            )
            print("[NLP] spaCy model 'en_core_web_sm' loaded.")
        except OSError:
            raise OSError(
                "spaCy model not found.\n"
                "Run:  python -m spacy download en_core_web_sm\n"
                "Then retry."
            )
    return _NLP_SINGLETON


# ── Result container ───────────────────────────────────────────────────────────

class CleanResult(NamedTuple):
    clean:     str | None   # lowercased, no punct, no stopwords - space-joined
    tokens:    str | None   # pipe-joined raw tokens (before stopword filter)
    lemmas:    str | None   # pipe-joined lemmas (stopwords removed)
    wordcount: int          # token count after full cleaning (for reviews)


# ── Preprocessing (applied before spaCy) ──────────────────────────────────────

_PUNCT_RE   = re.compile(r"[^\w\s]")   # anything that's not word-char or space
_SPACE_RE   = re.compile(r"\s+")

def _preprocess(text: str) -> str:
    """Lowercase -> strip punctuation -> collapse whitespace."""
    text = text.lower()
    text = _PUNCT_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text


# ── Core cleaning function ─────────────────────────────────────────────────────

def clean_field(nlp, text: str | None) -> CleanResult:
    """
    Clean a single text field.

    Parameters
    ----------
    nlp   : spaCy Language object from load_nlp()
    text  : raw string (may be None)

    Returns
    -------
    CleanResult with clean, tokens, lemmas, wordcount
    """
    if not text or not text.strip():
        return CleanResult(clean=None, tokens=None, lemmas=None, wordcount=0)

    preprocessed = _preprocess(text)
    doc = nlp(preprocessed)

    # Raw tokens (punct already stripped; keep non-space tokens)
    raw_tokens = [tok.text for tok in doc if not tok.is_space]

    # Filtered: no stop-words, non-empty
    filtered = [
        tok for tok in doc
        if not tok.is_stop
        and not tok.is_space
        and tok.text.strip()
    ]

    clean_words  = [tok.text   for tok in filtered]
    lemma_words  = [tok.lemma_ for tok in filtered]

    return CleanResult(
        clean     = " ".join(clean_words) or None,
        tokens    = " | ".join(raw_tokens) or None,
        lemmas    = " | ".join(lemma_words) or None,
        wordcount = len(clean_words),
    )


# ── Batch cleaning (uses nlp.pipe for throughput) ─────────────────────────────

def clean_batch(
    nlp,
    texts: list[str | None],
    batch_size: int = 256,
    n_process:  int = 1,
) -> list[CleanResult]:
    """
    Clean a list of texts efficiently using spaCy's nlp.pipe().

    Parameters
    ----------
    nlp        : spaCy Language object
    texts      : list of raw strings (None values are handled)
    batch_size : number of texts per spaCy batch
    n_process  : parallel workers (set >1 on multi-core machines)

    Returns
    -------
    list[CleanResult] -- same length and order as input
    """
    results: list[CleanResult] = []

    # Separate real texts from None/empty so we don't pass nulls to spaCy
    indices_to_process = []
    preprocessed_texts = []

    for i, text in enumerate(texts):
        if text and text.strip():
            indices_to_process.append(i)
            preprocessed_texts.append(_preprocess(text))

    # Pre-fill with empty results
    results = [CleanResult(None, None, None, 0)] * len(texts)

    # Process non-null texts in bulk
    docs = nlp.pipe(
        preprocessed_texts,
        batch_size=batch_size,
        n_process=n_process,
    )

    for idx, doc in zip(indices_to_process, docs):
        raw_tokens = [tok.text for tok in doc if not tok.is_space]
        filtered   = [
            tok for tok in doc
            if not tok.is_stop
            and not tok.is_space
            and tok.text.strip()
        ]
        clean_words = [tok.text   for tok in filtered]
        lemma_words = [tok.lemma_ for tok in filtered]

        results[idx] = CleanResult(
            clean     = " ".join(clean_words) or None,
            tokens    = " | ".join(raw_tokens) or None,
            lemmas    = " | ".join(lemma_words) or None,
            wordcount = len(clean_words),
        )

    return results


# ── Convenience: clean a whole DataFrame column ───────────────────────────────

def clean_column(nlp, column: list[str | None], **batch_kwargs) -> dict[str, list]:
    """
    Clean an entire column at once.

    Returns a dict ready to merge into a pandas/PyArrow table:
      {
        "<suffix>_clean":     [...],
        "<suffix>_tokens":    [...],
        "<suffix>_lemmas":    [...],
        "<suffix>_wordcount": [...],
      }
    """
    results = clean_batch(nlp, column, **batch_kwargs)
    return {
        "clean":     [r.clean     for r in results],
        "tokens":    [r.tokens    for r in results],
        "lemmas":    [r.lemmas    for r in results],
        "wordcount": [r.wordcount for r in results],
    }
