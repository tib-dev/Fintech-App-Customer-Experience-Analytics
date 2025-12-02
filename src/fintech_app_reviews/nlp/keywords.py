"""
keywords.py

TF-IDF keyword extraction and per-row keyword annotation.

Improvements:
- Handles missing or empty text safely.
- Optional lemmatization for better keyword matching.
- Safer regex compilation for keyword search.
- Logging for empty or failed groups.
- Clear docstrings and typing.
"""

import re
import logging
from typing import List, Dict, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# Optional NLP: Lemmatization
try:
    import nltk
    from nltk.stem import WordNetLemmatizer
    nltk.download("wordnet", quiet=True)
    nltk.download("omw-1.4", quiet=True)
    LEMMATIZER = WordNetLemmatizer()
except Exception:
    LEMMATIZER = None

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def extract_tfidf_keywords_per_group(
    df: pd.DataFrame,
    text_col: str,
    group_col: str,
    top_n: int = 50,
    ngram_range: tuple[int, int] = (1, 2),
    min_df: int = 2,
    max_features: int = 5000
) -> Dict[str, list[str]]:
    """
    Compute top TF-IDF keywords per group (e.g., per bank).

    Args:
        df (pd.DataFrame): DataFrame with text and group columns.
        text_col (str): Column containing text for TF-IDF.
        group_col (str): Column to group by.
        top_n (int): Top N keywords to return per group.
        ngram_range (tuple[int,int]): Min and max ngram sizes.
        min_df (int): Minimum document frequency.
        max_features (int): Max features for TF-IDF.

    Returns:
        dict: Mapping from group -> list of top keywords.
    """
    groups = {}
    for group, gdf in df.groupby(group_col):
        docs = gdf[text_col].fillna("").astype(str).tolist()
        if not docs:
            logger.warning("No documents for group '%s'", group)
            groups[group] = []
            continue

        min_df_here = min_df if len(docs) >= min_df else 1
        vect = TfidfVectorizer(
            ngram_range=ngram_range,
            min_df=min_df if len(docs) >= min_df else 1,
            max_features=max_features
        )

        try:
            X = vect.fit_transform(docs)
        except ValueError as e:
            logger.warning("TF-IDF failed for group '%s': %s", group, e)
            groups[group] = []
            continue

        features = np.array(vect.get_feature_names_out())
        mean_tfidf = np.asarray(X.mean(axis=0)).ravel()
        idx = mean_tfidf.argsort()[::-1][:top_n]
        groups[group] = features[idx].tolist()

    return groups


def attach_top_keywords_to_df(
    df: pd.DataFrame,
    text_col: str,
    global_tfidf: Sequence[str] | None = None,
    top_k: int = 5,
    separator: str = "|"
) -> pd.DataFrame:
    """
    Annotate each row with top keywords found in text.

    Args:
        df (pd.DataFrame): DataFrame containing text.
        text_col (str): Column containing text to scan.
        global_tfidf (Sequence[str] | None): Candidate keywords from TF-IDF.
        top_k (int): Max keywords per row.
        separator (str): Separator for multiple keywords.

    Returns:
        pd.DataFrame: Copy of DataFrame with 'keywords' column added.
    """
    out = df.copy()
    if global_tfidf is None:
        out["keywords"] = ""
        return out

    # Optional: sort longer ngrams first for matching
    candidates = sorted(global_tfidf, key=lambda t: (-(" " in t), len(t)))
    cand_patterns: List[Tuple[str, re.Pattern]] = []
    for t in candidates:
        try:
            cand_patterns.append((t, re.compile(rf"\b{re.escape(t)}\b", flags=re.IGNORECASE)))
        except re.error as e:
            logger.warning("Invalid regex for keyword '%s': %s", t, e)

    def _find_keywords(text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        # Optional: lemmatize
        if LEMMATIZER:
            text_tokens = [LEMMATIZER.lemmatize(w) for w in text.lower().split()]
            text_proc = " ".join(text_tokens)
        else:
            text_proc = text.lower()

        found = []
        for term, pat in cand_patterns:
            if pat.search(text_proc):
                found.append(term)
            if len(found) >= top_k:
                break
        return separator.join(found)

    out["keywords"] = out[text_col].fillna("").astype(str).apply(_find_keywords)
    return out
