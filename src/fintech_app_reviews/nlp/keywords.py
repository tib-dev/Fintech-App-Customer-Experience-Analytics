import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import re
from typing import List, Dict, Sequence


def extract_tfidf_keywords_per_group(
    df: pd.DataFrame,
    text_col: str,
    group_col: str,
    top_n: int = 50,
    ngram_range: tuple[int, int] = (1, 2),
    min_df: int = 2,
    max_features: int = 5000
) -> Dict[str, List[str]]:
    """
    Compute top TF-IDF keywords per group.

    Args:
        df (pd.DataFrame): Input DataFrame containing text and group columns.
        text_col (str): Column containing text for TF-IDF.
        group_col (str): Column to group by (e.g., bank).
        top_n (int): Top N keywords per group.
        ngram_range (tuple[int,int]): Min and max ngram sizes.
        min_df (int): Minimum document frequency.
        max_features (int): Maximum features for TF-IDF.

    Returns:
        Dict[str, List[str]]: Mapping from group -> top keywords list.
    """
    groups = {}
    for group, gdf in df.groupby(group_col):
        docs = gdf[text_col].fillna("").astype(str).tolist()
        if not docs:
            groups[group] = []
            continue
        min_df_here = min_df if len(docs) >= min_df else 1
        vect = TfidfVectorizer(ngram_range=ngram_range,
                               min_df=min_df_here,
                               max_features=max_features)
        try:
            X = vect.fit_transform(docs)
        except ValueError:
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
    Attach top keywords from global TF-IDF list to each row of DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        text_col (str): Column to scan for keywords.
        global_tfidf (Sequence[str]|None): List of candidate keywords.
        top_k (int): Maximum keywords per review.
        separator (str): Separator for multiple keywords.

    Returns:
        pd.DataFrame: Copy of DataFrame with 'keywords' column.
    """
    out = df.copy()
    if global_tfidf is None:
        out["keywords"] = ""
        return out

    candidates = sorted(global_tfidf, key=lambda t: (-(" " in t), len(t)))
    cand_patterns = [(t, re.compile(rf"\b{re.escape(t)}\b", flags=re.IGNORECASE))
                     for t in candidates]

    def _find_keywords(text: str) -> str:
        if not isinstance(text, str):
            return ""
        text = text.lower()
        found = []
        for term, pat in cand_patterns:
            if pat.search(text):
                found.append(term)
            if len(found) >= top_k:
                break
        return separator.join(found)

    out["keywords"] = out[text_col].fillna(
        "").astype(str).apply(_find_keywords)
    return out
# Stopwords (expandable)
STOPWORDS = {
    "the", "and", "a", "an", "is", "it", "this", "that", "to",
    "for", "in", "on", "of", "with", "at", "as", "our", "be"
}


def preprocess_text(text: str) -> str:
    """Clean text: lowercase, remove URLs, non-alphanumeric, stopwords."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [w for w in text.split() if w not in STOPWORDS and len(w) > 2]
    return " ".join(tokens)
