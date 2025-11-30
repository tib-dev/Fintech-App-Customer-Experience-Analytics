import logging
from typing import List, Dict
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_tfidf_keywords_per_group(df: pd.DataFrame,
                                     text_col: str,
                                     group_col: str,
                                     top_n: int = 20,
                                     ngram_range=(1,2)) -> Dict[str, List[str]]:
    """
    Extract top TF-IDF keywords per group (e.g., per bank).
    Returns a dict {group_name: [keywords]}
    """
    result = {}
    for group, group_df in df.groupby(group_col):
        try:
            texts = group_df[text_col].fillna("").tolist()
            if not texts:
                result[group] = []
                continue

            vectorizer = TfidfVectorizer(stop_words="english", ngram_range=ngram_range)
            tfidf_matrix = vectorizer.fit_transform(texts)
            feature_names = vectorizer.get_feature_names_out()
            scores = tfidf_matrix.sum(axis=0).A1
            top_indices = scores.argsort()[::-1][:top_n]
            top_keywords = [feature_names[i] for i in top_indices]
            result[group] = top_keywords
        except Exception as e:
            logger.exception("Failed to extract keywords for group %s: %s", group, e)
            result[group] = []
    return result


def attach_top_keywords_to_df(df: pd.DataFrame, text_col: str, top_k: int = 5, global_tfidf: List[str] = None):
    if global_tfidf is None:
        global_tfidf = []

    keywords_list = []
    for text in df[text_col].fillna(""):
        tokens = [kw for kw in global_tfidf if kw in text.lower()]
        keywords_list.append("|".join(tokens[:top_k]))

    df["keywords"] = keywords_list
    return df, keywords_list  # <- return both
