"""
Fintech App Reviews Analysis Pipeline

Steps:
1. Preprocess review text (clean, lowercase, remove stopwords)
2. Extract TF-IDF keywords per bank
3. Assign themes using rule-based patterns
4. Annotate sentiment using DistilBERT (parallel)
5. Aggregate sentiment metrics per bank/rating
6. Save results as CSV
"""

import logging
import pandas as pd
from typing import Dict, List
from fintech_app_reviews.nlp.keywords import extract_tfidf_keywords_per_group, attach_top_keywords_to_df, preprocess_text
from fintech_app_reviews.nlp.themes import compile_keyword_patterns, rule_assign_themes
from fintech_app_reviews.nlp.sentiment_bert import annotate_dataframe_parallel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# Theme rules
# -------------------------
theme_map: Dict[str, List[str]] = {
    "Account Access": ["login", "password", "pin", "otp", "locked", "signin", "sign in", "sign-in", "face id", "biometric", "fingerprint"],
    "Performance": ["slow", "lag", "loading", "load", "timeout", "time out", "transfer slow", "speed", "delay"],
    "Stability/Reliability": ["crash", "crashes", "freeze", "bug", "bugs", "error", "failed", "stopped working"],
    "UI/UX": ["ui", "interface", "design", "navigation", "layout", "ux", "confusing", "user friendly", "easy to use"],
    "Support": ["support", "customer service", "agent", "response", "help", "contact", "call center"],
    "Feature Request": ["fingerprint", "biometric", "transfer", "scan", "receipt", "balance", "notification", "offline"]
}
compiled_map = compile_keyword_patterns(theme_map)

# -------------------------
# Pipeline
# -------------------------


def run_pipeline(
    df: pd.DataFrame,
    text_col: str = "review",
    bank_col: str = "bank",
    top_n_keywords: int = 50
) -> pd.DataFrame:
    """
    Process reviews dataframe to extract keywords, assign themes, and compute sentiment.

    Args:
        df (pd.DataFrame): Input dataframe with columns ['review','bank'].
        text_col (str): Column name containing review text.
        bank_col (str): Column name containing bank.
        top_n_keywords (int): Number of TF-IDF keywords to extract per bank.

    Returns:
        pd.DataFrame: Annotated dataframe with 'txt_clean', 'keywords', 'themes', 'sentiment_label', 'sentiment_score'.
    """
    # -------------------------
    # Preprocess text
    # -------------------------
    logger.info("Preprocessing text...")
    df["txt_clean"] = df[text_col].apply(preprocess_text)

    # -------------------------
    # TF-IDF keywords per bank
    # -------------------------
    logger.info("Extracting top TF-IDF keywords per bank...")
    top_keywords_dict = extract_tfidf_keywords_per_group(
        df,
        text_col="txt_clean",
        group_col=bank_col,
        top_n=top_n_keywords
    )

    global_tfidf = sum(top_keywords_dict.values(), [])
    df = attach_top_keywords_to_df(
        df,
        text_col="txt_clean",
        global_tfidf=global_tfidf
    )

    # -------------------------
    # Rule-based theme assignment
    # -------------------------
    logger.info("Assigning themes...")

    def safe_assign_themes(text: str, compiled_map=compiled_map) -> List[str]:
        if not text or not isinstance(text, str):
            return []
        return rule_assign_themes(text, compiled_map)

    df["themes"] = df["txt_clean"].apply(safe_assign_themes)

    # -------------------------
    # BERT sentiment
    # -------------------------
    logger.info("Computing sentiment using DistilBERT...")
    df = annotate_dataframe_parallel(df, text_col="txt_clean")

    logger.info("Pipeline complete")
    return df

# -------------------------
# Aggregation helper
# -------------------------


def aggregate_sentiment(df: pd.DataFrame, group_cols=["bank", "rating"]) -> pd.DataFrame:
    """
    Aggregate mean sentiment score and counts by group (bank, rating).

    Args:
        df (pd.DataFrame): Annotated dataframe with sentiment_label and sentiment_score.
        group_cols (list[str]): Columns to group by.

    Returns:
        pd.DataFrame: Aggregated metrics.
    """
    agg_df = df.groupby(group_cols).agg(
        mean_sentiment_score=("sentiment_score", "mean"),
        positive_count=("sentiment_label", lambda x: (x == "positive").sum()),
        negative_count=("sentiment_label", lambda x: (x == "negative").sum()),
        neutral_count=("sentiment_label", lambda x: (x == "neutral").sum()),
        review_count=(df.columns[0], "count")
    ).reset_index()
    return agg_df
