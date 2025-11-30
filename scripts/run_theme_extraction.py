#!/usr/bin/env python3
"""
Run theme extraction pipeline:
 - Extract keywords using TF-IDF
 - Map keywords to themes using rule-based rules
 - Export enriched CSV
"""

import logging
import os
import pandas as pd

from fintech_app_reviews.config import load_config
from fintech_app_reviews.nlp.keywords import extract_tfidf_keywords_per_group, attach_top_keywords_to_df
from fintech_app_reviews.nlp.themes import map_keywords_to_themes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def run_theme_extraction(config_path="configs/nlp.yaml",
                         input_csv="data/processed/reviews_with_sentiment.csv",
                         output_csv="data/processed/reviews_with_themes.csv"):
    cfg = load_config(config_path)

    if not os.path.exists(input_csv):
        logger.error("Input CSV not found: %s", input_csv)
        return

    try:
        df = pd.read_csv(input_csv)
        logger.info("Loaded %d reviews", len(df))
    except Exception as e:
        logger.exception("Failed to load CSV: %s", e)
        return

    # 1) Extract keywords per bank
    try:
        groups = extract_tfidf_keywords_per_group(df, text_col="review_text", group_col="bank", top_n=50, ngram_range=(1,2))
        global_keywords = []
        for kws in groups.values():
            global_keywords.extend(kws[:30])
        global_keywords = list(dict.fromkeys(global_keywords))
        df = attach_top_keywords_to_df(df, text_col="review_text", top_k=5, global_tfidf=global_keywords)
        logger.info("Keywords extraction complete")
    except Exception as e:
        logger.exception("Keyword extraction failed: %s", e)
        df["keywords"] = ""

    # 2) Map keywords to themes
    try:
        df["themes"] = df["keywords"].fillna("").apply(lambda s: map_keywords_to_themes(s.split("|") if s else []))
        df["themes"] = df["themes"].apply(lambda lst: "|".join(lst) if isinstance(lst,list) else "")
        logger.info("Theme mapping complete")
    except Exception as e:
        logger.exception("Theme mapping failed: %s", e)
        df["themes"] = ""

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    try:
        df.to_csv(output_csv, index=False)
        logger.info("Saved themed reviews CSV: %s", output_csv)
    except Exception as e:
        logger.exception("Failed to save CSV: %s", e)


if __name__ == "__main__":
    run_theme_extraction()
