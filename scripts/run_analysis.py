#!/usr/bin/env python3
"""
Run sentiment annotation on cleaned reviews
"""

import logging
import os
import pandas as pd

from fintech_app_reviews.config import load_config
from fintech_app_reviews.nlp.sentiment import annotate_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def run_sentiment(config_path="configs/nlp.yaml",
                  input_csv="data/interim/cleaned_reviews.csv",
                  output_csv="data/processed/reviews_with_sentiment.csv"):
    cfg = load_config(config_path)
    engine_cfg = cfg.get("nlp", {}).get("sentiment", {})
    engine_preference = ["transformer", "vader"] if engine_cfg.get("engine", "vader") == "transformer" else ["vader"]

    if not os.path.exists(input_csv):
        logger.error("Input CSV not found: %s", input_csv)
        return

    try:
        df = pd.read_csv(input_csv)
        logger.info("Loaded %d reviews", len(df))
    except Exception as e:
        logger.exception("Failed to load CSV: %s", e)
        return

    try:
        df = annotate_dataframe(df, text_col="review_text", engine_preference=engine_preference)
        logger.info("Sentiment annotation complete")
    except Exception as e:
        logger.exception("Sentiment annotation failed: %s", e)
        return

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    try:
        df.to_csv(output_csv, index=False)
        logger.info("Saved sentiment CSV: %s", output_csv)
    except Exception as e:
        logger.exception("Failed to save CSV: %s", e)


if __name__ == "__main__":
    run_sentiment()
