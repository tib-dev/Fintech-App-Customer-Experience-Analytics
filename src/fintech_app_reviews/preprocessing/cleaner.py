
import logging
import pandas as pd
import numpy as np
# Assuming utils is correct
from fintech_app_reviews.utils.text_utils import clean_text

logger = logging.getLogger(__name__)


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw review data safely:
    - Standardize column names (review, rating).
    - Remove duplicates based on 'review_id'.
    - Clean text values and drop empty/short reviews.
    - Drop rows with missing ratings.
    """
    try:
        if df.empty:
            logger.warning("Cleaner received empty dataframe.")
            return df

        # 0. Standardize Column Names: Map common scraper output names to final required names.
        # This handles cases where raw data uses 'content' or 'score'
        rename_map = {
          'review_text': 'review',  # <- match your scraper output
          'score': 'rating',        # already optional fallback
         }
        df.rename(columns=rename_map, inplace=True)

        # Ensure the required columns for filtering are present. If not, cleaning can't proceed.
        if 'review' not in df.columns or 'rating' not in df.columns:
            logger.error(
                "Required columns ('review' or 'rating') are missing after standardization. Returning original DF columns for debugging.")
            # We return the original DF in a non-failing state for better error investigation, though the test will still fail
            # Returning empty DF to satisfy the test logic expectation for a clean fail.
            return pd.DataFrame()

        initial_count = len(df)

        # 1. Remove Duplicates
        if "review_id" in df.columns:
            df = df.drop_duplicates(subset=["review_id"], keep='first')
            logger.info(
                f"Removed {initial_count - len(df)} duplicate rows based on review_id.")
            initial_count = len(df)

        # 2. Clean Text
        # The clean_text function handles NaN/None values by returning ""
        df['review'] = df['review'].apply(clean_text)

        # 3. Drop rows with short/empty text (including those that were NaN/None)
        df = df[df["review"].str.len() > 2]
        logger.info(
            f"Dropped {initial_count - len(df)} rows with empty/short text.")
        initial_count = len(df)

        # 4. Handle Missing/Invalid Rating
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df = df.dropna(subset=["rating"])
        logger.info(
            f"Dropped {initial_count - len(df)} rows with missing or invalid ratings.")

        return df.reset_index(drop=True)

    except Exception as e:
        logger.error(f"Cleaner failed: {e}", exc_info=True)
        return pd.DataFrame()
