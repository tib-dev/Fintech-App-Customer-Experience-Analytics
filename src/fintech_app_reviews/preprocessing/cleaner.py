import logging
import pandas as pd
from fintech_app_reviews.utils.text_utils import clean_text

logger = logging.getLogger(__name__)


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw review data safely:
    - remove duplicates
    - drop empty texts
    - clean text values
    """
    try:
        if df.empty:
            logger.warning("Cleaner received empty dataframe.")
            return df

        df = df.drop_duplicates(subset=["review_id"])

        df["content"] = df["content"].apply(clean_text)
        df = df[df["content"].str.len() > 2]

        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df = df.dropna(subset=["rating"])

        return df.reset_index(drop=True)

    except Exception as e:
        logger.error(f"Cleaner failed: {e}", exc_info=True)
        return pd.DataFrame()
