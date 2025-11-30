# scripts/cleaner_review.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from src.fintech_app_reviews.config import load_config
from src.fintech_app_reviews.preprocessing.cleaner import clean_reviews
from src.fintech_app_reviews.preprocessing.date_normalizer import normalize_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [CLEANING_MAIN] - %(message)s",
)
logger = logging.getLogger("CLEANING_MAIN")


FINAL_COLUMNS: List[str] = ["review", "rating", "date", "bank", "source"]


def run_cleaning_pipeline() -> None:
    """
    Main function to orchestrate the data cleaning and normalization pipeline.
    Loads data from `raw_path/raw_reviews.csv`, runs cleaning, normalizes dates,
    selects final columns and writes `interim_path/interim_reviews.csv`.
    """

    config = load_config()
    if not config:
        logger.error(
            "Failed to load configuration. Exiting cleaning pipeline.")
        return

    output_config = config.get("output", {})

    # Input Path (Raw Data)
    raw_dir = Path(output_config.get("raw_path", "data/raw"))
    raw_path = raw_dir / "raw_reviews.csv"

    # Output Path (Interim Data)
    interim_dir = Path(output_config.get("interim_path", "data/interim"))
    interim_path = interim_dir / "interim_reviews.csv"

    # 1. Load Raw Data
    if not raw_path.exists():
        logger.error(
            f"Raw data file not found at: {raw_path}. Run scrape_reviews.py first."
        )
        return

    try:
        df_raw = pd.read_csv(raw_path)
        logger.info(
            f"Successfully loaded {len(df_raw)} raw reviews from {raw_path}.")
    except Exception as e:
        logger.exception(f"Error loading raw CSV from {raw_path}: {e}")
        return

    # 2. Run Cleaning (Deduplication, Text Clean, Rating Drop)
    logger.info("Starting core review cleaning and filtering...")
    try:
        df_cleaned = clean_reviews(df_raw)
    except Exception as e:
        logger.exception(f"clean_reviews failed: {e}")
        return

    if df_cleaned is None or df_cleaned.empty:
        logger.warning(
            "Cleaning resulted in an empty DataFrame. Skipping save.")
        return

    # 3. Normalize Date (Task 1 Requirement)
    logger.info("Starting date normalization (YYYY-MM-DD)...")

    # Allow for different input column names by mapping them into the final schema
    # Common names from scraper: 'review_text' -> 'review', 'review_date' -> 'date'
    mapped = df_cleaned.copy()

    # map text column
    if "review_text" in mapped.columns and "review" not in mapped.columns:
        mapped["review"] = mapped["review_text"]

    # map date column
    if "review_date" in mapped.columns and "date" not in mapped.columns:
        mapped["date"] = mapped["review_date"]

    # map rating column
    if "rating" not in mapped.columns and "score" in mapped.columns:
        mapped["rating"] = mapped["score"]

    # Ensure required columns exist (at least review and date ideally)
    if "date" in mapped.columns:
        # apply normalize_date with safety: replace bad values with NaN
        def safe_normalize(v):
            try:
                return normalize_date(v)
            except Exception:
                return np.nan

        mapped["date"] = mapped["date"].apply(safe_normalize)
        # drop rows where normalization failed or date is missing
        mapped = mapped.replace("", np.nan).dropna(subset=["date"])
    else:
        logger.warning(
            "Missing 'date' column for normalization; continuing without normalizing dates.")

    # 4. Final Column Selection and Missing Data Check (Task 1 Requirements)
    # Keep only columns that exist in the cleaned DataFrame and are also required
    present_final_cols = [c for c in FINAL_COLUMNS if c in mapped.columns]
    df_final = mapped[present_final_cols].copy()

    if df_final.empty:
        logger.warning(
            "Final DataFrame is empty after selecting final columns. Nothing to save.")
        return

    # Calculate missing data percentage (Task 1 requirement: <5%)
    total_cells = df_final.size
    missing_cells = int(df_final.isnull().sum().sum())
    missing_percentage = (missing_cells / total_cells) * \
        100 if total_cells > 0 else 0.0

    if missing_percentage > 5:
        logger.warning(
            f"⚠️ Warning: Final dataset has {missing_percentage:.2f}% missing cell values (Target <5%). Check data quality."
        )
    else:
        logger.info(
            f"✅ Missing data check passed. Missing percentage: {missing_percentage:.2f}%.")

    # 5. Save Interim Data
    if output_config.get("save_interim", True):
        try:
            interim_dir.mkdir(parents=True, exist_ok=True)
            df_final.to_csv(interim_path, index=False)
            logger.info(
                f"Cleaned and normalized interim data ({len(df_final)} rows) saved to: {interim_path.resolve()}"
            )
        except Exception as e:
            logger.exception(
                f"Failed to save cleaned data to {interim_path}: {e}")


if __name__ == "__main__":
    run_cleaning_pipeline()
