"""
Unified Cleaning Pipeline
----------------------------------

This script loads raw scraped reviews, applies text cleaning, and normalizes dates
to YYYY-MM-DD, enforces the final schema, and writes a cleaned CSV that is ready
for downstream NLP and analysis steps.

Usage:
    python scripts/clean_reviews.py

Input:
    data/raw/raw_reviews.csv

Output:
    data/processed/cleaned_reviews.csv
"""

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
    format="%(asctime)s - %(levelname)s - [CLEANING] - %(message)s",
)
logger = logging.getLogger("CLEANING")

FINAL_COLUMNS: List[str] = ["review", "rating", "date", "bank", "source"]


def run_cleaning_pipeline() -> None:
    config = load_config() or {}

    raw_path = Path("data/raw/raw_reviews.csv")
    output_path = Path("data/processed/cleaned_reviews.csv")

    if not raw_path.exists():
        logger.error(f"Raw file not found: {raw_path}. Run scraper first.")
        return

    # ---------------------------------------------------
    # 1. Load raw reviews
    # ---------------------------------------------------
    df = pd.read_csv(raw_path)
    logger.info(f"Loaded {len(df)} raw rows.")

    # ---------------------------------------------------
    # 2. Clean text, drop bad ratings, dedupe
    # ---------------------------------------------------
    df = clean_reviews(df)

    if df.empty:
        logger.error("Cleaning produced no data. Stopping.")
        return

    # ---------------------------------------------------
    # 3. Normalize dates (YYYY-MM-DD)
    # ---------------------------------------------------
    # Map flexible column names
    if "review_text" in df.columns and "review" not in df.columns:
        df["review"] = df["review_text"]

    if "review_date" in df.columns and "date" not in df.columns:
        df["date"] = df["review_date"]

    if "score" in df.columns and "rating" not in df.columns:
        df["rating"] = df["score"]

    # Normalize each date safely
    if "date" in df.columns:
        def safe_norm(x):
            try:
                return normalize_date(x)
            except Exception:
                return np.nan

        df["date"] = df["date"].apply(safe_norm)
        df = df.dropna(subset=["date"])

    # ---------------------------------------------------
    # 4. Enforce final schema
    # ---------------------------------------------------
    present = [c for c in FINAL_COLUMNS if c in df.columns]
    df_final = df[present].copy()

    # Missing cell check
    missing_pct = 100 * df_final.isnull().sum().sum() / df_final.size
    logger.info(f"Missing cell percentage: {missing_pct:.2f}%")

    # ---------------------------------------------------
    # 5. Save cleaned output
    # ---------------------------------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(output_path, index=False)

    logger.info(
        f"Saved cleaned dataset ({len(df_final)} rows) → {output_path.resolve()}"
    )


if __name__ == "__main__":
    run_cleaning_pipeline()
