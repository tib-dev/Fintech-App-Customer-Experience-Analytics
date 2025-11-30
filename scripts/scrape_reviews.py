from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.fintech_app_reviews.config import load_config
from src.fintech_app_reviews.scraper.google_play_scraper import scrape_app_reviews

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [SCRAPER_MAIN] - %(message)s",
)
logger = logging.getLogger("SCRAPER_MAIN")


def run_scraper_pipeline() -> pd.DataFrame | None:
    """
    Orchestrates the scraping process for multiple apps defined in the configuration.
    Returns the DataFrame if saved/created, otherwise None.
    """
    config = load_config()
    if not config:
        logger.error(
            "Failed to load configuration. Exiting scraping pipeline.")
        return None

    scraper_config = config.get("scraper", {})
    output_config = config.get("output", {})

    app_ids: List[str] = scraper_config.get("app_ids", [])
    bank_mapping: Dict[str, str] = config.get("bank_mapping", {})
    max_reviews: int = int(scraper_config.get("max_reviews", 100))
    sort_by: str = scraper_config.get("sort_by", "newest")
    network_timeout: int = int(config.get("network", {}).get("timeout", 5))
    retries: int = int(scraper_config.get("retries", 2))

    if not app_ids:
        logger.warning("No app IDs found in configuration. Nothing to scrape.")
        return None

    # Ensure at least one review targeted per app
    max_per_app = max(1, max_reviews // len(app_ids))

    all_reviews: List[Dict[str, Any]] = []

    for app_id in app_ids:
        bank_name = bank_mapping.get(app_id, "Unknown Bank")
        logger.info(
            f"Starting scrape for {bank_name} ({app_id}) - targeting {max_per_app} reviews."
        )

        last_exc: Exception | None = None
        for attempt in range(1, retries + 2):
            try:
                reviews = scrape_app_reviews(
                    app_id=app_id,
                    app_id_to_bank=bank_mapping,
                    max_reviews=max_per_app,
                    sort_by=sort_by,
                    timeout=network_timeout,
                )
                if not reviews:
                    logger.info(
                        f"No reviews returned for {app_id} on attempt {attempt}.")
                    reviews = []
                else:
                    # Ensure it's a list
                    if not isinstance(reviews, list):
                        logger.warning(
                            f"scrape_app_reviews for {app_id} did not return a list. "
                            "Casting to list."
                        )
                        reviews = list(reviews)

                all_reviews.extend(reviews)
                logger.info(
                    f"Collected {len(reviews)} reviews for {bank_name}.")
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    f"Attempt {attempt} failed for {app_id}: {exc!r}. "
                    f"{'Retrying' if attempt < retries + 1 else 'No more retries.'}"
                )
        if last_exc:
            logger.error(
                f"Failed to scrape {app_id} after retries: {last_exc!r}")

    if not all_reviews:
        logger.warning(
            "No reviews were collected across all apps. Skipping file save.")
        return None

    df = pd.DataFrame(all_reviews)

    # Optional deduplication if there's a review id in data
    if "reviewId" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["reviewId"])
        logger.info(f"Deduplicated reviews: {before} -> {len(df)} rows.")

    # Save Raw Data (CSV and Parquet)
    if output_config.get("save_raw", True):
        raw_dir = Path(output_config.get("raw_path", "data/raw"))
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_csv = raw_dir / "raw_reviews.csv"
        raw_parquet = raw_dir / "raw_reviews.parquet"

        try:
            df.to_csv(raw_csv, index=False)
            # Save parquet if pyarrow or fastparquet available (safe try)
            try:
                df.to_parquet(raw_parquet, index=False)
                logger.info(f"Saved raw parquet: {raw_parquet.resolve()}")
            except Exception:
                logger.debug(
                    "Parquet write failed or missing dependency; skipping parquet.")
            logger.info(f"Raw CSV ({len(df)} rows) saved: {raw_csv.resolve()}")
        except IOError as e:
            logger.error(f"Failed to save raw data to {raw_csv}: {e}")

    return df


if __name__ == "__main__":
    run_scraper_pipeline()
