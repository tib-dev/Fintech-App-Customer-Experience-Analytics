# src/fintech_app_reviews/scraper/google_play_scraper.py

import logging
from typing import List, Dict, Any, Optional
from google_play_scraper import reviews, Sort
import os
import sys

# --- Configuration Loader Imports ---
from fintech_app_reviews.config import load_config
# from fintech_app_reviews.utils.text_utils import clean_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Dynamic Path Calculation to Project Root ---
current_dir = os.path.dirname(os.path.abspath(__file__))
# Navigate up four levels to reach the project root
PROJECT_ROOT = os.path.abspath(
    os.path.join(current_dir, '..', '..', '..', '..'))
CONFIG_FILE_PATH = os.path.join(PROJECT_ROOT, 'configs', 'scraper.yaml')
# --- End Path Calculation ---

# Load configuration using the calculated path
try:
    CONFIG = load_config(path=CONFIG_FILE_PATH)
except FileNotFoundError:
    logger.error(
        f"Configuration file not found at: {CONFIG_FILE_PATH}. Check file path.")
    # In a real pipeline, you'd want to handle this gracefully
    CONFIG = {}  # Use an empty dict as a fallback to avoid crashing later
except Exception as e:
    logger.error(f"Failed to load configuration: {e}")
    CONFIG = {}

CONTEXT = CONFIG.get('context', {})


def scrape_app_reviews(app_id: str, app_id_to_bank: dict, max_reviews: int, sort_by: str):
    bank_name = app_id_to_bank.get(app_id, "Unknown Bank")
    logger.info(
        f"Scraping reviews for {bank_name} ({app_id}). Max reviews: {max_reviews}")

    all_reviews = []
    sort_enum = getattr(Sort, sort_by.upper(), Sort.NEWEST)
    lang = CONTEXT.get("language_code", "en")
    country = CONTEXT.get("country_code", "et")

    continuation_token = None
    batch_size = CONFIG.get("scraper", {}).get("batch_size", 200)
    remaining = max_reviews

    while remaining > 0:
        count = min(batch_size, remaining)
        try:
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=sort_enum,
                count=count,
                continuation_token=continuation_token  # pass the token from previous batch
            )
            if not result:
                break

            for r in result:
                all_reviews.append({
                    "review_id": r.get("reviewId"),
                    "review_text": r.get("content"),
                    "rating": r.get("score"),
                    "review_date": r.get("at").isoformat() if r.get("at") else None,
                    "user_name": r.get("userName"),
                    "thumbs_up_count": r.get("thumbsUpCount"),
                    "bank": bank_name,
                    "app_id": app_id,
                    "source": CONFIG.get("scraper", {}).get("platform", "google_play")
                })

            remaining -= len(result)
            if continuation_token is None:  # reached the end
                break
        except Exception as e:
            logger.error(f"Error scraping {app_id}: {e}", exc_info=True)
            break

    logger.info(
        f"Finished scraping {bank_name}. Total reviews collected: {len(all_reviews)}")
    return all_reviews
