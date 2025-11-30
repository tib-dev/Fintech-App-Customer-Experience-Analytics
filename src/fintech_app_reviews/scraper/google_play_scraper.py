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


def scrape_app_reviews(
    app_id: str,
    app_id_to_bank: Dict[str, str],
    max_reviews: int,
    sort_by: str,
    timeout: int
) -> List[Dict[str, Any]]:
    """
    Scrapes reviews for a single app ID from Google Play,
    handling pagination and standardizing the output format.
    """

    bank_name = app_id_to_bank.get(app_id, "Unknown Bank")
    logger.info(
        f"Scraping reviews for {bank_name} ({app_id}). Max reviews: {max_reviews}")

    all_reviews: List[Dict[str, Any]] = []
    continuation_token: Optional[str] = None

    # Resolve sorting method
    sort_enum = getattr(Sort, sort_by.upper(), Sort.NEWEST)

    # Get country and language codes from context
    lang = CONTEXT.get('language_code', 'en')
    #  FIX: Corrected CONSET to CONTEXT
    country = CONTEXT.get('country_code', 'et')

    batch_size = CONFIG.get('scraper', {}).get('batch_size', 200)

    while len(all_reviews) < max_reviews:

        count_needed = min(batch_size, max_reviews - len(all_reviews))

        if count_needed <= 0:
            break

        try:
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=sort_enum,
                count=count_needed,
                cont=continuation_token,
                timeout=timeout
            )

            if not result:
                logger.info(f"No more reviews found for {app_id}.")
                break

            for review in result:
                standardized_review = {
                    'review_id': review.get('reviewId'),
                    'review_text': review.get('content'),
                    'rating': review.get('score'),
                    'review_date': review.get('at').isoformat() if review.get('at') else None,
                    'user_name': review.get('userName'),
                    'thumbs_up_count': review.get('thumbsUpCount'),
                    'bank': bank_name,
                    'app_id': app_id,
                    'source': CONFIG.get('scraper', {}).get('platform', 'google_play')
                }
                all_reviews.append(standardized_review)

            logger.debug(
                f"Collected {len(result)} reviews. Total collected: {len(all_reviews)}")

            if continuation_token is None:
                logger.info(
                    f"Reached the end of available reviews for {app_id}.")
                break

        except Exception as e:
            logger.error(
                f"Error scraping {app_id} (Batch): {e}", exc_info=False)
            break

    logger.info(
        f"Finished scraping {bank_name}. Total reviews collected: {len(all_reviews)}.")
    return all_reviews
