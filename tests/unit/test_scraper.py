from scripts.scrape_reviews import run_scraper_pipeline
from src.fintech_app_reviews.scraper.google_play_scraper import scrape_app_reviews
import unittest
import pandas as pd
import os
import shutil
from unittest.mock import patch, MagicMock
from datetime import datetime
from google_play_scraper import Sort

# --- PATH FIX START ---
import sys
# Get the path to the project root
project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..'))
# Add the project root to the path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- PATH FIX END ---

# --- IMPORTANT: Adjust these imports if your project structure differs ---
# We need to assume scripts is in the root and imports src correctly


# --- Mock Data ---

# Base mock review structure
MOCK_REVIEW_DATA = {
    'reviewId': 'mock_id',
    'userName': 'TestUser',
    'score': 5,
    'content': 'Excellent service and great UI!',
    'at': datetime(2025, 1, 15, 12, 0, 0),
    'thumbsUpCount': 10
}

# 💥 FIX: Make Batch 1 return only 5 reviews (less than target 10) to force pagination
MOCK_SCRAPE_BATCH_1 = ([{**MOCK_REVIEW_DATA, 'reviewId': f'b1_r{i}'}
                       for i in range(5)], 'mock_token_1')

# Batch 2 returns the remaining 5 reviews and signals the end (None token)
MOCK_SCRAPE_BATCH_2 = (
    [{**MOCK_REVIEW_DATA, 'reviewId': f'b2_r{i}'} for i in range(5, 10)], None)

# Mock Configuration structure (Simulating config.yaml)
MOCK_CONFIG = {
    'context': {
        'language_code': 'en',
        'country_code': 'et'
    },
    'scraper': {
        'app_ids': ["com.cbe.mobile", "com.boa.mobile"],
        'max_reviews': 20,  # Total target reviews (10 per bank)
        'sort_by': 'newest',
        'platform': 'google_play',
        'batch_size': 200,
    },
    'bank_mapping': {
        'com.cbe.mobile': 'Commercial Bank of Ethiopia (CBE)',
        'com.boa.mobile': 'Bank of Abyssinia (BOA)',
    },
    'network': {
        'timeout': 5,
    },
    'output': {
        'save_raw': True,
        'raw_path': 'tests/temp/raw',
        'log_scrape_stats': True,
    }
}


class TestScraper(unittest.TestCase):
    """Test suite for the core scraping functions and orchestration pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.test_output_dir = MOCK_CONFIG['output']['raw_path']
        os.makedirs(cls.test_output_dir, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('tests/temp'):
            shutil.rmtree('tests/temp')

    def setUp(self):
        self.output_file = os.path.join(
            self.test_output_dir, 'raw_reviews.csv')
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    # --- Test Core Scraper Logic (scrape_app_reviews) ---

    @patch('src.fintech_app_reviews.scraper.google_play_scraper.load_config', return_value=MOCK_CONFIG)
    @patch('src.fintech_app_reviews.scraper.google_play_scraper.reviews')
    def test_scrape_app_reviews_success(self, mock_reviews, mock_load_config):
        """Tests successful scraping, correct data structure, and pagination logic."""

        # We need to simulate TWO batches being returned for a single app
        mock_reviews.side_effect = [MOCK_SCRAPE_BATCH_1, MOCK_SCRAPE_BATCH_2]

        app_id = "com.cbe.mobile"
        max_per_app = MOCK_CONFIG['scraper']['max_reviews'] // len(
            MOCK_CONFIG['scraper']['app_ids'])  # 10

        results = scrape_app_reviews(
            app_id=app_id,
            app_id_to_bank=MOCK_CONFIG['bank_mapping'],
            max_reviews=max_per_app,  # Pass 10 here
            sort_by='newest',
            timeout=5
        )

        self.assertEqual(len(results), max_per_app,
                         "Should collect the exact number of max_reviews per app (10).")

        # This assertion should now pass because Batch 1 (5 reviews) forces a second call to reach 10.
        self.assertEqual(mock_reviews.call_count, 2,
                         "Should call the external scraper twice for two batches.")

        mock_reviews.assert_any_call(
            app_id,
            lang='en',
            country='et',
            sort=Sort.NEWEST,
            count=10,  # The API is requested for the remaining reviews up to max_per_app, or batch_size
            cont=None,
            timeout=5
        )

        first_review = results[0]
        self.assertIn('review_id', first_review)
        self.assertEqual(first_review['bank'],
                         'Commercial Bank of Ethiopia (CBE)')

    # --- Test Orchestration Logic (run_scraper_pipeline) ---

    @patch('scripts.scrape_reviews.load_config', return_value=MOCK_CONFIG)
    @patch('scripts.scrape_reviews.scrape_app_reviews')
    def test_run_scraper_pipeline_success(self, mock_scrape_app_reviews, mock_load_config):
        """
        Tests the full pipeline orchestration, ensuring correct review distribution 
        and successful saving of a consolidated file.
        """

        max_per_app = MOCK_CONFIG['scraper']['max_reviews'] // len(
            MOCK_CONFIG['scraper']['app_ids'])  # 10

        # Mock data for CBE - 10 reviews, correctly labeled
        cbe_reviews = [
            {'bank': 'Commercial Bank of Ethiopia (CBE)',
             'review_id': f'cbe_id_{i}'}
            for i in range(max_per_app)
        ]

        # Mock data for BOA - 10 reviews, correctly labeled
        boa_reviews = [
            {'bank': 'Bank of Abyssinia (BOA)', 'review_id': f'boa_id_{i}'}
            for i in range(max_per_app, max_per_app * 2)
        ]

        # Use side_effect to return the correct data for each sequential call
        mock_scrape_app_reviews.side_effect = [cbe_reviews, boa_reviews]

        run_scraper_pipeline()

        self.assertEqual(mock_scrape_app_reviews.call_count, 2,
                         "Scraper should be called once for each app_id.")

        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), MOCK_CONFIG['scraper']['max_reviews'],
                         "Saved DataFrame should contain the total expected 20 reviews.")

        # Crucial check for correct bank distribution
        self.assertTrue((df['bank'] == 'Commercial Bank of Ethiopia (CBE)').any(
        ), "CBE reviews should be present.")
        self.assertTrue((df['bank'] == 'Bank of Abyssinia (BOA)').any(
        ), "BOA reviews should be present.")
