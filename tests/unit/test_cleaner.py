from src.fintech_app_reviews.utils.text_utils import clean_text
from src.fintech_app_reviews.preprocessing.cleaner import clean_reviews
import unittest
import pandas as pd
import numpy as np

# Import the functions to be tested
# clean_text is imported to ensure dependency resolution, but not directly tested here


class TestCleaner(unittest.TestCase):
    """
    Unit tests for the clean_reviews function in the preprocessing module.
    """

    def setUp(self):
        """Set up a sample raw DataFrame before each test."""
        self.raw_data = {
            'review_id': [
                'r1', 'r2', 'r3', 'r4', 'r5',
                'r1', 'r6', 'r7', 'r8', 'r9',
                'r10'
            ],
            'review': [
                'Great app, fast service!',
                'Buggy and crashed frequently.',
                '  Just awful. The UI is terrible. ',
                '1-star review',
                'I hate it',
                'Great app, fast service!',  # Duplicate of r1
                'Good',  # Length > 2, should be kept
                'A',  # Too short, should be dropped
                'Transaction failed. Rating is 4.0 stars.',
                np.nan,  # Missing content, should be dropped
                'Invalid'  # Valid text, but testing for rating issue below
            ],
            # Invalid/Missing rating
            'rating': [5, 1, 1, 1, 2, 5, 4, 4, 'Invalid', 3, np.nan],
            'date': ['2025-01-01'] * 11,
            'bank': ['CBE'] * 11,
            'user_name': ['A', 'B', 'C', 'D', 'E', 'A', 'F', 'G', 'H', 'I', 'J'],
        }
        self.df_raw = pd.DataFrame(self.raw_data)
        self.initial_count = len(self.df_raw)

    def test_empty_dataframe_returns_empty(self):
        """Test that cleaning an empty DataFrame results in an empty DataFrame."""
        df_empty = pd.DataFrame()
        result = clean_reviews(df_empty)
        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)

    def test_duplicate_removal(self):
        """Test that duplicates based on 'review_id' are removed."""
        result = clean_reviews(self.df_raw.copy())

        # r1 is duplicated (row index 5), so the second instance should be dropped.
        # r1 is now r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 (10 unique IDs)
        # Expected dropped due to clean_reviews logic:
        # - 1 duplicate (r1 at index 5)
        # - 1 short review (r7: 'A')
        # - 1 missing content (r9: np.nan)
        # - 1 invalid rating (r8: 'Invalid') -> dropped since it becomes NaN
        # - 1 missing rating (r10: np.nan)
        # Final expected count: 11 - 1 - 1 - 1 - 1 - 1 = 6

        # Check the IDs remaining after cleaning
        remaining_ids = set(result['review_id'])
        self.assertEqual(len(remaining_ids), 6)
        self.assertNotIn('r7', remaining_ids,
                         "Short review 'A' should be removed.")
        self.assertNotIn('r9', remaining_ids,
                         "Review with NaN content should be removed.")
        self.assertNotIn('r8', remaining_ids,
                         "Review with 'Invalid' rating should be removed.")
        self.assertNotIn('r10', remaining_ids,
                         "Review with NaN rating should be removed.")

    def test_text_cleaning(self):
        """Test that text is correctly cleaned (lowercased, stripped, etc.)."""
        df_copy = self.df_raw.copy()
        result = clean_reviews(df_copy)

        # Check review 'r3': '  Just awful. The UI is terrible. '
        # Expected: 'just awful. the ui is terrible.'
        cleaned_r3_review = result[result['review_id']
                                   == 'r3']['review'].iloc[0]
        self.assertEqual(cleaned_r3_review, 'just awful. the ui is terrible.')

        # Check review 'r1': 'Great app, fast service!'
        # Expected: 'great app, fast service!'
        cleaned_r1_review = result[result['review_id']
                                   == 'r1']['review'].iloc[0]
        self.assertEqual(cleaned_r1_review, 'great app, fast service!')

    def test_short_and_empty_text_removal(self):
        """Test that reviews with length <= 2 or NaN content are removed."""
        result = clean_reviews(self.df_raw.copy())

        # Review 'r7' ('A') and 'r9' (NaN) should be dropped by the cleaner.
        # Total rows should be 6 after all cleaning steps (11 initial - 5 drops)
        self.assertEqual(len(result), 6)
        self.assertNotIn('r7', result['review_id'].values)
        self.assertNotIn('r9', result['review_id'].values)

    def test_rating_coercion_and_drop(self):
        """Test that invalid ratings are coerced to numeric and NaN rows are dropped."""
        result = clean_reviews(self.df_raw.copy())

        # Check if the 'rating' column is now a numeric type
        self.assertTrue(pd.api.types.is_numeric_dtype(result['rating']))

        # 'r8' ('Invalid') and 'r10' (NaN) should have been dropped
        self.assertNotIn('r8', result['review_id'].values,
                         "Row with non-numeric rating was not dropped.")
        self.assertNotIn(
            'r10', result['review_id'].values, "Row with NaN rating was not dropped.")

        # Check the remaining valid ratings
        valid_ratings = result[result['review_id'] == 'r6']['rating'].iloc[0]
        self.assertEqual(valid_ratings, 4)

    def test_index_reset(self):
        """Test that the final DataFrame index is reset after cleaning."""
        result = clean_reviews(self.df_raw.copy())
        # The index should start at 0 and be sequential
        self.assertTrue((result.index == range(len(result))).all())


if __name__ == '__main__':
    # Running from the terminal: python tests/test_cleaner.py
    unittest.main()
