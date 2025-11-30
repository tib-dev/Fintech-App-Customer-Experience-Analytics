# src/fintech_app_reviews/utils/text_utils.py (Ensuring lowercasing is first)

import re
from typing import Any
import pandas as pd

def clean_text(text: Any) -> str:
    """Performs basic text cleaning: lowercasing, removing noise, and stripping whitespace."""

    # 1. Handle non-string inputs (e.g., NaN from Pandas, which should be treated as empty)
    if not isinstance(text, str):
        # Explicitly convert to string if it's not None/NaN, otherwise return empty string
        # However, for cleaning, we often just want a string representation.
        # But if it's NaN/None, we MUST return "" to prevent failure in downstream code.
        if pd.isna(text):  # Assuming pandas is available for robustness against NaN
            return ""
        text = str(text)

    # 2. Convert to lowercase (This is the line that failed in your test)
    text = text.lower()

    # 3. Remove HTML tags, links, and mentions
    text = re.sub(r'<[^<]+?>|https?://\S+|www\.\S+|@\w+|#\w+', '', text)

    # 4. Remove excessive whitespace and strip leading/trailing spaces
    text = re.sub(r'\s+', ' ', text).strip()

    return text
