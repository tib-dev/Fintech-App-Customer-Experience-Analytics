
import pandas as pd
from typing import Any


def normalize_date(date_input: Any) -> str | None:
    """
    Converts a date input (string or datetime) to the YYYY-MM-DD format.
    Returns None if conversion fails.
    """
    if pd.isna(date_input):
        return None

    try:
        # pd.to_datetime is robust for many formats, including the ones scraped
        date_obj = pd.to_datetime(date_input)
        # Format the datetime object to the required YYYY-MM-DD string
        return date_obj.strftime('%Y-%m-%d')
    except Exception:
        return None
