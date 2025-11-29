import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def safe_read_csv(path: str) -> pd.DataFrame:
    try:
        file = Path(path)
        if not file.exists():
            logger.error(f"CSV file missing: {path}")
            return pd.DataFrame()

        return pd.read_csv(file)

    except Exception as e:
        logger.error(f"Error reading CSV {path}: {e}", exc_info=True)
        return pd.DataFrame()


def safe_write_csv(df: pd.DataFrame, path: str):
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
    except Exception as e:
        logger.error(f"Failed to write CSV {path}: {e}", exc_info=True)
