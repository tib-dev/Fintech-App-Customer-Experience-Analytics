import yaml
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    """
    Load YAML config with safe fallback.
    """
    try:
        file = Path(path)
        if not file.exists():
            logger.error(f"Config file not found: {path}")
            return {}

        with open(file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in {path}: {e}")
        return {}

    except Exception as e:
        logger.error(f"Unexpected config load error: {e}")
        return {}
