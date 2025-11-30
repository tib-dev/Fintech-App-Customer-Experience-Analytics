import yaml
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def load_config(path: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file safely.

    Returns:
        dict: Parsed configuration data or an empty dict on failure.

    Behavior:
    - Checks if file exists
    - Handles YAML syntax errors
    - Handles unreadable files
    - Logs meaningful error messages
    """
    file_path = Path(path)

    try:
        if not file_path.exists():
            logger.error(f"Config file not found: {file_path.resolve()}")
            return {}

        if not file_path.is_file():
            logger.error(f"Config path is not a file: {file_path.resolve()}")
            return {}

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

            if data is None:
                logger.warning(f"Empty config file: {file_path.resolve()}")
                return {}

            if not isinstance(data, dict):
                logger.error(
                    f"Config file must contain a dictionary at root: {file_path.resolve()}"
                )
                return {}

            return data

    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error in {file_path.resolve()}: {e}")
        return {}

    except PermissionError:
        logger.error(f"Permission denied while reading: {file_path.resolve()}")
        return {}

    except Exception as e:
        logger.error(
            f"Unexpected error loading config '{file_path.resolve()}': {e}")
        return {}
