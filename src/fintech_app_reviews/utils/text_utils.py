import re
import logging

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    try:
        if not isinstance(text, str):
            return ""

        text = re.sub(r"\s+", " ", text)
        text = text.replace("\n", " ").strip()
        return text

    except Exception as e:
        logger.error(f"Text clean error: {e}", exc_info=True)
        return ""
