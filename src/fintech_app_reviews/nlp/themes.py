import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define your rule-based theme mapping
THEME_KEYWORDS = {
    "Account Access Issues": ["login", "password", "account locked", "authentication", "mfa"],
    "Transaction Performance": ["slow transfer", "timeout", "loading", "delay", "transfer failed"],
    "User Interface & Experience": ["ui", "ux", "design", "navigation", "buttons", "layout"],
    "Customer Support": ["support", "helpdesk", "response", "chat", "call center"],
    "Feature Requests": ["feature", "request", "add", "option", "fingerprint", "biometric"]
}


def map_keywords_to_themes(keywords: List[str]) -> List[str]:
    """
    Map a list of keywords to one or more predefined themes.
    """
    try:
        themes = set()
        for kw in keywords:
            kw_lower = kw.lower()
            for theme, theme_keywords in THEME_KEYWORDS.items():
                if any(tk in kw_lower for tk in theme_keywords):
                    themes.add(theme)
        return list(themes)
    except Exception as e:
        logger.exception("Error mapping keywords to themes: %s", e)
        return []


