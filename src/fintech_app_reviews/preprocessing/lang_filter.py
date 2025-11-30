import re
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0  # deterministic

# Allow only basic Latin + common punctuation
ENGLISH_CHARS = re.compile(r"[A-Za-z0-9 .,!?'\-]+")


def is_strict_english(text: str) -> bool:
    """
    Strict English filter:
    - Reject if any character falls outside allowed English range.
    - Require at least 80% English-like characters.
    - Use langdetect only if the text passes script checks.
    """
    if not isinstance(text, str) or not text.strip():
        return False

    # Quick rejection: if fullmatch fails, text contains foreign characters
    if not ENGLISH_CHARS.fullmatch(text.strip()):
        return False

    # Character ratio check
    letters = sum(ch.isalpha() for ch in text)
    english_letters = sum(ch.isascii() and ch.isalpha() for ch in text)

    if letters > 0:
        ratio = english_letters / letters
        if ratio < 0.8:
            return False

    # Short inputs – langdetect unstable
    if len(text) < 5:
        return True

    # Final gate: actual language detection
    try:
        return detect(text) == "en"
    except:
        return False
