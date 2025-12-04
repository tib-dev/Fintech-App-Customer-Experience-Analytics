#!/usr/bin/env python3
"""
TF-IDF + Rule-based theme assignment module.

This script performs the following tasks:
1. Cleans review text (lowercasing, removing stopwords, optional lemmatization).
2. Computes TF-IDF scores per bank to identify top terms.
3. Assigns themes to reviews using rule-based keyword matching.
4. Saves top terms and theme assignments to CSV.

Improvements:
- Added lemmatization for better keyword matching.
- Safer regex compilation with logging.
- Structured TF-IDF extraction per bank.
- Multi-label theme assignment with primary/secondary.
- Better error handling and directory validation.
"""

import argparse
import os
import re
import sys
import logging
from typing import List, Dict, Pattern

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# Optional NLP: Lemmatization
try:
    import nltk
    from nltk.stem import WordNetLemmatizer
    nltk.download("wordnet", quiet=True)
    nltk.download("omw-1.4", quiet=True)
    LEMMATIZER = WordNetLemmatizer()
except Exception:
    LEMMATIZER = None

# Stopwords
try:
    from nltk.corpus import stopwords
    STOP = set(stopwords.words("english"))
except Exception:
    STOP = {"the", "and", "a", "an", "is", "it", "this", "that", "to",
            "for", "in", "on", "of", "with", "at", "as"}

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------
# Text preprocessing
# -----------------------------
def preprocess_text(s: str) -> str:
    """
    Clean and preprocess review text.
    
    Steps:
    - Lowercase
    - Remove URLs
    - Remove special characters
    - Remove stopwords
    - Optional lemmatization

    Args:
        s (str): Raw text string

    Returns:
        str: Cleaned text
    """
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [w for w in s.split() if w not in STOP and len(w) > 2]
    if LEMMATIZER:
        tokens = [LEMMATIZER.lemmatize(w) for w in tokens]
    return " ".join(tokens)


# -----------------------------
# Compile keyword patterns
# -----------------------------
def compile_keyword_patterns(theme_map: Dict[str, List[str]]) -> Dict[str, List[Pattern]]:
    """
    Compile regex patterns for each theme keyword.

    Args:
        theme_map (dict): Dictionary mapping theme -> list of keywords

    Returns:
        dict: Dictionary mapping theme -> list of compiled regex patterns
    """
    compiled = {}
    for theme, kws in theme_map.items():
        patterns = []
        for kw in kws:
            kw = kw.strip()
            if not kw:
                continue
            try:
                esc = re.escape(kw)
                patterns.append(re.compile(rf"\b{esc}\b", flags=re.I))
            except re.error as e:
                logger.warning("Invalid regex for keyword '%s': %s", kw, e)
        compiled[theme] = patterns
    return compiled


# -----------------------------
# Rule-based theme assignment
# -----------------------------
def rule_assign_themes(text: str, compiled_map: Dict[str, List[Pattern]]) -> List[str]:
    """
    Assign themes to a single review text based on compiled keyword patterns.

    Args:
        text (str): Preprocessed review text
        compiled_map (dict): Theme -> list of compiled regex patterns

    Returns:
        list[str]: Matched themes (multi-label)
    """
    if not text:
        return []
    matched = []
    for theme, patterns in compiled_map.items():
        for pat in patterns:
            if pat.search(text):
                matched.append(theme)
                break
    return matched


# -----------------------------
# Main execution
# -----------------------------
def main():
    """
    Main function to:
    1. Load review CSV.
    2. Preprocess text.
    3. Compute TF-IDF per bank.
    4. Assign themes to reviews.
    5. Save results to CSV.
    """
    parser = argparse.ArgumentParser(description="TF-IDF + Rule-based theme extraction")
    parser.add_argument("--input", required=True, help="CSV path containing 'review' and 'bank'")
    parser.add_argument("--top_terms_out", required=True, help="Output CSV for TF-IDF top terms")
    parser.add_argument("--theme_assign_out", required=True, help="Output CSV for theme assignments")
    parser.add_argument("--min_df", type=int, default=3)
    parser.add_argument("--max_features", type=int, default=5000)
    parser.add_argument("--ngram_min", type=int, default=1)
    parser.add_argument("--ngram_max", type=int, default=2)
    args = parser.parse_args()

    # Validate input
    if not os.path.isfile(args.input):
        logger.error("Input file not found: %s", args.input)
        sys.exit(2)

    # Ensure output directories exist
    for path in [args.top_terms_out, args.theme_assign_out]:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)

    # Load CSV
    df = pd.read_csv(args.input)
    required_cols = {"review", "bank"}
    if missing := required_cols - set(df.columns):
        raise ValueError(f"Missing required columns: {missing}")

    # Optional: add review_id if missing
    if "review_id" not in df.columns:
        df["review_id"] = range(1, len(df) + 1)

    # Clean text
    df["txt_clean"] = df["review"].apply(preprocess_text)

    # -----------------------------
    # TF-IDF per bank
    # -----------------------------
    top_terms = []
    for bank in df["bank"].dropna().unique():
        sub = df[df["bank"] == bank]
        docs = sub["txt_clean"].astype(str).tolist()
        if not docs:
            continue
        min_df_use = args.min_df if len(docs) >= args.min_df else 1
        vect = TfidfVectorizer(
            ngram_range=(args.ngram_min, args.ngram_max),
            min_df=min_df_use,
            max_features=args.max_features
        )
        try:
            X = vect.fit_transform(docs)
        except ValueError as e:
            logger.warning("TF-IDF failed for bank '%s': %s", bank, e)
            continue

        features = np.array(vect.get_feature_names_out())
        mean_tf = np.asarray(X.mean(axis=0)).ravel()
        if mean_tf.size == 0:
            continue
        order = mean_tf.argsort()[::-1]
        for rank, idx in enumerate(order[:200], start=1):
            top_terms.append({
                "bank": bank,
                "term": features[idx],
                "score": float(mean_tf[idx]),
                "rank": rank
            })

    pd.DataFrame(top_terms).to_csv(args.top_terms_out, index=False)
    logger.info("TF-IDF top terms saved to %s", args.top_terms_out)

    # -----------------------------
    # Theme assignment
    # -----------------------------
    # Example theme mapping (user should provide)
    theme_map = {
        "navigation": ["fast navigation", "easy to use", "intuitive"],
        "crash": ["crash", "freeze", "bug"],
        "security": ["secure", "password", "2fa"],
        "payment": ["transfer", "payment", "send money"]
    }
    compiled_map = compile_keyword_patterns(theme_map)

    theme_rows = []
    for row in df.itertuples():
        text = getattr(row, "txt_clean", "")
        bank = getattr(row, "bank", None)
        rid = getattr(row, "review_id", None)
        matched = rule_assign_themes(text, compiled_map)
        theme_rows.append({
            "review_id": rid,
            "bank": bank,
            "theme_primary": matched[0] if matched else None,
            "theme_secondary": matched[1] if len(matched) > 1 else None,
            "all_themes": ";".join(matched) if matched else ""
        })

    pd.DataFrame(theme_rows).to_csv(args.theme_assign_out, index=False)
    logger.info("Theme assignments saved to %s", args.theme_assign_out)


if __name__ == "__main__":
    main()
