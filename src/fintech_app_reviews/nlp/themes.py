#!/usr/bin/env python3
"""
TF-IDF + rule-based theme assignment

More robust, secure, and optimized version.
"""

import argparse
import os
import re
import sys
from typing import List, Dict, Pattern

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


# ============================================================
# Stopwords (safe fallback if NLTK unavailable)
# ============================================================
try:
    from nltk.corpus import stopwords
    try:
        STOP = set(stopwords.words("english"))
    except LookupError:
        import nltk
        nltk.download("stopwords", quiet=True)
        STOP = set(stopwords.words("english"))
except Exception:
    STOP = {
        "the", "and", "a", "an", "is", "it", "this", "that", "to",
        "for", "in", "on", "of", "with", "at", "as"
    }


# ============================================================
# Text preprocessing
# ============================================================
def preprocess_text(s: str) -> str:
    if not isinstance(s, str):
        return ""

    s = s.lower()
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [w for w in s.split() if w not in STOP and len(w) > 2]
    return " ".join(tokens)


# ============================================================
# Compile theme patterns safely
# ============================================================
def compile_keyword_patterns(theme_map: Dict[str, List[str]]) -> Dict[str, List[Pattern]]:
    compiled = {}
    for theme, kws in theme_map.items():
        patterns = []
        for kw in kws:
            kw = kw.strip()
            if not kw:
                continue
            esc = re.escape(kw)
            patterns.append(re.compile(rf"\b{esc}\b", flags=re.I))
        compiled[theme] = patterns
    return compiled


# ============================================================
# Rule-based matching (multi-label)
# ============================================================
def rule_assign_themes(text: str, compiled_map: Dict[str, List[Pattern]]) -> List[str]:
    if not text:
        return []

    matches = []
    for theme, patterns in compiled_map.items():
        for pat in patterns:
            if pat.search(text):
                matches.append(theme)
                break
    return matches


# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="TF-IDF + rule-based theme extraction")
    parser.add_argument("--input", required=True,
                        help="CSV path containing review,bank")
    parser.add_argument("--top_terms_out", required=True,
                        help="Where to write TF-IDF top terms")
    parser.add_argument("--theme_assign_out", required=True,
                        help="Where to write theme assignments")
    parser.add_argument("--min_df", type=int, default=3)
    parser.add_argument("--max_features", type=int, default=5000)
    parser.add_argument("--ngram_min", type=int, default=1)
    parser.add_argument("--ngram_max", type=int, default=2)
    args = parser.parse_args()

    # -----------------------------
    # Validate paths
    # -----------------------------
    if not os.path.isfile(args.input):
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(2)

    out_dirs = [
        os.path.dirname(args.top_terms_out),
        os.path.dirname(args.theme_assign_out),
    ]
    for d in out_dirs:
        if d:
            os.makedirs(d, exist_ok=True)

    # -----------------------------
    # Load input
    # -----------------------------
    df = pd.read_csv(args.input)

    required = {"review", "bank"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Clean text
    df["txt_clean"] = df["review"].apply(preprocess_text)

    # -----------------------------
    # Theme rules
    # -----------------------------
    theme_map = {
        "Account Access": [
            "login", "password", "pin", "otp", "locked", "signin",
            "sign in", "sign-in", "face id", "biometric", "fingerprint",
        ],
        "Performance": [
            "slow", "lag", "loading", "load", "timeout", "time out",
            "transfer slow", "speed", "delay"
        ],
        "Stability/Reliability": [
            "crash", "crashes", "freeze", "bug", "bugs", "error",
            "failed", "stopped working"
        ],
        "UI/UX": [
            "ui", "interface", "design", "navigation", "layout",
            "ux", "confusing", "user friendly", "easy to use"
        ],
        "Support": [
            "support", "customer service", "agent", "response",
            "help", "contact", "call center"
        ],
        "Feature Request": [
            "fingerprint", "biometric", "transfer", "scan",
            "receipt", "balance", "notification", "offline"
        ],
    }
    compiled_map = compile_keyword_patterns(theme_map)

    # -----------------------------
    # TF-IDF per bank
    # -----------------------------
    top_terms = []
    banks = df["bank"].dropna().unique()

    for bank in banks:
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
        except ValueError:
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

    top_terms_df = pd.DataFrame(top_terms)
    top_terms_df.to_csv(args.top_terms_out, index=False)

    # -----------------------------
    # Rule-based theme assignment
    # -----------------------------
    theme_rows = []

    for row in df.itertuples():
        text = getattr(row, "txt_clean", "")
        bank = getattr(row, "bank", None)
        rid = getattr(row, "review_id", None)

        matched = rule_assign_themes(text, compiled_map)

        primary = matched[0] if matched else None
        secondary = matched[1] if len(matched) > 1 else None

        theme_rows.append({
            "review_id": rid,
            "bank": bank,
            "theme_primary": primary,
            "theme_secondary": secondary,
            "all_themes": ";".join(matched) if matched else ""
        })

    theme_assign_df = pd.DataFrame(theme_rows)
    theme_assign_df.to_csv(args.theme_assign_out, index=False)

    print(f"✓ TF-IDF saved to {args.top_terms_out} ({len(top_terms_df)} rows)")
    print(
        f"✓ Theme assignments saved to {args.theme_assign_out} ({len(theme_assign_df)} rows)")


if __name__ == "__main__":
    main()
