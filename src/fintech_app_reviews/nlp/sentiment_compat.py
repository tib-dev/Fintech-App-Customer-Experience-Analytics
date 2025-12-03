# src/fintech_app_reviews/nlp/sentiment_compat.py
from __future__ import annotations
import logging
import os
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Try to import the robust HF annotator implemented earlier.
# If it's colocated in sentiment.py, import from there; otherwise try sibling imports.
try:
    # preferred: if you have the robust file at fintech_app_reviews.nlp.sentiment
    from fintech_app_reviews.nlp.sentiment_bert import (
        annotate_with_hf,
        annotate_csv_in_chunks,
    )
except Exception:
    # fallback: try same module name
    try:
        from .sentiment_bert import annotate_with_hf, annotate_csv_in_chunks  # type: ignore
    except Exception:
        annotate_with_hf = None  # type: ignore
        annotate_csv_in_chunks = None  # type: ignore
        logger.warning(
            "Could not import annotate_with_hf from fintech_app_reviews.nlp.sentiment. "
            "Make sure the robust HF sentiment module is present."
        )


def hf_to_sentiment_columns(
    df: pd.DataFrame,
    hf_label_col: str = "hf_label",
    hf_score_col: str = "hf_score",
    out_label_col: str = "sentiment_label",
    out_score_col: str = "sentiment_score",
) -> pd.DataFrame:
    """
    Convert HF columns to pipeline-consistent sentiment_label (str) and sentiment_score (signed float).
    Keeps hf_* columns untouched.

    Args:
        df: input DataFrame (copy is returned).
        hf_label_col: name of HF label column (e.g., 'hf_label').
        hf_score_col: name of HF score column (probability 0..1).
        out_label_col: name for output label column (e.g., 'sentiment_label').
        out_score_col: name for output numeric score column (e.g., 'sentiment_score').

    Returns:
        DataFrame copy with the two output columns set.
    """
    if hf_label_col not in df.columns or hf_score_col not in df.columns:
        raise ValueError(
            f"Missing expected HF columns: {hf_label_col}, {hf_score_col}")

    out = df.copy()
    out[hf_label_col] = out[hf_label_col].astype(str).str.lower().replace(
        {"pos": "positive", "neg": "negative"}
    )
    out[hf_score_col] = pd.to_numeric(
        out[hf_score_col], errors="coerce").fillna(0.0).astype(float)

    def _signed(row):
        lab = row[hf_label_col]
        score = row[hf_score_col]
        if lab == "positive":
            return float(score)
        if lab == "negative":
            return -float(score)
        return 0.0

    out[out_score_col] = out.apply(_signed, axis=1)
    out[out_label_col] = out[hf_label_col].apply(
        lambda s: s if s in ("positive", "negative", "neutral") else "neutral"
    )
    return out


def compute_final_sentiment(
    df: pd.DataFrame,
    hf_score_col: str = "hf_score",
    hf_label_col: str = "hf_label",
    vader_label_col: str = "sentiment_label",
    out_col: str = "final_sentiment",
    hf_confidence_threshold: float = 0.85,
) -> pd.DataFrame:
    """
    Prefer HF label when HF score >= hf_confidence_threshold, else fallback to 'vader_label' (existing sentiment_label).

    Returns a copy of df with `out_col` set.
    """
    out = df.copy()
    if hf_label_col not in out.columns or hf_score_col not in out.columns:
        logger.info(
            "HF columns not found. Setting %s to %s (if present) or 'neutral'.",
            out_col,
            vader_label_col,
        )
        if vader_label_col in out.columns:
            out[out_col] = out[vader_label_col].fillna("neutral").astype(str)
        else:
            out[out_col] = "neutral"
        return out

    out[hf_score_col] = pd.to_numeric(
        out[hf_score_col], errors="coerce").fillna(0.0).astype(float)
    out[out_col] = out.get(vader_label_col, "").fillna("").astype(str)

    mask = out[hf_score_col] >= hf_confidence_threshold
    out.loc[mask, out_col] = out.loc[mask,
                                     hf_label_col].fillna(out.loc[mask, out_col])
    out[out_col] = out[out_col].fillna("neutral").astype(str)
    out[out_col] = out[out_col].apply(lambda s: s if s in (
        "positive", "negative", "neutral") else "neutral")
    return out


# -----------------------------------------------------------------------------
# Backwards-compatible annotate_dataframe_parallel wrapper
# -----------------------------------------------------------------------------
def annotate_dataframe_parallel(
    df: pd.DataFrame,
    text_col: str = "txt_clean",
    max_workers: int = 4,
    batch_size: int = 32,
    model_name: str = "distilbert-base-uncased-finetuned-sst-2-english",
    device: int = -1,
) -> pd.DataFrame:
    """
    Backwards-compatible wrapper intended to replace the older annotate_dataframe_parallel.
    Behaviour:
      - By default it runs a safe single-process batched HF annotation (via annotate_with_hf).
      - If max_workers > 1 this function will still run single-process but will log a warning:
        using threads with HF pipeline is not recommended.
    Args:
      df: DataFrame to annotate (must contain text_col).
      text_col: name of text column to analyze.
      max_workers: legacy parameter (threads); not used for HF pipeline but kept for compatibility.
      batch_size: HF inference batch size.
      model_name: HF model id.
      device: -1 for CPU, >=0 for GPU id.
    Returns:
      DataFrame copy with added columns: hf_label, hf_score, hf_score_signed, sentiment_label, sentiment_score.
    """
    if annotate_with_hf is None:
        raise RuntimeError(
            "annotate_with_hf is not available. Ensure the HF sentiment module is installed.")

    if max_workers and max_workers > 1:
        logger.warning(
            "annotate_dataframe_parallel was called with max_workers=%s. "
            "Transformers pipeline is not reliably thread-safe; running in single-process batched mode instead.",
            max_workers,
        )

    logger.info("Running HF annotation (model=%s device=%s batch_size=%d)",
                model_name, device, batch_size)
    annotated = annotate_with_hf(
        df, text_col=text_col, model_name=model_name, device=device, batch_size=batch_size)

    # normalized HF -> sentiment_label/sentiment_score for compatibility
    if "hf_label" in annotated.columns and "hf_score" in annotated.columns:
        annotated = hf_to_sentiment_columns(
            annotated,
            hf_label_col="hf_label",
            hf_score_col="hf_score",
            out_label_col="sentiment_label",
            out_score_col="sentiment_score",
        )
    else:
        logger.warning(
            "HF columns not present after annotation; no legacy sentiment columns created.")

    # compute signed score column for numeric aggregations if not present
    if "sentiment_score" not in annotated.columns and "hf_score_signed" in annotated.columns:
        annotated["sentiment_score"] = annotated["hf_score_signed"]

    coverage = annotated["sentiment_label"].notna().mean(
    ) if "sentiment_label" in annotated.columns else 0.0
    logger.info("Annotation complete. Sentiment coverage: %.2f%%",
                coverage * 100)
    return annotated


# -----------------------------------------------------------------------------
# Optional CLI helper for quick testing
# -----------------------------------------------------------------------------
def _cli_main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Compatibility wrapper for HF sentiment annotation")
    parser.add_argument("--input", required=True,
                        help="Input CSV file path (must contain txt_clean)")
    parser.add_argument("--output", required=True,
                        help="Output CSV path (annotated)")
    parser.add_argument("--text-col", default="txt_clean",
                        help="Text column name")
    parser.add_argument("--batch-size", type=int, default=32,
                        help="HF inference batch size")
    parser.add_argument("--device", type=int, default=-1,
                        help="Device: -1 CPU, >=0 GPU id")
    parser.add_argument(
        "--model", default="distilbert-base-uncased-finetuned-sst-2-english", help="HF model id")
    args = parser.parse_args()

    if annotate_csv_in_chunks is not None:
        # Use chunked annotator if available (safer for large files)
        annotate_csv_in_chunks(
            input_csv=args.input,
            output_csv=args.output,
            text_col=args.text_col,
            model_name=args.model,
            device=args.device,
            batch_size=args.batch_size,
            chunksize=2000,
        )
        # load result and ensure sentiment columns
        df_out = pd.read_csv(args.output, dtype=str)
        if "hf_label" in df_out.columns and "hf_score" in df_out.columns:
            df_out = hf_to_sentiment_columns(df_out)
            df_out.to_csv(args.output, index=False)
        logger.info("Wrote annotated file: %s", args.output)
    else:
        # small file path: load into memory and use annotate_with_hf
        df = pd.read_csv(args.input, dtype=str)
        annotated = annotate_dataframe_parallel(
            df, text_col=args.text_col, batch_size=args.batch_size, device=args.device, model_name=args.model)
        annotated.to_csv(args.output, index=False)
        logger.info("Wrote annotated file: %s", args.output)


if __name__ == "__main__":
    _cli_main()
