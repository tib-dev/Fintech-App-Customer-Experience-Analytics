#!/usr/bin/env python3
"""
Robust BERT sentiment inference utilities.

Features:
- Batched HF pipeline inference (safe single-process).
- Optional multiprocessing worker mode (each process loads its own model).
- Outputs hf_label, hf_score (probability), hf_score_signed (-score for negative, +score for positive).
- Chunked CSV processing helper for large files.
- Helper to combine HF with fallback (VADER) into final_sentiment.
"""
from __future__ import annotations
import os
import logging
from typing import Iterable, List, Optional, Tuple, Dict
import math

import pandas as pd
from tqdm import tqdm

# transformers is required
from transformers import pipeline, Pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# -------------------------
# Model init
# -------------------------
def init_hf_pipeline(model_name: str = "distilbert-base-uncased-finetuned-sst-2-english", device: int = -1) -> Pipeline:
    """
    Initialize and return a HF pipeline for sentiment analysis.

    Args:
        model_name: model id
        device: -1 for CPU, >=0 for GPU (CUDA device id)
    """
    logger.info("Loading HF pipeline model=%s device=%s", model_name, device)
    pipe = pipeline(
        "sentiment-analysis",
        model=model_name,
        device=device,            # device handling by transformers
        truncation=True
    )
    return pipe


# -------------------------
# Batch inference (synchronous - safe)
# -------------------------
def infer_batches(texts: List[str], model: Pipeline, batch_size: int = 32) -> List[Dict]:
    """
    Run inference in sequential batches. Returns list of raw pipeline outputs.
    Each output is typically {'label': 'POSITIVE'|'NEGATIVE', 'score': 0.99}.

    This is the recommended default; it is stable and works for CPU or GPU.
    """
    out = []
    n = len(texts)
    for i in range(0, n, batch_size):
        batch = texts[i: i + batch_size]
        # ensure safe strings and truncation
        batch_safe = [str(t)[:512] if t is not None else "" for t in batch]
        try:
            preds = model(batch_safe)
        except Exception as e:
            logger.exception(
                "HF inference failed on batch starting at %d: %s", i, e)
            # fallback: neutral predictions for this batch
            preds = [{"label": "NEUTRAL", "score": 0.0} for _ in batch_safe]
        out.extend(preds)
    return out


# -------------------------
# Convert HF outputs to DataFrame-friendly columns
# -------------------------
def normalize_hf_outputs(preds: Iterable[Dict]) -> List[Dict]:
    """
    Convert HF pipeline outputs to consistent dicts with:
    - hf_label: 'positive'|'negative'|'neutral'
    - hf_score: float probability (0..1) for the predicted class
    - hf_score_signed: numeric signed score (+score for positive, -score for negative, 0 for neutral)
    """
    out = []
    for p in preds:
        label = p.get("label", "").lower()
        # normalize common variants
        if label in ("positive", "pos", "label_1", "label-1"):
            lab = "positive"
        elif label in ("negative", "neg", "label_0", "label-0"):
            lab = "negative"
        else:
            # If model returns only POSITIVE/NEGATIVE, we still map; otherwise neutral fallback
            if label.startswith("label_"):
                # ambiguous: map by score sign later
                lab = label
            else:
                lab = "neutral"
        score = float(p.get("score", 0.0))
        signed = 0.0
        if lab == "positive":
            signed = score
        elif lab == "negative":
            signed = -score
        else:
            signed = 0.0
        out.append({"hf_label": lab, "hf_score": score,
                   "hf_score_signed": signed})
    return out


# -------------------------
# High-level annotate function (single DataFrame)
# -------------------------
def annotate_with_hf(
    df: pd.DataFrame,
    text_col: str = "txt_clean",
    model_name: str = "distilbert-base-uncased-finetuned-sst-2-english",
    device: int = -1,
    batch_size: int = 32,
    progress: bool = True,
) -> pd.DataFrame:
    """
    Annotate a DataFrame in-memory with HF sentiment columns:
      - hf_label (str)
      - hf_score (float)
      - hf_score_signed (float)
    Returns a new DataFrame (copy).
    """
    if text_col not in df.columns:
        raise ValueError(f"text_col '{text_col}' not found in DataFrame")

    # Prepare model
    model = init_hf_pipeline(model_name=model_name, device=device)

    texts = df[text_col].fillna("").astype(str).tolist()
    preds = infer_batches(texts, model, batch_size=batch_size)

    normalized = normalize_hf_outputs(preds)
    # ensure length match
    if len(normalized) != len(df):
        # pad or trim safely
        if len(normalized) < len(df):
            pad = [{"hf_label": "neutral", "hf_score": 0.0,
                    "hf_score_signed": 0.0}] * (len(df) - len(normalized))
            normalized.extend(pad)
        else:
            normalized = normalized[: len(df)]

    out_df = df.copy()
    out_df["hf_label"] = [r["hf_label"] for r in normalized]
    out_df["hf_score"] = [r["hf_score"] for r in normalized]
    out_df["hf_score_signed"] = [r["hf_score_signed"] for r in normalized]

    logger.info("Annotated DataFrame with HF predictions (n=%d)", len(out_df))
    return out_df


# -------------------------
# Chunked CSV processing helper (for large files)
# -------------------------
def annotate_csv_in_chunks(
    input_csv: str,
    output_csv: str,
    text_col: str = "txt_clean",
    model_name: str = "distilbert-base-uncased-finetuned-sst-2-english",
    device: int = -1,
    batch_size: int = 32,
    chunksize: int = 2000,
    progress: bool = True,
) -> None:
    """
    Read input CSV in chunks, annotate each chunk with HF predictions, and write incremental output CSV.
    This avoids loading the entire dataset into memory and allows resume if needed.
    """
    if not os.path.exists(input_csv):
        raise FileNotFoundError(input_csv)

    reader = pd.read_csv(input_csv, dtype=str, chunksize=chunksize)
    first = True
    total = 0
    model = init_hf_pipeline(model_name=model_name, device=device)

    for chunk in tqdm(reader, desc="Chunks", disable=not progress):
        # Ensure text col and normalize
        if text_col not in chunk.columns:
            raise ValueError(f"text_col '{text_col}' not in chunk columns")
        chunk[text_col] = chunk[text_col].fillna("").astype(str)
        texts = chunk[text_col].tolist()

        preds = infer_batches(texts, model, batch_size=batch_size)
        normalized = normalize_hf_outputs(preds)
        # attach to chunk
        chunk["hf_label"] = [r["hf_label"] for r in normalized]
        chunk["hf_score"] = [r["hf_score"] for r in normalized]
        chunk["hf_score_signed"] = [r["hf_score_signed"] for r in normalized]

        # append to output CSV
        if first:
            chunk.to_csv(output_csv, index=False, mode="w")
            first = False
        else:
            chunk.to_csv(output_csv, index=False, mode="a", header=False)
        total += len(chunk)
        logger.info("Processed %d rows (cumulative %d)", len(chunk), total)

    logger.info("Finished annotating CSV to %s (total rows=%d)",
                output_csv, total)


# -------------------------
# Final sentiment chooser (combine hf + vader)
# -------------------------
def choose_final_sentiment(
    df: pd.DataFrame,
    hf_confidence_threshold: float = 0.85,
    vader_col: str = "sentiment_label",
    hf_label_col: str = "hf_label",
    hf_score_col: str = "hf_score",
    out_col: str = "final_sentiment",
) -> pd.DataFrame:
    """
    Choose a final sentiment label per row:
      - If hf_score >= hf_confidence_threshold -> use hf_label
      - Else fallback to vader_col (existing VADER label)
    """
    out = df.copy()
    if hf_label_col not in out.columns or hf_score_col not in out.columns:
        logger.warning(
            "HF columns not present; final_sentiment will equal %s", vader_col)
        out[out_col] = out.get(vader_col)
        return out

    def _choose(row):
        try:
            s = row.get(hf_score_col, None)
            if s is not None and pd.notna(s) and float(s) >= hf_confidence_threshold:
                return row.get(hf_label_col)
            return row.get(vader_col)
        except Exception:
            return row.get(vader_col)

    out[out_col] = out.apply(_choose, axis=1)
    return out


# -------------------------
# CLI wrapper
# -------------------------
def run_cli():
    import argparse

    parser = argparse.ArgumentParser(
        description="Annotate CSV with HF (BERT) sentiment")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--text-col", default="txt_clean",
                        help="Text column name")
    parser.add_argument(
        "--model", default="distilbert-base-uncased-finetuned-sst-2-english", help="HF model id")
    parser.add_argument("--device", type=int, default=-1,
                        help="Device: -1 CPU, >=0 GPU id")
    parser.add_argument("--batch-size", type=int, default=32,
                        help="HF inference batch size")
    parser.add_argument("--chunksize", type=int, default=2000,
                        help="CSV read chunksize; 0 means load whole CSV")
    args = parser.parse_args()

    ensure_dir = os.path.dirname(args.output) or "."
    os.makedirs(ensure_dir, exist_ok=True)

    if args.chunksize and args.chunksize > 0:
        annotate_csv_in_chunks(
            input_csv=args.input,
            output_csv=args.output,
            text_col=args.text_col,
            model_name=args.model,
            device=args.device,
            batch_size=args.batch_size,
            chunksize=args.chunksize,
            progress=True,
        )
    else:
        df = pd.read_csv(args.input, dtype=str)
        df = annotate_with_hf(df, text_col=args.text_col, model_name=args.model,
                              device=args.device, batch_size=args.batch_size)
        df.to_csv(args.output, index=False)
        logger.info("Wrote annotated csv to %s", args.output)


if __name__ == "__main__":
    run_cli()
