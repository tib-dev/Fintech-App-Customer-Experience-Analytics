#!/usr/bin/env python3
"""
Task 4 — Insights (CSV-only mode)

Produces: rating distributions, sentiment summaries, theme shares, time trends, and example quotes.
Outputs:
 - plots/*.png
 - data/interim/theme_summary_by_bank.csv
 - data/interim/monthly_sentiment_by_bank.csv
 - data/interim/theme_sentiment_summary.csv
 - data/interim/theme_example_quotes.csv

Usage:
  python scripts/task4_insights.py --input data/processed/nlp_t.csv \
      --plots-dir plots --out-dir data/interim \
      --theme-col theme_primary
"""
from __future__ import annotations
import argparse
import os
import logging
from typing import Optional

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("task4_insights")

# --------------------
# Helpers / analysis
# --------------------


def load_data(csv_path: str):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)
    # normalize columns
    if "review_text" in df.columns and "review" not in df.columns:
        df = df.rename(columns={"review_text": "review"})
    # numeric conversions
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    if "vader_compound" in df.columns:
        df["vader_compound"] = pd.to_numeric(
            df["vader_compound"], errors="coerce")
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    return df


def ensure_dirs(*paths):
    for p in paths:
        if not p:
            continue
        os.makedirs(p, exist_ok=True)


def check_sample_sizes(df):
    if "bank" not in df.columns:
        logger.warning(
            "'bank' column not present in dataframe. Cannot compute sample sizes.")
        return pd.DataFrame()
    counts = df.groupby("bank").size().rename("count").reset_index()
    logger.info("Sample sizes per bank:\n%s", counts.to_string(index=False))
    return counts


def rating_distribution_plot(df, plots_dir: str):
    if "rating" not in df.columns or "bank" not in df.columns:
        logger.info(
            "Skipping rating distribution: missing 'rating' or 'bank' column")
        return
    df2 = df.dropna(subset=["rating"]).copy()
    df2["rating"] = df2["rating"].astype(int)
    for bank, g in df2.groupby("bank"):
        cnts = g["rating"].value_counts().sort_index()
        plt.figure(figsize=(6, 4))
        cnts.plot(kind="bar")
        plt.title(f"Rating distribution — {bank}")
        plt.xlabel("Stars")
        plt.ylabel("Count")
        plt.tight_layout()
        fpath = os.path.join(plots_dir, f"{bank}_rating_dist.png")
        plt.savefig(fpath)
        plt.close()
        logger.info("Wrote %s", fpath)


def sentiment_by_rating_plot(df, plots_dir: str):
    if "vader_compound" not in df.columns or "rating" not in df.columns or "bank" not in df.columns:
        logger.info(
            "Skipping sentiment_by_rating plot: missing required columns")
        return
    df2 = df.dropna(subset=["rating", "vader_compound"]).copy()
    df2["rating"] = df2["rating"].astype(int)
    plt.figure(figsize=(8, 5))
    sns.pointplot(data=df2, x="rating", y="vader_compound",
                  hue="bank", dodge=0.4)
    plt.title("Average VADER compound by rating")
    plt.xlabel("Rating")
    plt.ylabel("VADER compound")
    plt.tight_layout()
    out = os.path.join(plots_dir, "sentiment_by_rating.png")
    plt.savefig(out)
    plt.close()
    logger.info("Wrote %s", out)


def theme_share_plot(df, plots_dir: str, out_dir: str, theme_col: str = "theme_primary"):
    if theme_col not in df.columns or "bank" not in df.columns:
        logger.info("Skipping theme share: missing theme column or bank")
        return pd.DataFrame()
    rows = []
    for bank, g in df.groupby("bank"):
        # explode pipe-separated theme values
        cnts = g[theme_col].fillna("").astype(
            str).str.split("|").explode().value_counts()
        cnts = cnts[cnts.index != ""]
        if cnts.empty:
            logger.debug("No themes for bank %s", bank)
            continue
        pct = 100.0 * cnts / cnts.sum()
        tmp = pd.DataFrame({"bank": bank, "theme": cnts.index,
                           "count": cnts.values, "pct": pct.values})
        rows.append(tmp)

        # plot top 8
        top = tmp.head(8).sort_values("pct")
        plt.figure(figsize=(6, 4))
        plt.barh(top["theme"], top["pct"])
        plt.title(f"Top themes — {bank}")
        plt.xlabel("Percent of themed reviews")
        plt.tight_layout()
        plot_path = os.path.join(plots_dir, f"{bank}_top_themes.png")
        plt.savefig(plot_path)
        plt.close()
        logger.info("Wrote %s", plot_path)

    if rows:
        all_df = pd.concat(rows, ignore_index=True)
        out_path = os.path.join(out_dir, "theme_summary_by_bank.csv")
        all_df.to_csv(out_path, index=False)
        logger.info("Wrote theme summary to %s", out_path)
        return all_df
    return pd.DataFrame()


def monthly_trend_plot(df, plots_dir: str, out_dir: str):
    if "review_date" not in df.columns or "bank" not in df.columns:
        logger.info("Skipping monthly_trend_plot: missing review_date or bank")
        return pd.DataFrame()
    d = df.dropna(subset=["review_date"]).copy()
    d["month"] = pd.to_datetime(
        d["review_date"]).dt.to_period("M").dt.to_timestamp()
    agg = d.groupby(["bank", "month"]).agg(avg_sentiment=(
        "vader_compound", "mean"), count=("review", "count")).reset_index()
    for bank, g in agg.groupby("bank"):
        plt.figure(figsize=(8, 4))
        plt.plot(g["month"], g["avg_sentiment"], marker="o")
        plt.title(f"Monthly average Sentiment — {bank}")
        plt.xlabel("Month")
        plt.ylabel("Avg VADER compound")
        plt.tight_layout()
        p = os.path.join(plots_dir, f"{bank}_monthly_sentiment.png")
        plt.savefig(p)
        plt.close()
        logger.info("Wrote %s", p)
    out_path = os.path.join(out_dir, "monthly_sentiment_by_bank.csv")
    agg.to_csv(out_path, index=False)
    logger.info("Wrote monthly sentiment CSV to %s", out_path)
    return agg


def top_example_quotes(df, theme_name: str, bank: Optional[str] = None, n: int = 5):
    # checks
    if "theme_primary" not in df.columns and "themes" not in df.columns:
        logger.info("No theme columns present for extracting example quotes")
        return pd.DataFrame()
    cond = pd.Series([False]*len(df), index=df.index)
    if "theme_primary" in df.columns:
        cond = cond | df["theme_primary"].fillna(
            "").str.contains(theme_name, case=False, na=False)
    if "themes" in df.columns:
        cond = cond | df["themes"].fillna("").str.contains(
            theme_name, case=False, na=False)
    if bank:
        cond = cond & (df["bank"] == bank)
    sub = df.loc[cond].copy()
    if sub.empty:
        return sub
    # prefer negative sentiment first (if available)
    if "vader_compound" in sub.columns:
        sub["vscore"] = sub["vader_compound"].fillna(0)
        sub = sub.sort_values("vscore")  # ascending: most negative first
    else:
        sub = sub.sample(frac=1).reset_index(drop=True)
    cols = [c for c in ["bank", "review", "rating",
                        "vader_compound", "review_date"] if c in sub.columns]
    return sub[cols].head(n)

# --------------------
# Main CLI
# --------------------


def main():
    parser = argparse.ArgumentParser(
        description="Task 4 Insights (CSV-driven)")
    parser.add_argument("--input", required=True,
                        help="Path to processed reviews CSV (has review, bank, rating, vader_compound, review_date, etc.)")
    parser.add_argument("--plots-dir", default="plots",
                        help="Directory to write PNG plots")
    parser.add_argument("--out-dir", default="data/interim",
                        help="Directory to write interim CSVs")
    parser.add_argument("--theme-col", default="theme_primary",
                        help="Theme column name (default 'theme_primary')")
    parser.add_argument("--top-n-quotes", type=int, default=5,
                        help="Number of example quotes per theme/bank")
    args = parser.parse_args()

    ensure_dirs(args.plots_dir, args.out_dir)

    logger.info("Loading CSV: %s", args.input)
    df = load_data(args.input)
    logger.info("Total reviews loaded: %d", len(df))

    # quick checks and plots
    check_sample_sizes(df)
    rating_distribution_plot(df, args.plots_dir)
    sentiment_by_rating_plot(df, args.plots_dir)

    theme_summary_df = theme_share_plot(
        df, args.plots_dir, args.out_dir, theme_col=args.theme_col)
    monthly_agg = monthly_trend_plot(df, args.plots_dir, args.out_dir)

    # Build theme sentiment summary if theme_summary produced
    if not theme_summary_df.empty:
        theme_sent = []
        # use theme_col for grouping; if theme_col doesn't exist, skip
        if args.theme_col in df.columns:
            grouped = df.groupby(["bank", args.theme_col])
            for (bank, theme), g in grouped:
                if not theme or theme == "":
                    continue
                try:
                    avg = g["vader_compound"].astype(float).mean()
                except Exception:
                    avg = None
                cnt = len(g)
                theme_sent.append(
                    {"bank": bank, "theme": theme, "avg_sentiment": avg, "cnt": cnt})
            theme_sent_df = pd.DataFrame(theme_sent)
            # merge pct from theme_summary_df
            merged = theme_summary_df.merge(
                theme_sent_df, on=["bank", "theme"], how="left")
            merged = merged.sort_values(
                ["bank", "pct"], ascending=[True, False])
            out_path = os.path.join(
                args.out_dir, "theme_sentiment_summary.csv")
            merged.to_csv(out_path, index=False)
            logger.info("Wrote theme_sentiment_summary to %s", out_path)
        else:
            logger.info(
                "theme_col '%s' not present in df; skipping theme_sentiment_summary", args.theme_col)

    # Example quotes for a set of major themes (configurable list)
    themes_of_interest = [
        "Performance", "Stability/Reliability", "Account Access", "Support", "UI/UX"]
    example_out = []
    for theme in themes_of_interest:
        # per-bank and global
        # global examples
        q_global = top_example_quotes(
            df, theme_name=theme, bank=None, n=args.top_n_quotes)
        if not q_global.empty:
            q_global = q_global.assign(
                matched_theme=theme, sample_scope="global")
            example_out.append(q_global)
        # per-bank examples
        if "bank" in df.columns:
            for bank in df["bank"].dropna().unique():
                q_bank = top_example_quotes(
                    df, theme_name=theme, bank=bank, n=args.top_n_quotes)
                if not q_bank.empty:
                    q_bank = q_bank.assign(
                        matched_theme=theme, sample_scope=bank)
                    example_out.append(q_bank)

    if example_out:
        exdf = pd.concat(example_out, ignore_index=True)
        ex_out_file = os.path.join(args.out_dir, "theme_example_quotes.csv")
        exdf.to_csv(ex_out_file, index=False)
        logger.info("Wrote example quotes to %s", ex_out_file)
    else:
        logger.info("No example quotes found for themes of interest")

    logger.info("All done. Plots in %s, interim CSVs in %s",
                args.plots_dir, args.out_dir)


if __name__ == "__main__":
    main()
