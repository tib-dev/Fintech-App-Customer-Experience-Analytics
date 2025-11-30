import logging
from typing import List
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sns.set(style="whitegrid")


def plot_sentiment_distribution(df: pd.DataFrame, sentiment_col: str = "sentiment_label", output_path: str = None):
    """
    Plot the distribution of sentiment labels.
    """
    try:
        plt.figure(figsize=(6, 4))
        sns.countplot(x=sentiment_col, data=df, order=[
                      "positive", "neutral", "negative"])
        plt.title("Sentiment Distribution")
        plt.xlabel("Sentiment")
        plt.ylabel("Count")
        plt.tight_layout()

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(output_path)
            logger.info("Sentiment distribution plot saved to %s", output_path)
        else:
            plt.show()
        plt.close()
    except Exception as e:
        logger.exception("Failed to plot sentiment distribution: %s", e)


def plot_sentiment_by_group(df: pd.DataFrame, group_col: str = "bank", sentiment_score_col: str = "sentiment_score", output_path: str = None):
    """
    Plot mean sentiment score per group (e.g., per bank).
    """
    try:
        agg_df = df.groupby(group_col)[
            sentiment_score_col].mean().reset_index()
        plt.figure(figsize=(8, 5))
        sns.barplot(x=group_col, y=sentiment_score_col, data=agg_df)
        plt.title(f"Mean Sentiment Score by {group_col.capitalize()}")
        plt.xlabel(group_col.capitalize())
        plt.ylabel("Mean Sentiment Score")
        plt.xticks(rotation=45)
        plt.tight_layout()

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(output_path)
            logger.info("Sentiment by group plot saved to %s", output_path)
        else:
            plt.show()
        plt.close()
    except Exception as e:
        logger.exception("Failed to plot sentiment by group: %s", e)


def plot_theme_counts(df: pd.DataFrame, theme_col: str = "themes", output_path: str = None):
    """
    Plot counts of mapped themes across all reviews.
    """
    try:
        # Flatten list of themes
        all_themes: List[str] = []
        for themes in df[theme_col].dropna():
            if isinstance(themes, list):
                all_themes.extend(themes)
        theme_counts = pd.Series(all_themes).value_counts()

        plt.figure(figsize=(8, 5))
        sns.barplot(x=theme_counts.index, y=theme_counts.values)
        plt.title("Theme Counts Across Reviews")
        plt.xlabel("Theme")
        plt.ylabel("Count")
        plt.xticks(rotation=45)
        plt.tight_layout()

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(output_path)
            logger.info("Theme counts plot saved to %s", output_path)
        else:
            plt.show()
        plt.close()
    except Exception as e:
        logger.exception("Failed to plot theme counts: %s", e)
