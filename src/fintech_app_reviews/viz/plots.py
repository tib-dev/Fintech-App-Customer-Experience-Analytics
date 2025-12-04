
"""
Reusable plotting functions for fintech app reviews.

Includes:
- Ratings distribution per bank
- Sentiment score distribution per bank
- Theme distribution per bank (with counts & percentages)
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# -----------------------------
# Ratings distribution per bank
# -----------------------------


def plot_ratings_per_bank(df: pd.DataFrame, rating_col="rating", bank_col="bank", palette="viridis"):
    """
    Plot ratings distribution per bank using a stacked/separated countplot.
    """
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x=rating_col, hue=bank_col, palette=palette)
    plt.title("Ratings Distribution per Bank")
    plt.xlabel("Rating")
    plt.ylabel("Count")
    plt.legend(title="Bank")
    plt.tight_layout()
    plt.show()


# -----------------------------
# Sentiment score distribution per bank
# -----------------------------
def plot_sentiment_per_bank(df: pd.DataFrame, score_col="sentiment_score", bank_col="bank", palette="coolwarm"):
    """
    Plot sentiment score distribution per bank using a boxplot.
    """
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x=bank_col, y=score_col, palette=palette)
    plt.title("Sentiment Score Distribution per Bank")
    plt.xlabel("Bank")
    plt.ylabel("Sentiment Score")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()


# -----------------------------
# Theme distribution per bank
# -----------------------------
def plot_theme_distribution(df: pd.DataFrame, bank_col="bank", theme_col="theme_primary", colormap="tab20"):
    """
    Plot stacked bar chart of theme counts per bank with percentage labels.
    """
    theme_counts = df.groupby(
        bank_col)[theme_col].value_counts().unstack(fill_value=0)
    theme_pct = theme_counts.div(theme_counts.sum(axis=1), axis=0) * 100

    ax = theme_counts.plot(
        kind="bar",
        stacked=True,
        figsize=(10, 8),
        colormap=colormap
    )

    plt.title("Theme Distribution per Bank")
    plt.xlabel("Bank")
    plt.ylabel("Count")

    # Add percentage labels on each segment
    for i, bank in enumerate(theme_counts.index):
        cumulative = 0
        for theme in theme_counts.columns:
            count = theme_counts.loc[bank, theme]
            pct = theme_pct.loc[bank, theme]
            if count > 0:
                ax.text(
                    i,
                    cumulative + count / 2,
                    f"{pct:.1f}%",
                    ha='center', va='center',
                    fontsize=8,
                    color='black'
                )
            cumulative += count

    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()


def plot_monthly_sentiment(
    df: pd.DataFrame,
    date_col: str = "date",
    bank_col: str = "bank",
    sentiment_col: str = "sentiment_score",
):
    """
    Create a monthly sentiment trend line plot per bank.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain bank, date, and sentiment columns.
    date_col : str
        Name of the date column.
    bank_col : str
        Name of the bank column.
    sentiment_col : str
        Name of the sentiment score column.
    output_path : str
        Where to save the plot.
    """

    # Ensure date is datetime
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Drop invalid dates
    df = df.dropna(subset=[date_col])

    # Create month column
    df["month"] = df[date_col].dt.to_period("M").dt.to_timestamp()

    # Aggregate: monthly average sentiment per bank
    monthly = (
        df.groupby([bank_col, "month"])[sentiment_col]
        .mean()
        .reset_index()
        .sort_values("month")
    )

    # Plot
    plt.figure(figsize=(12, 6))

    banks = monthly[bank_col].unique()
    for bank in banks:
        subset = monthly[monthly[bank_col] == bank]
        plt.plot(subset["month"], subset[sentiment_col],
                 marker="o", label=bank)

    plt.title("Monthly Sentiment Trend per Bank")
    plt.xlabel("Month")
    plt.ylabel("Average Sentiment Score")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend(title="Bank")
    plt.tight_layout()
