import logging
import pandas as pd

try:
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
except ImportError:
    raise ImportError(
        "NLTK VADER not found. Install via `pip install nltk` and run `nltk.download('vader_lexicon')`.")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# Sentiment model init
# -------------------------


def init_sentiment_model():
    """
    Initialize VADER sentiment analyzer.
    """
    try:
        sid = SentimentIntensityAnalyzer()
        logger.info("VADER Sentiment Analyzer initialized")
        return sid
    except Exception as e:
        logger.exception("Failed to initialize VADER: %s", e)
        raise

# -------------------------
# Compute sentiment score
# -------------------------


def get_sentiment_score(text: str, model) -> dict:
    """
    Returns sentiment label and score for a single text using VADER.
    """
    try:
        if not text or not isinstance(text, str):
            return {"label": "neutral", "score": 0.0}
        comp = model.polarity_scores(text)
        score = comp["compound"]
        if score >= 0.05:
            label = "positive"
        elif score <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        return {"label": label, "score": score}
    except Exception as e:
        logger.exception("Error computing sentiment for text: %s", e)
        return {"label": "neutral", "score": 0.0}

# -------------------------
# Annotate dataframe
# -------------------------


def annotate_dataframe(df: pd.DataFrame, text_col: str = "review_text"):
    """
    Annotate dataframe with VADER sentiment_label and sentiment_score columns.
    """
    model = init_sentiment_model()
    sentiment_labels = []
    sentiment_scores = []

    for idx, text in enumerate(df[text_col].fillna("")):
        if idx % 100 == 0:
            logger.info("Processing row %d/%d", idx, len(df))
        result = get_sentiment_score(text, model)
        sentiment_labels.append(result["label"])
        sentiment_scores.append(result["score"])

    df["sentiment_label"] = sentiment_labels
    df["sentiment_score"] = sentiment_scores
    return df

# -------------------------
# Aggregate sentiment
# -------------------------


def aggregate_sentiment(df: pd.DataFrame, group_cols=["bank", "rating"]):
    """
    Aggregate mean sentiment scores and counts by bank/rating.
    """
    try:
        agg_df = df.groupby(group_cols).agg(
            mean_sentiment_score=("sentiment_score", "mean"),
            positive_count=("sentiment_label",
                            lambda x: (x == "positive").sum()),
            negative_count=("sentiment_label",
                            lambda x: (x == "negative").sum()),
            neutral_count=("sentiment_label", lambda x: (x == "neutral").sum())
        ).reset_index()
        return agg_df
    except Exception as e:
        logger.exception("Aggregation failed: %s", e)
        return pd.DataFrame()
