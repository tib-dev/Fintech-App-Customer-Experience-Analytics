import pandas as pd
from transformers import pipeline


def init_sentiment_model():
    """
    Initialize a DistilBERT sentiment analysis pipeline.

    Returns:
        transformers.Pipeline: Sentiment analysis pipeline.
    """
    return pipeline("sentiment-analysis",
                    model="distilbert-base-uncased-finetuned-sst-2-english")


def get_sentiment_score(text: str, model) -> dict:
    """
    Compute sentiment label and score using BERT.

    Args:
        text (str): Input text.
        model: Transformers sentiment pipeline.

    Returns:
        dict: {'label': 'positive'|'negative'|'neutral', 'score': float}
    """
    if not text:
        return {"label": "neutral", "score": 0.0}
    result = model(text[:512])[0]
    label = result["label"].lower()
    score = float(result["score"])
    return {"label": label, "score": score if label == "positive" else -score}


def annotate_dataframe(df: pd.DataFrame, text_col: str = "txt_clean") -> pd.DataFrame:
    """
    Annotate DataFrame with sentiment_label and sentiment_score columns.

    Args:
        df (pd.DataFrame): Input DataFrame.
        text_col (str): Column containing text to analyze.

    Returns:
        pd.DataFrame: Annotated DataFrame.
    """
    model = init_sentiment_model()
    results = df[text_col].fillna("").apply(
        lambda t: get_sentiment_score(t, model))
    df["sentiment_label"] = results.apply(lambda x: x["label"])
    df["sentiment_score"] = results.apply(lambda x: x["score"])
    return df


def aggregate_sentiment(df: pd.DataFrame, group_cols=["bank", "rating"]) -> pd.DataFrame:
    """
    Aggregate mean sentiment and counts by group.

    Args:
        df (pd.DataFrame): Annotated DataFrame.
        group_cols (list): Columns to group by.

    Returns:
        pd.DataFrame: Aggregated sentiment statistics.
    """
    agg_df = df.groupby(group_cols).agg(
        mean_sentiment_score=("sentiment_score", "mean"),
        positive_count=("sentiment_label", lambda x: (x == "positive").sum()),
        negative_count=("sentiment_label", lambda x: (x == "negative").sum()),
        neutral_count=("sentiment_label", lambda x: (x == "neutral").sum())
    ).reset_index()
    return agg_df
