import pandas as pd
from transformers import pipeline, Pipeline
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def init_sentiment_model() -> Pipeline:
    """
    Initialize a DistilBERT sentiment analysis pipeline.
    
    Returns:
        transformers.Pipeline: Sentiment analysis pipeline.
    """
    return pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english"
    )


def get_sentiment_score_batch(texts: list[str], model: Pipeline) -> list[dict]:
    """
    Compute sentiment scores for a batch of texts using BERT.
    
    Args:
        texts (list[str]): List of text strings.
        model (Pipeline): Transformers sentiment pipeline.
    
    Returns:
        list[dict]: List of dicts with 'label' and 'score'.
    """
    safe_texts = [t[:512] if isinstance(t, str) else "" for t in texts]
    try:
        results = model(safe_texts)
    except Exception as e:
        logger.warning("Sentiment model failed for batch: %s", e)
        results = [{"label": "neutral", "score": 0.0} for _ in safe_texts]

    out = []
    for r in results:
        label = r.get("label", "neutral").lower()
        score = float(r.get("score", 0.0))
        score = score if label == "positive" else -score if label == "negative" else 0.0
        out.append({"label": label, "score": score})
    return out


def annotate_dataframe_parallel(
    df: pd.DataFrame,
    text_col: str = "txt_clean",
    max_workers: int = 4,
    batch_size: int = 32
) -> pd.DataFrame:
    """
    Annotate a DataFrame with sentiment_label and sentiment_score in parallel.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
        text_col (str): Column containing text to analyze.
        max_workers (int): Number of parallel threads.
        batch_size (int): Number of texts per batch.
    
    Returns:
        pd.DataFrame: Annotated DataFrame with sentiment_label and sentiment_score.
    """
    model = init_sentiment_model()
    texts = df[text_col].fillna("").tolist()
    n_batches = ceil(len(texts) / batch_size)
    results: list[dict] = [None] * len(texts)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i in range(n_batches):
            batch_texts = texts[i*batch_size:(i+1)*batch_size]
            future = executor.submit(
                get_sentiment_score_batch, batch_texts, model)
            futures[future] = i

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                batch_result = future.result()
            except Exception as e:
                logger.warning("Batch %d failed: %s", batch_idx, e)
                batch_result = [{"label": "neutral", "score": 0.0}
                                for _ in range(batch_size)]

            start = batch_idx * batch_size
            results[start:start + len(batch_result)] = batch_result

    # Fill dataframe
    df["sentiment_label"] = [r["label"] for r in results]
    df["sentiment_score"] = [r["score"] for r in results]

    coverage = df["sentiment_label"].notna().mean()
    logger.info("Sentiment coverage: %.2f%%", coverage * 100)
    if coverage < 0.9:
        logger.warning("Sentiment coverage below 90%%")

    return df
