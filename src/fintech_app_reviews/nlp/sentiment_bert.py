import pandas as pd
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil


def init_sentiment_model():
    """
    Initialize a DistilBERT sentiment analysis pipeline.
    Returns a transformers pipeline object.
    """
    return pipeline("sentiment-analysis",
                    model="distilbert-base-uncased-finetuned-sst-2-english")


def get_sentiment_score_batch(texts: list[str], model) -> list[dict]:
    """
    Compute sentiment scores for a batch of texts using BERT.
    
    Args:
        texts (list[str]): List of text strings.
        model: Transformers sentiment pipeline.
    
    Returns:
        list[dict]: List of sentiment dicts {'label','score'}.
    """
    results = model([t[:512] if isinstance(t, str) else "" for t in texts])
    out = []
    for r in results:
        label = r["label"].lower()
        score = float(r["score"])
        score = score if label == "positive" else -score
        out.append({"label": label, "score": score})
    return out


def annotate_dataframe_parallel(df: pd.DataFrame,
                                text_col: str = "txt_clean",
                                max_workers: int = 4,
                                batch_size: int = 32) -> pd.DataFrame:
    """
    Annotate DataFrame with sentiment_label and sentiment_score columns in parallel.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
        text_col (str): Column with text to analyze.
        max_workers (int): Number of threads to use.
        batch_size (int): Number of texts per batch for BERT.
    
    Returns:
        pd.DataFrame: Annotated DataFrame.
    """
    model = init_sentiment_model()
    texts = df[text_col].fillna("").tolist()
    n_batches = ceil(len(texts) / batch_size)
    results = [None] * len(texts)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i in range(n_batches):
            batch_texts = texts[i*batch_size:(i+1)*batch_size]
            futures[executor.submit(
                get_sentiment_score_batch, batch_texts, model)] = i

        for future in as_completed(futures):
            batch_idx = futures[future]
            batch_result = future.result()
            start = batch_idx * batch_size
            results[start:start + len(batch_result)] = batch_result

    df["sentiment_label"] = [r["label"] for r in results]
    df["sentiment_score"] = [r["score"] for r in results]
    return df
