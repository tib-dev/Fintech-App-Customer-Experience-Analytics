# src/fintech_app_reviews/db/loader.py
from __future__ import annotations
import hashlib
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterable
from fintech_app_reviews.db.connector import make_engine
import logging
import pathlib

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# --------------------------------------------------------------
# Helpers
# --------------------------------------------------------------
def _chunked_iterable(iterable: Iterable, size: int):
    """Yield successive chunks from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def generate_review_id_int(row) -> int:
    """
    Generate a stable 64-bit integer review_id using bank, review text, and date.
    """
    raw = f"{row['bank']}|{row['review']}|{row.get('date','')}"
    h = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return int(h[:16], 16)  # 64-bit integer


def ensure_review_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure review_id column exists as integer."""
    if "review_id" not in df.columns or df["review_id"].isnull().all():
        df["review_id"] = df.apply(generate_review_id_int, axis=1)
        logger.info("Generated integer review_id for all rows")
    return df


# --------------------------------------------------------------
# Ensure tables exist
# --------------------------------------------------------------
def ensure_tables_exist(engine: Engine):
    schema_path = pathlib.Path(__file__).parent / "sql" / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"{schema_path} not found")
    sql_text = schema_path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql_text))
    logger.info("Ensured tables exist via sql/schema.sql")


# --------------------------------------------------------------
# Upsert banks
# --------------------------------------------------------------
def upsert_banks(engine: Engine, bank_rows: pd.DataFrame):
    """Insert or update banks, return bank_name -> bank_id mapping."""
    bank_rows = bank_rows.drop_duplicates(subset=["bank"])
    with engine.begin() as conn:
        for _, row in bank_rows.iterrows():
            conn.execute(
                text("""
                    INSERT INTO banks (bank_name, app_id)
                    VALUES (:bank_name, :app_id)
                    ON CONFLICT (bank_name)
                    DO UPDATE SET app_id = EXCLUDED.app_id
                """),
                {
                    "bank_name": row["bank"],
                    "app_id": row.get("app_id")
                }
            )
        res = conn.execute(text("SELECT bank_id, bank_name FROM banks"))
        mapping = {r["bank_name"]: r["bank_id"] for r in res.mappings()}
    logger.info("Upserted banks: %s", list(mapping.keys()))
    return mapping


# --------------------------------------------------------------
# Row preparation
# --------------------------------------------------------------
def _prepare_review_row(r: pd.Series, bank_map: dict):
    return {
        "review_id": int(r["review_id"]),
        "bank_id": int(bank_map[r["bank"]]),
        "review_text": r.get("review"),
        "rating": int(r["rating"]) if pd.notna(r.get("rating")) else None,
        "review_date": r.get("date"),
        "source": r.get("source", "google_play"),
        "sentiment_label": r.get("sentiment_label"),
        "sentiment_score": float(r.get("sentiment_score")) if pd.notna(r.get("sentiment_score")) else None,
    }


# --------------------------------------------------------------
# Insert reviews (batch upsert)
# --------------------------------------------------------------
def load_reviews_from_df(engine: Engine, df: pd.DataFrame, batch_size: int = 500):
    if df.empty:
        logger.info("No rows to load.")
        return

    if "bank" not in df.columns or "review" not in df.columns:
        raise ValueError("DataFrame must include 'bank' and 'review' columns.")

    df = ensure_review_ids(df)

    banks_df = df[["bank"]].copy()
    banks_df["app_id"] = df.get("app_id")
    bank_map = upsert_banks(engine, banks_df)

    rows = []
    for _, r in df.iterrows():
        if r["bank"] not in bank_map:
            logger.warning("Unknown bank: %s", r["bank"])
            continue
        rows.append(_prepare_review_row(r, bank_map))

    insert_sql = text("""
        INSERT INTO reviews (
            review_id, bank_id, review_text, rating, review_date,
            source, sentiment_label, sentiment_score
        ) VALUES (
            :review_id, :bank_id, :review_text, :rating, :review_date,
            :source, :sentiment_label, :sentiment_score
        )
        ON CONFLICT (review_id) DO UPDATE SET
            review_text = EXCLUDED.review_text,
            rating = EXCLUDED.rating,
            review_date = EXCLUDED.review_date,
            source = EXCLUDED.source,
            sentiment_label = EXCLUDED.sentiment_label,
            sentiment_score = EXCLUDED.sentiment_score
    """)

    total = 0
    try:
        with engine.begin() as conn:
            for chunk in _chunked_iterable(rows, batch_size):
                conn.execute(insert_sql, chunk)
                total += len(chunk)
                logger.info("Inserted batch %d (total %d)", len(chunk), total)
    except SQLAlchemyError as e:
        logger.exception("DB insert failed")
        raise

    logger.info("Finished loading %d reviews.", total)


# --------------------------------------------------------------
# Count reviews per bank
# --------------------------------------------------------------
def count_reviews_by_bank(engine: Engine):
    with engine.connect() as conn:
        res = conn.execute(text("""
            SELECT b.bank_name, COUNT(r.*) AS cnt
            FROM banks b
            LEFT JOIN reviews r ON r.bank_id = b.bank_id
            GROUP BY b.bank_name
        """))
        return {r["bank_name"]: r["cnt"] for r in res.mappings()}


# --------------------------------------------------------------
# CLI
# --------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to CSV file with reviews")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    eng = make_engine()
    ensure_tables_exist(eng)
    df = pd.read_csv(args.csv)
    load_reviews_from_df(eng, df, batch_size=args.batch_size)
    print("Counts:", count_reviews_by_bank(eng))
