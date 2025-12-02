# src/fintech_app_reviews/db/loader.py
from __future__ import annotations
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterable
from .connector import make_engine
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _chunked_iterable(iterable: Iterable, size: int):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def ensure_tables_exist(engine: Engine):
    """Run the schema file to create tables if necessary."""
    import pkg_resources
    import pathlib

    schema_path = pathlib.Path("sql/schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError("sql/schema.sql not found; please add it.")
    sql_text = schema_path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql_text))
    logger.info("Ensured tables exist via sql/schema.sql")


def upsert_banks(engine: Engine, bank_rows: pd.DataFrame):
    """
    bank_rows: DataFrame with columns ['bank','app_id']
    Ensures each bank exists and returns dict bank_name -> bank_id
    """
    with engine.begin() as conn:
        for _, row in bank_rows.drop_duplicates(subset=["bank"]).iterrows():
            bank_name = row["bank"]
            app_id = row.get("app_id")
            # Upsert: insert or do nothing then update app_id if changed
            conn.execute(
                text(
                    """
                    INSERT INTO banks (bank_name, app_id)
                    VALUES (:bank_name, :app_id)
                    ON CONFLICT (bank_name)
                    DO UPDATE SET app_id = EXCLUDED.app_id
                    """
                ),
                {"bank_name": bank_name, "app_id": app_id},
            )
        # fetch mapping
        res = conn.execute(text("SELECT bank_id, bank_name FROM banks"))
        mapping = {r["bank_name"]: r["bank_id"] for r in res.mappings()}
    logger.info("Upserted banks: %s", list(mapping.keys()))
    return mapping


def _prepare_review_row(r: pd.Series, bank_map: dict):
    return {
        "review_id": str(r["review_id"]),
        "bank_id": int(bank_map[r["bank"]]),
        "review_text": r.get("review"),
        "rating": int(r["rating"]) if pd.notna(r.get("rating")) else None,
        "review_date": r.get("review_date"),
        "user_name": r.get("user_name"),
        "app_version": r.get("app_version"),
        "source": r.get("source", "google_play"),
        "vader_compound": float(r.get("vader_compound")) if pd.notna(r.get("vader_compound")) else None,
        "vader_label": r.get("vader_label"),
        "hf_label": r.get("hf_label"),
        "hf_score": float(r.get("hf_score")) if pd.notna(r.get("hf_score")) else None,
        "theme_primary": r.get("theme_primary"),
        "theme_secondary": r.get("theme_secondary"),
    }


def load_reviews_from_df(engine: Engine, df: pd.DataFrame, batch_size: int = 500):
    """
    Bulk-insert reviews from a DataFrame. Performs upsert on conflict (review_id).
    Will do batch inserts per `batch_size`.
    """
    if df.empty:
        logger.info("No rows to load.")
        return

    # basic validation
    if "bank" not in df.columns or "review_id" not in df.columns:
        raise ValueError(
            "DataFrame must include at least 'bank' and 'review_id' columns")

    banks_df = df[["bank", "app_id"]].drop_duplicates(subset=["bank"])
    with engine.begin() as conn:
        bank_map = upsert_banks(engine, banks_df)

    # Prepare rows
    rows = []
    for _, r in df.iterrows():
        if r["bank"] not in bank_map:
            logger.warning("Skipping review with unknown bank: %s", r["bank"])
            continue
        rows.append(_prepare_review_row(r, bank_map))

    insert_sql = text(
        """
        INSERT INTO reviews (
            review_id, bank_id, review_text, rating, review_date,
            user_name, app_version, source,
            vader_compound, vader_label, hf_label, hf_score,
            theme_primary, theme_secondary
        ) VALUES (
            :review_id, :bank_id, :review_text, :rating, :review_date,
            :user_name, :app_version, :source,
            :vader_compound, :vader_label, :hf_label, :hf_score,
            :theme_primary, :theme_secondary
        )
        ON CONFLICT (review_id) DO UPDATE SET
            review_text = EXCLUDED.review_text,
            rating = EXCLUDED.rating,
            review_date = EXCLUDED.review_date,
            user_name = EXCLUDED.user_name,
            app_version = EXCLUDED.app_version,
            source = EXCLUDED.source,
            vader_compound = EXCLUDED.vader_compound,
            vader_label = EXCLUDED.vader_label,
            hf_label = EXCLUDED.hf_label,
            hf_score = EXCLUDED.hf_score,
            theme_primary = EXCLUDED.theme_primary,
            theme_secondary = EXCLUDED.theme_secondary
        """
    )

    # Batch execute
    total = 0
    try:
        with engine.begin() as conn:
            for chunk in _chunked_iterable(rows, batch_size):
                conn.execute(insert_sql, chunk)
                total += len(chunk)
                logger.info(
                    "Inserted/updated batch of %d reviews (total=%d)", len(chunk), total)
    except SQLAlchemyError as e:
        logger.exception("DB insert failed: %s", e)
        raise
    logger.info("Finished loading %d reviews.", total)


def count_reviews_by_bank(engine: Engine):
    with engine.connect() as conn:
        res = conn.execute(text("""
            SELECT b.bank_name, COUNT(r.*) AS cnt
            FROM banks b
            LEFT JOIN reviews r ON r.bank_id = b.bank_id
            GROUP BY b.bank_name
        """))
        return {r["bank_name"]: int(r["cnt"]) for r in res.mappings()}


if __name__ == "__main__":
    # convenience CLI when module run directly
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True,
                        help="Path to cleaned CSV (with bank column)")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    eng = make_engine()
    ensure_tables_exist(eng)
    df = pd.read_csv(args.csv)
    load_reviews_from_df(eng, df, batch_size=args.batch_size)
    print("Counts:", count_reviews_by_bank(eng))
