# tests/unit/test_db_loader.py
import os
import pandas as pd
import sqlite3
import tempfile
from sqlalchemy import create_engine
from src.fintech_app_reviews.db.loader import ensure_tables_exist, load_reviews_from_df, count_reviews_by_bank
from src.fintech_app_reviews.db.connector import CONFIG_PATH

# Use an in-memory sqlite DB for tests
def test_loader_inserts_and_counts(tmp_path):
    engine = create_engine("sqlite:///:memory:", echo=False)
    # Use the SQL schema but adapt: sqlite doesn't support 'serial' or 'timestamp with time zone' or ON CONFLICT
    # We'll create simplified tables for test
    schema = """
    CREATE TABLE banks (
        bank_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name VARCHAR(128) UNIQUE NOT NULL,
        app_id VARCHAR(256)
    );
    CREATE TABLE reviews (
        review_id VARCHAR(128) PRIMARY KEY,
        bank_id INTEGER,
        review_text TEXT,
        rating SMALLINT,
        review_date DATE,
        source VARCHAR(64)
    );
    """
    with engine.begin() as conn:
        conn.execute(schema)

    # Prepare sample df
    df = pd.DataFrame([
        {"review_id": "r1", "bank": "CBE", "app_id": "com.cbe", "review": "Great app", "rating": 5, "review_date": "2025-11-01", "source": "google_play"},
        {"review_id": "r2", "bank": "CBE", "app_id": "com.cbe", "review": "Slow transfer", "rating": 2, "review_date": "2025-11-02", "source": "google_play"},
        {"review_id": "r3", "bank": "BOA", "app_id": "com.boa", "review": "Login error", "rating": 1, "review_date": "2025-11-05", "source": "google_play"},
    ])

    # Call loader (we'll monkeypatch upsert_banks to work with sqlite's lack of ON CONFLICT)
    # Instead of importing private upsert_banks, just perform inserts to banks then call load_reviews_from_df
    with engine.begin() as conn:
        conn.execute("INSERT INTO banks (bank_name, app_id) VALUES ('CBE','com.cbe')")
        conn.execute("INSERT INTO banks (bank_name, app_id) VALUES ('BOA','com.boa')")

    # Now call load_reviews_from_df
    load_reviews_from_df(engine, df, batch_size=2)
    counts = count_reviews_by_bank(engine)
    assert counts.get("CBE", 0) == 2
    assert counts.get("BOA", 0) == 1
