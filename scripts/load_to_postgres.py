# scripts/load_to_postgres.py
"""
Simple CLI wrapper for loading cleaned CSV to Postgres using src.fintech_app_reviews.db.loader
Usage:
  python scripts/load_to_postgres.py --csv data/processed/reviews_with_sentiment.csv
"""
import argparse
import logging
from src.fintech_app_reviews.db.connector import make_engine
from src.fintech_app_reviews.db.loader import ensure_tables_exist, load_reviews_from_df, count_reviews_by_bank
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True,
                   help="Path to cleaned CSV file (must have bank column)")
    p.add_argument("--batch-size", type=int, default=500)
    args = p.parse_args()

    logger.info("Loading CSV: %s", args.csv)
    df = pd.read_csv(args.csv)
    engine = make_engine()
    ensure_tables_exist(engine)
    load_reviews_from_df(engine, df, batch_size=args.batch_size)
    counts = count_reviews_by_bank(engine)
    logger.info("Final counts per bank: %s", counts)
    print(counts)


if __name__ == "__main__":
    main()
