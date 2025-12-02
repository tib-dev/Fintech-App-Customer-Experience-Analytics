# src/fintech_app_reviews/db/connector.py
from __future__ import annotations
import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

CONFIG_PATH = os.getenv("DB_CONFIG_PATH", "configs/db.yaml")


def load_db_config(path: str | None = None) -> dict:
    path = path or CONFIG_PATH
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    db = cfg.get("database", {})
    # allow env var overrides
    db_user = os.getenv("DB_USER", db.get("username"))
    db_pass = os.getenv("DB_PASSWORD", db.get("password"))
    db_host = os.getenv("DB_HOST", db.get("host"))
    db_port = os.getenv("DB_PORT", str(db.get("port", 5432)))
    db_name = os.getenv("DB_NAME", db.get("database"))
    uri_template = db.get(
        "uri_template", "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )
    return {
        "username": db_user,
        "password": db_pass,
        "host": db_host,
        "port": int(db_port),
        "database": db_name,
        "uri": uri_template.format(
            username=quote_plus(str(db_user)),
            password=quote_plus(str(db_pass)),
            host=db_host,
            port=db_port,
            database=db_name,
        ),
    }


def make_engine(echo: bool = False) -> Engine:
    cfg = load_db_config()
    uri = cfg["uri"]
    try:
        engine = create_engine(uri, pool_size=5, max_overflow=10, echo=echo)
        return engine
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to create DB engine: {e}") from e
