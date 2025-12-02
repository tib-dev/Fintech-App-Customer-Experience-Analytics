-- Active: 1764596277064@@127.0.0.1@5432@customer_reviews
-- sql/schema.sql
-- Idempotent schema for bank_reviews
-- Run: psql -f sql/schema.sql (after creating DB 'bank_reviews')

-- Create tables if missing
CREATE TABLE IF NOT EXISTS banks (
    bank_id SERIAL PRIMARY KEY,
    bank_name VARCHAR(128) UNIQUE NOT NULL,
    app_id VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id VARCHAR(128) PRIMARY KEY,
    bank_id INTEGER REFERENCES banks(bank_id) ON DELETE CASCADE,
    review_text TEXT,
    rating SMALLINT,
    review_date DATE,
    user_name VARCHAR(256),
    app_version VARCHAR(64),
    source VARCHAR(64),
    vader_compound REAL,
    vader_label VARCHAR(16),
    hf_label VARCHAR(16),
    hf_score REAL,
    theme_primary VARCHAR(64),
    theme_secondary VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- performance indexes
CREATE INDEX IF NOT EXISTS idx_reviews_bank_id ON reviews(bank_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_review_date ON reviews(review_date);
