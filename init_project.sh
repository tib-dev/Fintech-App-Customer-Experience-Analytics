#!/usr/bin/env bash
set -e

echo "Initializing Fintech Review Analytics project structure..."

# -------------------------
# Helpers
# -------------------------
ensure_dir () {
    if [ ! -d "$1" ]; then
        echo "Creating directory: $1"
        mkdir -p "$1"
    else
        echo "Directory exists: $1"
    fi
}

ensure_file () {
    if [ ! -f "$1" ]; then
        echo "Creating file: $1"
        touch "$1"
    else
        echo "File exists: $1"
    fi
}

# -------------------------
# DIRECTORIES
# -------------------------
dirs=(
    ".github/workflows"
    "configs"
    "data/raw"
    "data/interim"
    "data/processed"
    "data/postgres_exports"
    "docs"
    "notebooks/exploration"
    "notebooks/analysis"
    "scripts"
    "src/reviews_project"
    "src/reviews_project/scraper"
    "src/reviews_project/preprocessing"
    "src/reviews_project/nlp"
    "src/reviews_project/db"
    "src/reviews_project/analysis"
    "src/reviews_project/viz"
    "src/reviews_project/utils"
    "tests/unit"
    "tests/integration"
)

for d in "${dirs[@]}"; do
    ensure_dir "$d"
done

# -------------------------
# FILES
# -------------------------
files=(
    ".github/workflows/ci.yml"
    ".github/workflows/codeql.yml"

    "configs/scraper.yaml"
    "configs/nlp.yaml"
    "configs/db.yaml"

    "docs/README_project_overview.md"
    "docs/scraping_methodology.md"
    "docs/nlp_pipeline.md"
    "docs/postgres_schema.md"
    "docs/insights_report_template.md"

    "scripts/scrape_reviews.py"
    "scripts/clean_reviews.py"
    "scripts/run_sentiment.py"
    "scripts/run_theme_extraction.py"
    "scripts/load_to_postgres.py"
    "scripts/generate_report.py"

    "src/reviews_project/__init__.py"
    "src/reviews_project/config.py"

    "src/reviews_project/scraper/__init__.py"
    "src/reviews_project/scraper/google_play_scraper.py"

    "src/reviews_project/preprocessing/__init__.py"
    "src/reviews_project/preprocessing/cleaner.py"
    "src/reviews_project/preprocessing/date_normalizer.py"

    "src/reviews_project/nlp/__init__.py"
    "src/reviews_project/nlp/sentiment.py"
    "src/reviews_project/nlp/keywords.py"
    "src/reviews_project/nlp/themes.py"

    "src/reviews_project/db/__init__.py"
    "src/reviews_project/db/schema.sql"
    "src/reviews_project/db/connector.py"
    "src/reviews_project/db/loader.py"

    "src/reviews_project/analysis/__init__.py"
    "src/reviews_project/analysis/sentiment_summary.py"
    "src/reviews_project/analysis/theme_summary.py"

    "src/reviews_project/viz/__init__.py"
    "src/reviews_project/viz/plots.py"
    "src/reviews_project/viz/wordcloud_gen.py"

    "src/reviews_project/utils/__init__.py"
    "src/reviews_project/utils/helpers.py"
    "src/reviews_project/utils/io_utils.py"
    "src/reviews_project/utils/text_utils.py"

    "tests/unit/test_scraper.py"
    "tests/unit/test_cleaner.py"
    "tests/unit/test_sentiment.py"
    "tests/unit/test_keywords.py"
    "tests/unit/test_db_loader.py"

    "tests/integration/test_full_scrape_to_db.py"
    "tests/integration/test_nlp_pipeline.py"

    "requirements.txt"
    "pyproject.toml"
    "README.md"
    ".gitignore"
)

for f in "${files[@]}"; do
    ensure_file "$f"
done

echo "Project structure for Fintech Review Analytics initialized successfully."