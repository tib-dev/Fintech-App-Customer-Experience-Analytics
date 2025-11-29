#!/usr/bin/env bash
set -euo pipefail

echo ""
echo "---------------------------------------------"
echo "  Initializing Fintech Review Analytics"
echo "---------------------------------------------"
echo ""

GREEN="\e[32m"
YELLOW="\e[33m"
CYAN="\e[36m"
RESET="\e[0m"

created_dirs=()
created_files=()

# ----------------------------------------
# Helpers
# ----------------------------------------
ensure_dir () {
    if [ ! -d "$1" ]; then
        mkdir -p "$1"
        touch "$1/.gitkeep"
        created_dirs+=("$1")
        echo -e "${GREEN}Created directory:${RESET} $1"
    else
        echo -e "${CYAN}Directory exists:${RESET} $1"
    fi
}

ensure_file () {
    if [ ! -f "$1" ]; then
        mkdir -p "$(dirname "$1")"
        touch "$1"
        created_files+=("$1")
        echo -e "${GREEN}Created file:${RESET} $1"
    else
        echo -e "${CYAN}File exists:${RESET} $1"
    fi
}

# ----------------------------------------
# DIRECTORIES
# ----------------------------------------
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
    "src/fintech_app_reviews"
    "src/fintech_app_reviews/scraper"
    "src/fintech_app_reviews/preprocessing"
    "src/fintech_app_reviews/nlp"
    "src/fintech_app_reviews/db"
    "src/fintech_app_reviews/analysis"
    "src/fintech_app_reviews/viz"
    "src/fintech_app_reviews/utils"
    "tests/unit"
    "tests/integration"
)

for d in "${dirs[@]}"; do
    ensure_dir "$d"
done

# ----------------------------------------
# FILES
# ----------------------------------------
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

    "scripts/__init__.py"
    "scripts/scrape_reviews.py"
    "scripts/clean_reviews.py"
    "scripts/run_sentiment.py"
    "scripts/run_theme_extraction.py"
    "scripts/load_to_postgres.py"
    "scripts/generate_report.py"

    "src/fintech_app_reviews/__init__.py"
    "src/fintech_app_reviews/config.py"

    "src/fintech_app_reviews/scraper/__init__.py"
    "src/fintech_app_reviews/scraper/google_play_scraper.py"

    "src/fintech_app_reviews/preprocessing/__init__.py"
    "src/fintech_app_reviews/preprocessing/cleaner.py"
    "src/fintech_app_reviews/preprocessing/date_normalizer.py"

    "src/fintech_app_reviews/nlp/__init__.py"
    "src/fintech_app_reviews/nlp/sentiment.py"
    "src/fintech_app_reviews/nlp/keywords.py"
    "src/fintech_app_reviews/nlp/themes.py"

    "src/fintech_app_reviews/db/__init__.py"
    "src/fintech_app_reviews/db/schema.sql"
    "src/fintech_app_reviews/db/connector.py"
    "src/fintech_app_reviews/db/loader.py"

    "src/fintech_app_reviews/analysis/__init__.py"
    "src/fintech_app_reviews/analysis/sentiment_summary.py"
    "src/fintech_app_reviews/analysis/theme_summary.py"

    "src/fintech_app_reviews/viz/__init__.py"
    "src/fintech_app_reviews/viz/plots.py"
    "src/fintech_app_reviews/viz/wordcloud_gen.py"

    "src/fintech_app_reviews/utils/__init__.py"
    "src/fintech_app_reviews/utils/helpers.py"
    "src/fintech_app_reviews/utils/io_utils.py"
    "src/fintech_app_reviews/utils/text_utils.py"

    "tests/unit/test_scraper.py"
    "tests/unit/test_cleaner.py"
    "tests/unit/test_sentiment.py"
    "tests/unit/test_keywords.py"
    "tests/unit/test_db_loader.py"

    "tests/integration/test_full_scrape_to_db.py"
    "tests/integration/test_nlp_pipeline.py"

    "requirements.txt"
    "requirements-dev.txt"
    "pyproject.toml"
    "README.md"
    ".gitignore"
)

for f in "${files[@]}"; do
    ensure_file "$f"
done

echo ""
echo -e "${GREEN}Project structure for Fintech Review Analytics initialized successfully.${RESET}"
echo ""

# Summary
echo "---------------------------------------------"
echo -e "${YELLOW}Summary${RESET}"
echo "---------------------------------------------"
echo "Directories created: ${#created_dirs[@]}"
echo "Files created: ${#created_files[@]}"

if (( ${#created_dirs[@]} > 0 )); then
    echo ""
    echo "New directories:"
    for d in "${created_dirs[@]}"; do
        echo " - $d"
    done
fi

if (( ${#created_files[@]} > 0 )); then
    echo ""
    echo "New files:"
    for f in "${created_files[@]}"; do
        echo " - $f"
    done
fi

echo ""
echo -e "${GREEN}All done.${RESET}"
