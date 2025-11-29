# Fintech Mobile App Review Analytics — Week 2 Challenge

Analyze user reviews of mobile banking apps for three Ethiopian banks (CBE, BOA, Dashen Bank) to provide actionable insights for improving app experience and customer retention.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Objective](#business-objective)
- [Dataset Overview](#dataset-overview)
- [Folder Structure](#folder-structure)
- [Setup & Installation](#setup--installation)
- [Tasks Completed](#tasks-completed)
- [Technologies Used](#technologies-used)
- [Key Insights](#key-insights)

---

## Project Overview

This project covers end-to-end analysis of user reviews for banking apps:

- Scraping user reviews from the Google Play Store
- Preprocessing reviews for NLP analysis
- Sentiment analysis (positive, negative, neutral)
- Theme extraction (bugs, UI, features, support, etc.)
- Storing cleaned data in PostgreSQL
- Visualizing insights and generating stakeholder reports

---

## Business Objective

Omega Consultancy aims to help Ethiopian banks improve their mobile apps by:

- Identifying key satisfaction drivers (e.g., fast transfers, smooth login)
- Highlighting pain points (e.g., crashes, slow load times)
- Informing product development and customer support strategies
- Supporting user retention and feature innovation

---

## Dataset Overview

**Scraped Google Play Reviews**:

| Column       | Description                        |
| ------------ | ---------------------------------- |
| review_text  | User feedback text                  |
| rating       | Star rating (1–5)                  |
| review_date  | Date of posting (YYYY-MM-DD)       |
| bank_name    | Bank / app name                     |
| source       | Source platform (Google Play)       |

- Minimum 400 reviews per bank (1,200+ total)
- Cleaned, normalized, and stored in PostgreSQL

---

## Folder Structure

```text
Fintech-App-Review-Analytics/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── codeql.yml
├── configs/
│   ├── scraper.yaml
│   ├── nlp.yaml
│   └── db.yaml
├── data/
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── postgres_exports/
├── docs/
│   ├── README_project_overview.md
│   ├── scraping_methodology.md
│   ├── nlp_pipeline.md
│   ├── postgres_schema.md
│   └── insights_report_template.md
├── notebooks/
│   ├── exploration/
│   └── analysis/
├── scripts/
│   ├── __init__.py
│   ├── scrape_reviews.py
│   ├── clean_reviews.py
│   ├── run_sentiment.py
│   ├── run_theme_extraction.py
│   ├── load_to_postgres.py
│   └── generate_report.py
├── src/fintech_app_reviews/
│   ├── __init__.py
│   ├── config.py
│   ├── scraper/
│   │   ├── __init__.py
│   │   └── google_play_scraper.py
│   ├── preprocessing/
│   │   ├── __init__.py
│   │   ├── cleaner.py
│   │   └── date_normalizer.py
│   ├── nlp/
│   │   ├── __init__.py
│   │   ├── sentiment.py
│   │   ├── keywords.py
│   │   └── themes.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── schema.sql
│   │   ├── connector.py
│   │   └── loader.py
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── sentiment_summary.py
│   │   └── theme_summary.py
│   ├── viz/
│   │   ├── __init__.py
│   │   ├── plots.py
│   │   └── wordcloud_gen.py
│   └── utils/
│       ├── __init__.py
│       ├── helpers.py
│       ├── io_utils.py
│       └── text_utils.py
├── tests/
│   ├── unit/
│   │   ├── test_scraper.py
│   │   ├── test_cleaner.py
│   │   ├── test_sentiment.py
│   │   ├── test_keywords.py
│   │   └── test_db_loader.py
│   └── integration/
│       ├── test_full_scrape_to_db.py
│       └── test_nlp_pipeline.py
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
├── README.md
└── .gitignore
```

## Setup & Installation

### Clone the repository:

```bash
git clone https://github.com/<username>/fintech-app-review-analytics.git
cd fintech-app-review-analytics
```

### Create a Python virtual environment and activate it:


```bash
python -m venv venv
# Activate (Windows)
venv\Scripts\activate
# Activate (Linux/Mac)
source venv/bin/activate
```

### Upgrade pip and install dependencies:

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### Launch Jupyter notebooks:

```bash
jupyter notebook
```

## Tasks Completed
