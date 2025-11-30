# ⚙️ Task 1: Scraping and Preprocessing Methodology - COMPLETION REPORT

This document details the technical steps and decisions made during the data collection and preprocessing phase (Task 1) and confirms the successful achievement of all Key Performance Indicators (KPIs).

## 1. Data Source and Tooling

| Component | Detail | Rationale |
| :--- | :--- | :--- |
| **Source** | Google Play Store | Target platform for mobile banking application reviews. |
| **Library** | `google-play-scraper` | Provides robust, high-volume review fetching. |
| **Target Banks** | **Commercial Bank of Ethiopia (CBE)**, **Bank of Abyssinia (BOA)**, **Dashen Bank** | Defined by the project scope. |
| **App IDs Used** | `com.combanketh.mobilebanking`, `com.boa.boaMobileBanking`, `com.dashen.dashensuperapp` | Specific package IDs used for data collection. |

## 2. Scraping Strategy

The scraping pipeline was executed via `scripts/scrape_reviews.py`, orchestrating calls to the modular scraper (`src/fintech_app_reviews/scraper/google_play_scraper.py`).

### 2.1 Configuration
The pipeline successfully utilized the following parameters defined in `configs/scraper.yaml`:

| Parameter | Value | Purpose |
| :--- | :--- | :--- |
| `max_reviews` | **600** | Successfully achieved **600 reviews per bank**, exceeding the 400+ KPI. |
| `batch_size` | 200 | Optimized for the `google-play-scraper` library using continuation tokens. |
| `sort_by` | NEWEST | Prioritized recent customer feedback. |
| `language_code` | 'en' | Focused on English reviews. |
| `country_code` | 'et' | Targeted the Ethiopian app store context. |

### 2.2 Data Extraction and Mapping
The raw data returned by the scraper was mapped to the following standardized column names during extraction:

| Raw Key (Scraper Output) | Mapped Key (Final CSV) |
| :--- | :--- |
| `content` | **review** |
| `score` | **rating** |
| `at` | **date** |
| *Added Metadata* | **bank**, **source**, **app\_id** |

## 3. Data Preprocessing Pipeline

The preprocessing stage was implemented in the modular scripts (`cleaner.py`, `date_normalizer.py`) and orchestrated by `scripts/cleaner_review.py`.

| Step | Module / Function | Description |
| :--- | :--- | :--- |
| **1. Deduplication** | `cleaner.clean_reviews` | Removed duplicate entries using the `review_id` field. |
| **2. Text Cleaning** | `text_utils.clean_text` | Converted text to **lowercase**, removed noise (HTML, links), and standardized whitespace. |
| **3. Content Filtering** | `cleaner.clean_reviews` | Dropped reviews with text length less than 3 characters (e.g., single-word comments). |
| **4. Rating Validation** | `cleaner.clean_reviews` | Coerced the `rating` column to numeric and dropped rows where the rating was invalid or missing. |
| **5. Date Normalization** | `date_normalizer.normalize_date` | Converted all date formats to the standardized **YYYY-MM-DD** string format. |

## 4. Final Quality Check and KPI Confirmation

The final orchestration script performed a crucial data quality check, leading to the following successful results:

| Metric | Target | **Actual Result** | Status |
| :--- | :--- | :--- | :--- |
| **Total Reviews Collected** | Min 1,200 (400 per bank) | **1,800** (600 per bank) | ✅ **ACHIEVED** |
| **Missing Data Percentage** | Must be <5% | **0%** | ✅ **ACHIEVED** |
| **Output File** | Clean CSV with 5 required columns | Saved to `data/interim/cleaned_reviews.csv` | ✅ **ACHIEVED** |

The clean, final DataFrame is saved to `data/interim/cleaned_reviews.csv` and is ready for the next phase: **Task 2: Data Analysis (NLP)**.