# ⚙️ Task 1: Scraping & Preprocessing Pipeline

This document summarizes the data collection and preprocessing workflow for Task 1, ensuring reproducibility and achievement of KPIs.

---

## 1. Data Source & Tools

| Component | Detail | Rationale |
| --- | --- | --- |
| **Platform** | Google Play Store | Target source for mobile banking app reviews |
| **Library** | `google-play-scraper` | Efficient, high-volume review extraction |
| **Target Banks** | CBE, BOA, Dashen Bank | Defined by project scope |
| **App IDs** | `com.combanketh.mobilebanking`, `com.boa.boaMobileBanking`, `com.dashen.dashensuperapp` | Ensures accurate review collection |

---

## 2. Scraping Pipeline

The scraper is orchestrated via `scripts/scrape_reviews.py`, using modular logic in `src/fintech_app_reviews/scraper/google_play_scraper.py`.

### 2.1 Configuration

Parameters from `configs/scraper.yaml`:

| Parameter | Value | Purpose |
| --- | --- | --- |
| `max_reviews` | 600 | Ensures sufficient review volume per bank |
| `batch_size` | 200 | Optimized for continuation token handling |
| `sort_by` | NEWEST | Prioritizes recent feedback |
| `language_code` | en | English-only reviews |
| `country_code` | et | Ethiopian app store context |

### 2.2 Data Mapping

Scraper outputs were standardized to final schema:

| Raw Field | Final Column |
| --- | --- |
| `content` | review |
| `score` | rating |
| `at` | date |
| N/A | bank, source, app_id |

---

## 3. Preprocessing Pipeline

Managed via `scripts/clean_reviews.py`, using `cleaner.py` and `date_normalizer.py`.

| Step | Function | Description |
| --- | --- | --- |
| Deduplication | `clean_reviews` | Remove duplicate reviews by `review_id` |
| Text Cleaning | `text_utils.clean_text` | Lowercase, remove HTML, URLs, emojis, special chars |
| Filtering | `clean_reviews` | Remove extremely short or empty reviews |
| Rating Validation | `clean_reviews` | Coerce to numeric, drop invalid entries |
| Date Normalization | `normalize_date` | Standardize all dates to `YYYY-MM-DD` |

---

## 4. Output & KPI Checks

| Metric | Target | Result | Status |
| --- | --- | --- | --- |
| Total Reviews | ≥1,200 (400 per bank) | 1,800 (600 per bank) | ✅ Achieved |
| Missing Data | <5% | 0% | ✅ Achieved |
| Output File | Clean CSV (`review,rating,date,bank,source`) | `data/interim/cleaned_reviews.csv` | ✅ Achieved |

The output CSV is ready for **Task 2: NLP Analysis**.

---

## 5. Usage

To run the full cleaning pipeline:

```bash
python scripts/clean_reviews.py
