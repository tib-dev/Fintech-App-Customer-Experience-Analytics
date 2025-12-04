#  Task 2: NLP Pipeline and Analysis Methodology

This document details the methods used for Sentiment Analysis and Thematic Analysis on the cleaned app review data, confirming the achievement of all Task 2 KPIs.

## 1. Pipeline Overview

The analysis pipeline is orchestrated by the `scripts/run_analysis.py`, script, which sequentially executes modular functions for sentiment scoring and keyword extraction, creating a final, enriched dataset saved to `data/processed/analyzed_reviews.csv`.

| Step | Module/Function | Description |
| :--- | :--- | :--- |
| **Input** | N/A | Loads `data/interim/cleaned_reviews.csv` (1,800 rows). |
| **Preprocessing** | `utils/text_utils.simple_preprocess` | Prepares text for TF-IDF: tokenization, stop-word removal, and non-alphabetic character removal. |
| **Sentiment** | `nlp/sentiment.analyze_sentiment` | Computes sentiment (Positive/Negative/Neutral) using the specified model/mock logic. |
| **Keywords** | `nlp/keywords.extract_keywords_tfidf` | Extracts top N recurring keywords and n-grams per bank using TF-IDF. |
| **Thematic Clustering** | `scripts/run_analysis.py` (Orchestration Logic) | Manually groups keywords into 3–5 actionable themes. |
| **Output** | N/A | Saves results including derived features to `data/processed/analyzed_reviews.csv`. |

---

## 2. Sentiment Analysis Methodology

### 2.1 Model and Tooling

* **Primary Model:** **`distilbert-base-uncased-finetuned-sst-2-english`** (Hugging Face Transformer).
* **Implementation:** Handled within `src/fintech_app_reviews/nlp/sentiment.py`. The model is utilized via the Hugging Face `pipeline` for efficient batch processing of reviews, classifying each review into one of three labels: `POSITIVE`, `NEGATIVE`, or `NEUTRAL`.

### 2.2 Aggregation and KPI Fulfillment

* **Aggregation:** Sentiment is aggregated by calculating the mean sentiment score for each bank and for each rating category (1-star through 5-star). This allows for deep dives, such as understanding if 3-star reviews for CBE are generally trending towards positive or negative feedback.
* **KPI Status:** Sentiment labels and scores were successfully computed and added to **100% of reviews**, meeting the **90%+ coverage KPI**.

| Feature Added | Tool/Logic | Description |
| :--- | :--- | :--- |
| **sentiment\_label** | Hugging Face Pipeline | `POSITIVE`, `NEGATIVE`, or `NEUTRAL`. |
| **sentiment\_score** | Hugging Face Pipeline | Confidence score (0.0 to 1.0) for the assigned label. |

---

## 3. Thematic Analysis Methodology

Thematic analysis was achieved through Keyword Extraction followed by a structured, rule-based clustering (manual grouping logic documented below).

### 3.1 Keyword Extraction (TF-IDF)

* **Tool:** **`sklearn.feature_extraction.text.TfidfVectorizer`** (implemented in `src/fintech_app_reviews/nlp/keywords.py`).
* **Process:** TF-IDF (Term Frequency-Inverse Document Frequency) was applied to calculate the importance of words and n-grams within the context of each bank's entire review set, prioritizing words that are highly specific to that bank's reviews.
* **Configuration:**
    * `ngram_range`: $(1, 3)$ (extracts single words, bigrams, and trigrams).
    * `top_n_keywords_per_bank`: 10 (provides a focused set of concepts for grouping).

### 3.2 Thematic Grouping Logic (KPI Fulfillment)

The extracted keywords were grouped into 4 overarching themes per bank by identifying conceptual overlap in the n-grams (e.g., grouping "login error" and "forgot password" into "Account Access Issues"). This successfully meets the **3+ themes per bank KPI**.

| Overarching Theme | Keyword Logic / Examples | Relevance |
| :--- | :--- | :--- |
| **Transaction Performance** | Keywords containing: `slow`, `fast`, `transfer`, `loading`, `delay`. | Measures efficiency and speed of core functions (payments, transfers). |
| **Account Access Issues** | Keywords containing: `login`, `error`, `buggy`, `crash`. | Highlights reliability issues preventing users from accessing or using the app. |
| **User Interface & Experience** | Keywords containing: `ui`, `simple`, `intuitive`, `confusing`, `poor`. | Assesses the usability and design quality of the mobile application. |
| **Reliability & Stability** | Keywords containing: `buggy`, `crash`, `reliable`, `issues`, `fix`. | A broader measure of the application's stability and robustness outside of specific transactions. |

---

## 4. Final Output Structure

The final dataset, reflecting the completion of Task 2, is saved to `data/processed/analyzed_reviews.csv` and contains the following columns:

| Column Name | Source | Description |
| :--- | :--- | :--- |
| `review_id` | Task 1 | Unique identifier. |
| `review` | Task 1 | Cleaned review text. |
| `rating` | Task 1 | User-assigned rating (1-5). |
| `date` | Task 1 | Review date (YYYY-MM-DD). |
| `bank` | Task 1 | Bank name. |
| `source` | Task 1 | Data source (`google_play`). |
| **sentiment\_label** | **Task 2 NLP** | Computed sentiment (`POSITIVE`, `NEGATIVE`, `NEUTRAL`). |
| **sentiment\_score** | **Task 2 NLP** | Confidence of the assigned sentiment label. |