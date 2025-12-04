# Task 4 — Insights & Recommendations

> Automatic report produced from themes & sentiment.

## Commercial Bank of Ethiopia (CBE)

### Top 3 Satisfaction Drivers
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| Customer Support | 15 | 3.5% | -0.055 | "good service and have lightly internet accessed" |
| Feature Requests | 19 | 4.4% | -0.260 | "please update the security policy. we can't take a screenshots!" |
| Transaction Performance | 9 | 2.1% | -0.319 | "not allowing to transfer and showing current statement updates." |

### Top 3 Pain Points
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| Transaction Performance | 9 | 2.1% | -0.319 | "not allowing to transfer and showing current statement updates." |
| Feature Requests | 19 | 4.4% | -0.260 | "please update the security policy. we can't take a screenshots!" |
| Customer Support | 15 | 3.5% | -0.055 | "good service and have lightly internet accessed" |

### Recommendations (linked to pain points)
- Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
- Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.
- Prioritize the top-requested features (notifications, receipts, offline mode) in a 90-day roadmap.
- Ship lightweight versions of high-demand features (e.g., receipts PDF) and measure impact.
- Add in-app support quick actions (chat, call, FAQs) and track first-response SLA.
- Train support agents on common flows and surface canned responses for frequent issues.

---

## Bank of Abyssinia (BOA)

### Top 3 Satisfaction Drivers
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| Customer Support | 10 | 2.1% | 0.581 | "don't trust this bank and its service." |
| User Interface / UX | 5 | 1.0% | -0.586 | "i would like to share feedback regarding the application. it frequently experiences disruptions and does not function properly, often freezing or failing to load. this issue is affecting workflow and efficiency. kindly review and address…" |
| Feature Requests | 22 | 4.5% | -0.637 | "after two weeks it require update why?" |

### Top 3 Pain Points
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| Transaction Performance | 38 | 7.8% | -0.752 | "has some nice interface but always freezes or slow to load .app developers please fix this issue." |
| Feature Requests | 22 | 4.5% | -0.637 | "after two weeks it require update why?" |
| User Interface / UX | 5 | 1.0% | -0.586 | "i would like to share feedback regarding the application. it frequently experiences disruptions and does not function properly, often freezing or failing to load. this issue is affecting workflow and efficiency. kindly review and address…" |

### Recommendations (linked to pain points)
- Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
- Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.
- Prioritize the top-requested features (notifications, receipts, offline mode) in a 90-day roadmap.
- Ship lightweight versions of high-demand features (e.g., receipts PDF) and measure impact.
- Run a small usability test on the transfers flow; simplify steps and add clear progress/confirmation messages.
- Implement UI fixes for confusing screens and add inline help/tooltips for key tasks.

---

## Dashen Bank

### Top 3 Satisfaction Drivers
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| User Interface / UX | 5 | 1.1% | 1.000 | "dashen superapp just keeps improving. the new theme options are beautiful, and the overall experience is faster and smoother than ever" |
| Feature Requests | 28 | 6.2% | 0.436 | "making transactions has never been easier. the new update is just amazing" |
| Customer Support | 9 | 2.0% | 0.329 | "all in one super app with smooth navigation, transaction and lifestyle services" |

### Top 3 Pain Points
| Theme | Count | Share (%) | Avg sentiment | Example quote |
|---|---:|---:|---:|---|
| Transaction Performance | 21 | 4.6% | -0.713 | "feature-rich? absolutely. but the speed? painfully slow. seriously, is it communicating with a server on mars or something?" |
| Customer Support | 9 | 2.0% | 0.329 | "all in one super app with smooth navigation, transaction and lifestyle services" |
| Feature Requests | 28 | 6.2% | 0.436 | "making transactions has never been easier. the new update is just amazing" |

### Recommendations (linked to pain points)
- Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
- Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.
- Add in-app support quick actions (chat, call, FAQs) and track first-response SLA.
- Train support agents on common flows and surface canned responses for frequent issues.
- Prioritize the top-requested features (notifications, receipts, offline mode) in a 90-day roadmap.
- Ship lightweight versions of high-demand features (e.g., receipts PDF) and measure impact.

---

## Ethics & Data Quality Notes

- Reviews are self-selected and may be negatively skewed; small-sample themes are filtered by `min_count`.
- Use these findings as input to prioritized engineering/UX work and validate with telemetry and A/B tests.
