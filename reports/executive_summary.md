# Executive Summary — Fintech App Review Analysis

- **Total reviews analyzed:** 1374

## Reviews by bank
- **Bank of Abyssinia (BOA)**: 486 reviews
- **Commercial Bank of Ethiopia (CBE)**: 433 reviews
- **Dashen Bank**: 455 reviews

## Key findings & top actions (per bank)

### Commercial Bank of Ethiopia (CBE)
- **Top driver:** Customer Support (3.5% of reviews, avg_sent=-0.05)
- **Top pain point:** Transaction Performance (2.1% of reviews, avg_sent=-0.32)
- **Recommendations:**
  - Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
  - Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.

### Bank of Abyssinia (BOA)
- **Top driver:** Customer Support (2.1% of reviews, avg_sent=0.58)
- **Top pain point:** Transaction Performance (7.8% of reviews, avg_sent=-0.75)
- **Recommendations:**
  - Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
  - Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.

### Dashen Bank
- **Top driver:** User Interface / UX (1.1% of reviews, avg_sent=1.00)
- **Top pain point:** Transaction Performance (4.6% of reviews, avg_sent=-0.71)
- **Recommendations:**
  - Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.
  - Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts.


### Notes
- Themes with fewer than 3 reviews are excluded to avoid noisy signals.

- Validate recommendations with telemetry (transfer latency, crash rates) and A/B testing.
