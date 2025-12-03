
"""
Task 4 report generator (Markdown)

- For each bank: lists top 3 satisfaction drivers and top 3 pain points
  with evidence: count, pct (share of bank reviews), avg_sentiment, and a sample quote.
- Generates bank-specific recommendations tied to pain points.
- Produces a one-page executive summary (Markdown-ready) and a full detailed report.
- Safe defaults: ignores themes with very small sample sizes (min_count).
"""
from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any, Optional
from textwrap import shorten

DEFAULT_MIN_COUNT = 3  # ignore themes with fewer than this many reviews


def _ensure_cols(df: pd.DataFrame):
    required = {"bank", "theme_primary", "review", "sentiment_score"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")


def _agg_theme_stats(df: pd.DataFrame) -> pd.DataFrame:
    # returns theme summary per bank with count, pct, avg_sentiment
    total_by_bank = df.groupby("bank").size().rename("bank_total")
    agg = (
        df.groupby(["bank", "theme_primary"])
        .agg(count=("review", "count"), avg_sentiment=("sentiment_score", "mean"))
        .reset_index()
    )
    agg = agg.merge(total_by_bank.reset_index(), on="bank")
    agg["pct"] = 100.0 * agg["count"] / agg["bank_total"]
    agg = agg.sort_values(["bank", "count"], ascending=[True, False])
    return agg


def _sample_quote_for_theme(df: pd.DataFrame, bank: str, theme: str) -> str:
    sub = df[(df["bank"] == bank) & (df["theme_primary"] == theme)].copy()
    if sub.empty:
        return ""
    # prefer most extreme negative for pain points (lowest sentiment), most positive for drivers
    # but caller will choose direction; here return median-ish representative
    # choose review with sentiment closest to avg
    avg = sub["sentiment_score"].astype(float).mean()
    sub["dist"] = (sub["sentiment_score"].astype(float) - avg).abs()
    chosen = sub.sort_values("dist").iloc[0]["review"]
    return shorten(str(chosen).strip(), 240, placeholder="…")


# Map theme -> recommended actions (concrete)
_THEME_TO_ACTION = {
    "Transaction Performance": [
        "Instrument and optimize transfer endpoints (p50/p95 latency), implement retry & idempotency on client and server.",
        "Investigate backend bottlenecks and CDN/TLS tuning; add performance dashboards and p95 alerts."
    ],
    "Feature Requests": [
        "Prioritize the top-requested features (notifications, receipts, offline mode) in a 90-day roadmap.",
        "Ship lightweight versions of high-demand features (e.g., receipts PDF) and measure impact."
    ],
    "User Interface / UX": [
        "Run a small usability test on the transfers flow; simplify steps and add clear progress/confirmation messages.",
        "Implement UI fixes for confusing screens and add inline help/tooltips for key tasks."
    ],
    "Customer Support": [
        "Add in-app support quick actions (chat, call, FAQs) and track first-response SLA.",
        "Train support agents on common flows and surface canned responses for frequent issues."
    ],
    "Account Access": [
        "Improve OTP reliability (retry logic, fallback providers) and increase biometrics support with clear error messages.",
        "Add account-access troubleshooting tips in-app and instrument auth failure metrics."
    ],
}


def _recommend_from_pains(pain_themes: List[str]) -> List[str]:
    recs: List[str] = []
    for t in pain_themes:
        actions = _THEME_TO_ACTION.get(t)
        if actions:
            # prefer 2 concrete actions per theme
            for a in actions[:2]:
                recs.append(a)
    # ensure at least two recommendations exist
    if not recs:
        recs = ["Investigate top negative themes and collect telemetry (logs, metrics, user flows).",
                "Prioritize quick fixes that reduce user friction and measure impact."]
    # dedupe while preserving order
    seen = set()
    out = []
    for r in recs:
        if r not in seen:
            out.append(r)
            seen.add(r)
        if len(out) >= 6:
            break
    return out


def generate_bank_section(df: pd.DataFrame, bank: str, agg: pd.DataFrame,
                          min_count: int = DEFAULT_MIN_COUNT) -> Dict[str, Any]:
    # collect top drivers (positive avg_sentiment) and top pains (negative avg_sentiment)
    bank_agg = agg[(agg["bank"] == bank) & (agg["count"] >= min_count)].copy()
    if bank_agg.empty:
        return {
            "bank": bank,
            "drivers": [],
            "pains": [],
            "recommendations": []
        }

    # drivers: sort by avg_sentiment desc, then count desc
    drivers_df = bank_agg.sort_values(["avg_sentiment", "count"], ascending=[False, False]).head(6)
    # pains: sort by avg_sentiment asc (most negative), then count desc
    pains_df = bank_agg.sort_values(["avg_sentiment", "count"], ascending=[True, False]).head(6)

    # choose top 3 drivers and top 3 pains but ensure they have reasonable counts
    drivers = []
    for _, r in drivers_df.iterrows():
        drivers.append({
            "theme": r["theme_primary"],
            "count": int(r["count"]),
            "pct": float(r["pct"]),
            "avg_sentiment": float(r["avg_sentiment"]),
            "sample": _sample_quote_for_theme(df, bank, r["theme_primary"])
        })
        if len(drivers) >= 3:
            break

    pains = []
    for _, r in pains_df.iterrows():
        pains.append({
            "theme": r["theme_primary"],
            "count": int(r["count"]),
            "pct": float(r["pct"]),
            "avg_sentiment": float(r["avg_sentiment"]),
            "sample": _sample_quote_for_theme(df, bank, r["theme_primary"])
        })
        if len(pains) >= 3:
            break

    # recommendations drawn from pain themes; ensure at least 2 recommendations
    pain_themes = [p["theme"] for p in pains]
    recommendations = _recommend_from_pains(pain_themes)

    return {
        "bank": bank,
        "drivers": drivers,
        "pains": pains,
        "recommendations": recommendations
    }


def generate_report_md(df: pd.DataFrame, min_count: int = DEFAULT_MIN_COUNT) -> str:
    """
    Generate full report in Markdown. Returns the Markdown string.
    """
    _ensure_cols(df)
    # coerce numeric
    df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce").fillna(0.0)
    agg = _agg_theme_stats(df)

    banks = list(df["bank"].dropna().unique())
    lines: List[str] = []
    lines.append("# Task 4 — Insights & Recommendations\n")
    lines.append("> Automatic report produced from themes & sentiment.\n")
    for bank in banks:
        section = generate_bank_section(df, bank, agg, min_count=min_count)
        lines.append(f"## {section['bank']}\n")
        # Drivers
        lines.append("### Top 3 Satisfaction Drivers")
        if section["drivers"]:
            lines.append("| Theme | Count | Share (%) | Avg sentiment | Example quote |")
            lines.append("|---|---:|---:|---:|---|")
            for d in section["drivers"]:
                lines.append(f"| {d['theme']} | {d['count']} | {d['pct']:.1f}% | {d['avg_sentiment']:.3f} | \"{d['sample']}\" |")
        else:
            lines.append("_No drivers meeting the minimum sample threshold._")

        # Pains
        lines.append("\n### Top 3 Pain Points")
        if section["pains"]:
            lines.append("| Theme | Count | Share (%) | Avg sentiment | Example quote |")
            lines.append("|---|---:|---:|---:|---|")
            for p in section["pains"]:
                lines.append(f"| {p['theme']} | {p['count']} | {p['pct']:.1f}% | {p['avg_sentiment']:.3f} | \"{p['sample']}\" |")
        else:
            lines.append("_No pain points meeting the minimum sample threshold._")

        # Recommendations
        lines.append("\n### Recommendations (linked to pain points)")
        if section["recommendations"]:
            for rec in section["recommendations"]:
                lines.append(f"- {rec}")
        else:
            lines.append("- Investigate and prioritize top negative themes; collect telemetry.")

        lines.append("\n---\n")

    # Footer: ethics note
    lines.append("## Ethics & Data Quality Notes\n")
    lines.append(
        "- Reviews are self-selected and may be negatively skewed; small-sample themes are filtered by `min_count`."
    )
    lines.append("- Use these findings as input to prioritized engineering/UX work and validate with telemetry and A/B tests.\n")

    return "\n".join(lines)


def generate_executive_summary_md(df: pd.DataFrame, min_count: int = DEFAULT_MIN_COUNT) -> str:
    """
    Produces a concise one-page executive summary in Markdown.
    """
    _ensure_cols(df)
    df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce").fillna(0.0)
    agg = _agg_theme_stats(df)

    # overall KPIs
    total_reviews = len(df)
    counts_by_bank = df.groupby("bank").size().reset_index(name="count")
    bank_lines = []
    for _, r in counts_by_bank.iterrows():
        bank_lines.append(f"- **{r['bank']}**: {int(r['count'])} reviews")

    # For each bank, pick top driver and top pain (highest avg_sentiment and lowest avg_sentiment)
    banks = list(df["bank"].dropna().unique())
    summary_lines = []
    for bank in banks:
        section = generate_bank_section(df, bank, agg, min_count=min_count)
        # best driver and worst pain (if present)
        top_driver = section["drivers"][0] if section["drivers"] else None
        top_pain = section["pains"][0] if section["pains"] else None
        summary_lines.append(f"### {bank}")
        if top_driver:
            summary_lines.append(f"- **Top driver:** {top_driver['theme']} ({top_driver['pct']:.1f}% of reviews, avg_sent={top_driver['avg_sentiment']:.2f})")
        else:
            summary_lines.append("- **Top driver:** No clear positive theme (small sample).")
        if top_pain:
            summary_lines.append(f"- **Top pain point:** {top_pain['theme']} ({top_pain['pct']:.1f}% of reviews, avg_sent={top_pain['avg_sentiment']:.2f})")
        else:
            summary_lines.append("- **Top pain point:** No clear negative theme (small sample).")
        # two quick recommendations (take first two recs)
        recs = section["recommendations"][:2] if section["recommendations"] else []
        if recs:
            summary_lines.append("- **Recommendations:**")
            for rec in recs:
                summary_lines.append(f"  - {rec}")
        summary_lines.append("")

    # assemble one-page summary
    lines: List[str] = []
    lines.append("# Executive Summary — Fintech App Review Analysis\n")
    lines.append(f"- **Total reviews analyzed:** {total_reviews}\n")
    lines.append("## Reviews by bank")
    lines.extend(bank_lines)
    lines.append("\n## Key findings & top actions (per bank)\n")
    lines.extend(summary_lines)
    lines.append("\n### Notes\n- Themes with fewer than {min_count} reviews are excluded to avoid noisy signals.\n".format(min_count=min_count))
    lines.append("- Validate recommendations with telemetry (transfer latency, crash rates) and A/B testing.\n")

    return "\n".join(lines)


def save_report_files(df: pd.DataFrame, md_path: str, exec_path: Optional[str] = None, min_count: int = DEFAULT_MIN_COUNT):
    """
    Generate and save both the detailed report and the one-page executive summary as Markdown files.
    """
    md = generate_report_md(df, min_count=min_count)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    if exec_path:
        summary_md = generate_executive_summary_md(df, min_count=min_count)
        with open(exec_path, "w", encoding="utf-8") as f:
            f.write(summary_md)
    return md_path, exec_path


# Usage example (not executed here):
# df = pd.read_csv("data/processed/nlp_t.csv")
# save_report_files(df, "docs/task4_report.md", "docs/executive_summary.md", min_count=3)
