"""
12_quality_filter.py
--------------------
v10 prep: Score every row on 5 dimensions and produce two submission CSVs.

Submission A: Top rows by score only (no category constraint) ~400-600 rows
Submission B: Top rows by score + category-balanced (min 20 per category)

Scoring (0-2 points each, max 10):
  1. Prompt complexity   — word count of question_tamil_v9
  2. Completion density  — length of answer_tamil_v10 (or answer_tamil fallback)
  3. Metadata specificity — how many fields are NOT all/none/empty
  4. Reasoning type value — L5/L4/contrastive/negative score highest
  5. Source quality       — TNAU/L-rows score highest, KCC lowest

Usage:
  python scripts/12_quality_filter.py --dry-run   # show score distribution only
  python scripts/12_quality_filter.py             # write both CSVs
"""

import csv
import argparse
from pathlib import Path
from collections import defaultdict, Counter

SOURCE_CSV  = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v7_source.csv"
OUTPUT_A    = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v10_A_score_only.csv"
OUTPUT_B    = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v10_B_balanced.csv"

# ── SCORING CONFIG ────────────────────────────────────────────────────────────

# 1. Prompt complexity — word count thresholds
PROMPT_SCORE = [
    (20, 2),   # >= 20 words → 2 points
    (10, 1),   # >= 10 words → 1 point
    (0,  0),   # < 10 words  → 0 points
]

# 2. Completion density — char length thresholds
COMPLETION_SCORE = [
    (500, 2),  # >= 500 chars → 2 points
    (200, 1),  # >= 200 chars → 1 point
    (0,   0),  # < 200 chars  → 0 points
]

# 3. Metadata specificity — fields that should NOT be all/none/empty
METADATA_FIELDS = [
    "crop_primary", "region", "season", "growth_stage",
    "soil_type", "irrigation_type", "weather_recent", "severity"
]
GENERIC_VALUES = {"all", "none", "", "unknown", "0", "9999", "others"}

METADATA_SCORE = [
    (5, 2),   # >= 5 specific fields → 2 points
    (2, 1),   # >= 2 specific fields → 1 point
    (0, 0),   # < 2 specific fields  → 0 points
]

# 4. Reasoning type value
REASONING_SCORES = {
    "resource_tradeoff":    2,
    "symptom_diagnosis":    2,
    "contrastive_outcome":  2,
    "preventive_action":    2,
    "crisis_routing":       2,
    "contingency_planning": 1,
    "diagnostic_advisory":  1,
    "financial_decision":   1,
    "water_management":     1,
    "scheme_navigation":    1,
    "soil_fertility":       1,
    "livestock_management": 1,
    "variety_selection":    1,
    "post_harvest":         1,
    "agronomic_advisory":   0,
    "general_advisory":     0,
}

# 5. Source quality
SOURCE_SCORES = {
    "L5_high_stakes":        2,
    "L4_diagnosis":          2,
    "L3_fine_grained":       2,
    "contrastive_pair_A":    2,
    "contrastive_pair_B":    2,
    "negative_space":        2,
    "agricultural_extension":1,
    "tnau":                  1,
    "district_government":   1,
    "district_govt":         1,
    "kcc_call_log":          0,
}

# Submission B — minimum rows per category
MIN_PER_CATEGORY = 20

# Score threshold for Submission A
MIN_SCORE_A = 5

# ── HELPERS ──────────────────────────────────────────────────────────────────

def score_prompt(row):
    q = row.get("question_tamil_v9", "").strip()
    words = len(q.split())
    for threshold, points in PROMPT_SCORE:
        if words >= threshold:
            return points
    return 0

def score_completion(row):
    ans = row.get("answer_tamil_v10", "").strip()
    if not ans:
        ans = row.get("answer_tamil", "").strip()
    chars = len(ans)
    for threshold, points in COMPLETION_SCORE:
        if chars >= threshold:
            return points
    return 0

def score_metadata(row):
    specific = sum(
        1 for f in METADATA_FIELDS
        if row.get(f, "").strip().lower() not in GENERIC_VALUES
    )
    for threshold, points in METADATA_SCORE:
        if specific >= threshold:
            return points
    return 0

def score_reasoning(row):
    rt = row.get("reasoning_type", "").strip()
    return REASONING_SCORES.get(rt, 0)

def score_source(row):
    st = row.get("source_type", "").strip()
    for key, points in SOURCE_SCORES.items():
        if key in st:
            return points
    return 0

def total_score(row):
    return (
        score_prompt(row) +
        score_completion(row) +
        score_metadata(row) +
        score_reasoning(row) +
        score_source(row)
    )

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False):
    print("=" * 60)
    print("  Quality Filter — v10 Submission Engineering")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    cols = list(rows[0].keys())
    print(f"Loaded: {len(rows)} rows\n")

    # Score all rows
    scored = []
    for r in rows:
        s = total_score(r)
        scored.append((s, r))

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # Score distribution
    score_counts = Counter(s for s, _ in scored)
    print("Score distribution:")
    for sc in range(10, -1, -1):
        if sc in score_counts:
            bar = "█" * (score_counts[sc] // 10)
            print(f"  {sc:2d}/10  {score_counts[sc]:>5}  {bar}")

    print()

    # ── SUBMISSION A: score >= MIN_SCORE_A ───────────────────────────────────
    rows_a = [r for s, r in scored if s >= MIN_SCORE_A]
    print(f"Submission A (score >= {MIN_SCORE_A}): {len(rows_a)} rows")

    # Category breakdown A
    cat_a = Counter(r.get("category","") for r in rows_a)
    print("  Top categories:")
    for cat, cnt in cat_a.most_common(8):
        print(f"    {cat:<30} {cnt:>4}")

    # ── SUBMISSION B: balanced ────────────────────────────────────────────────
    category_buckets = defaultdict(list)
    for s, r in scored:
        category_buckets[r.get("category", "")].append((s, r))

    rows_b = []
    # First pass: take min per category (highest scoring first)
    for cat, bucket in category_buckets.items():
        taken = [r for _, r in bucket[:MIN_PER_CATEGORY]]
        rows_b.extend(taken)

    # Second pass: fill with remaining high-scoring rows not already included
    ids_b = {r["id"] for r in rows_b}
    for s, r in scored:
        if s >= MIN_SCORE_A and r["id"] not in ids_b:
            rows_b.append(r)
            ids_b.add(r["id"])

    # Re-sort by score
    rows_b.sort(key=lambda r: total_score(r), reverse=True)

    print(f"\nSubmission B (balanced, min {MIN_PER_CATEGORY}/category): {len(rows_b)} rows")
    cat_b = Counter(r.get("category","") for r in rows_b)
    print("  Top categories:")
    for cat, cnt in cat_b.most_common(8):
        print(f"    {cat:<30} {cnt:>4}")

    # Score stats for both
    scores_a = [total_score(r) for r in rows_a]
    scores_b = [total_score(r) for r in rows_b]
    print(f"\nAvg score — A: {sum(scores_a)/len(scores_a):.2f} | B: {sum(scores_b)/len(scores_b):.2f}")

    if dry_run:
        print("\n[DRY RUN] No files written.")
        print(f"\nSample top-scored rows:")
        for s, r in scored[:5]:
            print(f"  [{s}/10] {r['id']} | {r.get('category')} | {r.get('reasoning_type')}")
            print(f"    Q: {r.get('question_tamil_v9','')[:70]}")
        return

    save_csv(OUTPUT_A, rows_a, cols)
    save_csv(OUTPUT_B, rows_b, cols)

    print(f"\n✓ Submission A → {OUTPUT_A.name} ({len(rows_a)} rows)")
    print(f"✓ Submission B → {OUTPUT_B.name} ({len(rows_b)} rows)")
    print(f"\nNext:")
    print(f"  Submit A: python scripts/04_adapt_data.py --file data/02_structured/{OUTPUT_A.name} --estimate")
    print(f"  Submit B: python scripts/04_adapt_data.py --file data/02_structured/{OUTPUT_B.name} --estimate")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
