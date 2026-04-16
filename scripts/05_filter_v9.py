"""
05_filter_v9.py
---------------
Prunes the dataset to maximize average quality score before Adaption submission.

Rules:
1. Deduplicates rows based on exact question_tamil_v9 prompt matches
2. Drops rows where answer_tamil_v10 is < 100 chars AND prompt has no context tag
3. Outputs a clean CSV ready for Adaption submission

Usage:
  python scripts/05_filter_v9.py --dry-run   # stats only
  python scripts/05_filter_v9.py             # write output
"""

import csv
import argparse
from pathlib import Path

INPUT_CSV  = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v7_source.csv"
OUTPUT_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v10_clean.csv"

def filter_dataset(dry_run=False):
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    print(f"Total starting rows: {len(reader)}")

    seen_prompts  = set()
    kept_rows     = []
    dropped_dupes = 0
    dropped_short = 0

    for row in reader:
        prompt = row.get("question_tamil_v9", "").strip()
        # Use answer_tamil_v10 if available, else fall back to answer_tamil
        answer = row.get("answer_tamil_v10", "").strip()
        if not answer:
            answer = row.get("answer_tamil", "").strip()

        # 1. Deduplication on prompt
        if prompt in seen_prompts:
            dropped_dupes += 1
            continue

        # 2. Drop low-signal rows — no context tag AND short answer
        has_context_tag = prompt.startswith("[")
        if not has_context_tag and len(answer) < 100:
            dropped_short += 1
            continue

        seen_prompts.add(prompt)
        kept_rows.append(row)

    print(f"\n--- Pruning Results ---")
    print(f"Dropped Duplicates : {dropped_dupes}")
    print(f"Dropped Short/Weak : {dropped_short}")
    print(f"Final Clean Rows   : {len(kept_rows)}")
    print(f"Output             : {OUTPUT_CSV.name}")

    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    fieldnames = list(reader[0].keys())
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(kept_rows)

    print(f"✓ Saved → {OUTPUT_CSV}")
    print(f"\nNext: python scripts/04_adapt_data.py --estimate")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    filter_dataset(dry_run=args.dry_run)
