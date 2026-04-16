"""
06_merge_kcc.py
---------------
Merge top KCC rows into the v7 source CSV.
Takes a targeted slice from kcc_extracted_clean.csv —
focused on thin categories, score 8+ only, deduplicated
against existing source rows.

Usage:
  python scripts/06_merge_kcc.py --dry-run   # preview only
  python scripts/06_merge_kcc.py             # write merge

Output:
  data/02_structured/tamil_agri_advisory_v7_source.csv  (updated in place)
  data/02_structured/kcc_merge_report.txt
"""

import os
import csv
import argparse
import shutil
from datetime import datetime
from collections import defaultdict

# ── CONFIG ───────────────────────────────────────────────────────────────────

SOURCE_CSV = "data/02_structured/tamil_agri_advisory_v7_source.csv"
KCC_CSV    = "data/02_structured/kcc_extracted_clean.csv"
REPORT_OUT = "data/02_structured/kcc_merge_report.txt"
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_kcc_merge_backup.csv"

# Source schema — must match exactly
SOURCE_COLS = [
    "id", "question_tamil", "question_tanglish", "question_english",
    "answer_tamil", "answer_english", "category", "crop_primary",
    "crop_companions", "cropping_system", "soil_type", "irrigation_type",
    "farming_practice", "region", "season", "growth_stage", "weather_recent",
    "severity", "source_type", "farm_scale", "budget_constraint",
]

# How many rows to take per category (score 8+ only)
CATEGORY_CAPS = {
    "pest_control":       40,
    "fertilizer":         40,
    "crop_management":    25,
    "variety_selection":  20,
    "irrigation":         48,
    "post_harvest":       30,
    "livestock_advisory": 50,
    "financial_support":  15,
    "general_advisory":   10,
}

MIN_SCORE = 8

# ── HELPERS ──────────────────────────────────────────────────────────────────

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def next_id(existing_rows):
    max_id = 0
    for r in existing_rows:
        rid = r.get("id", "")
        if rid.startswith("tn-agri-"):
            try:
                num = int(rid.split("-")[-1])
                max_id = max(max_id, num)
            except ValueError:
                pass
    return max_id + 1

# ── MAIN ─────────────────────────────────────────────────────────────────────

def merge(dry_run=False):
    print("=" * 60)
    print("KCC Merge into v7 source")
    print("=" * 60)

    source_rows = load_csv(SOURCE_CSV)
    print(f"Source rows loaded: {len(source_rows)}")

    # Build dedup set
    existing_questions = set()
    for r in source_rows:
        q = r.get("question_english", "").lower().strip()[:80]
        if q:
            existing_questions.add(q)
    print(f"Existing question fingerprints: {len(existing_questions)}")

    kcc_rows = load_csv(KCC_CSV)
    print(f"KCC rows loaded: {len(kcc_rows)}")

    kcc_rows = [r for r in kcc_rows if int(r.get("kcc_score", 0)) >= MIN_SCORE]
    print(f"KCC score {MIN_SCORE}+ rows: {len(kcc_rows)}")

    kcc_rows.sort(key=lambda r: int(r.get("kcc_score", 0)), reverse=True)

    category_counts = defaultdict(int)
    selected = []

    for r in kcc_rows:
        cat = r.get("category", "general_advisory")
        cap = CATEGORY_CAPS.get(cat, 0)
        if cap == 0:
            continue
        if category_counts[cat] >= cap:
            continue
        q = r.get("question_english", "").lower().strip()[:80]
        if q in existing_questions:
            continue
        selected.append(r)
        existing_questions.add(q)
        category_counts[cat] += 1

    print(f"\nSelected {len(selected)} KCC rows to merge")
    print("\nCategory breakdown:")
    for cat, cnt in sorted(category_counts.items(), key=lambda x: -x[1]):
        cap = CATEGORY_CAPS.get(cat, 0)
        print(f"  {cat:<30} {cnt:>4} / {cap}")

    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    # Backup
    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"\n✓ Backup → {BACKUP_CSV}")

    # Build new rows strictly matching source schema
    start_id = next_id(source_rows)
    new_rows = []

    for i, r in enumerate(selected):
        new_row = {col: "" for col in SOURCE_COLS}
        new_row["id"]                = f"tn-agri-{start_id + i}"
        new_row["question_english"]  = r.get("question_english", "").capitalize()
        new_row["question_tamil"]    = ""
        new_row["question_tanglish"] = ""
        new_row["answer_english"]    = r.get("answer_english", "")
        new_row["answer_tamil"]      = ""
        new_row["category"]          = r.get("category", "general_advisory")
        new_row["crop_primary"]      = r.get("crop_primary", "all")
        new_row["crop_companions"]   = "none"
        new_row["cropping_system"]   = "all"
        new_row["soil_type"]         = "all"
        new_row["irrigation_type"]   = "all"
        new_row["farming_practice"]  = "all"
        new_row["region"]            = r.get("region", "all")
        new_row["season"]            = r.get("season", "all")
        new_row["growth_stage"]      = "all"
        new_row["weather_recent"]    = "all"
        new_row["severity"]          = "medium"
        new_row["source_type"]       = "kcc_call_log"
        new_row["farm_scale"]        = "smallholder"
        new_row["budget_constraint"] = "medium"
        new_rows.append(new_row)

    all_rows = source_rows + new_rows
    save_csv(SOURCE_CSV, all_rows, SOURCE_COLS)

    # Report
    report = [
        "=" * 60,
        f"KCC Merge Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 60,
        f"Source rows before merge: {len(source_rows)}",
        f"KCC rows merged:          {len(new_rows)}",
        f"Source rows after merge:  {len(all_rows)}",
        "",
        "Category breakdown:",
    ]
    for cat, cnt in sorted(category_counts.items(), key=lambda x: -x[1]):
        report.append(f"  {cat:<30} {cnt:>4}")

    report_text = "\n".join(report)
    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"✓ Source updated → {SOURCE_CSV}")
    print(f"✓ Report → {REPORT_OUT}")
    print(f"\nSource now has {len(all_rows)} rows.")
    print("\nNext steps:")
    print("  1. python scripts/07_translate_new_rows.py --dry-run")
    print("  2. python scripts/07_translate_new_rows.py")
    print("  3. python scripts/03g_add_structural_diversity.py")
    print("  4. Submit to Adaption")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    merge(dry_run=args.dry_run)
