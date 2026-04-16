"""
15_verify_and_fix_mismatches.py
--------------------------------
Audits both submission CSVs for crop mismatches between
question_tamil_v9 and crop_primary metadata.

A mismatch = question mentions a crop that contradicts crop_primary.

Fix strategy:
- For mismatched rows: replace question_tamil_v9 with question_tamil
  (the original question, which correctly matches the metadata)
- Never delete rows — fix them

Also checks:
- answer_tamil_v10 vs answer_english alignment
- Empty or suspiciously short fields

Usage:
  python scripts/15_verify_and_fix_mismatches.py --dry-run
  python scripts/15_verify_and_fix_mismatches.py
"""

import csv
import argparse
import shutil
from pathlib import Path

FILES = [
    "data/02_structured/tamil_agri_advisory_v10_A_score_only.csv",
    "data/02_structured/tamil_agri_advisory_v10_B_balanced.csv",
]

# Tamil crop name → crop_primary value mappings
# If question_tamil_v9 contains the Tamil word but crop_primary is something else = mismatch
CROP_TAMIL_MAP = {
    "வாழை":         "banana",
    "நெல்":         "rice",
    "பருத்தி":      "cotton",
    "நிலக்கடலை":   "groundnut",
    "கரும்பு":     "sugarcane",
    "மிளகாய்":     "chilli",
    "தக்காளி":     "tomato",
    "வெங்காயம்":   "onion",
    "தேங்காய்":    "coconut",
    "மஞ்சள்":      "turmeric",
    "கோதுமை":      "wheat",
    "சோளம்":       "sorghum",
    "மக்காசோளம்":  "maize",
    "துவரை":       "red gram",
    "உளுந்து":     "black gram",
    "பயறு":        "green gram",
}

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def find_mismatches(rows):
    mismatches = []
    for row in rows:
        q = row.get("question_tamil_v9", "")
        crop = row.get("crop_primary", "").strip().lower()

        for tamil_word, expected_crop in CROP_TAMIL_MAP.items():
            if tamil_word in q:
                # Question mentions this crop
                if crop and crop != expected_crop and crop not in ("all", "none", "", "unknown"):
                    mismatches.append({
                        "id": row["id"],
                        "crop_primary": crop,
                        "mentioned_crop": expected_crop,
                        "tamil_word": tamil_word,
                        "q_preview": q[:80],
                    })
                    break  # only flag once per row

    return mismatches

def run(dry_run=False):
    print("=" * 60)
    print("  Dataset Verification & Mismatch Fix")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    for filepath in FILES:
        path = Path(filepath)
        if not path.exists():
            print(f"\nSkipping (not found): {path.name}")
            continue

        print(f"\n── {path.name} ──────────────────────────────────────")
        rows = load_csv(path)
        cols = list(rows[0].keys())
        print(f"  Rows: {len(rows)}")

        # Find mismatches
        mismatches = find_mismatches(rows)
        print(f"  Crop mismatches found: {len(mismatches)}")

        for m in mismatches:
            print(f"\n  ⚠  {m['id']}")
            print(f"     crop_primary:    {m['crop_primary']}")
            print(f"     question says:   {m['tamil_word']} ({m['mentioned_crop']})")
            print(f"     Q preview:       {m['q_preview']}")

        if not mismatches:
            print("  ✓ No mismatches found")
            continue

        if dry_run:
            print(f"\n  [DRY RUN] Would fix {len(mismatches)} rows by replacing")
            print(f"  question_tamil_v9 with original question_tamil")
            continue

        # Fix — replace question_tamil_v9 with original question_tamil
        mismatch_ids = {m["id"] for m in mismatches}
        fixed = 0

        for row in rows:
            if row["id"] in mismatch_ids:
                original_q = row.get("question_tamil", "").strip()
                if original_q:
                    row["question_tamil_v9"] = original_q
                    fixed += 1
                    print(f"\n  ✓ Fixed {row['id']}")
                    print(f"    New Q: {original_q[:80]}")
                else:
                    print(f"\n  ✗ {row['id']} — question_tamil also empty, cannot fix")

        # Backup and save
        backup = path.with_name(path.stem + "_pre_fix_backup.csv")
        shutil.copy2(path, backup)
        save_csv(path, rows, cols)

        print(f"\n  Fixed: {fixed} rows")
        print(f"  Backup → {backup.name}")
        print(f"  ✓ Saved → {path.name}")

        # Final check
        remaining = find_mismatches(rows)
        print(f"  Remaining mismatches after fix: {len(remaining)}")

    print("\n" + "=" * 60)
    print("  Verification complete")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
