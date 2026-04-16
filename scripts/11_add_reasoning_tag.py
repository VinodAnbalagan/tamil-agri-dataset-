"""
11_add_reasoning_tag.py
-----------------------
v10 prep: Add reasoning_type metadata column to every row.
Inspired by the fraud detection dataset that tagged cognitive patterns per row.

Maps source_type + category → reasoning_type tag.
No API calls — pure rule-based. Runs in seconds.

Reasoning types:
  symptom_diagnosis     — L4 rows: farmer describes symptoms, answer diagnoses
  resource_tradeoff     — L5 rows: high-stakes dilemma, no single right answer
  preventive_action     — negative space: correct answer is to wait/not act
  contrastive_outcome   — contrastive pairs: same topic, different context
  contingency_planning  — weather/drought/flood adaptive strategies
  scheme_navigation     — government scheme eligibility and application
  agronomic_advisory    — standard crop management advice
  crisis_routing        — mental health / financial distress → helpline
  diagnostic_advisory   — pest/disease identification + treatment
  financial_decision    — loan, insurance, market price decisions
  livestock_management  — animal husbandry and veterinary advisory
  soil_fertility        — fertilizer, soil health, micronutrient management
  water_management      — irrigation, water stress, AWD
  variety_selection     — seed/variety recommendation by season/region
  post_harvest          — storage, processing, value addition
  general_advisory      — catch-all for cross-category rows

Usage:
  python scripts/11_add_reasoning_tag.py --dry-run
  python scripts/11_add_reasoning_tag.py
"""

import csv
import argparse
import shutil
from pathlib import Path
from datetime import datetime

SOURCE_CSV = "data/02_structured/tamil_agri_advisory_v7_source.csv"
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_reasoningtag_backup.csv"

# ── MAPPING RULES ─────────────────────────────────────────────────────────────
# Priority order: source_type first, then category

SOURCE_TYPE_MAP = {
    "L4_diagnosis":       "symptom_diagnosis",
    "L5_high_stakes":     "resource_tradeoff",
    "negative_space":     "preventive_action",
    "contrastive_pair_A": "contrastive_outcome",
    "contrastive_pair_B": "contrastive_outcome",
    "L3_fine_grained":    "agronomic_advisory",
    "kcc_call_log":       None,  # fall through to category map
}

CATEGORY_MAP = {
    "crop_disease":        "diagnostic_advisory",
    "pest_control":        "diagnostic_advisory",
    "fertilizer":          "soil_fertility",
    "soil_health":         "soil_fertility",
    "irrigation":          "water_management",
    "weather_advisory":    "contingency_planning",
    "government_schemes":  "scheme_navigation",
    "financial_support":   "financial_decision",
    "market_price":        "financial_decision",
    "livestock_dairy":     "livestock_management",
    "livestock_advisory":  "livestock_management",
    "livestock_goat":      "livestock_management",
    "livestock_poultry":   "livestock_management",
    "aquaculture":         "livestock_management",
    "crop_management":     "agronomic_advisory",
    "variety_selection":   "variety_selection",
    "harvest_timing":      "post_harvest",
    "post_harvest":        "post_harvest",
    "sericulture":         "agronomic_advisory",
    "floriculture":        "agronomic_advisory",
    "women_agriculture":   "scheme_navigation",
    "mental_health_safety":"crisis_routing",
    "general_advisory":    "general_advisory",
}

def get_reasoning_tag(row: dict) -> str:
    source = row.get("source_type", "").strip()
    category = row.get("category", "").strip()

    # Check source_type first
    for key, tag in SOURCE_TYPE_MAP.items():
        if key in source:
            if tag is not None:
                return tag
            break  # fall through to category

    # Fall through to category
    return CATEGORY_MAP.get(category, "general_advisory")

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def run(dry_run=False):
    print("=" * 60)
    print("  Reasoning Tag Addition — v10 prep")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    cols = list(rows[0].keys())
    print(f"Loaded: {len(rows)} rows")

    # Add column if not present
    if "reasoning_type" not in cols:
        cols.append("reasoning_type")
        for r in rows:
            r["reasoning_type"] = ""

    # Apply tags
    from collections import Counter
    tag_counts = Counter()

    for r in rows:
        tag = get_reasoning_tag(r)
        r["reasoning_type"] = tag
        tag_counts[tag] += 1

    print(f"\nReasoning type distribution:")
    for tag, count in sorted(tag_counts.items(), key=lambda x: -x[1]):
        print(f"  {tag:<30} {count:>5}")

    if dry_run:
        print(f"\n[DRY RUN] No files written.")
        return

    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"\n✓ Backup → {BACKUP_CSV}")

    save_csv(SOURCE_CSV, rows, cols)
    print(f"✓ Written → {SOURCE_CSV}")
    print(f"\nNew column: reasoning_type")
    print(f"Add 'reasoning_type' to COLUMN_MAPPING context in 04_adapt_data.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
