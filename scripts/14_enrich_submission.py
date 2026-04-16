"""
14_enrich_submission.py
-----------------------
Final metadata enrichment pass on submission CSVs before Adaption.

Fixes:
1. farm_scale — set to 'smallholder' for all rows (this dataset is for small farmers)
2. budget_constraint — infer from category (organic/zero-budget vs standard)
3. reasoning_type — re-apply from source_type + category mapping
4. answer_tamil_v10 — ensure populated (fallback to answer_tamil if empty)

Works on any CSV — pass the file as argument.

Usage:
  python scripts/14_enrich_submission.py --file data/02_structured/tamil_agri_advisory_v10_A_score_only.csv
  python scripts/14_enrich_submission.py --file data/02_structured/tamil_agri_advisory_v10_B_balanced.csv
"""

import csv
import argparse
from pathlib import Path

# ── MAPPINGS ─────────────────────────────────────────────────────────────────

REASONING_SOURCE_MAP = {
    "L4_diagnosis":       "symptom_diagnosis",
    "L5_high_stakes":     "resource_tradeoff",
    "negative_space":     "preventive_action",
    "contrastive_pair_A": "contrastive_outcome",
    "contrastive_pair_B": "contrastive_outcome",
    "L3_fine_grained":    "agronomic_advisory",
    "kcc_call_log":       None,
}

REASONING_CATEGORY_MAP = {
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

BUDGET_CATEGORY_MAP = {
    "fertilizer":          "low-cost",
    "soil_health":         "zero-budget",
    "irrigation":          "low-cost",
    "crop_disease":        "low-cost",
    "pest_control":        "low-cost",
    "weather_advisory":    "zero-budget",
    "government_schemes":  "zero-budget",
    "financial_support":   "zero-budget",
    "mental_health_safety":"zero-budget",
    "livestock_advisory":  "low-cost",
    "livestock_dairy":     "low-cost",
    "livestock_goat":      "low-cost",
    "aquaculture":         "standard",
    "market_price":        "zero-budget",
    "crop_management":     "low-cost",
    "variety_selection":   "low-cost",
    "sericulture":         "standard",
    "floriculture":        "standard",
}

def get_reasoning_type(row):
    source = row.get("source_type", "").strip()
    category = row.get("category", "").strip()
    for key, tag in REASONING_SOURCE_MAP.items():
        if key in source:
            if tag is not None:
                return tag
            break
    return REASONING_CATEGORY_MAP.get(category, "general_advisory")

def get_budget_constraint(row):
    existing = row.get("budget_constraint", "").strip()
    if existing and existing.lower() not in ("", "all", "none", "unknown"):
        return existing
    category = row.get("category", "").strip()
    return BUDGET_CATEGORY_MAP.get(category, "low-cost")

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def run(filepath: str):
    path = Path(filepath)
    print(f"Loading: {path.name}")
    rows = load_csv(path)
    cols = list(rows[0].keys())
    print(f"Rows: {len(rows)}")

    # Add missing columns
    for col in ["reasoning_type", "answer_tamil_v10"]:
        if col not in cols:
            cols.append(col)
            for r in rows:
                r[col] = ""

    n_farm_scale    = 0
    n_budget        = 0
    n_reasoning     = 0
    n_answer_v10    = 0

    for r in rows:
        # 1. farm_scale — all smallholder
        if not r.get("farm_scale", "").strip() or r.get("farm_scale", "").strip().lower() in ("", "all", "none"):
            r["farm_scale"] = "smallholder"
            n_farm_scale += 1

        # 2. budget_constraint — infer from category
        r["budget_constraint"] = get_budget_constraint(r)
        n_budget += 1

        # 3. reasoning_type — re-apply mapping
        r["reasoning_type"] = get_reasoning_type(r)
        n_reasoning += 1

        # 4. answer_tamil_v10 — fallback to answer_tamil if empty
        if not r.get("answer_tamil_v10", "").strip():
            r["answer_tamil_v10"] = r.get("answer_tamil", "")
            n_answer_v10 += 1

    save_csv(path, rows, cols)

    print(f"\n── Enrichment Results ──────────────────────────")
    print(f"  farm_scale filled:      {n_farm_scale}")
    print(f"  budget_constraint set:  {n_budget}")
    print(f"  reasoning_type set:     {n_reasoning}")
    print(f"  answer_tamil_v10 fixed: {n_answer_v10}")
    print(f"✓ Saved → {path}")

    # Quick metadata check
    print(f"\n── Final Metadata Richness ─────────────────────")
    generic = {"all", "none", "", "unknown"}
    for col in ["region", "season", "growth_stage", "soil_type",
                "farm_scale", "budget_constraint", "reasoning_type"]:
        if col in cols:
            specific = sum(1 for r in rows
                          if r.get(col, "").strip().lower() not in generic)
            print(f"  {col:<22} {specific:>3}/{len(rows)} ({specific/len(rows)*100:.0f}%) specific")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to submission CSV")
    args = parser.parse_args()
    run(args.file)
