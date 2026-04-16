"""
03d_refine_answers.py
---------------------
Pre-Adaption answer quality pass. Runs after 03c_reclassify_crop_management.py.

Three steps:
  1. DROP   — pure stat lookups, contact info, no advisory value
  2. EXPAND — genuinely incomplete advisory rows (Groq adds depth)
  3. REPORT — what changed, final distribution

Uses Groq llama-3.3-70b-versatile — free, fast, OpenAI-compatible.

Expansion philosophy:
  - Never add facts not in the original answer
  - Add: why this works, when to do it, what to watch for, what comes next
  - Target: 200-350 word English answers
  - Only expand rows that are genuinely incomplete, not just short

Run from repo root:
    python scripts/03d_refine_answers.py --dry-run
    python scripts/03d_refine_answers.py
"""

import os
import re
import time
import argparse
import pandas as pd
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found. Add it to your .env file.")

client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1",
)

MODEL       = "llama-3.3-70b-versatile"
API_DELAY   = 2.5   # 2.5s = ~24 req/min — safely under Groq's 30 req/min limit
MAX_RETRIES = 3

SOURCE_CSV  = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v7_source.csv"
DROPPED_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "refined_dropped_rows.csv"

# Per-category minimum answer length — advisory categories need more depth
CATEGORY_MIN_LENGTH = {
    "weather_advisory":    300,
    "crop_disease":        250,
    "pest_control":        250,
    "livestock_dairy":     220,
    "livestock_goat":      220,
    "livestock_poultry":   220,
    "crop_management":     200,
    "aquaculture":         200,
    "irrigation":          180,
    "fertilizer":          180,
    "soil_health":         180,
    "harvest_timing":      150,
    "sericulture":         150,
    "floriculture":        150,
    "women_agriculture":   150,
    "government_schemes":  120,  # scheme facts OK when short
    "variety_selection":   100,  # variety lists complete when short
    "market_price":        100,
    "financial_support":   120,
    "farm_equipment":      120,
    "mental_health_safety": 100,
}

# =============================================================================
# Step 1: DROP — no advisory value
# =============================================================================

DROP_Q_PATTERNS = [
    r"\bhow many (sheep|goat|cattle|buffalo|poultry|farm)s?\b.*\bdistrict\b",
    r"\btotal number of (sheep|goat|cattle|poultry|farm)",
    r"\bhow many backyard\b",
    r"\bhow many commercial poultry\b",
    r"\bwhat is the (total |net )?area (of|under|in) .{0,30}district\b",
    r"\bwhat is the percentage of .{0,20}soil\b",
    r"\bwhat is the (total )?area of .{0,30}district\b",
    r"\bwhat is the (average |normal )?annual rainfall\b",
    r"\bwhat is the (average |normal )?rainfall .{0,30}(district|season|month)\b",
    r"\bcontact information\b",
    r"\bphone number\b",
    r"\bwhat is the address\b",
    r"^where can i find information on .{0,30}\?$",
    r"^what are the main crops covered\b",
    r"^how do i grow \w+\?$",
]

DROP_A_PATTERNS = [
    r"^there (are|is) \d[\d,.]+ (thousand )?(sheep|goat|cattle|poultry|farm)",
    r"^\d+[\d,.]*\s*(thousand|lakh|crore)?\s*(sheep|goat|cattle|farm)",
    r"^the (average |normal )?rainfall (in|of|during)",
    r"^black soil constitutes",
    r"^there is no data",
]


def should_drop(row: dict) -> tuple:
    q = str(row.get("question_english", "")).lower().strip()
    a = str(row.get("answer_english", "")).lower().strip()
    for p in DROP_Q_PATTERNS:
        if re.search(p, q, re.IGNORECASE):
            return True, f"stat_lookup"
    for p in DROP_A_PATTERNS:
        if re.search(p, a, re.IGNORECASE):
            return True, f"no_advisory_answer"
    return False, ""


# =============================================================================
# Step 2: EXPAND — only genuinely incomplete rows
# =============================================================================

# Answers that are short but factually complete — don't expand
COMPLETE_WHEN_SHORT = [
    r"^(vbn|co|adt|k|tmv|mtu|cr|drrh)\s*\d",   # variety code list
    r"\brs\.?\s*\d+.{0,20}(subsidy|per|ha)\b",   # scheme amount
    r"^\d+\s*kg/?ha",                              # seed rate
    r"^(january|february|march|april|may|june|july|august|september|october|november|december)",
    r"^sow\b.{0,60}(january|february|march|april|may|june|july|august|september|october|november|december)",
    r"^apply \d+",                                 # simple dose instruction
]

# Answers that look complete but are cop-outs — expand these
COPOUT_PATTERNS = [
    r"^contact your (local|nearest|district)",
    r"^consult (a|your|the) (vet|doctor|expert|officer)",
    r"^please contact",
    r"^for more (information|details)",
    r"^visit (the|your)",
]


def should_expand(row: dict) -> tuple:
    cat     = str(row.get("category", ""))
    ans     = str(row.get("answer_english", ""))
    ans_len = len(ans)
    threshold = CATEGORY_MIN_LENGTH.get(cat, 150)

    # Long enough — keep
    if ans_len >= threshold:
        return False, ""

    # Short but factually complete reference — keep
    for p in COMPLETE_WHEN_SHORT:
        if re.search(p, ans, re.IGNORECASE):
            return False, ""

    # Cop-out even if meets length — expand
    for p in COPOUT_PATTERNS:
        if re.search(p, ans, re.IGNORECASE):
            return True, "copout"

    # Short and doesn't end properly — expand
    if not ans.rstrip().endswith("."):
        return True, f"truncated_{cat}"

    # Short but ends with period — still expand if below threshold
    return True, f"short_{cat}"


def expansion_type(row: dict) -> str:
    q   = str(row.get("question_english", "")).lower()
    cat = str(row.get("category", ""))
    if any(k in q for k in ["when to sow", "sowing date", "when should i sow", "planting window"]):
        return "sowing_timing"
    if any(k in q for k in ["seed rate", "seed treatment", "spacing for", "fertilizer dose"]):
        return "agronomic_spec"
    if any(k in q for k in ["subsidy", "scheme", "government", "rs.", "eligib"]):
        return "government_scheme"
    if any(k in cat for k in ["livestock", "dairy", "goat", "poultry"]):
        return "livestock_advisory"
    if "weather" in cat or any(k in q for k in ["drought", "flood", "cyclone", "monsoon"]):
        return "weather_contingency"
    if any(k in cat for k in ["disease", "pest"]):
        return "disease_pest"
    return "general_advisory"


EXPAND_SYSTEM = """You are a Tamil Nadu TNAU agricultural extension officer.
Expand the given short answer to be more useful for a smallholder farmer.

RULES:
- Do NOT invent facts. Only elaborate on what is already stated.
- Add: why this works, exact timing, quantities, what to watch for, next steps.
- End with: consult your nearest KVK or district agriculture officer.
- Target: 150-300 words.
- Return ONLY the expanded English answer. No preamble."""

EXPAND_PROMPTS = {
    "sowing_timing": "Farmer asked: {question}\nShort answer: {answer}\nCrop: {crop} | Region: {region} | Season: {season}\n\nExpand to include: exact sowing dates, soil preparation, seed rate, spacing, first 2-week watch points, variety if applicable, KVK referral.",
    "agronomic_spec": "Farmer asked: {question}\nShort answer: {answer}\nCrop: {crop} | Region: {region}\n\nExpand to include: when to apply, why this rate works, signs of success/problems, safety precautions, KVK referral.",
    "government_scheme": "Farmer asked: {question}\nShort answer: {answer}\n\nExpand to include: who is eligible, how to apply (office, documents), when to apply, exactly what the farmer receives, KVK referral.",
    "livestock_advisory": "Farmer asked: {question}\nShort answer: {answer}\nAnimal: {crop} | Region: {region}\n\nExpand to include: immediate steps, why this works, signs of recovery, future prevention, veterinary officer referral.",
    "weather_contingency": "Farmer asked: {question}\nShort answer: {answer}\nCrop: {crop} | Region: {region} | Season: {season}\n\nExpand to include: immediate action (24-48hr), 1-2 week management, crop-specific tips, alternative crop if crop fails, KVK referral.",
    "disease_pest": "Farmer asked: {question}\nShort answer: {answer}\nCrop: {crop} | Region: {region}\n\nExpand to include: identification signs, treatment dose and method, why it works, expected recovery time, next-season prevention, KVK referral.",
    "general_advisory": "Farmer asked: {question}\nShort answer: {answer}\nCategory: {category} | Crop: {crop} | Region: {region}\n\nExpand to add practical context, why this works, steps, watch points, KVK referral. 150-250 words.",
}


def expand_answer(row: dict, dry_run: bool = False) -> str:
    exp_type = expansion_type(row)
    prompt = EXPAND_PROMPTS.get(exp_type, EXPAND_PROMPTS["general_advisory"]).format(
        question=row.get("question_english", ""),
        answer=row.get("answer_english", ""),
        crop=row.get("crop_primary", "all"),
        region=row.get("region", "all"),
        season=row.get("season", "all"),
        category=row.get("category", ""),
    )
    if dry_run:
        return f"[DRY RUN — {exp_type}]"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": EXPAND_SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.3,
                max_tokens=600,
            )
            return r.choices[0].message.content.strip()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(8 * attempt)
    return row.get("answer_english", "")


# =============================================================================
# Main
# =============================================================================

def run(dry_run: bool = False):
    print(f"\n{'='*65}")
    print(f"  Answer Refinement — {MODEL} (Groq)")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print(f"{'='*65}\n")

    df = pd.read_csv(SOURCE_CSV, dtype=str)
    df["_len"] = df["answer_english"].str.len().fillna(0).astype(int)
    print(f"Loaded: {len(df)} rows\n")

    results, dropped_rows = [], []
    n_dropped = n_expanded = n_kept = 0

    for _, row in df.iterrows():
        rd = row.to_dict()
        q = str(rd.get("question_english", ""))[:55]

        # DROP
        drop, reason = should_drop(rd)
        if drop:
            dropped_rows.append(rd)
            n_dropped += 1
            print(f"  DROP   {q} [{reason}]")
            continue

        # EXPAND
        expand, reason = should_expand(rd)
        if expand:
            ans_len = int(rd.get("_len", 0))
            exp_type = expansion_type(rd)
            print(f"  EXPAND {q} ({ans_len}c) [{exp_type}]")
            expanded = expand_answer(rd, dry_run=dry_run)
            if not dry_run:
                rd["answer_english"] = expanded
                rd["source_type"] = str(rd.get("source_type", "")) + "|expanded"
                time.sleep(API_DELAY)
            n_expanded += 1
        else:
            n_kept += 1

        rd.pop("_len", None)
        results.append(rd)

    for rd in dropped_rows:
        rd.pop("_len", None)

    print(f"\n{'='*65}")
    print(f"  REFINEMENT SUMMARY")
    print(f"{'='*65}")
    print(f"  Original : {len(df)}")
    print(f"  Dropped  : {n_dropped}")
    print(f"  Expanded : {n_expanded}")
    print(f"  Kept     : {n_kept}")
    print(f"  Final    : {len(results)}")

    if dry_run:
        print(f"\nDRY RUN — no files written.")
        return

    result_df = pd.DataFrame(results)
    orig_cols = [c for c in df.columns if c in result_df.columns and c != "_len"]
    result_df[orig_cols].to_csv(SOURCE_CSV, index=False, encoding="utf-8", quoting=1)
    print(f"\n✓ Written  → {SOURCE_CSV}")

    if dropped_rows:
        pd.DataFrame(dropped_rows)[orig_cols].to_csv(
            DROPPED_CSV, index=False, encoding="utf-8", quoting=1)
        print(f"✓ Dropped  → {DROPPED_CSV}")

    print(f"\n  Category distribution:")
    for cat, cnt in result_df["category"].value_counts().items():
        print(f"    {cat:<25} {cnt:>4} ({cnt/len(result_df)*100:.1f}%)")
    print(f"\nNext: python scripts/04_adapt_data.py --estimate")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
