"""
03f_enrich_metadata.py
----------------------
Backfills specific metadata from answer content.
Runs after 03e_expand_tamil.py.

Currently 69% of rows have season=all, many have region=all.
This script extracts implied season/region/growth_stage from answer text
and backfills where currently set to 'all'.

Uses Groq (llama-3.3-70b-versatile) — fast, free.

Run from repo root:
    python scripts/03f_enrich_metadata.py --dry-run
    python scripts/03f_enrich_metadata.py
"""

import os
import re
import json
import time
import argparse
import pandas as pd
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found.")

client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1",
)

MODEL     = "llama-3.3-70b-versatile"
API_DELAY = 2.0
MAX_RETRIES = 3

SOURCE_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v7_source.csv"

# =============================================================================
# Rule-based extraction — fast, no API call needed
# =============================================================================

SEASON_KEYWORDS = {
    "kuruvai":  ["kuruvai", "june-september", "june to september", "june–september",
                 "kharif.*delta", "early rice"],
    "kharif":   ["kharif", "june-october", "southwest monsoon", "rainfed.*dry zone",
                 "adipattam", "july.*sow", "june.*sow.*groundnut", "june.*sow.*cotton"],
    "samba":    ["samba", "august-january", "august to january", "northeast monsoon.*rice",
                 "main rice season", "october.*rice", "november.*rice"],
    "rabi":     ["rabi", "october-february", "post.*monsoon.*irrigat", "november.*sow.*pulse",
                 "november.*sow.*onion", "december.*sow", "january.*sow.*pulse"],
    "thaladi":  ["thaladi", "november-march", "late rice", "november.*rice.*delta"],
    "summer":   ["summer", "march-june", "april.*sow", "may.*sow", "february.*sow.*vegetable",
                 "summer crop", "hot season"],
}

REGION_KEYWORDS = {
    "delta":         ["delta", "thanjavur", "tiruvarur", "nagapattinam", "cauvery",
                      "kuruvai", "samba season", "tiruchirapalli", "mayiladuthurai"],
    "dry_zone":      ["dry zone", "coimbatore", "madurai", "dindigul", "virudhunagar",
                      "rainfed.*red soil", "black cotton soil", "dryland", "salem",
                      "namakkal", "dharmapuri", "krishnagiri", "erode.*dry"],
    "western_ghats": ["nilgiris", "ooty", "western ghats", "hill.*station", "kodaikanal",
                      "theni", "high altitude", "hilly region", "cardamom", "tea.*estate"],
    "coastal":       ["coastal", "thoothukudi", "ramanathapuram", "kanyakumari",
                      "sea.*water", "saline.*soil", "shrimp.*farm", "prawn.*culture",
                      "nagapattinam.*coastal"],
}

GROWTH_STAGE_KEYWORDS = {
    "germination":      ["germination", "seed.*sprout", "emergence"],
    "seedling":         ["seedling", "nursery stage", "transplant.*ready"],
    "vegetative":       ["vegetative", "tillering", "branching", "early growth",
                         "leaf.*stage", "vegetative stage"],
    "flowering":        ["flowering", "flower.*stage", "anthesis", "pollination"],
    "reproductive":     ["reproductive", "panicle", "grain.*fill", "pod.*form",
                         "fruit.*set", "fruiting"],
    "maturity":         ["maturity", "harvest.*ready", "grain.*mature", "physiological.*mature"],
    "post_harvest":     ["post.*harvest", "after.*harvest", "storage", "threshing"],
}

WEATHER_KEYWORDS = {
    "dry":    ["dry spell", "drought", "no rain", "water stress", "borewell.*dried",
               "moisture.*stress", "wilting"],
    "rainy":  ["heavy rain", "flood", "waterlog", "submerg", "excess.*rain",
               "northeast monsoon.*active"],
    "humid":  ["humid", "high.*humidity", "misty", "fog"],
}


def extract_from_text(text: str, keyword_map: dict) -> str:
    """Extract first matching value from text using keyword map."""
    text_lower = text.lower()
    for value, keywords in keyword_map.items():
        for kw in keywords:
            if re.search(kw, text_lower):
                return value
    return ""


def enrich_row_rule_based(row: dict) -> dict:
    """Fast rule-based enrichment — no API needed."""
    combined = " ".join([
        str(row.get("question_english", "")),
        str(row.get("answer_english", "")),
        str(row.get("question_tanglish", "")),
    ])

    updated = {}

    if row.get("season", "all") == "all":
        season = extract_from_text(combined, SEASON_KEYWORDS)
        if season:
            updated["season"] = season

    if row.get("region", "all") == "all":
        region = extract_from_text(combined, REGION_KEYWORDS)
        if region:
            updated["region"] = region

    if row.get("growth_stage", "all") == "all":
        stage = extract_from_text(combined, GROWTH_STAGE_KEYWORDS)
        if stage:
            updated["growth_stage"] = stage

    if row.get("weather_recent", "all") == "all":
        weather = extract_from_text(combined, WEATHER_KEYWORDS)
        if weather:
            updated["weather_recent"] = weather

    return updated


# =============================================================================
# LLM-based enrichment — for ambiguous rows
# =============================================================================

ENRICH_SYSTEM = """You extract metadata from Tamil Nadu agricultural Q&A pairs.
Return ONLY a JSON object with these fields (use null if genuinely unknown):
{
  "season": "<kuruvai|kharif|samba|rabi|thaladi|summer|all>",
  "region": "<delta|dry_zone|western_ghats|coastal|all>",
  "growth_stage": "<germination|seedling|vegetative|flowering|reproductive|maturity|post_harvest|all>",
  "weather_recent": "<dry|rainy|humid|all>",
  "farm_scale": "<marginal|small|medium|large|all>",
  "severity": "<low|medium|high|urgent>"
}
Use 'all' only if truly context-independent. No explanation."""


def enrich_row_llm(row: dict, fields_needed: list) -> dict:
    """LLM enrichment for rows where rule-based extraction failed."""
    prompt = f"""Question: {row.get('question_english', '')}
Answer: {row.get('answer_english', '')}
Category: {row.get('category', '')}
Crop: {row.get('crop_primary', '')}

Extract these metadata fields: {', '.join(fields_needed)}
Return JSON only."""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": ENRICH_SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.1,
                max_tokens=150,
            )
            raw = r.choices[0].message.content.strip()
            raw = re.sub(r"```json|```", "", raw).strip()
            result = json.loads(raw)
            # Only return non-null, non-'all' values for fields we need
            return {k: v for k, v in result.items()
                    if k in fields_needed and v and v != "all" and v != "null"}
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(4 * attempt)
    return {}


def run(dry_run: bool = False, use_llm: bool = False):
    print(f"\n{'='*65}")
    print(f"  Metadata Enrichment")
    print(f"  Rule-based: always | LLM fallback: {'ON' if use_llm else 'OFF'}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print(f"{'='*65}\n")

    df = pd.read_csv(SOURCE_CSV, dtype=str)
    print(f"Loaded: {len(df)} rows\n")

    # Before stats
    print("=== BEFORE ===")
    for col in ["season", "region", "growth_stage", "weather_recent"]:
        if col in df.columns:
            n_all = (df[col] == "all").sum()
            print(f"  {col:<20} all={n_all}/{len(df)} ({n_all/len(df)*100:.0f}%)")

    rule_updates = 0
    llm_updates  = 0

    for idx, row in df.iterrows():
        rd = row.to_dict()
        updates = enrich_row_rule_based(rd)

        # LLM fallback for rows still needing enrichment
        if use_llm and not dry_run:
            remaining = [f for f in ["season", "region", "growth_stage", "weather_recent"]
                         if rd.get(f, "all") == "all" and f not in updates]
            if remaining:
                llm_result = enrich_row_llm(rd, remaining)
                updates.update(llm_result)
                if llm_result:
                    llm_updates += 1
                time.sleep(API_DELAY)

        if updates:
            rule_updates += 1
            if not dry_run:
                for k, v in updates.items():
                    df.at[idx, k] = v

        # Progress every 100 rows
        row_num = list(df.index).index(idx) + 1
        if row_num % 100 == 0:
            print(f"  ... {row_num}/{len(df)} rows processed (rule={rule_updates}, llm={llm_updates})")

    print(f"\n=== RESULTS ===")
    print(f"  Rule-based updates : {rule_updates}")
    if use_llm:
        print(f"  LLM updates        : {llm_updates}")

    if not dry_run:
        print(f"\n=== AFTER ===")
        for col in ["season", "region", "growth_stage", "weather_recent"]:
            if col in df.columns:
                n_all = (df[col] == "all").sum()
                print(f"  {col:<20} all={n_all}/{len(df)} ({n_all/len(df)*100:.0f}%)")

    if dry_run:
        print("\nDRY RUN — no files written.")
        # Show sample of what would be updated
        sample_count = 0
        for idx, row in df.iterrows():
            updates = enrich_row_rule_based(row.to_dict())
            if updates and sample_count < 10:
                print(f"  [{row.get('id','?')}] {str(row.get('question_english',''))[:50]}")
                print(f"    Updates: {updates}")
                sample_count += 1
        return

    df.to_csv(SOURCE_CSV, index=False, encoding="utf-8", quoting=1)
    print(f"\n✓ Written → {SOURCE_CSV}")
    print(f"\nNext: python scripts/04_adapt_data.py --estimate")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--use-llm",  action="store_true",
                        help="Use LLM for rows where rule-based extraction fails")
    args = parser.parse_args()
    run(dry_run=args.dry_run, use_llm=args.use_llm)
