"""
18_expand_and_merge.py
-----------------------
Takes the KCC gold rows (from 17_extract_kcc_gold.py) and:
  1. Expands their short Tamil answers to full 5-part structure via Cohere
  2. Merges with the v13 submission CSV (metadata-fixed 194 rows)
  3. Outputs a final combined submission CSV

The KCC answers are real farmer call center responses — authentic but short.
This script rewrites them to match the 5-part TNAU structure the rubric rewards,
while preserving all the original metadata that came with the KCC data.

Run from repo root:
    python scripts/18_expand_and_merge.py --dry-run         # preview what will happen
    python scripts/18_expand_and_merge.py --limit 5          # test on 5 rows
    python scripts/18_expand_and_merge.py                    # full run
"""

import os
import csv
import time
import json
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

try:
    import cohere
except ImportError:
    print("ERROR: cohere not installed. Run: pip install cohere")
    exit(1)

COHERE_API_KEY = os.environ.get("COHERE_API_KEY", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found in .env")

co = cohere.Client(COHERE_API_KEY, timeout=600)
MODEL = "command-r-plus-08-2024"
DELAY = 5  # seconds between calls

def llm_call(system_prompt, user_prompt, max_tokens=1500):
    """Call Cohere with retry."""
    for attempt in range(3):
        try:
            response = co.chat(
                model=MODEL,
                preamble=system_prompt,
                message=user_prompt,
                max_tokens=max_tokens,
            )
            text = response.text.strip()
            if text and len(text) > 50:
                return text, "cohere"
        except Exception as e:
            err = str(e)[:50]
            if attempt < 2:
                wait = 15 * (attempt + 1)
                print(f" [retry {attempt+1}, wait {wait}s: {err}]", end="")
                time.sleep(wait)
            else:
                print(f" [failed: {err}]", end="")
    return "", "none"

KCC_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "kcc_gold_extracted.csv"
V13_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_crop_advisory_v13_submission.csv"
OUTPUT  = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_v14_combined.csv"

SYSTEM_PROMPT = """You are a Tamil Nadu Agricultural University (TNAU) extension officer.
Rewrite the given short agricultural answer into a comprehensive 5-part Tamil advisory.

MANDATORY 5-PART STRUCTURE:

1. நிலைமையை அங்கீகரித்தல் (Acknowledge) — one sentence about the farmer's situation
2. உடனடி நடவடிக்கை (Immediate Action) — exact dosage, timing, cost in rupees
3. காரணம் (Rationale) — why this fits this specific crop/soil/season
4. நீண்டகால தடுப்பு (Prevention) — sustainable future practice
5. KVK பரிந்துரை — end with: "மேலும் விவரங்களுக்கு உங்கள் அருகிலுள்ள KVK அல்லது மாவட்ட வேளாண் அலுவலரை தொடர்பு கொள்ளுங்கள்."

RULES:
- Write in natural Tamil script
- Use Tamil agricultural terms: தழைச்சத்து, மணிச்சத்து, சாம்பல்சத்து
- Keep ALL factual content from the original answer
- Add practical details: quantities per acre, costs in rupees, timing
- Minimum 400 Tamil characters
- Output ONLY the Tamil text — no English, no preamble"""

EXPAND_PROMPT = """Expand this short KCC call center answer into a full 5-part Tamil advisory.

CONTEXT:
- Crop: {crop_primary}
- Region: {region}  
- Season: {season}
- Soil: {soil_type}
- Category: {category}

FARMER QUESTION:
{question}

SHORT ANSWER (from KCC — keep all facts, expand structure):
{answer}

Write the full 5-part Tamil advisory. Keep every fact from the original. Output ONLY Tamil text."""


def expand_answer(row):
    prompt = EXPAND_PROMPT.format(
        crop_primary=row.get("crop_primary", ""),
        region=row.get("region", ""),
        season=row.get("season", ""),
        soil_type=row.get("soil_type", ""),
        category=row.get("category", ""),
        question=row.get("question", "")[:200],
        answer=row.get("answer", "")[:500],
    )
    text, provider = llm_call(SYSTEM_PROMPT, prompt)
    return text


def run(dry_run=False, limit=None):
    # Load KCC rows
    if not KCC_CSV.exists():
        print(f"ERROR: {KCC_CSV} not found. Run 17_extract_kcc_gold.py first.")
        return
    
    with open(KCC_CSV, "r", encoding="utf-8") as f:
        kcc_rows = list(csv.DictReader(f))
    
    # Remove debug columns
    submission_cols = ["id", "question", "answer", "category", "crop_primary",
                       "soil_type", "irrigation_type", "farming_practice",
                       "growth_stage", "region", "season", "severity",
                       "source_type", "reasoning_type"]
    
    kcc_clean = []
    for r in kcc_rows:
        clean = {k: r.get(k, "") for k in submission_cols}
        kcc_clean.append(clean)
    
    if limit:
        kcc_clean = kcc_clean[:limit]
    
    print(f"KCC rows to expand: {len(kcc_clean)}")
    
    if dry_run:
        print("\n[DRY RUN] Sample rows:")
        for r in kcc_clean[:5]:
            print(f"  {r['id']} | {r['category']} | {r['crop_primary']} | ans_len={len(r['answer'])}")
            print(f"    Q: {r['question'][:80]}")
            print(f"    A: {r['answer'][:100]}")
            print()
        return
    
    # Expand via Cohere
    print(f"\nExpanding {len(kcc_clean)} answers via Groq/OpenRouter...")
    expanded = 0
    errors = 0
    
    for i, row in enumerate(kcc_clean, 1):
        print(f"  [{i}/{len(kcc_clean)}] {row['id']} | {row['crop_primary']}...", end="")
        
        result = expand_answer(row)
        if result and len(result) > 200:
            row["answer"] = result
            expanded += 1
            print(f" ✓ ({len(result)} chars)")
        else:
            errors += 1
            print(f" ✗ (kept original)")
        
        time.sleep(DELAY)  # 5s between calls
    
    print(f"\nExpanded: {expanded}, Errors: {errors}")
    
    # Load v13 base
    if V13_CSV.exists():
        with open(V13_CSV, "r", encoding="utf-8") as f:
            v13_rows = list(csv.DictReader(f))
        print(f"\nv13 base rows: {len(v13_rows)}")
        
        # Use v13 columns as the standard
        v13_cols = list(v13_rows[0].keys())
        
        # Align KCC rows to v13 columns
        for r in kcc_clean:
            for col in v13_cols:
                if col not in r:
                    r[col] = ""
        
        combined = v13_rows + kcc_clean
        print(f"Combined: {len(combined)} rows")
    else:
        print(f"\nWARNING: {V13_CSV} not found. Outputting KCC rows only.")
        combined = kcc_clean
        v13_cols = submission_cols
    
    # Save
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=v13_cols, quoting=csv.QUOTE_ALL, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(combined)
    
    print(f"\nSaved: {OUTPUT} ({len(combined)} rows)")
    
    # Quality check
    all_count = 0
    total = len(combined) * 5
    for r in combined:
        for f in ["region", "season", "soil_type", "irrigation_type", "growth_stage"]:
            if r.get(f, "").strip().lower() == "all":
                all_count += 1
    print(f"Metadata fill rate: {(total - all_count)/total*100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)
