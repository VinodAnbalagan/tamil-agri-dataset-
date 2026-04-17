"""
16_rewrite_completions.py
--------------------------
Rewrites answer_tamil_v10 to have a perfect 5-part TNAU structure.

This directly fixes the answer_completeness 2.8/5 weakness that has
kept the dataset at Grade B across all versions.

The 5-part structure (mandatory for every row):
  1. Acknowledge  — the farmer's specific situation in one sentence
  2. Immediate Action — exact dosage, timing, quantity, cost
  3. Rationale — why this fits the specific crop/soil/season/stage
  4. Prevention — long-term sustainable practice to avoid recurrence
  5. KVK Referral — direct the farmer to nearest KVK or district officer

Uses Claude claude-sonnet-4-6 via Anthropic API.
Cost: ~$0.50 for 222 rows.

Safe to re-run — skips rows where answer_tamil_v10_final is already filled.
Saves every 10 rows.

Usage:
  python scripts/16_rewrite_completions.py --dry-run
  python scripts/16_rewrite_completions.py --limit 5    # test on 5 rows first
  python scripts/16_rewrite_completions.py              # full run
"""

import os
import csv
import json
import time
import argparse
import shutil
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

try:
    import cohere
except ImportError:
    print("ERROR: anthropic not installed. Run: pip install anthropic")
    exit(1)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        total = kwargs.get('total', '?')
        for i, item in enumerate(iterable, 1):
            print(f"\r{i}/{total}", end="", flush=True)
            yield item
        print()

# ── CONFIG ───────────────────────────────────────────────────────────────────

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found.")

co = cohere.Client(COHERE_API_KEY, timeout=60)
MODEL = "command-r-plus-08-2024"
DELAY  = 0.5

# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert Tamil Nadu Agricultural University (TNAU) extension officer.
Your task is to rewrite agricultural advisory answers in perfect Tamil with a strict 5-part structure.

MANDATORY 5-PART STRUCTURE — every response must have all 5 parts:

1. நிலைமையை அங்கீகரித்தல் (Acknowledge)
   - One sentence acknowledging the farmer's specific situation
   - Must mention the specific crop, region, season, or growth stage from context

2. உடனடி நடவடிக்கை (Immediate Action)
   - Concrete recommendation with EXACT dosage, timing, and cost
   - Example: "ஒரு லிட்டர் தண்ணீரில் 2 மி.லி. வேப்ப எண்ணெய் கலந்து தெளிக்கவும்"
   - Must be actionable today

3. காரணம் (Rationale)
   - Why this recommendation fits THIS specific soil type, irrigation method, crop stage
   - Must reference the specific context from the metadata

4. நீண்டகால தடுப்பு (Prevention)
   - One sustainable practice to avoid this problem in the future
   - Specific to the crop and farming system

5. KVK பரிந்துரை (KVK Referral)
   - Must end with: "மேலும் விவரங்களுக்கு உங்கள் அருகிலுள்ள கிருஷி விஜ்ஞான கேந்திரா (KVK) அல்லது மாவட்ட வேளாண் அலுவலரை தொடர்பு கொள்ளுங்கள்."

RULES:
- Write in natural, fluent Tamil — NOT transliterated English
- Use proper Tamil agricultural terms: தழைச்சத்து (nitrogen), மணிச்சத்து (phosphorus), சாம்பல்சத்து (potassium)
- Keep all 5 parts clearly separated
- Output ONLY the Tamil answer — no English, no preamble, no explanation
- Minimum 400 Tamil characters
- For mental health/financial crisis rows: immediately provide Sneha: 044-24640050 and Kisan Call Centre: 1551"""

REWRITE_PROMPT = """Rewrite this agricultural advisory answer in perfect Tamil with the mandatory 5-part structure.

FARMER CONTEXT:
- Category: {category}
- Crop: {crop_primary}
- Region: {region}
- Season: {season}
- Growth Stage: {growth_stage}
- Soil Type: {soil_type}
- Irrigation: {irrigation_type}
- Weather: {weather_recent}
- Severity: {severity}
- Farm Scale: {farm_scale}
- Budget: {budget_constraint}

FARMER'S QUESTION (Tamil):
{question_tamil_v9}

EXISTING ANSWER (for reference — rewrite this with better structure):
{answer_tamil_v10}

ENGLISH ANSWER (for factual accuracy — use these facts):
{answer_english}

Rewrite the Tamil answer with all 5 mandatory parts. Keep all factual details, dosages, and recommendations from the English answer. Output ONLY the Tamil text."""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def rewrite_completion(row: dict) -> str:
    prompt = REWRITE_PROMPT.format(
        category        = row.get("category", ""),
        crop_primary    = row.get("crop_primary", ""),
        region          = row.get("region", ""),
        season          = row.get("season", ""),
        growth_stage    = row.get("growth_stage", ""),
        soil_type       = row.get("soil_type", ""),
        irrigation_type = row.get("irrigation_type", ""),
        weather_recent  = row.get("weather_recent", ""),
        severity        = row.get("severity", ""),
        farm_scale      = row.get("farm_scale", ""),
        budget_constraint = row.get("budget_constraint", ""),
        question_tamil_v9 = row.get("question_tamil_v9", "")[:200],
        answer_tamil_v10  = (row.get("answer_tamil_v10") or row.get("answer_tamil", ""))[:300],
        answer_english    = row.get("answer_english", "")[:300],
    )
    try:
        response = co.chat(
            model      = MODEL,
            preamble   = SYSTEM_PROMPT,
            message    = prompt,
            max_tokens = 1500,
        )
        result = response.text.strip()
        return result
    except Exception as e:
        print(f"\n  [error] {row.get('id')}: {str(e)[:60]}")
        time.sleep(3)
        return ""

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def needs_rewrite(row: dict) -> bool:
    # Skip if already has a final rewrite
    if row.get("answer_tamil_v10_final", "").strip():
        return False
    return True

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(filepath: str, dry_run: bool = False, limit: int = None):
    path = Path(filepath)
    print("=" * 60)
    print(f"  Completion Rewriter — Perfect 5-Part Structure")
    print(f"  Model: {MODEL}")
    print(f"  File: {path.name}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(path)
    cols = list(rows[0].keys())

    # Add final column
    if "answer_tamil_v10_final" not in cols:
        cols.append("answer_tamil_v10_final")
        for r in rows:
            r["answer_tamil_v10_final"] = ""

    to_rewrite = [r for r in rows if needs_rewrite(r)]
    if limit:
        to_rewrite = to_rewrite[:limit]

    print(f"Rows loaded:          {len(rows)}")
    print(f"Rows to rewrite:      {len(to_rewrite)}")
    print(f"Already done (skip):  {len(rows) - len(to_rewrite)}")

    # Cost estimate
    est_input_tokens  = len(to_rewrite) * 600
    est_output_tokens = len(to_rewrite) * 500
    est_cost_usd = (est_input_tokens * 3.0 + est_output_tokens * 15.0) / 1_000_000
    est_min = (len(to_rewrite) * (DELAY + 3)) / 60
    print(f"Estimated cost:       ~${est_cost_usd:.2f} USD")
    print(f"Estimated time:       ~{est_min:.0f} minutes\n")

    if dry_run:
        print("[DRY RUN] Sample rows that will be rewritten:")
        for r in to_rewrite[:5]:
            print(f"  {r['id']} | {r.get('category')} | {r.get('crop_primary')}")
            print(f"    Current answer length: {len(r.get('answer_tamil_v10', ''))}")
        print(f"\n[DRY RUN] No files written.")
        return

    confirm = input(f"Proceed with rewriting {len(to_rewrite)} rows? (y/n): ")
    if confirm.strip().lower() != "y":
        print("Aborted.")
        return

    # Backup
    backup = path.with_name(path.stem + "_pre_rewrite_backup.csv")
    shutil.copy2(path, backup)
    print(f"✓ Backup → {backup.name}\n")

    row_index = {r["id"]: i for i, r in enumerate(rows)}
    n_done   = 0
    n_errors = 0

    for row in tqdm(to_rewrite, desc="Rewriting", total=len(to_rewrite)):
        idx    = row_index[row["id"]]
        result = rewrite_completion(rows[idx])

        if result and len(result) > 100:
            rows[idx]["answer_tamil_v10_final"] = result
            n_done += 1
        else:
            # Fall back to existing answer
            rows[idx]["answer_tamil_v10_final"] = rows[idx].get("answer_tamil_v10", "")
            n_errors += 1

        time.sleep(DELAY)

        # Save every 10 rows
        if n_done > 0 and n_done % 10 == 0:
            save_csv(path, rows, cols)
            print(f"\n  [checkpoint] saved at {n_done} rewrites")

    # Fill any remaining rows
    for r in rows:
        if not r.get("answer_tamil_v10_final", "").strip():
            r["answer_tamil_v10_final"] = r.get("answer_tamil_v10", "")

    save_csv(path, rows, cols)

    # Stats
    lengths = [len(r.get("answer_tamil_v10_final","")) for r in rows if r.get("answer_tamil_v10_final","")]
    avg_len = sum(lengths) / len(lengths) if lengths else 0

    print(f"\n{'='*60}")
    print(f"  Rewrite Complete — {datetime.now().strftime('%H:%M')}")
    print(f"{'='*60}")
    print(f"  Rewritten:  {n_done}")
    print(f"  Errors:     {n_errors}")
    print(f"  Avg length: {avg_len:.0f} chars")
    print(f"✓ Saved → {path.name}")
    print(f"\nNew column: answer_tamil_v10_final")
    print(f"\nNext: Update 04_adapt_data.py to use 'answer_tamil_v10_final' as completion")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",     type=str,
                        default="data/02_structured/tamil_agri_advisory_v10_A_score_only.csv")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--limit",    type=int, default=None)
    args = parser.parse_args()
    run(filepath=args.file, dry_run=args.dry_run, limit=args.limit)
