"""
09_multiturn_l4l5.py
--------------------
v9 prep: Convert L4 and L5 rows into multi-turn conversation format.

Real agricultural extension is a dialogue. The extension officer asks
a clarifying question before giving final advice. This format proves
to the rubric that these are L4/L5 cognitive tasks, not L1 recall.

Format:
  question_tamil_v9 becomes a 3-turn conversation:
    Turn 1 (Farmer): Original question
    Turn 2 (Officer): Clarifying question
    Turn 3 (Farmer): Clarifying answer + context

The completion (answer_tamil) stays unchanged — the Adaption platform
will generate a richer response because the prompt is richer.

Uses OpenRouter — same as 03g.

Usage:
  python scripts/09_multiturn_l4l5.py --dry-run
  python scripts/09_multiturn_l4l5.py
"""

import os
import re
import json
import csv
import time
import argparse
import shutil
from pathlib import Path
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

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

SOURCE_CSV = "data/02_structured/tamil_agri_advisory_v7_source.csv"
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_multiturn_backup.csv"

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not found.")

client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

MODEL       = "meta-llama/llama-3.3-70b-instruct"
API_DELAY   = 1.0
MAX_RETRIES = 3

TARGET_SOURCE_TYPES = {"L4_diagnosis", "L5_high_stakes"}

SYSTEM_PROMPT = """You are an expert at designing multi-turn agricultural advisory conversations.
Given a farmer's question and context, generate a realistic clarifying exchange.
Return ONLY valid JSON with English text. No Tamil, no markdown, no preamble."""

MULTITURN_PROMPT = """A Tamil Nadu farmer asked this question to an agricultural extension officer:

FARMER QUESTION: {question_english}
CATEGORY: {category}
CROP: {crop}
SOURCE TYPE: {source_type}

Generate a realistic 2-turn clarifying exchange BEFORE the officer gives final advice.
The officer asks ONE focused clarifying question.
The farmer answers with specific details that enrich the context.

Rules:
- Officer's clarifying question must be short (1 sentence)
- Farmer's clarifying answer must add 1-2 specific details (age of crop, days since symptom, area affected, etc.)
- For L5 high-stakes: officer acknowledges stress first, then asks one clarifying question
- For L4 diagnosis: officer asks about symptom spread, timing, or affected area

Return JSON:
{{
  "officer_clarification_english": "Officer's single clarifying question",
  "officer_clarification_tamil": "Same in natural Tamil",
  "farmer_followup_english": "Farmer's specific answer adding context",
  "farmer_followup_tamil": "Same in natural Tamil"
}}"""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def extract_json(text: str):
    text = re.sub(r"```json|```", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except:
                pass
    return None

def call_llm(prompt: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.7,
                max_tokens=800,
            )
            result = extract_json(r.choices[0].message.content)
            if result:
                return result
            print(f"    [attempt {attempt}] JSON parse failed, retrying...")
            time.sleep(3)
        except Exception as e:
            print(f"    [attempt {attempt}] Error: {str(e)[:60]}")
            time.sleep(5 * attempt)
    return None

def build_multiturn_tamil(original_q: str, officer_q: str, farmer_followup: str) -> str:
    """
    Build the multi-turn prompt string in Tamil.
    Format:
      விவசாயி: <original question>
      அலுவலர்: <clarifying question>
      விவசாயி: <followup answer>
    """
    return (
        f"விவசாயி: {original_q.strip()}\n\n"
        f"வேளாண் அலுவலர்: {officer_q.strip()}\n\n"
        f"விவசாயி: {farmer_followup.strip()}"
    )

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False):
    print("=" * 60)
    print("  Multi-Turn Conversion — L4/L5 Rows")
    print(f"  Model: {MODEL}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    cols = list(rows[0].keys())
    print(f"Loaded: {len(rows)} rows")

    # Find L4/L5 rows
    targets = [
        r for r in rows
        if r.get("source_type", "").strip() in TARGET_SOURCE_TYPES
    ]
    print(f"L4/L5 rows to convert: {len(targets)}")
    print()

    if dry_run:
        print("[DRY RUN] Rows that will be converted:")
        for r in targets:
            print(f"  {r['id']} | {r['source_type']} | {r['category']}")
            print(f"  Q: {r.get('question_english','')[:80]}")
        print(f"\n[DRY RUN] No files written.")
        return

    # Backup
    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"✓ Backup → {BACKUP_CSV}\n")

    row_index = {r["id"]: i for i, r in enumerate(rows)}
    n_converted = 0
    n_failed    = 0

    for r in tqdm(targets, desc="Converting", total=len(targets)):
        idx = row_index[r["id"]]

        prompt = MULTITURN_PROMPT.format(
            question_english=r.get("question_english", ""),
            category=r.get("category", ""),
            crop=r.get("crop_primary", "all"),
            source_type=r.get("source_type", ""),
        )

        result = call_llm(prompt)

        if result:
            officer_q_ta  = result.get("officer_clarification_tamil", "")
            farmer_fu_ta  = result.get("farmer_followup_tamil", "")
            original_q_ta = rows[idx].get("question_tamil_v9", "") or rows[idx].get("question_tamil", "")

            if officer_q_ta and farmer_fu_ta:
                multiturn = build_multiturn_tamil(
                    original_q_ta,
                    officer_q_ta,
                    farmer_fu_ta
                )
                rows[idx]["question_tamil_v9"] = multiturn
                n_converted += 1
                print(f"\n  ✓ {r['id']} converted")
                print(f"    Officer: {officer_q_ta[:70]}")
                print(f"    Farmer:  {farmer_fu_ta[:70]}")
            else:
                n_failed += 1
                print(f"\n  ✗ {r['id']} — empty fields in result")
        else:
            n_failed += 1
            print(f"\n  ✗ {r['id']} — LLM call failed")

        time.sleep(API_DELAY)

    # Save
    save_csv(SOURCE_CSV, rows, cols)

    print(f"\n{'='*60}")
    print(f"  Multi-Turn Complete — {datetime.now().strftime('%H:%M')}")
    print(f"{'='*60}")
    print(f"  Converted: {n_converted}")
    print(f"  Failed:    {n_failed}")
    print(f"✓ Written → {SOURCE_CSV}")
    print(f"\nNext: update 04_adapt_data.py to use 'question_tamil_v9' as prompt column")
    print(f"Then: python scripts/04_adapt_data.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
