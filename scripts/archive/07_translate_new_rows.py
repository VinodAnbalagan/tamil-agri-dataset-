"""
07_translate_new_rows.py
------------------------
Translate question_english and answer_english to Tamil for rows
where question_tamil or answer_tamil are blank.

Uses Cohere command-r-plus — same as original pipeline.
Only processes rows with missing Tamil — safe to re-run.

Usage:
  python scripts/07_translate_new_rows.py --dry-run   # preview count only
  python scripts/07_translate_new_rows.py             # translate and save
  python scripts/07_translate_new_rows.py --limit 10  # test on 10 rows
"""

import os
import csv
import time
import argparse
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

try:
    import cohere
except ImportError:
    print("ERROR: cohere not installed. Run: uv pip install cohere")
    exit(1)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        total = kwargs.get('total', '?')
        desc  = kwargs.get('desc', '')
        for i, item in enumerate(iterable, 1):
            print(f"\r{desc} {i}/{total}", end="", flush=True)
            yield item
        print()

# ── CONFIG ───────────────────────────────────────────────────────────────────

SOURCE_CSV = "data/02_structured/tamil_agri_advisory_v7_source.csv"
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_translation_backup.csv"

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found. Add it to your .env file.")

co        = cohere.Client(COHERE_API_KEY)
MODEL     = "command-r-plus-08-2024"
DELAY_SEC = 1.0

SYSTEM_PROMPT = """You are an expert Tamil agricultural translator.
Translate the given English agricultural text into natural, clear Tamil script.
- Use standard Tamil script (not Tanglish)
- Keep technical terms like crop names, chemical names, dosages in their common Tamil usage
- Keep numbers, units (ml, kg, gm, acres) as-is
- Output ONLY the Tamil translation, nothing else
- No explanations, no English, no quotes"""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def translate(text: str) -> str:
    if not text or not text.strip():
        return ""
    try:
        response = co.chat(
            model=MODEL,
            preamble=SYSTEM_PROMPT,
            message=f"Translate to Tamil:\n{text.strip()}",
        )
        return response.text.strip()
    except Exception as e:
        print(f"\n  [translate error] {e}")
        time.sleep(5)
        return ""

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def needs_translation(row: dict) -> bool:
    q_tamil = row.get("question_tamil", "").strip()
    a_tamil = row.get("answer_tamil", "").strip()
    q_en    = row.get("question_english", "").strip()
    a_en    = row.get("answer_english", "").strip()
    return (not q_tamil and q_en) or (not a_tamil and a_en)

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False, limit=None):
    print("=" * 60)
    print("  Tamil Translation Pass")
    print(f"  Model: {MODEL}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    cols = list(rows[0].keys())
    print(f"Loaded: {len(rows)} rows")

    to_translate = [r for r in rows if needs_translation(r)]
    if limit:
        to_translate = to_translate[:limit]

    print(f"Rows needing translation: {len(to_translate)}")
    est_min = (len(to_translate) * DELAY_SEC * 2) / 60
    print(f"Estimated time: ~{est_min:.1f} minutes\n")

    if dry_run:
        print("[DRY RUN] Sample rows that need translation:")
        for r in to_translate[:5]:
            print(f"  {r['id']} | q_tamil={bool(r.get('question_tamil','').strip())} | a_tamil={bool(r.get('answer_tamil','').strip())}")
            print(f"    Q: {r.get('question_english','')[:80]}")
        print(f"\n[DRY RUN] No files written.")
        return

    # Backup
    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"✓ Backup → {BACKUP_CSV}")

    row_index = {r["id"]: i for i, r in enumerate(rows)}
    n_translated = 0
    n_errors     = 0

    for row in tqdm(to_translate, desc="Translating", total=len(to_translate)):
        idx = row_index[row["id"]]

        q_en = rows[idx].get("question_english", "").strip()
        a_en = rows[idx].get("answer_english", "").strip()
        q_ta = rows[idx].get("question_tamil", "").strip()
        a_ta = rows[idx].get("answer_tamil", "").strip()

        if not q_ta and q_en:
            translated_q = translate(q_en)
            if translated_q:
                rows[idx]["question_tamil"] = translated_q
                n_translated += 1
            else:
                n_errors += 1
            time.sleep(DELAY_SEC)

        if not a_ta and a_en:
            translated_a = translate(a_en)
            if translated_a:
                rows[idx]["answer_tamil"] = translated_a
                n_translated += 1
            else:
                n_errors += 1
            time.sleep(DELAY_SEC)

        # Save every 50 translations
        if n_translated > 0 and n_translated % 50 == 0:
            save_csv(SOURCE_CSV, rows, cols)
            print(f"\n  [checkpoint] saved at {n_translated} translations")

    # Final save
    save_csv(SOURCE_CSV, rows, cols)

    print(f"\n{'='*60}")
    print(f"  Translation Complete — {datetime.now().strftime('%H:%M')}")
    print(f"{'='*60}")
    print(f"  Translated: {n_translated}")
    print(f"  Errors:     {n_errors}")
    print(f"✓ Written → {SOURCE_CSV}")
    print(f"\nNext: python scripts/03g_add_structural_diversity.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)
