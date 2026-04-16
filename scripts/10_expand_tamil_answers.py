"""
10_expand_tamil_answers.py
--------------------------
v10 prep: Recast answer_english (already expanded by 03d, 200-300 words)
into rich, native Tamil extension-officer phrasing.

Safe to re-run — skips rows where answer_tamil_v10 already filled.
Checkpoints every 25 rows.

Uses Cohere command-r-plus-08-2024.

Usage:
  python scripts/10_expand_tamil_answers.py --dry-run
  python scripts/10_expand_tamil_answers.py
  python scripts/10_expand_tamil_answers.py --limit 50
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
        for i, item in enumerate(iterable, 1):
            print(f"\r{i}/{total}", end="", flush=True)
            yield item
        print()

# ── CONFIG ───────────────────────────────────────────────────────────────────

SOURCE_CSV = "data/02_structured/tamil_agri_advisory_v7_source.csv"
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_v10_backup.csv"

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found.")

# Use httpx timeout — Cohere client supports this
co = cohere.Client(COHERE_API_KEY, timeout=30)  # 30 second hard timeout
MODEL     = "command-r-plus-08-2024"
DELAY_SEC = 0.5   # faster now that timeouts are short

# Truncate long answers before sending — prevents timeouts on huge texts
MAX_ENGLISH_CHARS = 800

# Only recast if English is this much longer than Tamil
MIN_GAP_CHARS = 200

# ── PROMPT ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert Tamil Agricultural Extension Officer and a professional translator.
I will give you a detailed agricultural advisory answer in English.
Your task is to recast it into natural, fluent, and highly professional Tamil.

RULES:
1. DO NOT translate word-for-word. Use native Tamil sentence structures.
2. Use proper Tamil agricultural terminology where appropriate:
   - nitrogen → தழைச்சத்து
   - phosphorus → மணிச்சத்து
   - potassium → சாம்பல்சத்து
   - Keep familiar chemical/brand names as-is if farmers use them
3. Maintain all formatting, bullet points, and exact factual details (dosages, days, quantities).
4. The tone must be warm, authoritative, and helpful — like a TNAU officer speaking to a farmer.
5. Output ONLY the Tamil text. No English preamble, no quotes, no explanations."""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def recast(text: str) -> str:
    if not text or not text.strip():
        return ""
    # Truncate to avoid timeout on very long texts
    text = text.strip()[:MAX_ENGLISH_CHARS]
    try:
        response = co.chat(
            model=MODEL,
            preamble=SYSTEM_PROMPT,
            message=text,
        )
        return response.text.strip()
    except Exception as e:
        print(f"\n  [recast error] {str(e)[:60]}")
        time.sleep(2)  # short backoff, not 5 seconds
        return ""

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def needs_recast(row: dict) -> bool:
    en = row.get("answer_english", "").strip()
    ta = row.get("answer_tamil", "").strip()
    en_len = len(en)
    ta_len = len(ta)

    # Already has a rich v10 answer — skip
    if row.get("answer_tamil_v10", "").strip():
        return False

    # Empty Tamil
    if ta_len < 10 and en_len > 50:
        return True

    # English significantly longer than Tamil
    if en_len - ta_len > MIN_GAP_CHARS:
        return True

    return False

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False, limit=None):
    print("=" * 60)
    print("  Tamil Answer Expansion — v10 prep")
    print(f"  Model: {MODEL} | Timeout: 30s | Max chars: {MAX_ENGLISH_CHARS}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    cols = list(rows[0].keys())
    print(f"Loaded: {len(rows)} rows")

    if "answer_tamil_v10" not in cols:
        cols.append("answer_tamil_v10")
        for r in rows:
            r["answer_tamil_v10"] = ""

    to_recast = [r for r in rows if needs_recast(r)]
    if limit:
        to_recast = to_recast[:limit]

    print(f"Rows needing recast: {len(to_recast)}")
    print(f"Rows already done (skip): {len(rows) - len(to_recast)}")
    est_min = (len(to_recast) * (DELAY_SEC + 5)) / 60
    print(f"Estimated time: ~{est_min:.0f} minutes\n")

    if dry_run:
        print("[DRY RUN] Sample rows:")
        for r in to_recast[:5]:
            en_len = len(r.get("answer_english", ""))
            ta_len = len(r.get("answer_tamil", ""))
            print(f"  {r['id']} | EN={en_len} TA={ta_len} gap={en_len-ta_len}")
        print(f"\n[DRY RUN] No files written.")
        return

    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"✓ Backup → {BACKUP_CSV}")

    row_index = {r["id"]: i for i, r in enumerate(rows)}
    n_recast = 0
    n_errors = 0

    for row in tqdm(to_recast, desc="Recasting", total=len(to_recast)):
        idx = row_index[row["id"]]
        answer_en = rows[idx].get("answer_english", "").strip()

        result = recast(answer_en)
        if result:
            rows[idx]["answer_tamil_v10"] = result
            n_recast += 1
        else:
            rows[idx]["answer_tamil_v10"] = rows[idx].get("answer_tamil", "")
            n_errors += 1

        time.sleep(DELAY_SEC)

        if n_recast > 0 and n_recast % 25 == 0:
            save_csv(SOURCE_CSV, rows, cols)
            print(f"\n  [checkpoint] saved at {n_recast} recasts")

    # Fill remaining rows with original answer_tamil
    for r in rows:
        if not r.get("answer_tamil_v10", "").strip():
            r["answer_tamil_v10"] = r.get("answer_tamil", "")

    save_csv(SOURCE_CSV, rows, cols)

    print(f"\n{'='*60}")
    print(f"  Recast Complete — {datetime.now().strftime('%H:%M')}")
    print(f"{'='*60}")
    print(f"  Recast:  {n_recast}")
    print(f"  Errors:  {n_errors}")
    print(f"✓ Written → {SOURCE_CSV}")
    print(f"\nNext: python scripts/12_quality_filter.py --dry-run")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)
