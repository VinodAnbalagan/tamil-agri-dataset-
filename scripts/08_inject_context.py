"""
08_inject_context.py
--------------------
v9 prep: Inject metadata context directly into question_tamil prompt.

Transforms terse KCC-style questions like:
  "பருத்தி இலைகளில் மஞ்சள் புள்ளிகள்"

Into context-rich prompts like:
  "[பருத்தி | காரீப் பருவம் | பூக்கும் நிலை | வறண்ட வானிலை]
   என் பருத்தி இலைகளில் மஞ்சள் புள்ளிகள் உள்ளன. என்ன செய்வது?"

Rules:
- Only inject context if the question_tamil is short (<= 15 words)
- Never modify rows that already have rich questions
- Never touch question_english or answer fields
- Writes to a NEW column: question_tamil_v9 (keeps original intact)
- Safe to re-run

Usage:
  python scripts/08_inject_context.py --dry-run   # preview only
  python scripts/08_inject_context.py             # write output
  python scripts/08_inject_context.py --limit 20  # test on 20 rows
"""

import os
import csv
import argparse
import shutil
from datetime import datetime
from pathlib import Path
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
BACKUP_CSV = "data/02_structured/tamil_agri_advisory_v7_source_pre_v9_backup.csv"

# Only inject context for short questions (terse KCC style)
MAX_WORDS_FOR_INJECTION = 15

# ── TAMIL TRANSLATIONS FOR METADATA VALUES ───────────────────────────────────

CATEGORY_TAMIL = {
    "pest_control":       "பூச்சி மேலாண்மை",
    "crop_disease":       "பயிர் நோய்",
    "fertilizer":         "உரமிடல்",
    "irrigation":         "நீர்ப்பாசனம்",
    "crop_management":    "பயிர் மேலாண்மை",
    "variety_selection":  "ரக தேர்வு",
    "weather_advisory":   "வானிலை ஆலோசனை",
    "government_schemes": "அரசு திட்டங்கள்",
    "livestock_dairy":    "கால்நடை",
    "livestock_advisory": "கால்நடை ஆலோசனை",
    "post_harvest":       "அறுவடைக்கு பின்",
    "soil_health":        "மண் ஆரோக்கியம்",
    "market_price":       "சந்தை விலை",
    "financial_support":  "நிதி உதவி",
    "harvest_timing":     "அறுவடை நேரம்",
    "aquaculture":        "மீன்வளம்",
    "sericulture":        "பட்டுவளர்ப்பு",
    "floriculture":       "பூக்கள் சாகுபடி",
    "general_advisory":   "பொது ஆலோசனை",
    "women_agriculture":  "பெண் விவசாயம்",
    "mental_health_safety": "மன நலம்",
}

SEASON_TAMIL = {
    "kharif":  "காரீப் பருவம்",
    "rabi":    "ரபி பருவம்",
    "samba":   "சம்பா பருவம்",
    "kuruvai": "குறுவை பருவம்",
    "summer":  "கோடை பருவம்",
    "all":     "",  # don't inject "all"
}

GROWTH_STAGE_TAMIL = {
    "germination":        "முளைப்பு நிலை",
    "vegetative":         "வளர்ச்சி நிலை",
    "tillering":          "துவர்ப்பு நிலை",
    "flowering":          "பூக்கும் நிலை",
    "fruiting":           "காய்க்கும் நிலை",
    "pod_filling":        "காய் நிரப்பு நிலை",
    "boll_development":   "காய் வளர்ச்சி நிலை",
    "panicle_initiation": "கதிர் உருவாக்கம்",
    "grand_growth":       "முழு வளர்ச்சி நிலை",
    "maturity":           "முதிர்ச்சி நிலை",
    "harvest":            "அறுவடை நிலை",
    "all":                "",
}

WEATHER_TAMIL = {
    "dry":    "வறண்ட வானிலை",
    "humid":  "ஈரப்பதமான வானிலை",
    "rainy":  "மழைக்காலம்",
    "flood":  "வெள்ளம்",
    "drought":"வறட்சி",
    "all":    "",
}

SEVERITY_TAMIL = {
    "urgent": "அவசரம்",
    "high":   "அதிக தீவிரம்",
    "medium": "",  # don't inject medium — it's noise
    "low":    "",
}

# ── HELPERS ──────────────────────────────────────────────────────────────────

def word_count(text: str) -> int:
    if not text:
        return 0
    return len(text.split())

def build_context_tag(row: dict) -> str:
    """Build a Tamil context tag from metadata fields."""
    parts = []

    # Crop
    crop = row.get("crop_primary", "").strip()
    if crop and crop.lower() not in ("all", "unknown", "none", "others", "0", "9999"):
        parts.append(crop)

    # Season
    season = row.get("season", "").strip().lower()
    season_ta = SEASON_TAMIL.get(season, "")
    if season_ta:
        parts.append(season_ta)

    # Growth stage
    stage = row.get("growth_stage", "").strip().lower()
    stage_ta = GROWTH_STAGE_TAMIL.get(stage, "")
    if stage_ta:
        parts.append(stage_ta)

    # Weather
    weather = row.get("weather_recent", "").strip().lower()
    weather_ta = WEATHER_TAMIL.get(weather, "")
    if weather_ta:
        parts.append(weather_ta)

    # Severity (only urgent/high)
    severity = row.get("severity", "").strip().lower()
    severity_ta = SEVERITY_TAMIL.get(severity, "")
    if severity_ta:
        parts.append(severity_ta)

    if not parts:
        return ""

    return "[" + " | ".join(parts) + "]"

def inject_context(question_tamil: str, context_tag: str) -> str:
    """Prepend context tag to question."""
    if not context_tag:
        return question_tamil
    if not question_tamil or not question_tamil.strip():
        return question_tamil
    return f"{context_tag}\n{question_tamil.strip()}"

def needs_injection(row: dict) -> bool:
    """Return True if this row's question is short enough to benefit from injection."""
    q = row.get("question_tamil", "").strip()
    if not q:
        return False
    # Already has context tag
    if q.startswith("["):
        return False
    # Already rich enough
    if word_count(q) > MAX_WORDS_FOR_INJECTION:
        return False
    return True

# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False, limit=None):
    print("=" * 60)
    print("  v9 Context Injection")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print(f"  Threshold: inject if question <= {MAX_WORDS_FOR_INJECTION} words")
    print("=" * 60)

    with open(SOURCE_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    cols = list(rows[0].keys())

    print(f"Loaded: {len(rows)} rows")

    # Add v9 column if not present
    if "question_tamil_v9" not in cols:
        cols.append("question_tamil_v9")

    to_inject = [r for r in rows if needs_injection(r)]
    if limit:
        to_inject = to_inject[:limit]

    print(f"Rows needing injection: {len(to_inject)}")
    print(f"Rows already rich (skip): {len(rows) - len(to_inject)}")

    if dry_run:
        print("\n[DRY RUN] Sample injections:")
        for r in to_inject[:8]:
            tag = build_context_tag(r)
            q_orig = r.get("question_tamil", "")[:60]
            q_new = inject_context(r.get("question_tamil", ""), tag)[:100]
            print(f"\n  ID: {r['id']} | words: {word_count(r.get('question_tamil',''))}")
            print(f"  Category: {r.get('category')} | Season: {r.get('season')} | Stage: {r.get('growth_stage')}")
            print(f"  BEFORE: {q_orig}")
            print(f"  TAG:    {tag}")
            print(f"  AFTER:  {q_new}")
        print(f"\n[DRY RUN] No files written.")
        return

    # Backup
    shutil.copy2(SOURCE_CSV, BACKUP_CSV)
    print(f"\n✓ Backup → {BACKUP_CSV}")

    # Build index
    row_index = {r["id"]: i for i, r in enumerate(rows)}

    n_injected = 0
    n_skipped  = 0

    for r in tqdm(to_inject, desc="Injecting", total=len(to_inject)):
        idx = row_index[r["id"]]
        tag = build_context_tag(rows[idx])
        q_orig = rows[idx].get("question_tamil", "")

        if tag:
            rows[idx]["question_tamil_v9"] = inject_context(q_orig, tag)
            n_injected += 1
        else:
            rows[idx]["question_tamil_v9"] = q_orig
            n_skipped += 1

    # For rows that didn't need injection, copy original to v9
    for r in rows:
        if "question_tamil_v9" not in r or not r["question_tamil_v9"]:
            r["question_tamil_v9"] = r.get("question_tamil", "")

    # Write
    with open(SOURCE_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'='*60}")
    print(f"  Context Injection Complete — {datetime.now().strftime('%H:%M')}")
    print(f"{'='*60}")
    print(f"  Injected:  {n_injected}")
    print(f"  No tag:    {n_skipped}")
    print(f"  Unchanged: {len(rows) - len(to_inject)}")
    print(f"✓ Written → {SOURCE_CSV}")
    print(f"\nNew column: question_tamil_v9")
    print(f"For v9 submission: set prompt column to 'question_tamil_v9' in 04_adapt_data.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)
