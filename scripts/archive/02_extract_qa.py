"""
02_extract_qa.py
----------------
Reads scraped JSONs from data/01_raw/ and uses Cohere to extract
structured Q&A rows matching the v8 schema.

v8 schema additions:
  - farm_scale: marginal / small / medium / large
  - budget_constraint: zero-budget / low-cost / standard
  These two fields force contextualised, constraint-aware advice and
  feed into the Adaption context columns for reasoning traces.

Run from repo root:
    python scripts/02_extract_qa.py                    # process all
    python scripts/02_extract_qa.py --slug rice_disease_blast
    python scripts/02_extract_qa.py --category crop_disease
    python scripts/02_extract_qa.py --dry-run
    python scripts/02_extract_qa.py --retry-failed
"""

import os
import json
import csv
import time
import argparse
import re
from datetime import datetime
from pathlib import Path

import cohere
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found. Check your .env file.")

co = cohere.ClientV2(api_key=COHERE_API_KEY)

RAW_DIR       = Path(__file__).parent.parent / "data" / "01_raw"
OUTPUT_DIR    = Path(__file__).parent.parent / "data" / "02_structured"
SOURCES_FILE  = Path(__file__).parent.parent / "sources_and_mappings.csv"
OUTPUT_FILE   = OUTPUT_DIR / "extracted_qa_master.csv"
PROGRESS_FILE = OUTPUT_DIR / "progress.json"

API_DELAY_SECONDS = 4
MAX_RETRIES       = 3
RETRY_BACKOFF     = 8
MAX_TEXT_CHARS    = 5000
MIN_CHARS         = 400

# ---------------------------------------------------------------------------
# Schema — v8 adds farm_scale and budget_constraint
# ---------------------------------------------------------------------------

SCHEMA_COLUMNS = [
    "id", "question_tamil", "question_tanglish", "question_english",
    "answer_tamil", "answer_english",
    "category", "crop_primary", "crop_companions", "cropping_system",
    "soil_type", "irrigation_type", "farming_practice",
    "region", "season", "growth_stage", "weather_recent",
    "severity", "source_type",
    "farm_scale",        # NEW: marginal / small / medium / large
    "budget_constraint", # NEW: zero-budget / low-cost / standard
    "_source_url", "_source_slug",
]

# ---------------------------------------------------------------------------
# Prompt — v8 adds farm_scale + budget_constraint to schema and examples
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert Tamil Nadu agricultural extension officer and dataset builder.
Your job is to read raw text scraped from the TNAU (Tamil Nadu Agricultural University) Agritech portal
and convert it into structured Q&A rows for a Tamil agricultural advisory dataset.

RULES:
1. Extract ONLY facts explicitly stated in the source text. Never hallucinate doses, timings, or product names.
2. Write questions as a real Tamil Nadu smallholder farmer would ask — specific, worried, practical.
3. Write answers as a TNAU extension officer — warm, clear, actionable, grounded in the source.
4. Generate DIVERSE intent types: diagnosis, prevention, contrastive, negative_space.
5. Make metadata SPECIFIC — never use 'all' when the source gives a real value.
6. Tamil must be natural Tamil script — not transliteration or Tanglish.

INTENT TYPES:
- diagnosis: farmer describes a symptom, asks what it is and what to do
- prevention: farmer asks how to prevent a problem before it occurs
- contrastive: same crop question in two different growth stages — different answers
- negative_space: correct answer is to wait, not act, or consult KVK

FARM SCALE VALUES:
- marginal: less than 1 acre — recommend manual, low-cost, zero-input solutions
- small: 1-2 acres — balance between cost and effectiveness
- medium: 2-10 acres — standard commercial recommendations appropriate
- large: more than 10 acres — mechanised solutions, bulk inputs appropriate
- all: scale-independent advice (schemes, disease ID, etc.)

BUDGET CONSTRAINT VALUES:
- zero-budget: farmer cannot buy commercial inputs — recommend Panchagavya, NSKE, Jeevamrutham, farm-made solutions only
- low-cost: farmer can afford basic inputs but not premium products
- standard: no budget constraint mentioned — standard TNAU recommendations
- all: budget-independent advice

CRITICAL METADATA RULES:
- source says 'Samba season' → season='samba'
- source says 'delta region' or 'Thanjavur' → region='delta'
- source says 'flowering stage' → growth_stage='flowering'
- source says 'humid conditions' → weather_recent='humid'
- Only use 'all' when content genuinely applies across all contexts
- negative_space rows: severity='low' or 'medium', answer advises patience/KVK
- urgent pest/disease rows: severity='urgent', answer includes specific action
- zero-budget rows: NEVER recommend commercial pesticides or fertilizers

OUTPUT FORMAT: Return ONLY a raw JSON array. No markdown. No preamble. No explanation.
Each object must have exactly these fields:
question_english, question_tanglish, question_tamil,
answer_english, answer_tamil,
category, crop_primary, crop_companions, cropping_system,
soil_type, irrigation_type, farming_practice,
region, season, growth_stage, weather_recent,
severity, source_type, intent_type,
farm_scale, budget_constraint"""


def build_user_prompt(raw_json: dict) -> str:
    slug        = raw_json["slug"]
    category    = raw_json["category"]
    crop        = raw_json["crop_primary"]
    region_hint = raw_json.get("region_hint", "all")
    season_hint = raw_json.get("season_hint", "all")
    description = raw_json["description"]
    source_url  = raw_json["source_url"]
    raw_text    = raw_json["raw_text"]

    if len(raw_text) > MAX_TEXT_CHARS:
        raw_text = raw_text[:MAX_TEXT_CHARS] + "\n[truncated]"

    return f"""SOURCE: {slug}
Category: {category} | Crop: {crop} | Region: {region_hint} | Season: {season_hint}
Description: {description}
URL: {source_url}

TEXT:
{raw_text}

TASK: Generate 2-3 Q&A rows from this text.
- At least one 'diagnosis' intent
- At least one 'prevention' or 'negative_space' intent
- Try to vary farm_scale and budget_constraint across rows — not all 'all'
- Use region='{region_hint}' and season='{season_hint}' as defaults unless text gives something more specific
- Every row must be traceable to a fact in the text above
- Return ONLY the raw JSON array, nothing else."""


# ---------------------------------------------------------------------------
# JSON repair
# ---------------------------------------------------------------------------

def repair_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        text = "\n".join(lines).strip()
    text = re.sub(r",\s*(\]|\})", r"\1", text)
    if not text.rstrip().endswith("]"):
        last_brace = text.rfind("}")
        if last_brace != -1:
            text = text[:last_brace + 1] + "\n]"
    return text


# ---------------------------------------------------------------------------
# Cohere call with retry
# ---------------------------------------------------------------------------

def call_cohere(user_prompt: str, slug: str) -> list[dict]:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = co.chat(
                model="command-r-plus-08-2024",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
            )
            raw_text = response.message.content[0].text.strip()
            repaired = repair_json(raw_text)
            rows = json.loads(repaired)
            if not isinstance(rows, list):
                raise ValueError(f"Expected JSON array, got {type(rows)}")
            return rows
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                print(f"         attempt {attempt} failed ({str(e)[:60]}) — retrying in {wait}s")
                time.sleep(wait)
    raise last_error


# ---------------------------------------------------------------------------
# Progress tracker
# ---------------------------------------------------------------------------

def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"done": [], "failed": [], "last_id": 195}


def save_progress(progress: dict) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)


# ---------------------------------------------------------------------------
# Sources log
# ---------------------------------------------------------------------------

def append_source(source_url, slug, category, crop, description, row_ids):
    file_exists = SOURCES_FILE.exists()
    with open(SOURCES_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "source", "url", "type", "what_it_grounded", "rows_using_this"
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "source": description,
            "url": source_url,
            "type": "Extension Portal",
            "what_it_grounded": f"{category} — {crop}",
            "rows_using_this": ", ".join(row_ids),
        })


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def append_rows_to_csv(rows: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = OUTPUT_FILE.exists()
    all_keys = SCHEMA_COLUMNS + [k for k in rows[0] if k not in SCHEMA_COLUMNS]
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# ID generator
# ---------------------------------------------------------------------------

def next_id(last_num: int) -> tuple[str, int]:
    new_num = last_num + 1
    return f"tn-agri-{new_num:03d}", new_num


# ---------------------------------------------------------------------------
# File loader
# ---------------------------------------------------------------------------

SKIP_SLUGS = {
    "fertilizer_banana", "fertilizer_chilli", "fertilizer_tomato",
    "fertilizer_banana_subpage", "fertilizer_chilli_subpage",
    "fertilizer_tomato_subpage", "district_contingency_plan",
    "district_contingency_subpage", "coconut_marketing",
    "coconut_marketing_subpage", "paddy_direct_seeding",
    "tnau_paddy_expert_system", "tnau_rice_diseases",
    "banana_irrigation", "sugarcane_irrigation",
}


def load_raw_files(slug_filter=None, category_filter=None) -> list[dict]:
    results = []
    for f in sorted(RAW_DIR.glob("*.json")):
        if f.name == ".gitkeep":
            continue
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
        if data.get("slug") in SKIP_SLUGS:
            continue
        if slug_filter and data.get("slug") != slug_filter:
            continue
        if category_filter and data.get("category") != category_filter:
            continue
        if data.get("char_count", 0) < MIN_CHARS:
            print(f"  SKIP {data['slug']} — {data.get('char_count',0)} chars")
            continue
        results.append(data)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_all(slug_filter=None, category_filter=None,
                dry_run=False, retry_failed=False) -> None:

    raw_files = load_raw_files(slug_filter, category_filter)
    if not raw_files:
        print("No files to process.")
        return

    progress = load_progress()

    if retry_failed:
        raw_files = [f for f in raw_files if f["slug"] in progress["failed"]]
        progress["failed"] = []
        print(f"Retrying {len(raw_files)} previously failed slugs\n")

    if not slug_filter:
        raw_files = [f for f in raw_files if f["slug"] not in progress["done"]]

    if not raw_files:
        print("All files already processed. Use --retry-failed to retry errors.")
        return

    print(f"\n{'='*65}")
    print(f"  Extract Q&A v8 — {len(raw_files)} source files")
    print(f"  Schema: + farm_scale, budget_constraint")
    print(f"  Model: command-r-plus-08-2024")
    print(f"{'='*65}\n")

    if dry_run:
        for d in raw_files:
            print(f"  [{d['category']:<20}] {d['slug']}  ({d['char_count']:,} chars)")
        print(f"\nTotal: {len(raw_files)} files → est. {len(raw_files)*3} rows")
        return

    for i, raw_data in enumerate(raw_files, 1):
        slug     = raw_data["slug"]
        category = raw_data["category"]

        print(f"[{i:02d}/{len(raw_files)}] {slug}  ({raw_data['char_count']:,} chars)")

        try:
            rows = call_cohere(build_user_prompt(raw_data), slug)
        except Exception as e:
            print(f"         FAILED — {str(e)[:80]}")
            progress["failed"].append(slug)
            save_progress(progress)
            time.sleep(API_DELAY_SECONDS)
            continue

        row_ids = []
        for row in rows:
            row_id, progress["last_id"] = next_id(progress["last_id"])
            row_ids.append(row_id)
            row["id"]           = row_id
            row["_source_url"]  = raw_data["source_url"]
            row["_source_slug"] = slug

        append_rows_to_csv(rows)
        append_source(raw_data["source_url"], slug, category,
                      raw_data["crop_primary"], raw_data["description"], row_ids)

        progress["done"].append(slug)
        save_progress(progress)

        print(f"         → {len(rows)} rows  (IDs: {row_ids[0]}–{row_ids[-1]})  ✓")
        time.sleep(API_DELAY_SECONDS)

    print(f"\n{'='*65}\n  DONE\n{'='*65}")
    print(f"  Output      : {OUTPUT_FILE}")
    print(f"  Done        : {len(progress['done'])}")
    print(f"  Failed      : {len(progress['failed'])}")
    if progress["failed"]:
        for s in progress["failed"]:
            print(f"    - {s}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",         type=str, default=None)
    parser.add_argument("--category",     type=str, default=None)
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()

    process_all(
        slug_filter     = args.slug,
        category_filter = args.category,
        dry_run         = args.dry_run,
        retry_failed    = args.retry_failed,
    )
