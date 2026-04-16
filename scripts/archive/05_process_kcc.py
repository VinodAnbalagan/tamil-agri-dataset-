"""
05_process_kcc.py
-----------------
Clean, filter, and extract high-value rows from KCC (Kisan Call Centre)
Tamil Nadu call logs for use in the Tamil Agricultural Advisory Dataset.

Output:
  data/02_structured/kcc_extracted_clean.csv   — filtered, clean Q&A rows
  data/02_structured/kcc_extraction_report.txt — summary stats

Usage:
  python scripts/05_process_kcc.py
  python scripts/05_process_kcc.py --dry-run     # stats only, no file written
  python scripts/05_process_kcc.py --limit 500   # cap rows for testing
"""

import os
import re
import csv
import glob
import argparse
import unicodedata
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────

KCC_DIR   = "data/01_raw_manual/kcc"
OUT_CSV   = "data/02_structured/kcc_extracted_clean.csv"
OUT_REPORT= "data/02_structured/kcc_extraction_report.txt"

# ── FILTER RULES ────────────────────────────────────────────────────────────

# DROP rows where answer matches these patterns — useless referrals
REFERRAL_PATTERNS = [
    r"contact.*\d{5,}",          # phone numbers
    r"contact.*department",
    r"contact.*office",
    r"contact.*director",
    r"contact.*officer",
    r"contact.*joint",
    r"contact.*number",
    r"\d{10}",                   # raw phone numbers
    r"^\s*recommended for contact",
    r"^\s*contact to ",
]

# DROP rows where query matches — not useful
JUNK_QUERY_PATTERNS = [
    r"weather (detail|report|today)",
    r"market rate",              # market_price rows — too volatile/stale
    r"rainfall (detail|today)",
    r"rain detail",
    r"have.*rain",
    r"no rain",
]

# DROP these sectors entirely
DROP_SECTORS = {"9999", "FISHERIES"}

# DROP query types that are just lookups
DROP_QUERY_TYPES = {
    "Market Information",
    "Weather",
    "9999",
    "General",
}

# KEEP only these sectors (empty = keep all not in DROP_SECTORS)
KEEP_SECTORS = {"AGRICULTURE", "HORTICULTURE", "ANIMAL HUSBANDRY"}

# Minimum answer length (characters) — too short = useless
MIN_ANS_LEN = 30

# Maximum answer length — cap runaway answers
MAX_ANS_LEN = 2000

# ── CATEGORY MAPPING ─────────────────────────────────────────────────────────
# Map KCC QueryType → our dataset category

QUERY_TYPE_TO_CATEGORY = {
    "plant protection":           "pest_control",
    "fertilizer use and availability": "fertilizer",
    "nutrient management":        "fertilizer",
    "seeds":                      "variety_selection",
    "seeds and planting material":"variety_selection",
    "cultural practices":         "crop_management",
    "crop production":            "crop_management",
    "soil and water conservation":"irrigation",
    "irrigation":                 "irrigation",
    "animal production":          "livestock_advisory",
    "animal health":              "livestock_advisory",
    "animal disease":             "livestock_advisory",
    "crop protection":            "pest_control",
    "crop disease":               "crop_disease",
    "post harvest":               "post_harvest",
    "scheme":                     "financial_support",
    "insurance":                  "financial_support",
    "credit":                     "financial_support",
    "subsidy":                    "financial_support",
}

def map_category(query_type: str) -> str:
    qt = query_type.strip().lower()
    for key, cat in QUERY_TYPE_TO_CATEGORY.items():
        if key in qt:
            return cat
    return "general_advisory"

# ── SEASON MAPPING ───────────────────────────────────────────────────────────

SEASON_MAP = {
    "KHARIF": "kharif",
    "RABI":   "rabi",
    "ZAID":   "summer",
    "SUMMER": "summer",
    "ANNUAL": "all",
}

def map_season(season: str) -> str:
    return SEASON_MAP.get(season.strip().upper(), "all")

# ── TEXT CLEANING ─────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    if not text or str(text).strip() in ("", "9999", "0"):
        return ""
    # Fix common encoding artifacts
    text = text.replace("a¿¿", "-")
    text = text.replace("a€“", "-")
    text = text.replace("a€™", "'")
    text = text.replace("a€œ", '"')
    text = text.replace("a€", '"')
    # Normalise unicode
    text = unicodedata.normalize("NFKC", text)
    # Strip leading "recommended for" boilerplate
    text = re.sub(r"^recommended for\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^recommended\s+for\s+", "", text, flags=re.IGNORECASE)
    # Collapse whitespace
    text = " ".join(text.split())
    return text.strip()

def clean_crop(crop: str) -> str:
    if not crop or str(crop).strip() in ("", "9999", "0", "1137", "1279", "1280", "1075"):
        return "unknown"
    return clean_text(crop)

def clean_district(district: str) -> str:
    if not district or str(district).strip() in ("", "9999", "0"):
        return "all"
    d = district.strip().title()
    # Fix common name issues
    d = d.replace("Chennai(Madras)", "Chennai")
    return d

# ── QUALITY SCORING ───────────────────────────────────────────────────────────

def score_row(query: str, answer: str, category: str) -> int:
    """
    Score 0-10. Higher = more useful for dataset.
    We keep rows scoring >= 5.
    """
    score = 5  # base

    # Answer length bonus
    ans_len = len(answer)
    if ans_len > 200: score += 2
    elif ans_len > 100: score += 1
    elif ans_len < 50: score -= 2

    # Query length bonus — longer = more specific
    if len(query) > 60: score += 1

    # Category bonus — pest/disease/fertilizer are most valuable
    if category in ("pest_control", "crop_disease", "fertilizer"): score += 1

    # Contains dosage/rate info — good actionable detail
    if re.search(r"\d+\s*(ml|gm|kg|g|l|lit|%|ppm|ha|acre|/)", answer, re.IGNORECASE):
        score += 1

    # Penalty: very generic answer
    if re.search(r"^(yes|no|ok|good)\s*$", answer, re.IGNORECASE):
        score -= 3

    return max(0, min(10, score))

# ── REFERRAL / JUNK DETECTION ─────────────────────────────────────────────────

def is_referral(answer: str) -> bool:
    a = answer.lower()
    for pat in REFERRAL_PATTERNS:
        if re.search(pat, a):
            return True
    return False

def is_junk_query(query: str) -> bool:
    q = query.lower()
    for pat in JUNK_QUERY_PATTERNS:
        if re.search(pat, q):
            return True
    return False

# ── OUTPUT SCHEMA ─────────────────────────────────────────────────────────────

OUTPUT_COLS = [
    "id", "question_tamil", "question_english",
    "answer_tamil", "answer_english",
    "category", "crop_primary", "region",
    "season", "source_type", "source_url",
    "kcc_district", "kcc_block", "kcc_sector",
    "kcc_query_type", "kcc_created_on", "kcc_score",
]

# ── MAIN ──────────────────────────────────────────────────────────────────────

def process(dry_run=False, limit=None):
    files = sorted(glob.glob(os.path.join(KCC_DIR, "*.csv")))
    if not files:
        print(f"ERROR: No CSV files found in {KCC_DIR}")
        return

    print(f"Found {len(files)} KCC files")
    print("=" * 60)

    stats = {
        "total_read":       0,
        "dropped_sector":   0,
        "dropped_referral": 0,
        "dropped_junk_q":   0,
        "dropped_short_ans":0,
        "dropped_low_score":0,
        "kept":             0,
    }

    extracted = []

    for fpath in files:
        fname = os.path.basename(fpath)
        file_count = 0
        file_kept  = 0

        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stats["total_read"] += 1
                    file_count += 1

                    if limit and len(extracted) >= limit:
                        break

                    sector     = str(row.get("Sector", "")).strip()
                    query_type = str(row.get("QueryType", "")).strip()
                    query_raw  = str(row.get("QueryText", "")).strip()
                    answer_raw = str(row.get("KccAns", "")).strip()
                    crop_raw   = str(row.get("Crop", "")).strip()
                    district   = str(row.get("DistrictName", "")).strip()
                    block      = str(row.get("BlockName", "")).strip()
                    season_raw = str(row.get("Season", "")).strip()
                    created_on = str(row.get("CreatedOn", "")).strip()

                    # ── FILTER ──────────────────────────────────────────────

                    # Drop bad sectors
                    if sector in DROP_SECTORS or sector == "9999":
                        stats["dropped_sector"] += 1
                        continue

                    if KEEP_SECTORS and sector not in KEEP_SECTORS:
                        stats["dropped_sector"] += 1
                        continue

                    # Drop bad query types
                    if query_type.strip() in DROP_QUERY_TYPES:
                        stats["dropped_junk_q"] += 1
                        continue

                    # Clean text
                    query  = clean_text(query_raw)
                    answer = clean_text(answer_raw)

                    if not query or not answer:
                        stats["dropped_short_ans"] += 1
                        continue

                    # Drop referrals
                    if is_referral(answer):
                        stats["dropped_referral"] += 1
                        continue

                    # Drop junk queries
                    if is_junk_query(query):
                        stats["dropped_junk_q"] += 1
                        continue

                    # Drop very short answers
                    if len(answer) < MIN_ANS_LEN:
                        stats["dropped_short_ans"] += 1
                        continue

                    # Truncate very long answers
                    if len(answer) > MAX_ANS_LEN:
                        answer = answer[:MAX_ANS_LEN].rsplit(" ", 1)[0] + "..."

                    # ── SCORE ────────────────────────────────────────────────
                    category = map_category(query_type)
                    score    = score_row(query, answer, category)

                    if score < 6:
                        stats["dropped_low_score"] += 1
                        continue

                    # ── BUILD ROW ────────────────────────────────────────────
                    crop     = clean_crop(crop_raw)

                    # Drop generic crop
                    if crop.lower() in ('others', 'unknown', '0', '9999'):
                        stats['dropped_low_score'] += 1
                        continue
                    region   = clean_district(district)
                    season   = map_season(season_raw)

                    # ID will be assigned after dedup
                    extracted.append({
                        "id":               "",   # assigned below
                        "question_tamil":   "",   # Tamil translation — run separately
                        "question_english": query.capitalize(),
                        "answer_tamil":     "",   # Tamil translation — run separately
                        "answer_english":   answer,
                        "category":         category,
                        "crop_primary":     crop,
                        "region":           region,
                        "season":           season,
                        "source_type":      "kcc_call_log",
                        "source_url":       "https://mkisan.gov.in/",
                        "kcc_district":     region,
                        "kcc_block":        block.strip(),
                        "kcc_sector":       sector,
                        "kcc_query_type":   query_type.strip(),
                        "kcc_created_on":   created_on,
                        "kcc_score":        score,
                    })

                    stats["kept"] += 1
                    file_kept += 1

        except Exception as e:
            print(f"  ERROR reading {fname}: {e}")
            continue

        print(f"  {fname[:50]:<50}  read={file_count:>6}  kept={file_kept:>4}")

        if limit and len(extracted) >= limit:
            print(f"  [limit {limit} reached]")
            break

    # ── DEDUP on (question_english, crop_primary) ────────────────────────────
    print("\nDeduplicating...")
    seen = set()
    deduped = []
    for r in extracted:
        key = (r["question_english"].lower()[:80], r["crop_primary"].lower())
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    dupes = len(extracted) - len(deduped)
    print(f"  Removed {dupes} duplicate rows")

    # ── SORT by score desc ───────────────────────────────────────────────────
    deduped.sort(key=lambda r: int(r["kcc_score"]), reverse=True)

    # ── ASSIGN IDs ───────────────────────────────────────────────────────────
    for i, r in enumerate(deduped, start=1):
        r["id"] = f"kcc-{i:04d}"

    # ── STATS ────────────────────────────────────────────────────────────────
    stats["kept_after_dedup"] = len(deduped)

    report_lines = [
        "=" * 60,
        f"KCC Extraction Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 60,
        f"Total rows read:          {stats['total_read']:>8,}",
        f"Dropped (bad sector):     {stats['dropped_sector']:>8,}",
        f"Dropped (referral ans):   {stats['dropped_referral']:>8,}",
        f"Dropped (junk query):     {stats['dropped_junk_q']:>8,}",
        f"Dropped (short answer):   {stats['dropped_short_ans']:>8,}",
        f"Dropped (low score):      {stats['dropped_low_score']:>8,}",
        f"Kept before dedup:        {stats['kept']:>8,}",
        f"Removed duplicates:       {dupes:>8,}",
        f"FINAL KEPT:               {stats['kept_after_dedup']:>8,}",
        "",
        "Category breakdown:",
    ]

    from collections import Counter
    cat_counts = Counter(r["category"] for r in deduped)
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        report_lines.append(f"  {cat:<30} {cnt:>5}")

    report_lines += [
        "",
        "Top crops:",
    ]
    crop_counts = Counter(r["crop_primary"] for r in deduped)
    for crop, cnt in crop_counts.most_common(15):
        report_lines.append(f"  {crop:<30} {cnt:>5}")

    report_lines += [
        "",
        "Score distribution:",
    ]
    score_counts = Counter(int(r["kcc_score"]) for r in deduped)
    for sc in sorted(score_counts.keys(), reverse=True):
        report_lines.append(f"  Score {sc}: {score_counts[sc]:>5}")

    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    # ── WRITE OUTPUT ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(deduped)

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"\n✓ Written → {OUT_CSV}")
    print(f"✓ Report  → {OUT_REPORT}")
    print(f"\nNext step:")
    print(f"  Review kcc_extracted_clean.csv, then merge top rows into v7 source.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats only, don't write files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total rows extracted (for testing)")
    args = parser.parse_args()

    process(dry_run=args.dry_run, limit=args.limit)
