"""
17_extract_kcc_gold.py
----------------------
Extracts 50-80 high-quality KCC rows WITH their native metadata intact.

The KCC CSVs already contain: DistrictName, Season, Crop, QueryType, KccAns (Tamil).
This script maps those to our schema columns so the Adaption platform gets real context.

Strategy:
  1. Read all KCC CSVs (679MB, ~500K+ rows)
  2. Filter to Tamil Nadu only, with Tamil answers > 80 chars
  3. Map district → region, crop → crop_primary, query type → category
  4. Deduplicate by question similarity
  5. Pick the best 60 rows across diverse categories
  6. Expand short Tamil answers to 5-part structure via Cohere
  7. Output as submission-ready CSV

Run from repo root:
    python scripts/17_extract_kcc_gold.py --scan          # just count and preview
    python scripts/17_extract_kcc_gold.py --extract       # extract + expand
    python scripts/17_extract_kcc_gold.py --extract --limit 10  # test on 10
"""

import os
import csv
import re
import time
import argparse
from pathlib import Path
from collections import Counter
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

KCC_DIR = Path(__file__).parent.parent / "data" / "01_raw_manual" / "kcc"
OUTPUT  = Path(__file__).parent.parent / "data" / "02_structured" / "kcc_gold_extracted.csv"

# ── DISTRICT → REGION MAPPING ───────────────────────────────────────────────
# Based on TNAU agro-ecological zones
DISTRICT_TO_REGION = {
    # Delta / Cauvery
    "THANJAVUR": "delta", "TIRUVARUR": "delta", "NAGAPATTINAM": "delta",
    "MAYILADUTHURAI": "delta", "CUDDALORE": "delta", "ARIYALUR": "delta",
    "TIRUCHIRAPPALLI": "delta", "PERAMBALUR": "delta", "KARUR": "delta",
    # Coastal
    "CHENNAI": "coastal", "THIRUVALLUR": "coastal", "KANCHEEPURAM": "coastal",
    "CHENGALPATTU": "coastal", "RAMANATHAPURAM": "coastal",
    "THOOTHUKUDI": "coastal", "TIRUNELVELI": "coastal", "KANYAKUMARI": "coastal",
    # Dry zone
    "MADURAI": "dry_zone", "SIVAGANGAI": "dry_zone", "DINDIGUL": "dry_zone",
    "PUDUKKOTTAI": "dry_zone", "VIRUDHUNAGAR": "dry_zone",
    "SALEM": "dry_zone", "NAMAKKAL": "dry_zone", "DHARMAPURI": "dry_zone",
    "KRISHNAGIRI": "dry_zone", "ERODE": "dry_zone", "TIRUPPUR": "dry_zone",
    "VILLUPURAM": "dry_zone", "VILUPPURAM": "dry_zone", "VILLUPPURAM": "dry_zone",
    "TIRUVANNAMALAI": "dry_zone", "KALLAKURICHI": "dry_zone",
    "VELLORE": "dry_zone", "RANIPET": "dry_zone", "TIRUPATHUR": "dry_zone",
    "TENKASI": "dry_zone", "THENI": "dry_zone",
    # Western Ghats
    "COIMBATORE": "western_ghats", "THE NILGIRIS": "western_ghats",
    "NILGIRIS": "western_ghats",
}

# ── CROP MAPPING ─────────────────────────────────────────────────────────────
CROP_MAP = {
    "Paddy (Dhan)": ("rice", "clay_alluvial", "canal"),
    "Groundnut (pea nut/mung phalli)": ("groundnut", "red_laterite", "rainfed"),
    "Cotton": ("cotton", "black_cotton", "rainfed"),
    "Sugarcane": ("sugarcane", "alluvial", "canal"),
    "Banana": ("banana", "alluvial", "drip"),
    "Coconut": ("coconut", "coastal_saline", "drip"),
    "Maize": ("maize", "red_laterite", "rainfed"),
    "Onion": ("onion", "red_loamy", "borewell"),
    "Tomato": ("tomato", "red_loamy", "borewell"),
    "Chilli": ("chilli", "red_loamy", "borewell"),
    "Turmeric": ("turmeric", "red_loamy", "borewell"),
    "Brinjal": ("brinjal", "red_loamy", "borewell"),
    "Mango": ("mango", "red_loamy", "borewell"),
    "Drum Stick": ("moringa", "red_loamy", "rainfed"),
    "Sorghum": ("sorghum", "red_laterite", "rainfed"),
    "Black gram": ("blackgram", "red_laterite", "rainfed"),
    "Green gram": ("greengram", "red_laterite", "rainfed"),
    "Pigeon pea (red gram/arhar/tur)": ("redgram", "red_laterite", "rainfed"),
    "Tapioca (cassava)": ("tapioca", "red_loamy", "rainfed"),
    "Jasmine": ("jasmine", "red_loamy", "borewell"),
    "Rose": ("rose", "red_loamy", "drip"),
    "Sunflower": ("sunflower", "red_laterite", "rainfed"),
    # KCC name variants
    "Black Gram (urd bean)": ("blackgram", "red_laterite", "rainfed"),
    "Chillies": ("chilli", "red_loamy", "borewell"),
    "Maize (Makka)": ("maize", "red_laterite", "rainfed"),
    "Cotton (Kapas)": ("cotton", "black_cotton", "rainfed"),
    "Sugarcane (Noble Cane)": ("sugarcane", "alluvial", "canal"),
    "Sesame (Gingelly/Til)/Sesamum": ("sesame", "red_laterite", "rainfed"),
    "Green Gram (Moong)": ("greengram", "red_laterite", "rainfed"),
    "Red Gram (Tur/Arhar)": ("redgram", "red_laterite", "rainfed"),
    "Tapioca (cassava)": ("tapioca", "red_loamy", "rainfed"),
    "Finger Millet (Ragi)": ("ragi", "red_laterite", "rainfed"),
    "Pearl Millet (Bajra)": ("pearl_millet", "sandy_loam", "rainfed"),
    "Sorghum (Jowar)": ("sorghum", "red_laterite", "rainfed"),
}

# ── QUERY TYPE → CATEGORY ────────────────────────────────────────────────────
QUERY_TO_CATEGORY = {
    "Plant Protection": "pest_control",
    "Nutrient Management": "fertilizer",
    "Varieties": "variety_selection",
    "Seeds": "variety_selection",
    "Seeds and Planting Material": "variety_selection",
    "Cultural Practices": "crop_management",
    "Weather": "weather_advisory",
    "Agriculture Mechanization": "government_schemes",
    "Schemes": "government_schemes",
    "Weed Management": "soil_health",
    "Harvesting, Threshing & Storage": "harvest_timing",
    "Post Harvest Technology": "harvest_timing",
    "Marketing": "market_price",
    "Soil and soil fertility": "soil_health",
    "Irrigation": "irrigation",
    "Integrated Pest Management": "pest_control",
    "Disease Management": "crop_disease",
    # KCC query type variants
    "Market Information": "market_price",
    "Sowing Time and Weather": "weather_advisory",
    "Fertilizer Use and Availability": "fertilizer",
    "Government Schemes": "government_schemes",
    "Crop Insurance": "financial_support",
    "Intercultural Operations": "crop_management",
    "Animal Husbandry": "livestock_dairy",
    "Fisheries": "aquaculture",
}

# ── SEASON MAPPING ────────────────────────────────────────────────────────────
def map_season(month):
    if month in (6, 7, 8, 9):
        return "kharif"
    elif month in (10, 11, 12, 1):
        return "samba"
    elif month in (2, 3, 4, 5):
        return "summer"
    return "kharif"


def is_good_kcc_row(row):
    """Filter for high-quality KCC rows worth keeping."""
    ans = row.get("KccAns", "")
    query = row.get("QueryText", "")
    crop = row.get("Crop", "")
    
    # Must be Tamil Nadu
    if row.get("StateName", "") != "TAMILNADU":
        return False
    
    # Must have a Tamil answer with substance
    # Tamil chars are in Unicode range 0B80-0BFF
    tamil_chars = len(re.findall(r'[\u0B80-\u0BFF]', ans))
    if tamil_chars < 40:
        return False
    
    # Answer must be > 80 chars total
    if len(ans) < 80:
        return False
    
    # Must have a real crop (not "Others")
    if crop in ("Others", "", "NA"):
        return False
    
    # Must have a real query
    if len(query) < 20:
        return False
    
    # Skip one-word answers or pure referral answers
    if "தொடர்பு எண்" in ans and len(ans) < 150:
        return False  # Just a phone number referral
    
    return True


def map_kcc_row(row, idx):
    """Map a KCC row to our submission schema."""
    crop_info = CROP_MAP.get(row.get("Crop", ""), None)
    district = row.get("DistrictName", "").strip().upper()
    region = DISTRICT_TO_REGION.get(district, "dry_zone")
    category = QUERY_TO_CATEGORY.get(row.get("QueryType", "").strip(), "crop_management")
    
    month = 7  # default
    try:
        month = int(row.get("month", 7))
    except:
        pass
    
    season = map_season(month)
    
    if crop_info:
        crop_primary, soil_type, irrigation_type = crop_info
    else:
        crop_primary = row.get("Crop", "").lower().replace(" ", "_")
        soil_type = "red_loamy"
        irrigation_type = "rainfed"
    
    # Build the question in Tamil (KCC has it in the answer field, prefixed with கேள்வி:)
    ans_text = row.get("KccAns", "")
    query_text = row.get("QueryText", "")
    
    # Extract Tamil question if embedded in answer
    tamil_question = ""
    if "கேள்வி" in ans_text and "பதில்" in ans_text:
        parts = ans_text.split("பதில்", 1)
        tamil_question = parts[0].replace("கேள்வி:", "").replace("கேள்வி :", "").strip()
        tamil_answer = parts[1].strip().lstrip(":").strip()
    else:
        tamil_question = query_text  # English question as fallback
        tamil_answer = ans_text
    
    # Build context tag
    context_parts = [crop_primary]
    if season != "all":
        context_parts.append(season)
    context_tag = f"[{' | '.join(context_parts)}]"
    
    return {
        "id": f"tn-kcc-{idx:04d}",
        "question": f"{context_tag}\n{tamil_question}" if tamil_question else f"{context_tag}\n{query_text}",
        "answer": tamil_answer,  # Will be expanded by Cohere later
        "category": category,
        "crop_primary": crop_primary,
        "soil_type": soil_type,
        "irrigation_type": irrigation_type,
        "farming_practice": "conventional",
        "growth_stage": "vegetative",
        "region": region,
        "season": season,
        "severity": "medium",
        "source_type": "agricultural_extension",
        "reasoning_type": "agronomic_advisory",
        # Keep originals for reference
        "_district": district,
        "_block": row.get("BlockName", ""),
        "_original_crop": row.get("Crop", ""),
        "_query_type": row.get("QueryType", ""),
        "_answer_length": len(tamil_answer),
    }


def scan_kcc(limit_files=None):
    """Scan all KCC files and report what's available."""
    csv_files = sorted(KCC_DIR.glob("*.csv"))
    if limit_files:
        csv_files = csv_files[:limit_files]
    
    total_rows = 0
    good_rows = 0
    crop_counts = Counter()
    district_counts = Counter()
    query_type_counts = Counter()
    answer_lengths = []
    
    for fpath in csv_files:
        print(f"  Scanning {fpath.name[:20]}... ({fpath.stat().st_size / 1024 / 1024:.0f}MB)")
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("StateName", "") != "TAMILNADU":
                        continue
                    total_rows += 1
                    if is_good_kcc_row(row):
                        good_rows += 1
                        crop_counts[row.get("Crop", "")] += 1
                        district_counts[row.get("DistrictName", "")] += 1
                        query_type_counts[row.get("QueryType", "").strip()] += 1
                        answer_lengths.append(len(row.get("KccAns", "")))
        except Exception as e:
            print(f"    Error: {e}")
    
    print(f"\n{'='*60}")
    print(f"  KCC Data Scan Results")
    print(f"{'='*60}")
    print(f"  Total TN rows:     {total_rows:,}")
    print(f"  Good rows:         {good_rows:,} ({good_rows/max(total_rows,1)*100:.1f}%)")
    if answer_lengths:
        print(f"  Avg answer length: {sum(answer_lengths)//len(answer_lengths)} chars")
        print(f"  Long answers (>200 chars): {sum(1 for l in answer_lengths if l > 200)}")
    
    print(f"\n  Top crops:")
    for crop, count in crop_counts.most_common(15):
        print(f"    {crop}: {count}")
    
    print(f"\n  Top query types:")
    for qt, count in query_type_counts.most_common(10):
        print(f"    {qt}: {count}")
    
    print(f"\n  Top districts:")
    for dist, count in district_counts.most_common(10):
        region = DISTRICT_TO_REGION.get(dist.strip().upper(), "unknown")
        print(f"    {dist} ({region}): {count}")
    
    return good_rows


def extract_kcc(target_rows=60, limit_files=None):
    """Extract the best KCC rows with full metadata."""
    csv_files = sorted(KCC_DIR.glob("*.csv"))
    if limit_files:
        csv_files = csv_files[:limit_files]
    
    # Collect ALL good rows first
    candidates = []
    for fpath in csv_files:
        print(f"  Reading {fpath.name[:20]}...")
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if is_good_kcc_row(row):
                        candidates.append(row)
        except Exception as e:
            print(f"    Error: {e}")
    
    print(f"\n  Total candidates: {len(candidates)}")
    
    # Sort by answer length (longer = better quality)
    candidates.sort(key=lambda r: len(r.get("KccAns", "")), reverse=True)
    
    # Pick diverse rows: max 5 per crop, max 8 per district, spread across query types
    selected = []
    crop_counts = Counter()
    district_counts = Counter()
    category_counts = Counter()
    seen_questions = set()
    
    for row in candidates:
        crop = row.get("Crop", "")
        district = row.get("DistrictName", "")
        qtype = row.get("QueryType", "").strip()
        query = row.get("QueryText", "")[:50]  # First 50 chars for dedup
        category = QUERY_TO_CATEGORY.get(qtype, "crop_management")
        
        # Diversity limits
        if crop_counts[crop] >= 5:
            continue
        if district_counts[district] >= 6:
            continue
        if category_counts[category] >= 12:
            continue
        
        # Rough dedup
        if query in seen_questions:
            continue
        
        # Skip very short Tamil answers even if they passed filter
        ans = row.get("KccAns", "")
        tamil_chars = len(re.findall(r'[\u0B80-\u0BFF]', ans))
        if tamil_chars < 50:
            continue
        
        selected.append(row)
        crop_counts[crop] += 1
        district_counts[district] += 1
        category_counts[category] += 1
        seen_questions.add(query)
        
        if len(selected) >= target_rows:
            break
    
    print(f"  Selected: {len(selected)} rows")
    
    # Map to our schema
    mapped = []
    for i, row in enumerate(selected, 1):
        mapped_row = map_kcc_row(row, i)
        mapped.append(mapped_row)
    
    # Save
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    
    # Submission columns (exclude _ prefixed debug columns)
    submission_cols = [k for k in mapped[0].keys() if not k.startswith("_")]
    debug_cols = [k for k in mapped[0].keys() if k.startswith("_")]
    all_cols = submission_cols + debug_cols
    
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(mapped)
    
    print(f"\n  Saved: {OUTPUT}")
    print(f"  Columns: {submission_cols}")
    
    # Stats
    print(f"\n  Category distribution:")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")
    
    print(f"\n  Region distribution:")
    region_counts = Counter(r["region"] for r in mapped)
    for reg, count in sorted(region_counts.items(), key=lambda x: -x[1]):
        print(f"    {reg}: {count}")
    
    # Check metadata fill
    fields = ["region", "season", "soil_type", "irrigation_type"]
    for f in fields:
        all_count = sum(1 for r in mapped if r[f] == "all")
        print(f"  {f}: {all_count}/{len(mapped)} are 'all' ({all_count/len(mapped)*100:.1f}%)")
    
    return mapped


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan", action="store_true", help="Just scan and report")
    parser.add_argument("--extract", action="store_true", help="Extract best rows")
    parser.add_argument("--target", type=int, default=60, help="Target number of rows")
    parser.add_argument("--limit-files", type=int, default=None, help="Limit number of KCC files to read")
    args = parser.parse_args()
    
    if args.scan:
        scan_kcc(limit_files=args.limit_files)
    elif args.extract:
        extract_kcc(target_rows=args.target, limit_files=args.limit_files)
    else:
        print("Usage:")
        print("  python scripts/17_extract_kcc_gold.py --scan")
        print("  python scripts/17_extract_kcc_gold.py --extract --target 60")
        print("  python scripts/17_extract_kcc_gold.py --scan --limit-files 3  # quick test")