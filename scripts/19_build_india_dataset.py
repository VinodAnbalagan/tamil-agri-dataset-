"""
19_build_india_dataset.py (v2)
-------------------------------
Generates an India-wide agricultural advisory dataset with proper distribution.

Design principles (same as Tamil Grade A):
  - 14 zones × 3 crops × 4 categories = 168 rows + 1 mental health = 169
  - 12 rows per zone (perfectly balanced)
  - 13 categories (not just 4 repeated)
  - Each zone gets DIFFERENT category assignments (no repetition)
  - Zone-specific problems referenced in questions
  - Real metadata from Planning Commission agro-climatic zones
  - 99%+ metadata fill rate

Run from repo root:
    python scripts/19_build_india_dataset.py --generate
    python scripts/19_build_india_dataset.py --expand --limit 5
    python scripts/19_build_india_dataset.py --expand
"""

import os
import csv
import time
import argparse
from pathlib import Path
from collections import Counter
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

OUTPUT_QA    = Path(__file__).parent.parent / "data" / "02_structured" / "india_agri_advisory_raw.csv"
OUTPUT_FINAL = Path(__file__).parent.parent / "data" / "02_structured" / "india_agri_advisory_submission.csv"

# ── ZONE DATA (from Agroclimatic Zones of India PDF) ────────────────────────

ZONES = {
    "Western Himalayan": {
        "states": "Jammu & Kashmir, Himachal Pradesh, Uttarakhand",
        "climate": "temperate to cold", "rainfall": "75-150 cm",
        "soils": ["silty_loam", "alluvial"],
        "irrigation": "canal",
        "crops": ["rice", "maize", "wheat", "potato", "apple"],
        "problems": "soil erosion, landslides, frost damage, poor market access",
        "seasons": ["kharif", "rabi"],
    },
    "Eastern Himalayan": {
        "states": "Sikkim, Arunachal Pradesh, Nagaland, Meghalaya, Manipur, Mizoram, Tripura",
        "climate": "sub-humid", "rainfall": "200-400 cm",
        "soils": ["laterite", "brown_hill"],
        "irrigation": "rainfed",
        "crops": ["rice", "tea", "maize", "potato", "orange"],
        "problems": "shifting cultivation, soil degradation, heavy rainfall, poor infrastructure",
        "seasons": ["kharif", "rabi"],
    },
    "Lower Gangetic Plains": {
        "states": "West Bengal, Eastern Bihar",
        "climate": "hot and humid", "rainfall": "100-200 cm",
        "soils": ["clay_alluvial", "silty_clay"],
        "irrigation": "canal",
        "crops": ["rice", "jute", "potato", "mango", "banana"],
        "problems": "waterlogging, flooding, small holdings, pest pressure",
        "seasons": ["kharif", "rabi", "summer"],
    },
    "Middle Gangetic Plains": {
        "states": "Eastern Uttar Pradesh, Bihar",
        "climate": "hot and humid", "rainfall": "100-200 cm",
        "soils": ["alluvial", "clay_loam"],
        "irrigation": "tubewell",
        "crops": ["rice", "wheat", "sugarcane", "potato", "mustard"],
        "problems": "zinc deficiency in rice, alkaline soil, waterlogging, poverty",
        "seasons": ["kharif", "rabi"],
    },
    "Upper Gangetic Plains": {
        "states": "Central & Western Uttar Pradesh, Uttarakhand",
        "climate": "sub-humid", "rainfall": "75-150 cm",
        "soils": ["sandy_loam", "clay_loam"],
        "irrigation": "tubewell",
        "crops": ["wheat", "sugarcane", "rice", "potato", "mango"],
        "problems": "saline/alkaline soil from over-irrigation, sugarcane price crashes",
        "seasons": ["kharif", "rabi"],
    },
    "Trans-Gangetic Plains": {
        "states": "Punjab, Haryana, Delhi, Rajasthan (Ganganagar)",
        "climate": "semi-arid", "rainfall": "70-125 cm",
        "soils": ["alluvial", "sandy_loam"],
        "irrigation": "tubewell",
        "crops": ["wheat", "rice", "cotton", "sugarcane", "mustard"],
        "problems": "falling water table, stubble burning, salinity, rice-wheat fatigue",
        "seasons": ["kharif", "rabi"],
    },
    "Eastern Plateau and Hills": {
        "states": "Jharkhand, Chhattisgarh, Odisha (western)",
        "climate": "sub-humid", "rainfall": "80-150 cm",
        "soils": ["red_laterite", "alluvial"],
        "irrigation": "rainfed",
        "crops": ["rice", "groundnut", "ragi", "soybean", "maize"],
        "problems": "nutrient-leached acidic soil, scarce groundwater, tribal poverty",
        "seasons": ["kharif", "rabi"],
    },
    "Central Plateau and Hills": {
        "states": "Madhya Pradesh, Rajasthan (Bundelkhand), UP (Bundelkhand)",
        "climate": "semi-arid", "rainfall": "50-100 cm",
        "soils": ["black_cotton", "red_loamy"],
        "irrigation": "rainfed",
        "crops": ["soybean", "wheat", "gram", "cotton", "mustard"],
        "problems": "drought, soil erosion, deforestation, erratic monsoon",
        "seasons": ["kharif", "rabi"],
    },
    "Western Plateau and Hills": {
        "states": "Maharashtra (Deccan), southern Madhya Pradesh",
        "climate": "hot semi-arid", "rainfall": "25-75 cm",
        "soils": ["black_cotton", "laterite"],
        "irrigation": "rainfed",
        "crops": ["jowar", "cotton", "sugarcane", "groundnut", "grape"],
        "problems": "rain shadow, water scarcity, only 12% irrigated, farmer debt",
        "seasons": ["kharif", "rabi"],
    },
    "Southern Plateau and Hills": {
        "states": "Karnataka, Tamil Nadu (interior), Andhra Pradesh (Rayalaseema)",
        "climate": "semi-arid", "rainfall": "50-100 cm",
        "soils": ["red_loamy", "black_cotton"],
        "irrigation": "borewell",
        "crops": ["rice", "ragi", "groundnut", "cotton", "coconut"],
        "problems": "drought, falling water table, fluoride in groundwater",
        "seasons": ["kharif", "rabi"],
    },
    "East Coast Plains and Hills": {
        "states": "Andhra Pradesh (coastal), Odisha (coastal), Tamil Nadu (coastal)",
        "climate": "sub-humid maritime", "rainfall": "75-150 cm",
        "soils": ["alluvial", "coastal_saline"],
        "irrigation": "canal",
        "crops": ["rice", "groundnut", "sugarcane", "banana", "coconut"],
        "problems": "cyclones, saltwater intrusion, soil alkalinity, flooding",
        "seasons": ["kharif", "rabi"],
    },
    "West Coast Plains and Ghats": {
        "states": "Kerala, Karnataka (coastal), Goa, Maharashtra (Konkan)",
        "climate": "humid tropical", "rainfall": "200-300 cm",
        "soils": ["laterite", "red_loamy"],
        "irrigation": "rainfed",
        "crops": ["rice", "coconut", "arecanut", "rubber", "pepper"],
        "problems": "soil acidity, laterite infertility, landslides, heavy monsoon",
        "seasons": ["kharif", "rabi"],
    },
    "Gujarat Plains and Hills": {
        "states": "Gujarat",
        "climate": "arid to semi-arid", "rainfall": "50-100 cm",
        "soils": ["black_cotton", "alluvial"],
        "irrigation": "tubewell",
        "crops": ["groundnut", "cotton", "rice", "wheat", "bajra"],
        "problems": "salinity, drought, industrial pollution, Kutch desertification",
        "seasons": ["kharif", "rabi"],
    },
    "Western Dry Region": {
        "states": "Rajasthan (western, Thar desert)",
        "climate": "hot arid desert", "rainfall": "<25 cm",
        "soils": ["sandy", "desert"],
        "irrigation": "rainfed",
        "crops": ["bajra", "jowar", "moth", "guar", "wheat"],
        "problems": "famine, drought, sand dunes, only 6% irrigated, water scarcity",
        "seasons": ["kharif", "rabi"],
    },
}

# ── QUESTION TEMPLATES BY CATEGORY ──────────────────────────────────────────
# Each template is unique — different farmer voice, different problem framing

TEMPLATES = {
    "pest_control": [
        "My {crop} field in {state} is infested with insects eating the leaves. I can see holes and droppings. What pesticide should I use and how much per acre?",
        "Small caterpillars are destroying my {crop} crop. The infestation is spreading fast. What is the most cost-effective way to control them on {soil} soil?",
    ],
    "crop_disease": [
        "The leaves of my {crop} are turning yellow with brown patches. Some plants are wilting. Is this a fungal disease? What treatment do you recommend for {soil} soil in {season}?",
        "My {crop} plants have white powdery coating on the leaves. The fruits are deformed. What disease is this and how do I save the remaining crop?",
    ],
    "fertilizer": [
        "I am growing {crop} on {soil} soil in {state}. What is the correct NPK dose per acre and when should I apply each split?",
        "My {crop} leaves are pale green and growth is stunted. I think there is a nutrient deficiency. What micronutrient should I apply and how much will it cost?",
    ],
    "weather_advisory": [
        "Heavy rain is forecast for next week in {state}. My {crop} is at {growth_stage} stage. Should I harvest early or wait? What precautions should I take?",
        "There has been no rain for 3 weeks and my {crop} on {soil} soil is wilting. How do I save the crop with limited water?",
    ],
    "irrigation": [
        "Water is scarce this {season}. I grow {crop} on {soil} soil in {state}. How can I reduce water usage without losing yield?",
        "I want to switch from flood irrigation to drip for my {crop}. What is the cost per acre and what government subsidy is available?",
    ],
    "soil_health": [
        "My {soil} soil in {state} has become hard and unproductive after years of growing {crop}. How do I restore its fertility without spending too much?",
        "My soil test shows pH is too high/low for {crop}. What amendments should I apply per acre to correct it?",
    ],
    "variety_selection": [
        "I want to plant {crop} this {season} on {soil} soil in {state}. Which improved variety from ICAR do you recommend for best yield?",
        "Which {crop} variety is most resistant to drought and pests for the {zone_name} region?",
    ],
    "crop_management": [
        "What companion crop can I grow with {crop} to improve soil health and get additional income in {state}?",
        "My {crop} spacing seems wrong — plants are crowding each other. What is the correct spacing for {soil} soil with {irrigation} irrigation?",
    ],
    "harvest_timing": [
        "How do I know when my {crop} is ready for harvest? What is the correct moisture content for safe storage?",
        "I harvested my {crop} but it is getting damaged in storage. What is the proper drying and storage method?",
    ],
    "government_schemes": [
        "I am a small farmer with 2 acres in {state}. What government schemes can help me get subsidized seeds, fertilizers, and crop insurance?",
        "How do I register for PM-KISAN and PMFBY? What documents do I need and where do I apply in {state}?",
    ],
    "financial_support": [
        "My {crop} crop failed due to drought in {state}. I have a KCC loan I cannot repay. What relief options are available?",
        "The market price of {crop} is below MSP. Where can I sell at MSP and how does the procurement process work?",
    ],
    "market_price": [
        "Middlemen are offering a very low price for my {crop} harvest in {state}. How can I sell directly to consumers or at a government mandi?",
        "How do I register on e-NAM to sell my {crop} online? What are the charges and benefits?",
    ],
}


def generate_qa_pairs():
    """Generate properly distributed Q&A pairs."""
    rows = []
    row_id = 1

    # Assign categories to zones in a rotating pattern so each zone
    # gets DIFFERENT categories — not all zones asking about pest_control
    all_categories = list(TEMPLATES.keys())  # 12 categories (excl mental health)

    for zone_idx, (zone_name, zone_data) in enumerate(ZONES.items()):
        crops = zone_data["crops"][:3]  # top 3 crops per zone
        state = zone_data["states"].split(",")[0].strip()
        soil = zone_data["soils"][0]
        irrigation = zone_data["irrigation"]
        season = zone_data["seasons"][0]

        # 12 category slots across 3 crops (4 categories each)
        # Rotate starting position so each zone gets different categories
        start_cat = (zone_idx * 4) % len(all_categories)
        zone_cats = []
        for i in range(12):
            cat_idx = (start_cat + i) % len(all_categories)
            zone_cats.append(all_categories[cat_idx])

        # 3 crops × 4 categories = 12 rows per zone
        for crop_idx, crop in enumerate(crops):
            # Crop 0: cats[0:4], Crop 1: cats[4:8], Crop 2: cats[8:12]
            crop_cats = zone_cats[crop_idx * 4 : (crop_idx + 1) * 4]

            for cat in crop_cats:
                # Pick template (alternate between the two per category)
                template_idx = row_id % 2
                templates = TEMPLATES[cat]
                template = templates[template_idx % len(templates)]

                # Determine growth stage based on category
                growth_map = {
                    "pest_control": "vegetative", "crop_disease": "vegetative",
                    "fertilizer": "pre_sowing", "weather_advisory": "flowering",
                    "irrigation": "vegetative", "soil_health": "pre_sowing",
                    "variety_selection": "pre_sowing", "crop_management": "vegetative",
                    "harvest_timing": "harvest", "government_schemes": "pre_sowing",
                    "financial_support": "harvest", "market_price": "harvest",
                }
                growth_stage = growth_map.get(cat, "vegetative")

                # Determine severity based on category
                severity_map = {
                    "pest_control": "high", "crop_disease": "high",
                    "fertilizer": "medium", "weather_advisory": "high",
                    "irrigation": "high", "soil_health": "medium",
                    "variety_selection": "low", "crop_management": "medium",
                    "harvest_timing": "medium", "government_schemes": "low",
                    "financial_support": "urgent", "market_price": "medium",
                }
                severity = severity_map.get(cat, "medium")

                # Format question
                question = template.format(
                    crop=crop, state=state, soil=soil.replace("_", " "),
                    season=season, growth_stage=growth_stage,
                    zone_name=zone_name, irrigation=irrigation,
                )

                context_tag = f"[{crop} | {zone_name} | {season} | {severity}]"

                rows.append({
                    "id": f"in-agri-{row_id:04d}",
                    "question": f"{context_tag}\n{question}",
                    "answer": "",
                    "category": cat,
                    "crop_primary": crop,
                    "soil_type": soil,
                    "irrigation_type": irrigation,
                    "farming_practice": "conventional",
                    "growth_stage": growth_stage,
                    "region": zone_name.lower().replace(" ", "_").replace("'", ""),
                    "season": season,
                    "severity": severity,
                    "source_type": "agricultural_extension",
                    "reasoning_type": cat.replace("_", "_"),
                    "_states": zone_data["states"],
                    "_climate": zone_data["climate"],
                    "_rainfall": zone_data["rainfall"],
                    "_problems": zone_data["problems"],
                })
                row_id += 1

    # Mental health row
    rows.append({
        "id": f"in-agri-{row_id:04d}",
        "question": "I am drowning in debt. My crops failed and I have nothing left. I feel like giving up on everything.",
        "answer": "You are not alone. Please call Kisan Call Centre: 1551 (toll-free, 24/7). For emotional support, call iCall: 9152987821 or Vandrevala Foundation: 1860-2662-345. Government assistance is available — visit your District Collector's office for drought relief. PM-KISAN provides Rs.6000/year. PMFBY crop insurance can compensate losses. Do not make any decisions alone. There are people waiting to help you.",
        "category": "mental_health_safety",
        "crop_primary": "all", "soil_type": "all", "irrigation_type": "all",
        "farming_practice": "all", "growth_stage": "all", "region": "all",
        "season": "all", "severity": "urgent", "source_type": "crisis_routing",
        "reasoning_type": "crisis_routing",
        "_states": "All India", "_climate": "all", "_rainfall": "all",
        "_problems": "farmer debt crisis, suicide prevention",
    })

    # Save
    OUTPUT_QA.parent.mkdir(parents=True, exist_ok=True)
    submission_cols = [k for k in rows[0].keys() if not k.startswith("_")]
    debug_cols = [k for k in rows[0].keys() if k.startswith("_")]
    all_cols = submission_cols + debug_cols

    with open(OUTPUT_QA, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    # Stats
    print(f"Generated {len(rows)} Q&A pairs")
    print(f"Saved: {OUTPUT_QA}")

    cats = Counter(r["category"] for r in rows)
    print(f"\nCategories ({len(cats)}):")
    for c, n in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")

    zones = Counter(r["region"] for r in rows)
    print(f"\nZones ({len(zones)}):")
    for z, n in sorted(zones.items(), key=lambda x: -x[1]):
        print(f"  {z}: {n}")

    crops = Counter(r["crop_primary"] for r in rows)
    print(f"\nCrops ({len(crops)}):")
    for c, n in sorted(crops.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")

    fields = ["region", "season", "soil_type", "irrigation_type", "growth_stage"]
    print("\nMetadata:")
    for f in fields:
        all_count = sum(1 for r in rows if r.get(f, "") == "all")
        print(f"  {f}: {all_count}/{len(rows)} 'all'")

    return rows


# ── COHERE EXPANSION ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior agricultural extension officer from the Indian Council of Agricultural Research (ICAR), with 20 years of field experience advising smallholder farmers across India's agro-climatic zones.

MANDATORY 5-PART STRUCTURE for every response:

1. **Situation Assessment** — Acknowledge the farmer's specific situation. Mention the crop, agro-climatic zone, state, soil type, and season.

2. **Immediate Action** — Give one concrete action with exact quantities per acre, timing, and cost in rupees.
   Example: "Apply 50 kg DAP per acre at sowing. Cost: approximately Rs.1,350."

3. **Rationale** — Explain why this recommendation fits this specific zone, soil type, and season. Reference at least 2 local conditions.

4. **Long-term Prevention** — One sustainable, low-cost practice for future seasons.

5. **KVK Referral** — End with: "For more details, contact your nearest Krishi Vigyan Kendra (KVK) or district agriculture officer."

RULES:
- Write in clear, simple English a farmer with limited education can understand
- Include exact dosages in kg/acre, ml/litre, rupee costs
- Reference the specific agro-climatic zone and state
- For mental health/crisis: lead with Kisan Call Centre 1551 and iCall 9152987821
- For government schemes: state exact amounts (PM-KISAN Rs.6000/year, PMFBY percentages)
- Minimum 250 words per answer
- Do NOT use jargon without explaining it
- Do NOT give generic advice — everything must reference the specific zone and conditions"""

EXPAND_PROMPT = """Write a comprehensive agricultural advisory answer for this farmer.

CONTEXT:
- Agro-climatic zone: {zone_name}
- States: {states}
- Climate: {climate}
- Rainfall: {rainfall}
- Crop: {crop}
- Soil: {soil}
- Irrigation: {irrigation}
- Season: {season}
- Severity: {severity}
- Zone problems: {problems}

FARMER QUESTION:
{question}

Write a detailed 5-part advisory. Be specific to this zone and crop. Include quantities and costs in rupees."""


def expand_answers(limit=None):
    """Expand with Cohere."""
    COHERE_API_KEY = os.environ.get("COHERE_API_KEY", "")
    if not COHERE_API_KEY:
        raise ValueError("COHERE_API_KEY not found in .env")

    import cohere
    co = cohere.Client(COHERE_API_KEY, timeout=600)
    DELAY = 5

    if not OUTPUT_QA.exists():
        print(f"ERROR: {OUTPUT_QA} not found. Run --generate first.")
        return

    with open(OUTPUT_QA, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    to_expand = [r for r in rows if not r.get("answer", "").strip() or len(r.get("answer", "")) < 100]
    if limit:
        to_expand = to_expand[:limit]

    print(f"Rows to expand: {len(to_expand)}")
    expanded = 0
    errors = 0

    for i, row in enumerate(to_expand, 1):
        rid = row["id"]
        print(f"  [{i}/{len(to_expand)}] {rid} | {row['crop_primary']} | {row['region']}...", end="")

        prompt = EXPAND_PROMPT.format(
            zone_name=row.get("region", "").replace("_", " "),
            states=row.get("_states", ""),
            climate=row.get("_climate", ""),
            rainfall=row.get("_rainfall", ""),
            crop=row.get("crop_primary", ""),
            soil=row.get("soil_type", ""),
            irrigation=row.get("irrigation_type", ""),
            season=row.get("season", ""),
            severity=row.get("severity", ""),
            problems=row.get("_problems", ""),
            question=row.get("question", "")[:300],
        )

        for attempt in range(3):
            try:
                response = co.chat(
                    model="command-r-plus-08-2024",
                    preamble=SYSTEM_PROMPT,
                    message=prompt,
                    max_tokens=2000,
                )
                text = response.text.strip()
                if text and len(text) > 200:
                    for r in rows:
                        if r["id"] == rid:
                            r["answer"] = text
                            break
                    expanded += 1
                    print(f" ok ({len(text)} chars)")
                    break
                else:
                    print(f" too short", end="")
            except Exception as e:
                err = str(e)[:50]
                if attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f" [retry {attempt+1}, {wait}s: {err}]", end="")
                    time.sleep(wait)
                else:
                    print(f" [failed: {err}]", end="")
                    errors += 1

        time.sleep(DELAY)

    print(f"\nExpanded: {expanded}, Errors: {errors}")

    # Save submission version (no debug columns)
    submission_cols = [k for k in rows[0].keys() if not k.startswith("_")]
    with open(OUTPUT_FINAL, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=submission_cols, quoting=csv.QUOTE_ALL, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved: {OUTPUT_FINAL} ({len(rows)} rows)")

    # Also save full with debug columns
    all_cols = list(rows[0].keys())
    with open(OUTPUT_QA, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--expand", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if args.generate:
        generate_qa_pairs()
    elif args.expand:
        expand_answers(limit=args.limit)
    else:
        print("Usage:")
        print("  python scripts/19_build_india_dataset.py --generate")
        print("  python scripts/19_build_india_dataset.py --expand --limit 5")
        print("  python scripts/19_build_india_dataset.py --expand")
