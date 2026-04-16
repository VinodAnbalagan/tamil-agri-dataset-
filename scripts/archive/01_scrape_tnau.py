"""
01_scrape_tnau.py
-----------------
Scrapes TNAU Agritech portal leaf pages for Tamil agricultural content.
All URLs verified via link inspection — these are direct static HTML pages,
not JavaScript-rendered index pages.

Saves raw content as JSON to data/01_raw/ with full provenance metadata.

Run from repo root:
    python scripts/01_scrape_tnau.py              # scrape all targets
    python scripts/01_scrape_tnau.py --dry-run    # print targets only
    python scripts/01_scrape_tnau.py --slug <slug> # scrape one target
    python scripts/01_scrape_tnau.py --category <cat> # scrape one category

Output: data/01_raw/<slug>.json per page scraped

URL fix log:
  v1  Initial 58 targets
  v2  fertilizer_banana    -> plant_nutri/banana_nitro.html + banana_pota.html
      fertilizer_chilli    -> horti_vegetables_chilli_fertigation.html
      fertilizer_tomato    -> plant_nutri/tomato_nitro.html + tomato_cal.html
      district_contingency -> paddy_direct_seeding (external redirect, no content)
      coconut_marketing    -> horti_plantation_crops_coconut.html (JS tabs)
  v3  paddy_direct_seeding -> rice_season_varieties (directseedling_ta.html = JS, 0 chars)
                              agri_seasonandvarieties_rice.html confirmed 26,756 chars
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
from datetime import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "http://agritech.tnau.ac.in/",
}

REQUEST_DELAY_SECONDS = 2
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "01_raw")
BASE = "http://agritech.tnau.ac.in"

TARGETS = [

    # =========================================================================
    # CROP DISEASE
    # =========================================================================
    ("rice_disease_blast",
     f"{BASE}/crop_protection/rice_diseases/rice_1.html",
     "crop_disease", "rice", "delta", "samba",
     "TNAU rice blast — leaf blast vs neck blast, symptoms, fungicide schedule"),

    ("rice_disease_brown_spot",
     f"{BASE}/crop_protection/rice_diseases/rice_2.html",
     "crop_disease", "rice", "delta", "samba",
     "TNAU rice brown spot — Helminthosporium, symptoms, management"),

    ("rice_disease_blb",
     f"{BASE}/crop_protection/rice_diseases/rice_3.html",
     "crop_disease", "rice", "delta", "kharif",
     "TNAU bacterial leaf blight — kresek phase, yellowing, copper bactericide"),

    ("rice_disease_sheath_blight",
     f"{BASE}/crop_protection/rice_diseases/rice_6.html",
     "crop_disease", "rice", "delta", "samba",
     "TNAU rice sheath blight — Rhizoctonia, humid conditions, validamycin"),

    ("rice_disease_false_smut",
     f"{BASE}/crop_protection/rice_diseases/rice_7.html",
     "crop_disease", "rice", "delta", "samba",
     "TNAU rice false smut — flowering stage, propiconazole, seed treatment"),

    ("rice_disease_grain_discolouration",
     f"{BASE}/crop_protection/rice_diseases/rice_13.html",
     "crop_disease", "rice", "delta", "all",
     "TNAU rice grain discolouration — harvest stage, storage, causes"),

    ("chilli_diseases",
     f"{BASE}/horticulture/horti_vegetables_chilli_cultural.html",
     "crop_disease", "chilli", "dry_zone", "kharif",
     "TNAU chilli cultural practices — anthracnose, leaf curl, Phytophthora"),

    ("tomato_diseases",
     f"{BASE}/horticulture/horti_vegetables_tomato_cultural%20practices.html",
     "crop_disease", "tomato", "all", "all",
     "TNAU tomato cultural practices — early blight, late blight, wilt"),

    ("turmeric_diseases",
     f"{BASE}/horticulture/horti_spice%20crops_turmeric.html",
     "crop_disease", "turmeric", "western_ghats", "kharif",
     "TNAU turmeric — leaf blotch, rhizome rot, leaf spot — Erode context"),

    ("coconut_disease_management",
     f"{BASE}/horticulture/horti_pcrops_coconut_dis_mgnt.html",
     "crop_disease", "coconut", "coastal", "all",
     "TNAU coconut disease management — bud rot, stem bleeding, root wilt"),

    ("onion_diseases",
     f"{BASE}/horticulture/horti_vegetables_bellaryonion.html",
     "crop_disease", "onion", "dry_zone", "rabi",
     "TNAU onion — purple blotch, thrips, downy mildew, basal rot"),

    ("brinjal_diseases",
     f"{BASE}/horticulture/horti_vegetables_brinjal_cultural.html",
     "crop_disease", "brinjal", "all", "all",
     "TNAU brinjal cultural practices — little leaf, fruit borer, wilt"),

    # =========================================================================
    # PEST CONTROL
    # =========================================================================
    ("coconut_pest_management",
     f"{BASE}/horticulture/horti_pcrops_coconut_pest_dis_mgnt.html",
     "pest_control", "coconut", "coastal", "all",
     "TNAU coconut pest — rhinoceros beetle, red palm weevil, eriophyid mite"),

    ("chilli_pest_management",
     f"{BASE}/horticulture/horti_vegetables_chilli_Manuring.html",
     "pest_control", "chilli", "dry_zone", "kharif",
     "TNAU chilli manuring and pest — mites, thrips, ETL thresholds"),

    ("tomato_pest_management",
     f"{BASE}/horticulture/horti_vegetables_tomato_fertigation.html",
     "pest_control", "tomato", "all", "all",
     "TNAU tomato fertigation and integrated pest management"),

    ("brinjal_pest_management",
     f"{BASE}/horticulture/horti_vegetables_brinjal_manuring.html",
     "pest_control", "brinjal", "all", "all",
     "TNAU brinjal manuring — shoot and fruit borer, ETL, neem oil"),

    ("horticulture_plant_protection",
     f"{BASE}/horticulture/horti_plantprotection_pest.html",
     "pest_control", "all", "all", "all",
     "TNAU horticulture plant protection — cross-crop pest management"),

    ("papaya_pest_management",
     f"{BASE}/horticulture/horti_fruits_papaya.html",
     "pest_control", "papaya", "all", "all",
     "TNAU papaya — mealybug, papaya ring spot virus, fruit fly"),

    # =========================================================================
    # FERTILIZER
    # =========================================================================
    ("fertilizer_rice_min_nutri",
     f"{BASE}/agriculture/agri_min_nutri.html",
     "fertilizer", "rice", "delta", "all",
     "TNAU micronutrient management for rice — zinc, boron, soil application"),

    ("fertilizer_banana_nitrogen",
     f"{BASE}/horticulture/plant_nutri/banana_nitro.html",
     "fertilizer", "banana", "delta", "all",
     "TNAU banana nitrogen deficiency — yellowing, foliar spray, split dose"),

    ("fertilizer_banana_potassium",
     f"{BASE}/horticulture/plant_nutri/banana_pota.html",
     "fertilizer", "banana", "delta", "all",
     "TNAU banana potassium deficiency — bunch filling, leaf edge burn, K dose"),

    ("fertilizer_chilli_fertigation",
     f"{BASE}/horticulture/horti_vegetables_chilli_fertigation.html",
     "fertilizer", "chilli", "dry_zone", "kharif",
     "TNAU chilli fertigation — drip NPK schedule, Ca/Mg for fruit set"),

    ("fertilizer_turmeric",
     f"{BASE}/horticulture/horti_spice_fert_turmeric.html",
     "fertilizer", "turmeric", "western_ghats", "kharif",
     "TNAU turmeric fertilizer schedule — NPK split doses, FYM, Erode"),

    ("fertilizer_coconut",
     f"{BASE}/horticulture/horti_plantation_fert_coconut.html",
     "fertilizer", "coconut", "coastal", "all",
     "TNAU coconut fertilizer — NPK by age, green manure, potash timing"),

    ("fertilizer_onion",
     f"{BASE}/horticulture/horti_veg_fert_onion.html",
     "fertilizer", "onion", "dry_zone", "rabi",
     "TNAU onion fertilizer — NPK splits, sulphur, fertigation"),

    ("fertilizer_tomato_nitrogen",
     f"{BASE}/horticulture/plant_nutri/tomato_nitro.html",
     "fertilizer", "tomato", "all", "all",
     "TNAU tomato nitrogen — deficiency symptoms, top-dress timing"),

    ("fertilizer_tomato_calcium",
     f"{BASE}/horticulture/plant_nutri/tomato_cal.html",
     "fertilizer", "tomato", "all", "all",
     "TNAU tomato calcium — blossom end rot, foliar spray, soil pH link"),

    ("organic_farming_index",
     f"{BASE}/org_farm/orgfarm_index.html",
     "fertilizer", "all", "all", "all",
     "TNAU organic farming — Panchagavya, vermicompost, green manure rates"),

    # =========================================================================
    # IRRIGATION
    # =========================================================================
    ("coconut_drip_irrigation",
     f"{BASE}/horticulture/horti_pcrops_coconut_drip.html",
     "irrigation", "coconut", "coastal", "summer",
     "TNAU coconut drip irrigation — schedule, water saving, fertigation"),

    ("coconut_water_management",
     f"{BASE}/horticulture/horti_pcrops_coconut_watermgnt.html",
     "irrigation", "coconut", "coastal", "all",
     "TNAU coconut water management — basin irrigation, seasonal schedules"),

    ("brinjal_irrigation",
     f"{BASE}/horticulture/horti_vegetables_brinjal_irrigation.html",
     "irrigation", "brinjal", "all", "all",
     "TNAU brinjal irrigation — furrow, drip, critical water stages"),

    ("tomato_irrigation",
     f"{BASE}/horticulture/horti_vegetables_tomato_irrigation.html",
     "irrigation", "tomato", "all", "all",
     "TNAU tomato irrigation — critical stages, drip scheduling, water stress"),

    ("turmeric_irrigation",
     f"{BASE}/horticulture/horti_spice_turmeric_manuring.html",
     "irrigation", "turmeric", "western_ghats", "kharif",
     "TNAU turmeric irrigation — sprinkler vs flood, water requirement"),

    ("disaster_management_drought",
     f"{BASE}/agriculture/agri_majorareas_disastermgt.html",
     "irrigation", "all", "all", "all",
     "TNAU disaster management — drought and flood crop protocols"),

    ("micro_irrigation_schemes",
     f"{BASE}/horticulture/horti_schemes_microirrigation.html",
     "irrigation", "all", "all", "all",
     "TNAU micro irrigation schemes — drip subsidy, sprinkler, government support"),

    # =========================================================================
    # WEATHER ADVISORY
    # v3 fix: paddy_direct_seeding (JS, 0 chars) -> rice_season_varieties
    #         agri_seasonandvarieties_rice.html confirmed 26,756 chars
    # =========================================================================
    ("agro_meteorological_advisory",
     f"{BASE}/agrometeorologicaladvisory/agro_meteorological_advisory.html",
     "weather_advisory", "all", "all", "all",
     "TNAU agro-meteorological advisory — district weather-based crop advisory"),

    ("dryland_technologies",
     f"{BASE}/drylandtechnologies.html",
     "weather_advisory", "all", "dry_zone", "kharif",
     "TNAU dryland technologies — drought-tolerant varieties, conservation"),

    ("turmeric_soil_management",
     f"{BASE}/horticulture/horti_spice_turmeric_soil.html",
     "weather_advisory", "turmeric", "western_ghats", "kharif",
     "TNAU turmeric soil — drainage in heavy rain, raised bed, waterlogging"),

    # FIXED v3: was directseedling_ta.html (JS-rendered, 0 chars)
    ("rice_season_varieties",
     f"{BASE}/agriculture/agri_seasonandvarieties_rice.html",
     "weather_advisory", "rice", "delta", "all",
     "TNAU rice seasons and varieties — kuruvai/samba/thaladi windows, districts, variety selection"),

    ("chilli_season_sowing",
     f"{BASE}/horticulture/horti_vegetables_chili_season_sowing.html",
     "weather_advisory", "chilli", "dry_zone", "kharif",
     "TNAU chilli season and sowing — optimal window, rain dependency"),

    ("brinjal_season",
     f"{BASE}/horticulture/horti_vegetables_brinjal_season.html",
     "weather_advisory", "brinjal", "all", "all",
     "TNAU brinjal season — sowing calendar, monsoon timing, heat tolerance"),

    # =========================================================================
    # MARKET PRICE
    # =========================================================================
    ("agri_marketing_index",
     f"{BASE}/agricultural_marketing/agrimark_index.html",
     "market_price", "all", "all", "all",
     "TNAU agricultural marketing — price discovery, AGMARKNET, regulated markets"),

    ("govt_schemes_services",
     f"{BASE}/govt_schemes_services/govt_serv_schems.html",
     "market_price", "all", "all", "all",
     "TNAU government schemes — PM-KISAN, KCC, FPO, crop insurance overview"),

    ("minimum_support_price",
     f"{BASE}/msp.html",
     "market_price", "all", "all", "all",
     "TNAU MSP — minimum support prices for major crops"),

    ("crop_insurance",
     f"{BASE}/crop_insurance/crop_ins.html",
     "market_price", "all", "all", "rainy",
     "TNAU crop insurance — PMFBY, enrollment, claim process"),

    ("farmers_producer_organisation",
     f"{BASE}/farm_association/farm_asso_organi.html",
     "market_price", "all", "all", "all",
     "TNAU FPO — collective marketing, price negotiation, registration"),

    ("coconut_main_page",
     f"{BASE}/horticulture/horti_plantation%20crops_coconut.html",
     "market_price", "coconut", "coastal", "all",
     "TNAU coconut overview — varieties, yield, value addition, marketing"),

    # =========================================================================
    # GOVERNMENT SCHEMES
    # =========================================================================
    ("banking_credit",
     f"{BASE}/banking/credit_bank.html",
     "government_schemes", "all", "all", "all",
     "TNAU banking and credit — KCC, agricultural loans, interest subvention"),

    ("women_in_agriculture",
     f"{BASE}/women_in_agri/women_empowerment.html",
     "government_schemes", "all", "all", "all",
     "TNAU women in agriculture — SHG, Mahalir Thittam, NABARD schemes"),

    ("kisan_call_centre",
     f"{BASE}/kisan/kisan.html",
     "government_schemes", "all", "all", "all",
     "TNAU Kisan Call Centre — 1800-180-1551, query categories, escalation"),

    ("kvk_services",
     f"{BASE}/kvk/kvk.html",
     "government_schemes", "all", "all", "all",
     "TNAU KVK — district extension, soil testing, training programmes"),

    # =========================================================================
    # SOIL HEALTH
    # =========================================================================
    ("sustainable_agriculture",
     f"{BASE}/sustainable_agri/susagri.html",
     "soil_health", "all", "all", "all",
     "TNAU sustainable agriculture — soil health cards, organic inputs, SRI"),

    ("coconut_soil_planting",
     f"{BASE}/horticulture/horti_pcrops_coconut_soilplanting.html",
     "soil_health", "coconut", "coastal", "all",
     "TNAU coconut soil — laterite, sandy loam, pit preparation"),

    ("turmeric_soil",
     f"{BASE}/horticulture/horti_spice_turmeric_soil.html",
     "soil_health", "turmeric", "western_ghats", "all",
     "TNAU turmeric soil — loamy, well-drained, pH 5.5-7.0"),

    # =========================================================================
    # HARVEST TIMING
    # =========================================================================
    ("turmeric_harvest",
     f"{BASE}/horticulture/horti_spice_turmeric_harvest.html",
     "harvest_timing", "turmeric", "western_ghats", "all",
     "TNAU turmeric harvest — maturity indicators, curing, boiling, polishing"),

    ("coconut_harvest",
     f"{BASE}/horticulture/horti_pcrops_coconut_harvest.html",
     "harvest_timing", "coconut", "coastal", "all",
     "TNAU coconut harvest — maturity, copra drying, nut interval, post-harvest"),

    ("tomato_seed_varieties",
     f"{BASE}/horticulture/horti_vegetables_tomato_varieties.html",
     "harvest_timing", "tomato", "all", "all",
     "TNAU tomato varieties — days to harvest, yield, disease resistance"),

    # =========================================================================
    # LIVESTOCK / SERICULTURE / AQUACULTURE
    # =========================================================================
    ("animal_husbandry_index",
     f"{BASE}/animal_husbandry/animhus_index.html",
     "livestock_dairy", "cattle", "all", "all",
     "TNAU animal husbandry — cattle management, disease, vaccination"),

    ("sericulture_index",
     f"{BASE}/sericulture/seri_index.html",
     "sericulture", "silkworm", "all", "all",
     "TNAU sericulture — silkworm rearing, mulberry cultivation, disease"),

    ("fishery_index",
     f"{BASE}/fishery/fish_index.html",
     "aquaculture", "all", "coastal", "all",
     "TNAU fishery — inland fish, shrimp, pond management, water quality"),

]

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def fetch_page(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.encoding = "utf-8"
        return r.status_code, r.text
    except requests.RequestException as e:
        print(f"    ERROR: {e}")
        return 0, ""


def extract_text(html):
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    lines, seen = [], set()
    for tag in soup.find_all(["p", "li", "td", "h2", "h3", "h4", "h5"]):
        text = tag.get_text(separator=" ", strip=True)
        if len(text) > 50 and text not in seen:
            lines.append(text)
            seen.add(text)
    return "\n\n".join(lines)


def save_raw(slug, url, category, crop, region_hint,
             season_hint, description, raw_text, status_code):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    doc = {
        "slug": slug, "source_url": url, "category": category,
        "crop_primary": crop, "region_hint": region_hint,
        "season_hint": season_hint, "description": description,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "http_status": status_code, "char_count": len(raw_text),
        "raw_text": raw_text,
    }
    filepath = os.path.join(OUTPUT_DIR, f"{slug}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    return filepath


def scrape_all(targets, dry_run=False):
    from collections import Counter
    print(f"\n{'='*65}")
    print(f"  TNAU Scraper — {len(targets)} targets")
    print(f"  Output → {os.path.abspath(OUTPUT_DIR)}")
    print(f"  Delay  → {REQUEST_DELAY_SECONDS}s between requests")
    print(f"{'='*65}\n")
    cats = Counter(t[2] for t in targets)
    print("  Targets by category:")
    for cat, count in sorted(cats.items()):
        print(f"    {cat:<25} {count} pages")
    print()

    results = []
    for i, (slug, url, category, crop, region_hint, season_hint, description) in enumerate(targets, 1):
        print(f"[{i:02d}/{len(targets)}] {slug}")
        print(f"         {category} | {crop} | {region_hint} | {season_hint}")
        print(f"         {url}")
        if dry_run:
            print("         DRY RUN — skipping\n")
            continue

        status, html = fetch_page(url)
        if status == 0:
            print("         FAILED\n"); results.append((slug, "FAILED", 0)); continue
        if status == 403:
            print("         BLOCKED\n"); results.append((slug, "BLOCKED", 0)); continue
        if status == 404:
            print("         NOT FOUND\n"); results.append((slug, "NOT_FOUND", 0)); continue
        if status != 200:
            print(f"         HTTP {status}\n"); results.append((slug, f"HTTP_{status}", 0)); continue

        raw_text = extract_text(html)
        char_count = len(raw_text)
        if char_count < 200:
            print(f"         WARNING — only {char_count} chars")
        filepath = save_raw(slug, url, category, crop, region_hint,
                            season_hint, description, raw_text, status)
        print(f"         OK — {char_count:,} chars → {os.path.basename(filepath)}")
        results.append((slug, "OK", char_count))
        time.sleep(REQUEST_DELAY_SECONDS)
        print()

    if not dry_run:
        print(f"\n{'='*65}\n  SUMMARY\n{'='*65}")
        ok        = [r for r in results if r[1] == "OK"]
        not_found = [r for r in results if r[1] == "NOT_FOUND"]
        blocked   = [r for r in results if r[1] == "BLOCKED"]
        failed    = [r for r in results if r[1] == "FAILED"]
        low       = [r for r in ok if r[2] < 500]
        print(f"  OK          : {len(ok)}")
        print(f"  NOT FOUND   : {len(not_found)}")
        print(f"  BLOCKED     : {len(blocked)}")
        print(f"  FAILED      : {len(failed)}")
        if low:
            print(f"\n  LOW CONTENT (< 500 chars):")
            for s, _, c in low: print(f"    - {s} ({c} chars)")
        if not_found:
            print(f"\n  NOT FOUND:")
            for s, _, _ in not_found: print(f"    - {s}")
        print()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--slug", type=str, default=None)
    parser.add_argument("--category", type=str, default=None)
    args = parser.parse_args()

    targets = TARGETS
    if args.slug:
        targets = [t for t in TARGETS if t[0] == args.slug]
        if not targets:
            print(f"No slug '{args.slug}'. Available: {[t[0] for t in TARGETS]}")
    elif args.category:
        targets = [t for t in TARGETS if t[2] == args.category]
        if not targets:
            print(f"No category '{args.category}'.")

    scrape_all(targets, dry_run=args.dry_run)
