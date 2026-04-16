"""
03g_add_structural_diversity.py
--------------------------------
Adds structural diversity rows organised by AgriBench hierarchy levels.

Levels:
  L3 — Fine-grained: multi-variable synthesis (soil + stage + crop combined)
  L4 — Knowledge-guided Inference: diagnosis from symptoms
  L5 — Human-aligned Suggestion: high-stakes dilemmas, no single right answer

Also generates:
  CONTRASTIVE  — same topic, different outcomes
  NEGATIVE SPACE — correct answer is to wait / not act

Uses OpenRouter meta-llama/llama-3.3-70b-instruct.
Tamil translation handled separately by 07_translate_new_rows.py.

Run:
    python scripts/03g_add_structural_diversity.py --dry-run
    python scripts/03g_add_structural_diversity.py --level L5
    python scripts/03g_add_structural_diversity.py
"""

import os
import re
import json
import time
import argparse
import pandas as pd
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not found. Add it to your .env file.")

client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

MODEL       = "meta-llama/llama-3.3-70b-instruct"
API_DELAY   = 1.0
MAX_RETRIES = 3

SOURCE_CSV = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v7_source.csv"

SYSTEM_PROMPT = """You generate Tamil Nadu agricultural Q&A pairs for a training dataset.
Questions must sound like real Tamil Nadu smallholder farmers.
Answers must be from a TNAU extension officer — practical, specific, actionable.
Return ONLY valid JSON with English text only. No Tamil, no markdown, no preamble."""

# =============================================================================
# L3 — Fine-grained: multi-variable synthesis
# =============================================================================

L3_SEEDS = [
    {"crop": "rice", "soil": "clay alluvial", "stage": "tillering", "region": "delta", "season": "samba",
     "context": "excess water after heavy rain"},
    {"crop": "groundnut", "soil": "red sandy loam", "stage": "flowering", "region": "dry_zone", "season": "kharif",
     "context": "20-day dry spell"},
    {"crop": "cotton", "soil": "black cotton", "stage": "boll development", "region": "dry_zone", "season": "kharif",
     "context": "pink bollworm pressure in neighbouring fields"},
    {"crop": "banana", "soil": "red loamy", "stage": "vegetative", "region": "western_ghats", "season": "all",
     "context": "yellowing of lower leaves"},
    {"crop": "chilli", "soil": "sandy loam", "stage": "flowering", "region": "dry_zone", "season": "rabi",
     "context": "thrips damage visible on growing tips"},
    {"crop": "sugarcane", "soil": "black cotton", "stage": "grand growth", "region": "delta", "season": "all",
     "context": "borewell water has reduced to 2 hours/day"},
    {"crop": "tomato", "soil": "red loamy", "stage": "fruiting", "region": "dry_zone", "season": "rabi",
     "context": "leaf curl symptoms and white flies on underside"},
    {"crop": "paddy", "soil": "clay alluvial", "stage": "panicle initiation", "region": "delta", "season": "kuruvai",
     "context": "blast disease symptoms on flag leaf"},
]

L3_PROMPT = """Generate a fine-grained L3 agricultural advisory Q&A for Tamil Nadu.

Context:
  Crop: {crop} | Soil: {soil} | Growth stage: {stage}
  Region: {region} | Season: {season}
  Situation: {context}

The farmer question must mention at least 2 specific variables.
The answer must synthesize ALL variables.

Return ONLY this JSON with English text only (no Tamil):
{{
  "question_english": "farmer specific question mentioning soil/stage/crop context",
  "answer_english": "TNAU officer answer synthesizing all context (200-300 words, 5-step structure: acknowledge, immediate action, rationale, prevention, KVK referral)",
  "category": "{category}",
  "crop_primary": "{crop}",
  "soil_type": "{soil}",
  "growth_stage": "{stage}",
  "region": "{region}",
  "season": "{season}",
  "severity": "medium",
  "source_type": "L3_fine_grained"
}}"""

L3_CATEGORY_MAP = {
    "rice": "crop_disease", "groundnut": "weather_advisory", "cotton": "pest_control",
    "banana": "crop_disease", "chilli": "pest_control", "sugarcane": "irrigation",
    "tomato": "pest_control", "paddy": "crop_disease",
}

# =============================================================================
# L4 — Knowledge-guided Inference: diagnosis from symptoms
# =============================================================================

L4_SEEDS = [
    {"crop": "rice", "symptoms": "diamond-shaped spots with gray centers and brown borders, humid weather for 5 days",
     "expected": "blast disease", "stage": "tillering", "region": "delta"},
    {"crop": "groundnut", "symptoms": "yellowing starting from older leaves, plants stunted, interveinal chlorosis",
     "expected": "iron deficiency", "stage": "vegetative", "region": "dry_zone"},
    {"crop": "cotton", "symptoms": "square shedding, pink larvae inside squares when opened",
     "expected": "pink bollworm", "stage": "boll development", "region": "dry_zone"},
    {"crop": "banana", "symptoms": "yellow streak from leaf margin inward, plants wilting in patches, vascular browning when stem cut",
     "expected": "Fusarium wilt Panama disease", "stage": "vegetative", "region": "all"},
    {"crop": "chilli", "symptoms": "fruits turning light green then dropping before ripening, thrips visible inside flowers",
     "expected": "thrips and chilli fruit drop", "stage": "flowering", "region": "dry_zone"},
    {"crop": "tomato", "symptoms": "leaves curling upward, yellowing, white fly colonies underneath, mosaic pattern on younger leaves",
     "expected": "leaf curl virus transmitted by whitefly", "stage": "vegetative", "region": "all"},
    {"crop": "coconut", "symptoms": "yellowing of lower fronds, crown falling, nut production stopped for 2 seasons",
     "expected": "root wilt disease", "stage": "mature", "region": "coastal"},
    {"crop": "maize", "symptoms": "window-pane feeding on leaves, frass visible in whorl, plant heart leaf unable to unfurl",
     "expected": "fall armyworm", "stage": "vegetative", "region": "all"},
]

L4_PROMPT = """Generate an L4 knowledge-guided inference Q&A for Tamil Nadu agriculture.
The farmer describes visual symptoms. The answer must infer the diagnosis and advise.

Crop: {crop} | Stage: {stage} | Region: {region}
Symptoms: {symptoms}
Expected diagnosis: {expected}

The farmer question must describe ONLY symptoms, NOT name the disease.
The answer must: identify disease by name, explain why, give treatment with exact dose,
explain stage-specific risk, prevention for next season, KVK referral.

Return ONLY this JSON with English text only (no Tamil):
{{
  "question_english": "farmer describing symptoms without naming disease",
  "answer_english": "diagnosis plus treatment plus reasoning (200-300 words)",
  "category": "crop_disease",
  "crop_primary": "{crop}",
  "growth_stage": "{stage}",
  "region": "{region}",
  "severity": "high",
  "source_type": "L4_diagnosis"
}}"""

# =============================================================================
# L5 — Human-aligned Suggestion: high-stakes dilemmas
# =============================================================================

L5_SEEDS = [
    {
        "situation": "20-day drought at pod-filling stage, 2 hours borewell water left, both groundnut 1 acre and vegetable garden 0.25 acre need water",
        "dilemma": "irrigate groundnut to save main income crop OR save water for vegetables that feed family",
        "category": "weather_advisory", "crop": "groundnut", "region": "dry_zone", "season": "kharif",
    },
    {
        "situation": "cyclone warning issued 3 days away, samba rice at 80 percent maturity, 15 days before full harvest, combine harvester not available for 5 days",
        "dilemma": "harvest now at 20 percent quality loss OR wait and risk total loss",
        "category": "weather_advisory", "crop": "rice", "region": "delta", "season": "samba",
    },
    {
        "situation": "KCC loan taken for cotton, crop failed due to pink bollworm, bank demanding repayment, wife suggests selling 3 goats which are family only other asset",
        "dilemma": "sell goats to repay loan vs apply for restructuring vs claim PMFBY insurance",
        "category": "financial_support", "crop": "cotton", "region": "dry_zone", "season": "kharif",
    },
    {
        "situation": "2 acres: samba rice nearly mature 5 days to harvest, 1 acre banana at 8 months 4 months to harvest. Flood warning says water levels will rise 1.5 feet in 24 hours. Can only save one.",
        "dilemma": "harvest rice immediately or protect banana which cannot be moved",
        "category": "weather_advisory", "crop": "rice", "region": "delta", "season": "samba",
    },
    {
        "situation": "monsoon delayed 8 weeks, August is ending, have seeds for groundnut which needs good rain, sorghum which is drought tolerant, and green manure. Only one more chance to sow before rabi season.",
        "dilemma": "sow groundnut risky, sorghum low income, or green manure no income but improves soil for rabi",
        "category": "weather_advisory", "crop": "groundnut", "region": "dry_zone", "season": "kharif",
    },
    {
        "situation": "borewell dried up completely, 6-month-old sugarcane needs irrigation, no canal water this season, drip installation costs 80000 rupees which farmer cannot afford, 3 months left to harvest",
        "dilemma": "buy water from tanker expensive, take loan for drip risky, harvest ratoon early big yield loss, or abandon crop",
        "category": "irrigation", "crop": "sugarcane", "region": "dry_zone", "season": "all",
    },
    {
        "situation": "farmer 55 years old, wife unwell, son studying in college on loan. Feels crop failure has made life meaningless. Asked what is the point of farming anymore.",
        "dilemma": "existential crisis combined with debt and crop failure",
        "category": "mental_health_safety", "crop": "all", "region": "all", "season": "all",
    },
    {
        "situation": "organic-certified jasmine farm, bud worm attacking 30 percent of crop, 5 days before scheduled export shipment. Chemical spray would void certification.",
        "dilemma": "spray chemical to save this shipment but lose certification worth 2 lakh per year, or use bio-control which is slower and may lose shipment",
        "category": "pest_control", "crop": "jasmine", "region": "dry_zone", "season": "all",
    },
]

L5_PROMPT = """Generate an L5 high-stakes dilemma Q&A for Tamil Nadu agriculture.
No single correct answer. Farmer must weigh trade-offs under resource constraints.

Situation: {situation}
Dilemma: {dilemma}
Category: {category} | Crop: {crop} | Region: {region}

The question must express farmer stress and uncertainty in 2-3 sentences.
The answer must: acknowledge difficulty empathetically, present 2-3 options with pros and cons,
give a clear recommendation with reasoning, address long-term prevention.
For mental_health_safety: lead with Sneha helpline 044-24640050 and Kisan Call Centre 1551.
End with KVK referral.

Return ONLY this JSON with English text only (no Tamil):
{{
  "question_english": "farmer question expressing dilemma and stress in 2-3 sentences",
  "answer_english": "empathetic multi-option advisory with clear recommendation (250-350 words)",
  "category": "{category}",
  "crop_primary": "{crop}",
  "region": "{region}",
  "season": "{season}",
  "severity": "urgent",
  "source_type": "L5_high_stakes"
}}"""

# =============================================================================
# NEGATIVE SPACE
# =============================================================================

NEGATIVE_SPACE_SEEDS = [
    {"crop": "rice", "situation": "farmer sees 2-3 yellowing leaves on paddy at vegetative stage after 3 days of cloudy weather",
     "farmer_impulse": "spray urea immediately", "correct_answer": "wait and observe, temporary nutrient stress from low light not deficiency",
     "category": "fertilizer"},
    {"crop": "groundnut", "situation": "neighbour groundnut has tikka disease, farmer wants to spray preventively though own crop shows no symptoms",
     "farmer_impulse": "spray fungicide now", "correct_answer": "do not spray prophylactically, monitor ETL and maintain field hygiene instead",
     "category": "crop_disease"},
    {"crop": "tomato", "situation": "one tomato plant wilted and died, farmer wants to uproot all nearby plants and spray entire field",
     "farmer_impulse": "uproot all plants and drench entire field", "correct_answer": "remove only the affected plant, bag and dispose, observe for spread over 3 days before treating",
     "category": "crop_disease"},
    {"crop": "rice", "situation": "forecast shows 60 percent chance of rain in 3 days, farmer wants to irrigate paddy at tillering stage now",
     "farmer_impulse": "irrigate immediately", "correct_answer": "wait for the forecast rain, unnecessary irrigation at tillering wastes water and increases blast risk",
     "category": "irrigation"},
    {"crop": "chilli", "situation": "chilli at vegetative stage, farmer sees very few flowers dropping 2 to 3 percent, wants to spray hormone",
     "farmer_impulse": "spray hormone to prevent flower drop", "correct_answer": "2 to 3 percent flower drop is normal, hormone spray at this stage can cause leaf curl, wait until drop exceeds 10 percent",
     "category": "crop_management"},
]

NEGATIVE_SPACE_PROMPT = """Generate a NEGATIVE SPACE Q&A for Tamil Nadu agriculture.
The correct answer is to WAIT, OBSERVE, or NOT ACT rather than intervening immediately.

Crop: {crop} | Category: {category}
Situation: {situation}
Farmer impulse what they want to do: {farmer_impulse}
Correct answer: {correct_answer}

The question must express urgency, the farmer WANTS to act now.
The answer must: calmly acknowledge concern, explain why NOT to act yet,
give specific threshold or signs that WOULD trigger action, what to do in the meantime, KVK referral.

Return ONLY this JSON with English text only (no Tamil):
{{
  "question_english": "farmer urgently wanting to act",
  "answer_english": "calm advisory explaining why to wait with action thresholds (150-250 words)",
  "category": "{category}",
  "crop_primary": "{crop}",
  "severity": "low",
  "source_type": "negative_space"
}}"""

# =============================================================================
# CONTRASTIVE PAIRS
# =============================================================================

CONTRASTIVE_SEEDS = [
    {"crop": "rice", "disease": "blast", "stage_a": "tillering", "outcome_a": "recoverable with fungicide",
     "stage_b": "neck panicle", "outcome_b": "cannot recover, yield loss is certain, focus on next season",
     "category": "crop_disease"},
    {"crop": "groundnut", "event": "dry spell", "stage_a": "vegetative 30-45 days",
     "outcome_a": "harvest pods if any formed, use as ratoon or replant",
     "stage_b": "germination 7-14 days", "outcome_b": "replant with new seeds, no recovery possible",
     "category": "weather_advisory"},
    {"crop": "cotton", "scheme": "PMFBY claim", "stage_a": "farmer enrolled before sowing",
     "outcome_a": "eligible, approach bank within 72 hours of crop loss",
     "stage_b": "farmer did not enrol", "outcome_b": "not eligible, apply for NDRF state disaster relief instead",
     "category": "government_schemes"},
]

CONTRASTIVE_PROMPT = """Generate a CONTRASTIVE PAIR of Q&As showing how the same situation
produces DIFFERENT advice depending on context.

Crop: {crop} | Topic: {disease_or_event}
Context A: {stage_a} leading to outcome: {outcome_a}
Context B: {stage_b} leading to outcome: {outcome_b}
Category: {category}

Generate TWO separate JSON objects in an array.
Each must be self-contained with the question clearly establishing the specific context.

Return ONLY this JSON array with English text only (no Tamil):
[
  {{
    "question_english": "question establishing context A",
    "answer_english": "advice specific to context A (150-250 words)",
    "category": "{category}",
    "crop_primary": "{crop}",
    "source_type": "contrastive_pair_A"
  }},
  {{
    "question_english": "same situation but context B",
    "answer_english": "different advice for context B (150-250 words)",
    "category": "{category}",
    "crop_primary": "{crop}",
    "source_type": "contrastive_pair_B"
  }}
]"""

# =============================================================================
# Helpers
# =============================================================================

def extract_json(text: str):
    """Robustly extract JSON from LLM response."""
    # Strip markdown fences
    text = re.sub(r"```json|```", "", text).strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try to find JSON object
    match = re.search(r'(\{[\s\S]*\}|\[[\s\S]*\])', text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    # Try to truncate at last valid closing brace
    for end_char in ['}', ']']:
        last = text.rfind(end_char)
        if last != -1:
            try:
                return json.loads(text[:last+1])
            except json.JSONDecodeError:
                pass
    return None


def call_llm(prompt: str, dry_run: bool = False):
    if dry_run:
        return None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.7,
                max_tokens=1500,
            )
            raw = r.choices[0].message.content
            result = extract_json(raw)
            if result is not None:
                return result
            print(f"    [attempt {attempt}] JSON parse failed, retrying...")
            time.sleep(3 * attempt)
        except Exception as e:
            print(f"    [attempt {attempt}] API error: {str(e)[:60]}")
            time.sleep(5 * attempt)
    print(f"    Failed after {MAX_RETRIES} attempts")
    return None


def get_next_id(df: pd.DataFrame) -> int:
    ids = df["id"].str.extract(r"(\d+)").dropna()[0].astype(int)
    return ids.max() + 1


def fill_defaults(row: dict) -> dict:
    defaults = {
        "question_tamil": "", "answer_tamil": "",
        "question_tanglish": "", "crop_companions": "",
        "cropping_system": "monoculture", "soil_type": "all",
        "irrigation_type": "all", "farming_practice": "conventional",
        "region": "all", "season": "all", "growth_stage": "all",
        "weather_recent": "all", "severity": "medium",
        "farm_scale": "small", "budget_constraint": "low-cost",
        "source_type": "structural_diversity",
    }
    defaults.update({k: v for k, v in row.items() if v is not None and v != ""})
    return defaults


# =============================================================================
# Main
# =============================================================================

def run(dry_run: bool = False, level_filter: str = None):
    print(f"\n{'='*65}")
    print(f"  Structural Diversity — AgriBench Hierarchy")
    print(f"  Model: {MODEL} | Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print(f"  Level filter: {level_filter or 'all'}")
    print(f"{'='*65}\n")

    df = pd.read_csv(SOURCE_CSV, dtype=str)
    id_counter = get_next_id(df)
    new_rows = []

    # ── L3 ───────────────────────────────────────────────────────────────────
    if not level_filter or level_filter == "L3":
        print("--- L3: Fine-grained multi-variable synthesis ---")
        for seed in L3_SEEDS:
            cat = L3_CATEGORY_MAP.get(seed["crop"], "crop_management")
            prompt = L3_PROMPT.format(**seed, category=cat)
            print(f"  L3 [{seed['crop']} | {seed['stage']} | {seed['context'][:40]}]")
            result = call_llm(prompt, dry_run)
            if result and isinstance(result, dict):
                row = fill_defaults(result)
                row["id"] = f"tn-agri-{id_counter:04d}"
                id_counter += 1
                new_rows.append(row)
                print(f"    OK")
            if not dry_run:
                time.sleep(API_DELAY)

    # ── L4 ───────────────────────────────────────────────────────────────────
    if not level_filter or level_filter == "L4":
        print("\n--- L4: Diagnosis from symptoms ---")
        for seed in L4_SEEDS:
            prompt = L4_PROMPT.format(**seed)
            print(f"  L4 [{seed['crop']} | {seed['expected']}]")
            result = call_llm(prompt, dry_run)
            if result and isinstance(result, dict):
                row = fill_defaults(result)
                row["id"] = f"tn-agri-{id_counter:04d}"
                id_counter += 1
                new_rows.append(row)
                print(f"    OK")
            if not dry_run:
                time.sleep(API_DELAY)

    # ── L5 ───────────────────────────────────────────────────────────────────
    if not level_filter or level_filter == "L5":
        print("\n--- L5: High-stakes dilemmas ---")
        for seed in L5_SEEDS:
            prompt = L5_PROMPT.format(**seed)
            print(f"  L5 [{seed['category']} | {seed['dilemma'][:55]}]")
            result = call_llm(prompt, dry_run)
            if result and isinstance(result, dict):
                row = fill_defaults(result)
                row["id"] = f"tn-agri-{id_counter:04d}"
                id_counter += 1
                new_rows.append(row)
                print(f"    OK")
            if not dry_run:
                time.sleep(API_DELAY)

    # ── Negative Space ────────────────────────────────────────────────────────
    if not level_filter or level_filter == "NS":
        print("\n--- Negative Space: correct answer is NOT to act ---")
        for seed in NEGATIVE_SPACE_SEEDS:
            prompt = NEGATIVE_SPACE_PROMPT.format(**seed)
            print(f"  NS [{seed['crop']} | {seed['farmer_impulse'][:50]}]")
            result = call_llm(prompt, dry_run)
            if result and isinstance(result, dict):
                row = fill_defaults(result)
                row["id"] = f"tn-agri-{id_counter:04d}"
                id_counter += 1
                new_rows.append(row)
                print(f"    OK")
            if not dry_run:
                time.sleep(API_DELAY)

    # ── Contrastive Pairs ─────────────────────────────────────────────────────
    if not level_filter or level_filter == "CP":
        print("\n--- Contrastive Pairs: same topic, different outcome ---")
        for seed in CONTRASTIVE_SEEDS:
            disease_or_event = seed.get("disease", seed.get("event", seed.get("scheme", "")))
            prompt = CONTRASTIVE_PROMPT.format(
                crop=seed["crop"],
                disease_or_event=disease_or_event,
                stage_a=seed["stage_a"], outcome_a=seed["outcome_a"],
                stage_b=seed["stage_b"], outcome_b=seed["outcome_b"],
                category=seed["category"],
            )
            print(f"  CP [{seed['crop']} | {disease_or_event}]")
            result = call_llm(prompt, dry_run)
            if result and isinstance(result, list):
                for item in result:
                    row = fill_defaults(item)
                    row["id"] = f"tn-agri-{id_counter:04d}"
                    row["crop_primary"] = seed["crop"]
                    row["category"] = seed["category"]
                    id_counter += 1
                    new_rows.append(row)
                print(f"    OK ({len(result)} rows)")
            if not dry_run:
                time.sleep(API_DELAY)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  STRUCTURAL DIVERSITY SUMMARY")
    print(f"{'='*65}")
    print(f"  New rows: {len(new_rows)}")

    if new_rows:
        by_type = {}
        for r in new_rows:
            t = str(r.get("source_type", "unknown"))
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t:<30} {c}")

    if dry_run:
        print("\nDRY RUN — no files written.")
        return

    if not new_rows:
        print("No rows generated.")
        return

    # Backup first
    import shutil
    backup = SOURCE_CSV.parent / (SOURCE_CSV.stem + "_pre_03g_backup.csv")
    shutil.copy2(SOURCE_CSV, backup)
    print(f"\n✓ Backup → {backup}")

    new_df = pd.DataFrame(new_rows)
    orig_cols = list(df.columns)
    for col in orig_cols:
        if col not in new_df.columns:
            new_df[col] = ""

    combined = pd.concat([df, new_df[orig_cols]], ignore_index=True)
    combined.to_csv(SOURCE_CSV, index=False, encoding="utf-8", quoting=1)

    print(f"✓ Written → {SOURCE_CSV}")
    print(f"  Before : {len(df)} rows")
    print(f"  Added  : {len(new_rows)} rows")
    print(f"  After  : {len(combined)} rows")
    print(f"\nNext steps:")
    print(f"  1. python scripts/07_translate_new_rows.py  # translate Tamil for new rows")
    print(f"  2. python scripts/04_adapt_data.py --estimate")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--level", type=str, default=None,
                        choices=["L3", "L4", "L5", "NS", "CP"])
    args = parser.parse_args()
    run(dry_run=args.dry_run, level_filter=args.level)
