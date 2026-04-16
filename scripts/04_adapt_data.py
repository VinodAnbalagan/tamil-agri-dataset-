"""
04_adapt_data.py
----------------
Runs the v10 clean CSV through the Adaption Adaptive Data platform.

v10 configuration:
  - prompt:      question_tamil_v9    — context-injected prompts
  - completion:  answer_tamil_v10     — fully expanded native Tamil answers
  - input:       tamil_agri_advisory_v10_clean.csv
  - prompt_rephrase: ON — platform enriches context-tagged prompts
  - reasoning_traces: ON
  - deduplication: ON
  - context: 15 columns including reasoning_type

Run from repo root:
    python scripts/04_adapt_data.py --estimate        # cost check, no run
    python scripts/04_adapt_data.py --test            # 10 rows only
    python scripts/04_adapt_data.py                   # full run
    python scripts/04_adapt_data.py --dataset-id <id> # resume existing upload
"""

import os
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from adaption import Adaption, DatasetTimeout

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")
ADAPTION_API_KEY = os.environ.get("ADAPTION_API_KEY")
if not ADAPTION_API_KEY:
    raise ValueError("ADAPTION_API_KEY not found. Check your .env file.")

client = Adaption(api_key=ADAPTION_API_KEY)

V10_CSV    = Path(__file__).parent.parent / "data" / "02_structured" / "tamil_agri_advisory_v10_clean.csv"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "03_adapted"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BLUEPRINT = """## Identity
- You are an agricultural extension officer of Tamil Nadu Agricultural University (TNAU) tasked with advising small and marginal farmers in Tamil Nadu.
- Your persona is that of a knowledgeable, friendly, and patient field officer who communicates **in simple, clear Tamil**.
- You must provide **practical, actionable solutions** and **avoid technical jargon**.

## Tone and Language
- Use plain Tamil sentences that a farmer with limited formal education can understand.
- Keep the tone respectful, supportive, and encouraging.
- Present all advice in **bullet-point format**.

## Prompt Enrichment Guidance
- The farmer's question may begin with a context tag in square brackets e.g. [பருத்தி | காரீப் பருவம் | பூக்கும் நிலை]. Use this context to enrich the question into a natural, specific Tamil farmer question. Remove the bracket tag in the final output — absorb the context into the question naturally.

## Answer Structure (mandatory for every response)
1. **Acknowledge** the farmer's specific situation in one concise sentence.
2. **Immediate Action**: give a concrete recommendation that includes exact quantities, timing, and cost (if applicable).
3. **Rationale**: explain why this recommendation fits the farmer's soil type, irrigation method, crop stage, or other relevant conditions.
4. **Long-Term Recommendation / Prevention**: suggest a sustainable practice or future step to avoid recurrence.
5. **Reference**: direct the farmer to the nearest Krishi Vigyan Kendra (KVK) or the district agriculture officer for further assistance.

## Special Cases
- **Disease or severe crop issue**: first provide preventive or immediate remedial steps, then advise the farmer to contact the local agriculture officer.
- **Debt, financial stress, or mental distress**: immediately supply helpline numbers:
  - Sneha: 044-24640050
  - Kisan Call Centre: 1551
  - Follow with any relevant scheme information.
- **Government scheme queries**: always state the **exact subsidy percentage and the rupee amount** as given by the official source.

## Constraints
- Use only the prescribed answer structure; do not add extra sections or omit any of the five parts.
- Do not use technical jargon; replace any necessary term with a simple Tamil equivalent.
- If you lack specific information, advise the farmer to consult the nearest KVK or district officer rather than guessing.

## Capabilities
- Provide crop-specific advice (soil preparation, sowing, irrigation, fertilization, pest/disease management).
- Explain government schemes, subsidies, and eligibility with precise monetary figures.
- Offer mental-health and financial-distress resources promptly.
- Recommend best practices for long-term farm sustainability.

## Edge Cases
- When multiple issues are presented, prioritize **disease/pest control** first, then **financial/mental health** concerns.
- If the farmer asks for a solution that could be unsafe (e.g., misuse of chemicals), refuse and instead give a safe alternative while still following the answer structure.

## Conflict Resolution
- **Safety constraints** override all other instructions. If any request conflicts with safety, refuse the unsafe action and provide a safe alternative.

## Reference to Local Support
- Always conclude with a recommendation to contact the nearest KVK or the district agriculture officer:
  "உங்கள் அருகிலுள்ள KVK அல்லது மாவட்ட வேளாண் அலுவலரை தொடர்பு கொள்ளுங்கள்."""

# v10 — 15 context columns including reasoning_type
COLUMN_MAPPING = {
    "prompt":     "question_tamil_v9",
    "completion": "answer_tamil_v10",
    "context": [
        "category", "crop_primary", "soil_type", "irrigation_type",
        "farming_practice", "region", "season", "growth_stage",
        "weather_recent", "severity", "cropping_system",
        "farm_scale", "budget_constraint",
        "reasoning_type",   # NEW — cognitive pattern tag
        "source_type",      # NEW — provenance signal
    ],
}

JOB_SPEC_V10 = {
    "recipes": {
        "prompt_rephrase":    True,
        "reasoning_traces":   True,
        "deduplication":      True,
        "metadata_injection": True,
    }
}


def run(input_csv, test_mode=False, estimate_only=False, dataset_id=None):
    print(f"\n{'='*65}")
    print(f"  Adaption — v10 Tamil Agricultural Advisory Dataset")
    print(f"  Input      : {input_csv.name}")
    print(f"  Prompt col : question_tamil_v9 (context-injected)")
    print(f"  Completion : answer_tamil_v10 (expanded native Tamil)")
    print(f"  Context    : 15 columns including reasoning_type")
    print(f"  Config     : prompt_rephrase=ON, reasoning_traces=ON")
    print(f"  Mode       : {'ESTIMATE ONLY' if estimate_only else 'TEST (10 rows)' if test_mode else 'FULL RUN'}")
    print(f"{'='*65}\n")

    if dataset_id:
        print(f"Resuming dataset: {dataset_id}\n")
    else:
        print(f"Uploading {input_csv.name}...")
        result = client.datasets.upload_file(
            str(input_csv),
            name=f"tamil-agri-v10-{datetime.utcnow().strftime('%Y%m%d')}"
        )
        dataset_id = result.dataset_id
        print(f"Dataset ID : {dataset_id}")

        print("Waiting for ingestion...")
        while True:
            status = client.datasets.get_status(dataset_id)
            if status.row_count is not None:
                print(f"Ingested   : {status.row_count} rows\n")
                break
            time.sleep(2)

    job_spec = dict(JOB_SPEC_V10)
    if test_mode:
        job_spec["max_rows"] = 10

    print("Estimating cost...")
    estimate = client.datasets.run(
        dataset_id,
        column_mapping=COLUMN_MAPPING,
        job_specification=job_spec,
        estimate=True,
    )
    print(f"  Estimated time    : ~{estimate.estimated_minutes} minutes")
    print(f"  Estimated credits : {estimate.estimated_credits_consumed}\n")

    if estimate_only:
        print("ESTIMATE ONLY — no run started.")
        print(f"To run: python scripts/04_adapt_data.py --dataset-id {dataset_id}")
        return

    if input("Start adaptation run? (y/n): ").strip().lower() != "y":
        print(f"Aborted. Resume: python scripts/04_adapt_data.py --dataset-id {dataset_id}")
        return

    print("\nStarting adaptation run...")
    run_result = client.datasets.run(
        dataset_id,
        column_mapping=COLUMN_MAPPING,
        job_specification=job_spec,
    )
    print(f"Run ID     : {run_result.run_id}")
    print(f"Est. time  : ~{run_result.estimated_minutes} minutes")
    print(f"Credits    : {run_result.estimated_credits_consumed}")
    print(f"Dataset ID : {dataset_id}  ← save this\n")

    print("Waiting for adaptation...")
    try:
        final = client.datasets.wait_for_completion(dataset_id, timeout=7200)
        print(f"Adaptation status: {final.status}")
        if hasattr(final, "error") and final.error:
            print(f"Error: {final.error.message}")
            return
    except DatasetTimeout:
        print(f"Timed out — resume: python scripts/04_adapt_data.py --dataset-id {dataset_id}")
        return

    print("\nWaiting for evaluation scoring...")
    for _ in range(72):
        ev = client.datasets.get_evaluation(dataset_id)
        if ev.status in ("succeeded", "failed", "skipped"):
            break
        time.sleep(5)

    print(f"\n{'='*65}\n  QUALITY REPORT — v10\n{'='*65}")
    if ev.status == "succeeded" and ev.quality:
        q = ev.quality
        print(f"  Score before  : {q.score_before}")
        print(f"  Score after   : {q.score_after}")
        print(f"  Improvement   : {q.improvement_percent:.1f}%")
        if hasattr(q, "grade_before"):     print(f"  Grade before  : {q.grade_before}")
        if hasattr(q, "grade_after"):      print(f"  Grade after   : {q.grade_after}")
        if hasattr(q, "percentile_after"): print(f"  Percentile    : {q.percentile_after}")
    else:
        ds_check = client.datasets.get(dataset_id)
        if ds_check.evaluation_summary:
            s = ds_check.evaluation_summary
            print(f"  Score after   : {s.score_after}")
            if hasattr(s, "improvement_percent"): print(f"  Improvement   : {s.improvement_percent:.1f}%")
            if hasattr(s, "grade_after"):         print(f"  Grade after   : {s.grade_after}")

    print(f"\n  v9 reference  : 7.7/10, Grade B, 15.3%")
    print(f"  v10 target    : Grade A (9.0+)")

    print(f"\nWaiting for dataset to be ready for download...")
    for _ in range(60):
        ds = client.datasets.get(dataset_id)
        print(f"  Status: {ds.status}")
        if ds.status == "succeeded":
            break
        if ds.status == "failed":
            print("Failed — check dashboard.")
            return
        time.sleep(10)

    print("\nDownloading...")
    url = client.datasets.download(dataset_id)

    timestamp   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    label       = "test" if test_mode else "v10"
    output_path = OUTPUT_DIR / f"tamil_agri_advisory_{label}_adapted_{timestamp}.csv"

    response = requests.get(url, timeout=300)
    response.raise_for_status()
    with open(output_path, "wb") as f:
        f.write(response.content)

    print(f"Saved → {output_path}  ({len(response.content)/1024:.0f} KB)")
    print(f"\nDataset ID : {dataset_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test",       action="store_true")
    parser.add_argument("--estimate",   action="store_true")
    parser.add_argument("--dataset-id", type=str, default=None)
    parser.add_argument("--file",       type=str, default=str(V10_CSV))
    args = parser.parse_args()

    run(
        input_csv     = Path(args.file),
        test_mode     = args.test,
        estimate_only = args.estimate,
        dataset_id    = args.dataset_id,
    )
