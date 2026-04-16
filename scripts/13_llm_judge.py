"""
13_llm_judge.py
---------------
LLM-as-a-Judge for the Tamil Agricultural Advisory Dataset.
Uses Cohere command-r-plus-08-2024 to evaluate rows on 5 dimensions
and generate concrete improvement recommendations.

Goal: Find and fix weak rows BEFORE submitting to Adaption.
      Turn this into a golden dataset.

Usage:
  python scripts/13_llm_judge.py --dry-run
  python scripts/13_llm_judge.py --limit 100 --random
  python scripts/13_llm_judge.py --strategic
"""

import os
import csv
import json
import time
import random
import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
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

SOURCE_CSV  = "data/02_structured/tamil_agri_advisory_v7_source.csv"
REPORT_CSV  = "data/02_structured/llm_judge_report.csv"
SUMMARY_TXT = "data/02_structured/llm_judge_summary.txt"

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY not found.")

co    = cohere.Client(COHERE_API_KEY, timeout=30)
MODEL = "command-r-plus-08-2024"
DELAY = 1.0

# ── JUDGE PROMPT ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert evaluator of Tamil agricultural advisory datasets for AI training.
Evaluate Q&A rows from a dataset for Tamil Nadu smallholder farmers.
Score on 5 dimensions (1-5 each). Return ONLY valid JSON, no markdown, no preamble."""

JUDGE_PROMPT = """Evaluate this row on 5 dimensions (1-5 each):

1. QUESTION_AUTHENTICITY
   5 = sounds exactly like a real Tamil farmer speaking naturally
   3 = acceptable but slightly formal or translated-sounding
   1 = clearly synthetic, unnatural, or just a topic label

2. ANSWER_COMPLETENESS
   5 = full 5-part: Acknowledge + Immediate Action + Rationale + Prevention + KVK referral
   3 = has most parts but missing 1-2
   1 = short, incomplete, or just a fact dump

3. TAMIL_QUALITY
   5 = natural fluent Tamil with proper agricultural terminology
   3 = acceptable but English sentence structure or awkward phrasing
   1 = transliterated English, Tanglish, or machine-translated feel

4. CONTEXT_ALIGNMENT
   5 = answer specifically addresses the crop, region, season, growth stage in metadata
   3 = partially addresses context but gives generic advice
   1 = ignores specific context entirely

5. ADVISORY_VALUE
   5 = highly actionable: specific dosages, timing, quantities, local references
   3 = useful but could be more specific
   1 = vague, generic, or unhelpful

ROW TO EVALUATE:
{context}

Return ONLY this JSON with no markdown:
{{
  "question_authenticity": <1-5>,
  "answer_completeness": <1-5>,
  "tamil_quality": <1-5>,
  "context_alignment": <1-5>,
  "advisory_value": <1-5>,
  "total": <sum 5-25>,
  "verdict": "KEEP" or "FIX" or "DROP",
  "weakness": "single biggest weakness in one sentence",
  "improvement": "one specific concrete action to make this row Grade A"
}}"""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def build_row_context(row: dict) -> str:
    return f"""ID: {row.get('id')}
Category: {row.get('category')} | Crop: {row.get('crop_primary')} | Region: {row.get('region')}
Season: {row.get('season')} | Stage: {row.get('growth_stage')} | Severity: {row.get('severity')}
Reasoning type: {row.get('reasoning_type', 'unknown')}

QUESTION (Tamil):
{row.get('question_tamil', '')[:300]}

QUESTION (English):
{row.get('question_english', '')[:200]}

ANSWER (Tamil):
{(row.get('answer_tamil_v10') or row.get('answer_tamil', ''))[:500]}

ANSWER (English):
{row.get('answer_english', '')[:400]}"""


def judge_row(row: dict) -> dict:
    context = build_row_context(row)
    prompt  = JUDGE_PROMPT.format(context=context)
    try:
        response = co.chat(
            model=MODEL,
            preamble=SYSTEM_PROMPT,
            message=prompt,
        )
        raw = response.text.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        result = json.loads(raw)
        result["id"]             = row.get("id")
        result["category"]       = row.get("category")
        result["reasoning_type"] = row.get("reasoning_type", "")
        result["source_type"]    = row.get("source_type", "")
        return result
    except Exception as e:
        return {
            "id":             row.get("id"),
            "category":       row.get("category"),
            "reasoning_type": row.get("reasoning_type", ""),
            "source_type":    row.get("source_type", ""),
            "question_authenticity": 0,
            "answer_completeness":   0,
            "tamil_quality":         0,
            "context_alignment":     0,
            "advisory_value":        0,
            "total":      0,
            "verdict":    "ERROR",
            "weakness":   f"evaluation failed: {str(e)[:60]}",
            "improvement": "retry",
        }


def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def strategic_sample(rows: list, n_per_category: int = 10) -> list:
    sampled  = []
    seen_ids = set()

    # Always include high-value structural rows
    priority = [r for r in rows if any(t in r.get("source_type", "")
                for t in ["L3", "L4", "L5", "negative_space", "contrastive"])]
    for r in priority:
        sampled.append(r)
        seen_ids.add(r["id"])

    # Sample per category
    category_buckets = defaultdict(list)
    for r in rows:
        if r["id"] not in seen_ids:
            category_buckets[r.get("category", "")].append(r)

    for cat, bucket in category_buckets.items():
        take = random.sample(bucket, min(n_per_category, len(bucket)))
        for r in take:
            sampled.append(r)
            seen_ids.add(r["id"])

    random.shuffle(sampled)
    return sampled


def generate_summary(results: list, all_rows: list) -> str:
    valid = [r for r in results if r.get("total", 0) > 0]
    if not valid:
        return "No valid results — all rows errored."

    avg_score       = sum(r["total"] for r in valid) / len(valid)
    verdict_counts  = Counter(r["verdict"] for r in valid)
    category_scores = defaultdict(list)
    for r in valid:
        category_scores[r["category"]].append(r["total"])

    dim_avgs = {}
    for dim in ["question_authenticity", "answer_completeness", "tamil_quality",
                "context_alignment", "advisory_value"]:
        scores = [r.get(dim, 0) for r in valid if r.get(dim, 0) > 0]
        dim_avgs[dim] = sum(scores) / len(scores) if scores else 0

    weakness_counts     = Counter(r.get("weakness", "") for r in valid)
    improvement_samples = [r.get("improvement", "") for r in valid
                           if 0 < r.get("total", 25) < 15][:10]

    lines = [
        "=" * 70,
        "LLM JUDGE REPORT — Tamil Agricultural Advisory Dataset",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"Model: {MODEL}",
        f"Rows evaluated: {len(valid)} / {len(all_rows)} total",
        "=" * 70,
        "",
        "── OVERALL SCORE ────────────────────────────────────────────────────",
        f"  Average score : {avg_score:.1f} / 25",
        f"  Grade equiv.  : {'A (18-25)' if avg_score >= 18 else 'B (13-17)' if avg_score >= 13 else 'C (8-12)' if avg_score >= 8 else 'D (<8)'}",
        "",
        "── DIMENSION BREAKDOWN ──────────────────────────────────────────────",
    ]
    for dim, avg in sorted(dim_avgs.items(), key=lambda x: x[1]):
        bar = "█" * int(avg) + "░" * (5 - int(avg))
        lines.append(f"  {dim:<25} {avg:.1f}/5  {bar}")

    lines += [
        "",
        "── VERDICT DISTRIBUTION ─────────────────────────────────────────────",
        f"  KEEP  : {verdict_counts.get('KEEP',  0):>5} ({verdict_counts.get('KEEP',  0)/len(valid)*100:.1f}%)",
        f"  FIX   : {verdict_counts.get('FIX',   0):>5} ({verdict_counts.get('FIX',   0)/len(valid)*100:.1f}%)",
        f"  DROP  : {verdict_counts.get('DROP',  0):>5} ({verdict_counts.get('DROP',  0)/len(valid)*100:.1f}%)",
        f"  ERROR : {verdict_counts.get('ERROR', 0):>5}",
        "",
        "── WEAKEST CATEGORIES ───────────────────────────────────────────────",
    ]
    cat_avg = {cat: sum(s)/len(s) for cat, s in category_scores.items()}
    for cat, avg in sorted(cat_avg.items(), key=lambda x: x[1])[:8]:
        lines.append(f"  {cat:<30} avg {avg:.1f}/25")

    lines += [
        "",
        "── TOP WEAKNESSES IDENTIFIED ────────────────────────────────────────",
    ]
    for weakness, count in weakness_counts.most_common(10):
        if weakness and "evaluation failed" not in weakness:
            lines.append(f"  [{count:>3}x] {weakness[:70]}")

    lines += [
        "",
        "── GOLDEN DATASET RECOMMENDATIONS ──────────────────────────────────",
        "",
    ]
    recs = []
    if dim_avgs.get("question_authenticity", 5) < 3.5:
        recs.append("1. QUESTION REWRITE: Rewrite low-authenticity questions using real\n"
                    "   farmer phrasing patterns. Target >= 4.0")
    if dim_avgs.get("answer_completeness", 5) < 3.5:
        recs.append("2. ANSWER STRUCTURE: Append missing KVK referral and Prevention\n"
                    "   sections to FIX-tagged rows. Target >= 4.0")
    if dim_avgs.get("tamil_quality", 5) < 3.5:
        recs.append("3. TAMIL QUALITY: Re-run 10_expand_tamil_answers.py on\n"
                    "   low-scoring rows. Target >= 4.0")
    if dim_avgs.get("context_alignment", 5) < 3.5:
        recs.append("4. CONTEXT ALIGNMENT: Drop rows where >= 5 metadata fields are 'all'.")
    if dim_avgs.get("advisory_value", 5) < 3.5:
        recs.append("5. ADVISORY SPECIFICITY: Drop rows with answer_tamil_v10 < 300 chars.")

    below = sum(1 for r in valid if r["total"] < 13)
    recs.append(f"6. DROP WEAK ROWS: Remove all rows scoring < 13/25.\n"
                f"   Currently {below} rows below threshold ({below/len(valid)*100:.1f}% of sample).")

    if improvement_samples:
        recs.append("7. SPECIFIC IMPROVEMENTS:\n" +
                    "\n".join(f"   - {imp}" for imp in improvement_samples if imp))

    lines.extend(recs)
    lines += [
        "",
        "── SCORE DISTRIBUTION ───────────────────────────────────────────────",
    ]
    score_dist = Counter(r["total"] for r in valid)
    for sc in range(25, 4, -1):
        if sc in score_dist:
            bar = "█" * min(score_dist[sc], 40)
            lines.append(f"  {sc:2d}/25  {score_dist[sc]:>5}  {bar}")

    lines += ["", "=" * 70]
    return "\n".join(lines)


# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(dry_run=False, limit=None, sample_random=False, strategic=False):
    print("=" * 60)
    print("  LLM Judge — Tamil Agricultural Advisory Dataset")
    print(f"  Model: {MODEL} (Cohere)")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    print("=" * 60)

    rows = load_csv(SOURCE_CSV)
    print(f"Loaded: {len(rows)} rows")

    if strategic:
        to_judge = strategic_sample(rows, n_per_category=10)
        print(f"Strategic sample: {len(to_judge)} rows")
    elif sample_random and limit:
        to_judge = random.sample(rows, min(limit, len(rows)))
        print(f"Random sample: {len(to_judge)} rows")
    elif limit:
        to_judge = rows[:limit]
        print(f"First {len(to_judge)} rows")
    else:
        to_judge = rows
        print(f"Judging all {len(to_judge)} rows")

    if limit:
        to_judge = to_judge[:limit]
        print(f"Capped at: {len(to_judge)} rows")

    est_min = (len(to_judge) * (DELAY + 4)) / 60
    print(f"Estimated time: ~{est_min:.0f} minutes\n")

    if dry_run:
        print("[DRY RUN] Sample rows:")
        for r in to_judge[:5]:
            print(f"  {r['id']} | {r.get('category')} | {r.get('reasoning_type')}")
        print(f"\nTotal: {len(to_judge)} rows")
        print(f"\n[DRY RUN] No files written.")
        return

    results = []
    errors  = 0

    for row in tqdm(to_judge, desc="Judging", total=len(to_judge)):
        result = judge_row(row)
        results.append(result)

        if result.get("verdict") == "ERROR":
            errors += 1
        elif result.get("total", 25) < 13:
            print(f"\n  ⚠  {result['id']} scored {result['total']}/25 [{result['verdict']}]")
            print(f"     Weakness: {result.get('weakness','')[:70]}")

        time.sleep(DELAY)

    report_fields = [
        "id", "category", "reasoning_type", "source_type",
        "question_authenticity", "answer_completeness", "tamil_quality",
        "context_alignment", "advisory_value", "total",
        "verdict", "weakness", "improvement",
    ]
    with open(REPORT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=report_fields,
                                quoting=csv.QUOTE_ALL, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    summary = generate_summary(results, rows)
    with open(SUMMARY_TXT, "w", encoding="utf-8") as f:
        f.write(summary)

    print("\n" + summary)
    print(f"\n✓ Report  → {REPORT_CSV}")
    print(f"✓ Summary → {SUMMARY_TXT}")
    print(f"  Errors  : {errors}")
    print(f"\nNext: Review summary then run 12_quality_filter.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--limit",     type=int, default=None)
    parser.add_argument("--random",    action="store_true")
    parser.add_argument("--strategic", action="store_true")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        limit=args.limit,
        sample_random=args.random,
        strategic=args.strategic,
    )
