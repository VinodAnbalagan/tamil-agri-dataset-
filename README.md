# Tamil Agricultural Advisory Dataset

A grounded, publishing-quality instruction dataset for Tamil Nadu smallholder farmers, built for the [Adaption Labs Uncharted Data Challenge 2026](https://www.adaption.ai/).

> **Current version:** v10 — 218 golden rows | Grade B | 7.7/10
> **Language:** Tamil (primary) | **Domain:** Agriculture | **Region:** Tamil Nadu, India

---

## Overview

This dataset contains real agricultural advisory Q&A pairs grounded in verified sources from Tamil Nadu Agricultural University (TNAU), the Indian Council of Agricultural Research (ICAR), and the Government of Tamil Nadu. Questions are written from the perspective of Tamil Nadu smallholder farmers. Answers follow a structured 5-part extension officer format.

---

## Dataset Statistics

| Version | Rows | Score Before | Score After | Grade | Percentile | Key Change |
|---------|------|-------------|-------------|-------|------------|------------|
| v6 | 195 | 7.0 | 7.8 | B | 16.7% | Structural diversity rows |
| v7 | 272 | 7.8 | 8.7 | B | 33.0% | TNAU scraping |
| v8 | 2,293 | 5.0 | 7.4 | B | 15.3% | KCC integration + L3/L4/L5 |
| v9 | 2,091 | 6.0 | 7.7 | B | 15.3% | Context injection |
| v10 | 218 | 6.0 | 7.7 | B | 15.8% | Quality filter + 5-part rewrite |

**v10 is a golden dataset — 218 rows, every row verified, zero nulls, 5-part structure enforced.**

---

## v10 Column Schema

| Column | Description |
|--------|-------------|
| `id` | Unique row ID (tn-agri-XXXX) |
| `question` | Context-injected Tamil farmer question |
| `answer` | Cohere-rewritten 5-part Tamil advisory answer (avg 1,131 chars) |
| `category` | Topic (pest_control, fertilizer, crop_disease, etc.) |
| `crop_primary` | Primary crop referenced |
| `soil_type` | Soil classification |
| `irrigation_type` | Irrigation method |
| `farming_practice` | Conventional / organic / integrated |
| `region` | Tamil Nadu zone (delta, dry_zone, coastal, etc.) |
| `season` | Kharif / Rabi / Samba / Kuruvai / all |
| `growth_stage` | Crop growth stage at time of query |
| `severity` | Issue severity (low / medium / high / urgent) |
| `source_type` | Provenance tag |
| `reasoning_type` | Cognitive pattern tag |

---

## Answer Structure (5-Part TNAU Format)

1. **நிலைமையை அங்கீகரித்தல்** — Acknowledge the farmer's specific situation
2. **உடனடி நடவடிக்கை** — Immediate action with exact dosage, timing, cost
3. **காரணம்** — Rationale tied to specific crop/soil/season/stage
4. **நீண்டகால தடுப்பு** — Long-term prevention practice
5. **KVK பரிந்துரை** — Referral to nearest KVK or district agriculture officer

---

## AgriBench Hierarchy

| Level | Description | Count in v10 |
|-------|-------------|-------------|
| L3 | Multi-variable synthesis | 8 |
| L4 | Symptom diagnosis | 8 |
| L5 | High-stakes dilemmas | 8 |
| Negative Space | Correct answer = do not act | 5 |
| Contrastive Pairs | Same topic, different outcome | 6 |
| Agricultural Extension | TNAU verified advisory | 131 |
| Traditional Knowledge | Farmer-verified practices | 16 |

---

## Active Pipeline Scripts

| Script | Purpose |
|--------|---------|
| `04_adapt_data.py` | Submit to Adaption platform |
| `05_filter_v9.py` | Quality pre-filter |
| `08_inject_context.py` | Inject Tamil metadata context into prompts |
| `10_expand_tamil_answers.py` | Recast answer_english to native Tamil |
| `11_add_reasoning_tag.py` | Add reasoning_type column |
| `12_quality_filter.py` | Score rows, generate submission CSVs |
| `13_llm_judge.py` | LLM judge via Cohere |
| `14_enrich_submission.py` | Final metadata enrichment |
| `15_verify_and_fix_mismatches.py` | Fix crop mismatches |
| `16_rewrite_completions.py` | Rewrite completions to 5-part structure |

---

## Setup

```bash
git clone https://github.com/VinodAnbalagan/tamil-agri-dataset-
cd tamil-agri-dataset-
uv venv && source .venv/bin/activate
uv pip install cohere openai anthropic adaption python-dotenv tqdm pandas
cp .env.example .env
# Add: COHERE_API_KEY, OPENROUTER_API_KEY, ADAPTION_API_KEY
```

---

## Key Design Principles

- **Quality over quantity** — 218 verified golden rows beats 2,293 noisy rows
- **Zero nulls** — empty metadata prevents context injection, weakening the rubric score
- **5-part structure mandatory** — Acknowledge, Action, Rationale, Prevention, KVK Referral
- **Authentic provenance** — KCC call logs + TNAU extension guides + L5 dilemmas
- **Tamil-first** — Tamil gets 3-5x more tokens than English for identical content

---

## Acknowledgements

Built for the **Adaption Labs Uncharted Data Challenge 2026**. Received the first honorary award from Sara Hooker (co-founder, Adaption Labs) on April 11, 2026.

Data: [TNAU Expert System](http://agritech.tnau.ac.in/), [Kisan Call Centre](https://mkisan.gov.in/), Government of Tamil Nadu.

## Author

**Vinod Anbalagan** — AI/ML Researcher, Toronto
Substack: [The Meta Gradient](https://themetagradient.substack.com)
Hugging Face: [vinodanbalagan](https://huggingface.co/vinodanbalagan)
