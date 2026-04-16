# Tamil Agricultural Advisory Dataset

A grounded, publishing-quality instruction dataset for Tamil Nadu smallholder farmers, built for the [Adaption Labs Uncharted Data Challenge 2026](https://www.adaption.ai/).

> **Current version:** v8 — 2,293 source rows | Grade B | 7.4/10 | 15.3 percentile  
> **Language:** Tamil (100%) | **Domain:** Agriculture | **Region:** Tamil Nadu, India

---

## Overview

This dataset contains real agricultural advisory Q&A pairs grounded in verified sources from Tamil Nadu Agricultural University (TNAU), the Indian Council of Agricultural Research (ICAR), and the Government of Tamil Nadu. Questions are written from the perspective of Tamil Nadu smallholder farmers. Answers follow a structured 5-part extension officer format.

The dataset was built to address a real gap: most agricultural AI datasets are English-first and generic. Tamil Nadu farmers face specific challenges — delta paddy cultivation, dryland groundnut, coconut pest management, government scheme navigation — that require domain-specific, language-specific training data.

---

## Dataset Statistics

| Version | Rows | Score | Grade | Percentile |
|---------|------|-------|-------|------------|
| v6 | 195 | 7.8 | B | 16.7% |
| v7/v8 | 2,293 | 7.4 | B | 15.3% |

**Category distribution (v8):**

| Category | Rows |
|----------|------|
| weather_advisory | 379 |
| government_schemes | 255 |
| crop_management | 232 |
| variety_selection | 129 |
| fertilizer | 73 |
| irrigation | 61 |
| livestock_dairy | 56 |
| pest_control | 47 |
| crop_disease | 39 |
| Other categories | 269 |

---

## Data Sources

| Source | Type | Rows |
|--------|------|------|
| TNAU Crop Production Guides | Web scrape | ~400 |
| TNAU Horticulture Production Guides | Web scrape | ~300 |
| ICAR District Contingency Plans | PDF extraction | ~600 |
| Tamil Nadu Farmers Welfare Dept | PDF extraction | ~200 |
| Kisan Call Centre (KCC) logs — Tamil Nadu | Real farmer call logs | 278 |
| Manually crafted L3/L4/L5 rows | Expert synthesis | 35 |
| TNAU Class 12 Agricultural Practices | Textbook extraction | ~480 |

---

## Dataset Schema

Each row contains:

| Column | Description |
|--------|-------------|
| `id` | Unique row ID (format: `tn-agri-XXXX` or `kcc-XXXX`) |
| `question_tamil` | Farmer question in Tamil script |
| `question_tanglish` | Farmer question in Tanglish (optional) |
| `question_english` | Farmer question in English |
| `answer_tamil` | Expert answer in Tamil script |
| `answer_english` | Expert answer in English |
| `category` | Topic category (pest_control, fertilizer, crop_disease, etc.) |
| `crop_primary` | Primary crop referenced |
| `crop_companions` | Companion crops (if any) |
| `cropping_system` | Monoculture / intercrop / etc. |
| `soil_type` | Soil classification |
| `irrigation_type` | Irrigation method |
| `farming_practice` | Conventional / organic / etc. |
| `region` | Tamil Nadu region (delta, dry_zone, coastal, etc.) |
| `season` | Kharif / Rabi / Samba / Kuruvai / all |
| `growth_stage` | Crop growth stage at time of query |
| `weather_recent` | Recent weather context |
| `severity` | Issue severity (low / medium / high / urgent) |
| `source_type` | Provenance tag |
| `farm_scale` | Smallholder / marginal / small / medium |
| `budget_constraint` | Zero-budget / low-cost / standard |

---

## AgriBench Hierarchy

Rows are classified by complexity following the AgriBench framework:

| Level | Description | Count |
|-------|-------------|-------|
| L1 | Basic factual recall | ~150 |
| L2 | 1-2 variable advisory | ~1,800 |
| L3 | Multi-variable synthesis | 8 |
| L4 | Symptom diagnosis | 8 |
| L5 | High-stakes dilemmas | 18+ |
| Negative Space | Correct answer = do not act | 5 |
| Contrastive Pairs | Same topic, different outcome | 6 |

---

## Pipeline

```
data/01_raw/                    ← TNAU scraped JSONs
data/01_raw_manual/             ← ICAR PDFs, KCC logs (not in git, large files)
data/02_structured/             ← Cleaned source CSV
data/03_adapted/                ← Adaption platform output
```

### Scripts (run in order)

| Script | Purpose |
|--------|---------|
| `01_scrape_tnau.py` | Scrape TNAU expert system pages |
| `02_extract_qa.py` | Extract Q&A from raw JSONs via LLM |
| `03d_refine_answers.py` | Expand short answers, drop stat lookups |
| `03f_enrich_metadata.py` | Fix generic metadata (season=all, region=all) |
| `03g_add_structural_diversity.py` | Generate L3/L4/L5 rows via OpenRouter |
| `04_adapt_data.py` | Submit to Adaption platform |
| `05_process_kcc.py` | Clean and filter KCC call logs |
| `06_merge_kcc.py` | Merge top KCC rows into source CSV |
| `07_translate_new_rows.py` | Translate new English rows to Tamil |

### Setup

```bash
# Clone and setup
git clone https://github.com/vinodanbalagan/tamil-agri-dataset
cd tamil-agri-dataset
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt   # or: uv sync

# Add API keys to .env
cp .env.example .env
# Edit .env with your keys: GROQ_API_KEY, COHERE_API_KEY, ADAPTION_API_KEY, OPENROUTER_API_KEY
```

---

## Blueprint (Adaption)

The dataset was adapted using the following system prompt for the TNAU extension officer persona:

- **Identity**: Tamil Nadu Agricultural University extension officer
- **Language**: Simple, clear Tamil accessible to farmers with limited formal education
- **Answer structure**: 5-part mandatory format (Acknowledge → Immediate Action → Rationale → Long-Term Prevention → KVK Referral)
- **Special handling**: Mental health/debt crises → Sneha helpline (044-24640050) + Kisan Call Centre (1551)
- **Constraint**: Never alter the farmer's original question

---

## Key Design Decisions

**Why Tamil-first?** Tamil gets 3-5x more tokens than English for identical agricultural content. A dataset that trains on Tamil text directly is more token-efficient than English translation at inference time. The domain signal in Tamil is also stronger — farmers ask questions in Tamil idiom, not translated English.

**Why KCC data?** The Kisan Call Centre logs are real farmer queries answered by real extension officers. They provide authentic farmer language, district-specific context, and verified agronomic advice — exactly the grounding that synthetic datasets lack.

**Why L5 rows?** Most agricultural datasets contain L1-L2 advisory rows (simple dose/variety lookups). High-stakes dilemmas — "cyclone warning, rice at 80% maturity, harvest now or wait?" — require the model to reason about trade-offs, not just retrieve facts. These rows are what separate a good dataset from a great one.

---

## Versioning

| Version | Date | Key Change |
|---------|------|------------|
| v1-v5 | Mar 2026 | Initial dataset, TNAU scraping, manual rows |
| v6 | Apr 11 2026 | Structural diversity audit, 195 rows, Grade B 7.8 |
| v7/v8 | Apr 15 2026 | KCC integration, L3/L4/L5 rows, 2293 rows, Grade B 7.4 |

---

## Acknowledgements

Built as part of the **Adaption Labs Uncharted Data Challenge 2026**. This work received the first honorary award from Sara Hooker (co-founder, Adaption Labs) on April 11, 2026.

Data sources: [TNAU Expert System](http://www.agritech.tnau.ac.in/), [ICAR](https://www.icar.org.in/), [Kisan Call Centre](https://mkisan.gov.in/), Government of Tamil Nadu.

---

## Author

**Vinod Anbalagan** — AI/ML Researcher, Toronto  
Substack: [The Meta Gradient](https://themetagradient.substack.com)  
Hugging Face: [vinodanbalagan](https://huggingface.co/vinodanbalagan)
