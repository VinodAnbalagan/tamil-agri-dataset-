# Agricultural Advisory Datasets — India

Two grounded, publishing-quality instruction datasets for Indian smallholder farmers, built for the [Adaption Labs Uncharted Data Challenge 2026](https://adaptionlabs.ai/).

| Dataset | Rows | Grade | Score | Languages | HuggingFace |
|---------|------|-------|-------|-----------|-------------|
| **Tamil Agricultural Advisory** | 187 | A | 9.4/10 | Tamil | [vinod-anbalagan/tamil-agri-advisory-qa](https://huggingface.co/datasets/vinod-anbalagan/tamil-agri-advisory-qa) |
| **India Agricultural Advisory** | 131 | *pending* | *pending* | 13 languages | [vinod-anbalagan/india-agri-advisory](https://huggingface.co/datasets/vinod-anbalagan/india-agri-advisory) |

**India dataset languages:** Bengali, Gujarati, Hindi, Kashmiri, Malayalam, Marathi, Punjabi, Tamil, Telugu, Urdu, Kannada, Assamese, Odia — covering 15 agro-climatic zones, 17 crops, and over 1 billion speakers.

---

# Tamil Agricultural Advisory Dataset

A grounded, publishing-quality instruction dataset for Tamil Nadu smallholder farmers, built for the [Adaption Labs Uncharted Data Challenge 2026](https://adaptionlabs.ai/).

> **Version:** v13 — 187 rows | Grade A | 9.4/10 | 57.7th percentile
> **Language:** Tamil (primary) | **Domain:** Agriculture | **Region:** Tamil Nadu, India
> **License:** CC BY 4.0

---

## Overview

This dataset contains real agricultural advisory Q&A pairs grounded in verified sources from Tamil Nadu Agricultural University (TNAU), the Kisan Call Centre (KCC), the Indian Council of Agricultural Research (ICAR), and the Government of Tamil Nadu. Questions are written from the perspective of Tamil Nadu smallholder farmers. Answers follow a structured 5-part extension officer format.

Tamil has over 80 million speakers globally, yet almost no high-quality agricultural NLP data exists publicly in Tamil. This dataset is built to change that.

---

## Why This Dataset Exists

My family farmed in Tamil Nadu for generations — rice at scale in the Palar river basin near Kancheepuram district, and other crops on smaller plots. When I began working in AI, I kept looking for Tamil agricultural datasets to build on. They didn't exist. This dataset is the beginning of what should exist.

**Row 15** — a farmer expressing that life feels meaningless under debt — exists because that question gets asked, and an AI system that cannot respond to it with care and a helpline number is not safe to deploy in Tamil Nadu.

---

## Dataset Statistics

| Version | Rows    | Score Before | Score After | Grade | Percentile | Key Change                        |
| ------- | ------- | ------------ | ----------- | ----- | ---------- | --------------------------------- |
| v6      | 195     | 7.0          | 7.8         | B     | 16.7%      | Structural diversity rows         |
| v7      | 272     | 7.8          | 8.7         | B     | 33.0%      | TNAU source expansion             |
| v8      | 2,293   | 5.0          | 7.4         | B     | 15.3%      | KCC integration + L3/L4/L5        |
| v9      | 2,091   | 6.0          | 7.7         | B     | 15.3%      | Context injection                 |
| v10     | 218     | 6.0          | 7.7         | B     | 15.8%      | Quality filter + 5-part rewrite   |
| v12     | 200     | 5.0          | 8.9         | B     | 43.9%      | TNAU metadata fix (56→91% fill)   |
| **v13** | **187** | **6.0**      | **9.4**     | **A** | **57.7%**  | **Adapted re-feed + blueprint**   |

### The Grade B→A Breakthrough

After 10 consecutive Grade B submissions across 6 weeks, the breakthrough came from a single insight: **metadata specificity, not row count, drives the score.**

57% of rows had `season=all`. 45% had `irrigation_type=all`. The platform couldn't generate contextualised advice when the context columns were empty. A TNAU agro-ecological zone lookup table raised the metadata fill rate from 56.8% to 91.2%. The score jumped from 7.7 to 8.9, then to 9.4 after an adapted re-feed.

The raw data was never the problem. 679MB of Kisan Call Centre records with district-level metadata sat in the repository the entire time. The pipeline had stripped the tags.

---

## Column Schema

| Column             | Description                                                                |
| ------------------ | -------------------------------------------------------------------------- |
| `id`               | Unique row ID (tn-agri-XXXX)                                               |
| `question`         | Context-injected Tamil farmer question                                     |
| `answer`           | 5-part Tamil advisory answer (avg 1,131 chars)                             |
| `category`         | Topic (20 categories — pest_control to mental_health_safety)               |
| `crop_primary`     | Primary crop (40+ crops, livestock, and aquaculture species)               |
| `soil_type`        | Soil classification (clay_alluvial, red_laterite, black_cotton, etc.)      |
| `irrigation_type`  | Irrigation method (canal, rainfed, drip, borewell, tank_fed)               |
| `farming_practice` | Conventional / organic / integrated / traditional                          |
| `region`           | Tamil Nadu agro-ecological zone (delta, dry_zone, coastal, western_ghats)  |
| `season`           | Kharif / Rabi / Samba / Kuruvai / Summer                                   |
| `growth_stage`     | Crop growth stage at time of query                                         |
| `severity`         | Issue severity (low / medium / high / urgent)                              |
| `source_type`      | Provenance (agricultural_extension, traditional_knowledge, crisis_routing) |
| `reasoning_type`   | Cognitive pattern (agronomic_advisory, diagnostic, contingency, etc.)      |

---

## Categories (20)

| Category               | Description                                          |
| ---------------------- | ---------------------------------------------------- |
| `crop_disease`         | Plant disease diagnosis and treatment                |
| `floriculture`         | Jasmine, crossandra, marigold — Madurai and Dindigul |
| `weather_advisory`     | Sowing decisions, drought, flood, heat stress        |
| `pest_control`         | Pest identification and TNAU-grounded management     |
| `sericulture`          | Silkworm diseases, mulberry cultivation — Salem      |
| `aquaculture`          | Shrimp, inland fish, rice-fish — Nagapattinam        |
| `crop_management`      | Intercropping, pollination, general husbandry        |
| `irrigation`           | AWD, drip, farm ponds, water conservation            |
| `variety_selection`    | Cultivar recommendations by zone and season          |
| `harvest_timing`       | When to harvest, post-harvest storage                |
| `soil_health`          | pH, salinity, composting, weed management            |
| `women_agriculture`    | SHG, Mahalir Thittam, value addition, land rights    |
| `livestock_dairy`      | Cattle, milk production, Aavin                       |
| `livestock_goat`       | Goat diseases, PPR, bloat, market selling            |
| `livestock_poultry`    | Newcastle Disease, egg production, heat stress       |
| `fertilizer`           | NPK, Panchagavya, organic inputs                     |
| `government_schemes`   | PM-KISAN, KCC, FPO, PMFBY, organic certification     |
| `financial_support`    | Crop insurance, flood compensation, loan relief      |
| `market_price`         | e-NAM, Uzhavar Sandhai, avoiding middlemen           |
| `mental_health_safety` | Crisis routing — Sneha 044-24640050, Kisan 1551      |

---

## Answer Structure (5-Part TNAU Format)

1. **நிலைமையை அங்கீகரித்தல்** — Acknowledge the farmer's specific situation
2. **உடனடி நடவடிக்கை** — Immediate action with exact dosage, timing, cost
3. **காரணம்** — Rationale tied to specific crop/soil/season/stage
4. **நீண்டகால தடுப்பு** — Long-term prevention practice
5. **KVK பரிந்துரை** — Referral to nearest KVK or district agriculture officer

---

## Data Sources

| Source                     | What It Grounded                                      | Rows |
| -------------------------- | ----------------------------------------------------- | ---- |
| TNAU Agritech Portal       | Crop diseases, pest management, season/variety guides | 60+  |
| Kisan Call Centre (KCC)    | Real farmer questions with district-level metadata    | 40+  |
| TANUVAS                    | Livestock and veterinary advisory                     | 15+  |
| District Government Pages  | Region-specific schemes, soil data, crop patterns     | 30+  |
| TNAU Crop Production Guide | Dosages, varieties, cultural practices                | 20+  |
| ICAR-CRIDA                 | Contingency plans, drought management                 | 10+  |
| Agromet Advisory Bulletins | District weather-crop advisories                      | 10+  |

---

## Key Design Principles

- **Quality over quantity** — 187 verified rows beats 2,293 noisy rows
- **Metadata is not decorative** — every "all" replaced with a real TNAU agro-ecological value; 91% fill rate
- **Zero nulls** — every cell filled, every row holds up to human inspection
- **5-part structure mandatory** — Acknowledge, Action, Rationale, Prevention, KVK Referral
- **Authentic provenance** — KCC call logs + TNAU extension guides + crisis routing
- **Tamil-first** — native Tamil script, proper agricultural terminology
- **The lesson** — 10 Grade B runs taught us that row count, answer length, and structural diversity all matter less than metadata specificity

---

## What Makes This Dataset Different

Most agricultural datasets are either high-volume/low-context (KCC logs with no metadata) or high-structure/low-authenticity (academic datasets with no farmer voice). This dataset combines both: **Tamil farmer questions** with **deep structural metadata** (soil type, irrigation, season, growth stage, region) that allows AI systems to give contextualised advice.

It is also one of the only agricultural datasets in the world to include a **farmer mental health safety row** with crisis helpline routing.

---

## Pipeline Scripts

| Script                            | Purpose                                    |
| --------------------------------- | ------------------------------------------ |
| `04_adapt_data.py`                | Submit to Adaption platform                |
| `05_filter_v9.py`                 | Quality pre-filter                         |
| `08_inject_context.py`            | Inject metadata context into prompts       |
| `10_expand_tamil_answers.py`      | Recast answers to native Tamil             |
| `11_add_reasoning_tag.py`         | Add reasoning_type column                  |
| `12_quality_filter.py`            | Score rows, generate submission CSVs       |
| `13_llm_judge.py`                 | LLM judge via Cohere                       |
| `14_enrich_submission.py`         | Final metadata enrichment                  |
| `15_verify_and_fix_mismatches.py` | Fix crop mismatches                        |
| `16_rewrite_completions.py`       | Rewrite completions to 5-part structure    |
| `17_extract_kcc_gold.py`          | Extract best KCC rows with native metadata |
| `18_expand_and_merge.py`          | Expand KCC answers + merge with submission |
| `19_build_india_dataset.py`       | Generate India-wide dataset (15 agro-climatic zones) |

---

## Setup

```bash
git clone https://github.com/VinodAnbalagan/tamil-agri-dataset-
cd tamil-agri-dataset-
uv venv && source .venv/bin/activate
uv pip install cohere adaption python-dotenv tqdm pandas
cp .env.example .env
# Add: COHERE_API_KEY, ADAPTION_API_KEY
```

---

## Intended Uses

- Training Tamil-language agricultural advisory chatbots
- Building voice-based advisory systems for low-literacy farmers (WhatsApp, IVR)
- Evaluating Tamil NLP model performance on domain-specific, low-resource tasks
- Fine-tuning multilingual models for Dravidian language agricultural domains
- Research into context-aware AI for the Global South

---

## Acknowledgements

Built for the **Adaption Labs Uncharted Data Challenge 2026**.

Received the first honorary award from Sara Hooker (co-founder, Adaption Labs) on April 11, 2026.

Data sources: [TNAU Agritech Portal](http://agritech.tnau.ac.in/), [Kisan Call Centre](https://mkisan.gov.in/), Government of Tamil Nadu, TANUVAS, ICAR-CRIDA.

---

# India Agricultural Advisory Dataset

> **Version:** v1 — 131 rows | Grade *pending* | 13 languages
> **Languages:** Bengali, Gujarati, Hindi, Kashmiri, Malayalam, Marathi, Punjabi, Tamil, Telugu, Urdu, Kannada, Assamese, Odia
> **Domain:** Agriculture | **Region:** All India (15 agro-climatic zones)
> **License:** CC BY 4.0

An India-wide agricultural advisory dataset covering all 15 Planning Commission agro-climatic zones. Built from ICAR extension knowledge and India's agro-climatic zone framework. Every row carries real metadata: zone, state, soil type, crop, season, and irrigation method. Localized to 13 Indian languages via Adaption's translation pipeline.

| Dimension | Count |
|-----------|-------|
| Agro-climatic zones | 15 |
| Crops | 17 |
| Categories | 7 |
| Languages | 13 |
| Metadata fill rate | 99.2% |

See [data/README.md](data/README.md) for full documentation.

---

## Citation

```bibtex
@dataset{anbalagan2026tamil_agri,
  title={Tamil Agricultural Advisory Dataset},
  author={Anbalagan, Vinod},
  year={2026},
  publisher={Hugging Face},
  url={https://huggingface.co/datasets/vinod-anbalagan/tamil-agri-advisory-qa},
  license={CC BY 4.0}
}
```

## Author

**Vinod Anbalagan** — AI/ML Researcher, Toronto
Substack: [The Meta Gradient](https://substack.com/@vinodanbalagan)
Hugging Face: [vinod-anbalagan](https://huggingface.co/vinod-anbalagan)

---

_Everything intelligent adapts. Tamil farmers deserve AI that adapts to them._
