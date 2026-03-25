# Owner-Occupation Inference Pipeline for £2m+ Residential Properties (England & Wales)

## Project purpose
This repository provides a reproducible, reviewable Python pipeline to estimate the **defensible range** of owner-occupation among residential properties worth **£2m+** in England and Wales. It is designed for policy analysis and explicit uncertainty handling rather than false-precision point estimates.

## Research questions
### Primary
- What is the defensible range of owner-occupation for £2m+ properties?

### Secondary
- How sensitive is policy design to that range?
- What share can be classified with high confidence?
- What share depends on proxy logic rather than direct evidence?
- Which assumptions most affect results?
- What percentage are company owned, trust owned, or individual owned?
- How does the estimate vary by geography and ownership type?

## Repository structure
- `data/raw/` raw downloaded data (preserved unchanged)
- `data/interim/` cleaned and standardised datasets
- `data/processed/` linked and classified analysis-ready data
- `src/` pipeline modules
- `docs/` methods, source inventory, assumptions
- `outputs/` metrics, diagnostics, policy-facing notes
- `notebooks/` optional exploratory work (not required for pipeline)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to run
```bash
python src/run_pipeline.py
```

## Pipeline stages
1. Download accessible sources and log metadata (`src/download_data.py`)
2. Prepare core sources (`prepare_price_paid`, `prepare_ukhpi`, `prepare_epc`, `prepare_ownership`, `prepare_addresses`)
3. Build candidate populations (`v1` transaction-based, `v2` current-value proxy)
4. Link properties across sources with staged matching
5. Classify owner-occupation with confidence tiers
6. Estimate conservative/central/upper range
7. Run sensitivity scenarios and produce policy outputs
8. Write audit summary of counts, match rates, and key assumption risks

## Key outputs
- `outputs/headline_metrics.csv`
- `outputs/classification_confidence_summary.csv`
- `outputs/owner_occupation_range_by_region.csv`
- `outputs/ownership_type_distribution.csv`
- `outputs/sensitivity_scenarios.csv`
- `outputs/sensitivity_note.md`
- `outputs/policy_brief_note.md`
- `outputs/audit_summary.md`

## Core caveats
- This is an inference exercise; no single open dataset provides full occupancy truth.
- Output should be interpreted as a **range with confidence composition**, not a definitive exact percentage.
- Aggregate contextual tables are documented but not forced into property-level classification.
- Paid/licensed sources (e.g., OS GB Address) are optional and handled via graceful fallback.

## Reproducibility and traceability
- Raw files are logged with URL, timestamp, status, path, size, and content type in `outputs/download_log.csv`.
- Each module can run independently and writes deterministic outputs given the same inputs.
