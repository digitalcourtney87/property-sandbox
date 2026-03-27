# CLAUDE.md

## Project intent

This pipeline estimates the **defensible range of owner-occupation among residential properties worth £2m+** in England & Wales. It supports policy analysis for HVCTS (High Value Council Tax Supplement) by answering: what proportion of high-value homes are owner-occupied vs corporate/investment-held?

The output is a **range with confidence composition** (conservative / central / upper), not a single point estimate. This is deliberate — no open dataset provides complete occupancy truth, so false precision is worse than an honest range.

## Key results (latest pipeline run)

- **V1 range** (transacted >= £2M, n=53,736): 30.0% – 61.9%, central 45.9%
- **V2 range** (HPI-uplifted >= £2M, n=92,119): 36.8% – 66.9%, central 51.8%
- **VOA-adjusted central**: 62.2% (accounts for unobserved 68% of Band H population)
- **Best defensible policy range**: 52–62% owner-occupied
- **VOA Band H total**: 166,230 properties nationally
- **CTB Band H vacancy**: 1,930 empty (1.2% vacancy rate)

## How to run

```bash
python src/run_pipeline.py
```

To refresh downloads first: `python src/download_data.py && python src/run_pipeline.py`

No external dependencies required — the pipeline runs on the Python standard library alone. Optional deps (pandas, pyarrow, rapidfuzz) are listed in `requirements.txt` for richer analysis.

## Repository layout

```
src/                    Pipeline modules (all runnable from run_pipeline.py)
data/raw/               Raw downloaded data (gitignored, preserved unchanged)
data/interim/           Cleaned standardised datasets (.parquet = JSONL fallback)
data/processed/         Linked and classified analysis-ready data
outputs/                Metrics, diagnostics, policy-facing notes, stakeholder report
docs/                   Methodology, assumptions, source inventory, procurement roadmap
```

## Pipeline architecture

The pipeline is a single-threaded sequential process orchestrated by `src/run_pipeline.py`:

1. **Download** (`download_data.py`) — fetches PPD, UKHPI, VOA CTSOP; streams large files to disk
2. **Prepare sources** — each `prepare_*.py` module cleans one data source:
   - `prepare_price_paid.py` — streams ~10M rows, early-filters to >= £1.5M
   - `prepare_ukhpi.py` — builds region-level HPI uplift factors
   - `prepare_epc.py` — extracts tenure + TRANSACTION_TYPE signals, deduplicates by UPRN
   - `prepare_ownership.py` — classifies CCOD/OCOD using structured Proprietorship Category field
   - `prepare_voa.py` — parses VOA CTSOP Band H counts (total high-value population proxy)
   - `prepare_ctb.py` — parses CTB Table 5.08 for Band H empty property counts
3. **Build candidate populations** (`link_properties.py`) — V1 (transacted) and V2 (HPI-uplifted)
4. **Link properties** (`link_properties.py`) — postcode + address matching (exact then Jaccard fuzzy at 60%)
5. **Classify** (`classify_owner_occupation.py`) — hierarchy of evidence with confidence tiers
6. **Sensitivity** (`sensitivity_analysis.py`) — scenario analysis varying key assumptions
7. **Outputs** — headline metrics, audit summary, policy brief, sensitivity note

## Data sources

| Source | Location | Access |
|--------|----------|--------|
| Price Paid Data (PPD) | `data/raw/price_paid/` | Auto-downloaded |
| UK HPI | `data/raw/ukhpi/` | Auto-downloaded |
| VOA CTSOP | `data/raw/voa/` | Auto-downloaded |
| EPC | `data/raw/epc/` | Manual — register at epc.opendatacommunities.org |
| CCOD/OCOD (ownership) | `data/raw/ownership/` | Manual — register at use-land-property-data.service.gov.uk |
| CTB statistics | `data/raw/voa/ctb_2025.csv` | Manual download from gov.uk |

See `docs/data_acquisition_guide.md` for detailed procurement instructions.

## Key methodology concepts

- **Three candidate populations**: V1 (transacted >= £2M), V2 (HPI-uplifted >= £2M), VOA Band H (council tax proxy for total high-value stock)
- **Transaction bias**: V1 over-represents corporate/investment buyers who transact more frequently than long-term owner-occupiers
- **Classification hierarchy**: corporate/rental signals → EPC owner signal → individual ownership → PPD property type/tenure tiebreakers → uncertain
- **Confidence tiers**: high (direct evidence), medium (strong proxy), low (sparse/conflicting)
- **CCOD/OCOD are corporate by definition** — every row represents a company-owned property. The structured `Proprietorship Category (1)` field is the reliable classifier, not name-based heuristics
- **EPC TRANSACTION_TYPE** (marketed sale, rental private, etc.) is a stronger occupancy signal than TENURE alone
- **EPC deduplication**: UPRN (97.6% coverage) > BUILDING_REFERENCE_NUMBER > postcode+address, keeping latest by lodgement date
- **Building number guard**: address similarity returns 0 if both addresses start with different numbers (prevents "1 High St" matching "2 High St")
- **VOA-adjusted estimate**: weights observed V1 central by coverage proportion + assumes 70% owner-occupation for unobserved Band H properties

## Configuration

All tuneable parameters live in `src/config.py` (`PipelineConfig` dataclass):
- `min_price_threshold`: £2,000,000 (the policy threshold)
- `threshold_band_floor`: £1,500,000 (near-threshold tracking band)
- `fuzzy_match_cutoff`: 60 (Jaccard word-level similarity threshold)
- `ppd_download_limit`: 15 (number of yearly PPD files to fetch)

## File format note

Intermediate files use `.parquet` extensions but contain JSONL (one JSON object per line). This is a dependency-light fallback when pandas/pyarrow are unavailable. The `io_utils.py` module handles reading/writing transparently.

## Key outputs

- `outputs/stakeholder_report.md` — comprehensive stakeholder-facing report
- `outputs/headline_metrics.csv` / `headline_metrics_v2.csv` — conservative/central/upper estimates
- `outputs/sensitivity_scenarios.csv` — all scenario results
- `outputs/sensitivity_note.md` — narrative sensitivity analysis
- `outputs/audit_summary.md` — stage row counts, confidence tiers, risk flags
- `outputs/policy_brief_note.md` — short policy brief
- `outputs/validation_sample.csv` — 200-property stratified sample for ground-truth validation

## Coding conventions

- Pure standard library Python (no required external deps)
- CSV headers are normalised to lowercase on read (`io_utils.py`)
- Address matching uses word-level Jaccard similarity with building-number guard
- Each prepare module can run independently given raw data
- Streaming/early-filter pattern for large files (PPD reads ~10M rows, keeps ~0.5%)
- No pandas in core pipeline — all data is `list[dict]`

## Known limitations

- Ownership linkage is the weakest join — CCOD/OCOD only cover company-owned properties, not individuals
- 70.2% of V1 candidates have unresolved ownership (no CCOD/OCOD match = likely individual, but not proven)
- V1 covers only 32.3% of estimated Band H population
- EPC coverage is incomplete and may not reflect current tenure
- No authoritative universal property identifier available in open data (UPRN access requires PSGA licence)
