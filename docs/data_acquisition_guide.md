# Data Acquisition Guide

The pipeline auto-downloads Price Paid Data and UK HPI. Two additional core sources require manual registration (both free).

## EPC (Energy Performance Certificates) — property-level tenure signal

EPC data provides the strongest occupancy signal: whether a property is owner-occupied, rented private, or rented social.

### Steps
1. Go to https://epc.opendatacommunities.org/
2. Register for an account (email verification required)
3. Log in and navigate to **Domestic EPC** bulk downloads
4. Download the ZIP files for the regions you need (or all of England & Wales)
5. Each ZIP contains a `certificates.csv` with columns including `POSTCODE`, `ADDRESS`, `TENURE`
6. Place the ZIP files in `data/raw/epc/`
7. Re-run the pipeline: `python src/run_pipeline.py`

### What this unlocks
- High-confidence owner-occupation classification where EPC tenure = "owner-occupied"
- High-confidence non-owner-occupation where EPC tenure = "rented (private)" or "rented (social)"
- Moves properties from "uncertain/low" to "owner_occupied_likely/high" or "not_owner_occupied_likely/high"

## CCOD/OCOD (Company Ownership) — ownership type signal

CCOD (UK Companies that Own Property) and OCOD (Overseas Companies that Own Property) reveal which properties are held by corporate or overseas entities — a strong non-owner-occupation signal.

### Steps
1. Go to https://use-land-property-data.service.gov.uk/
2. Create an account
3. Navigate to datasets and request access to:
   - **UK companies that own property in England and Wales** (CCOD)
   - **Overseas companies that own property in England and Wales** (OCOD)
4. Accept the licence terms for each dataset
5. Download the CSV files
6. Place both CSV files in `data/raw/ownership/`
7. Re-run the pipeline: `python src/run_pipeline.py`

### What this unlocks
- High-confidence non-owner-occupation for company-owned properties (UK and overseas)
- Medium-confidence classification for trust/foundation ownership
- Individual ownership inference by elimination (properties NOT in CCOD/OCOD)

## What happens without these sources

Without EPC and ownership data the pipeline still runs, but:
- All 23,000+ candidate properties classify as `uncertain/low`
- Headline range is 0% (conservative) to 100% (upper) — uninformative
- The framework is structurally validated but analytically empty

With both sources, expect meaningful discrimination across confidence tiers and a much narrower defensible range.

## Memory efficiency

The full EPC dataset is ~25M+ records and CCOD/OCOD is ~5M+ records. The pipeline handles this by:

1. **Streaming**: files are read row-by-row from disk, never loaded entirely into memory
2. **Postcode pre-filter**: only rows matching the ~29K candidate postcodes from Price Paid Data are kept
3. **Result**: ~25M EPC rows are filtered down to ~50K-100K relevant rows in ~50MB of memory

You can safely place the full bulk downloads in `data/raw/epc/` and `data/raw/ownership/` without worrying about memory — the pipeline filters them automatically.

## Auto-downloaded sources (no action needed)

| Source | Status | Location |
|--------|--------|----------|
| Price Paid Data | Auto-downloaded | `data/raw/price_paid/` |
| UK House Price Index | Auto-downloaded | `data/raw/ukhpi/` |
| Land/Property API root | Auto-downloaded | `data/raw/land_property_api/` |
| Contextual tables (dwelling stock, rents, etc.) | Auto-downloaded | `data/raw/<dataset>/` |
| OS GB Address | Paid/licensed — graceful fallback | Not required |
