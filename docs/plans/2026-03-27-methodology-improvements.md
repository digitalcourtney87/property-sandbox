# Methodology Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve the owner-occupation estimation pipeline by using existing data fields more effectively, deduplicating EPC records, adding VOA population calibration, classifying V2 candidates, and improving address matching and sensitivity analysis.

**Architecture:** Each task modifies one or two source files. Data prep changes (Tasks 1-3) feed into linking (Task 4) and classification (Task 5). VOA download (Task 6) feeds into calibration (Task 7). All converge in the updated pipeline orchestrator (Task 8).

**Tech Stack:** Python 3 stdlib only (csv, json, zipfile, io, statistics, pathlib). No new dependencies.

---

### Task 1: Use CCOD/OCOD structured fields instead of name string matching

CCOD = "UK Companies that Own Property" — every row is corporate-owned by definition.
OCOD = "Overseas Companies" — every row is overseas corporate by definition.
Both have a `Proprietorship Category (1)` field with structured values like "Limited Company or Public Limited Company", "Corporate Body", "Local Authority", etc.

The current code ignores this field and instead uses naive string matching on the proprietor name, which misclassifies ~1.4% of entries as "individual" (e.g. "THE CROWN ESTATE" doesn't match patterns like "ltd"/"limited").

Also check all 4 proprietor name/category slots — if any is corporate, flag the property.

**Files:**
- Modify: `src/prepare_ownership.py`

**Step 1: Rewrite `classify_owner_name` → `classify_proprietor`**

Replace the name-only classifier with one that uses the structured category field first, falling back to name matching only when category is absent.

```python
def classify_proprietor(name: str, category: str = "", source_type: str = "") -> tuple[str, str]:
    """Classify ownership using structured category field first, name as fallback.

    source_type: "ccod" | "ocod" | "" — derived from filename.
    category: the Proprietorship Category field from CCOD/OCOD.
    """
    cat = clean_text(category)
    n = clean_text(name)

    # OCOD: every row is overseas corporate by definition
    if source_type == "ocod":
        return "overseas_company", "high"

    # CCOD: every row is UK corporate/institutional by definition
    # Use the structured category for finer classification
    if source_type == "ccod" or cat:
        if any(x in cat for x in ["local authority", "county council"]):
            return "UK_public_body", "high"
        if any(x in cat for x in ["housing association", "housing society"]):
            return "UK_housing_association", "high"
        if any(x in cat for x in ["limited company", "public limited", "corporate body",
                                    "limited liability", "unlimited company",
                                    "registered society", "co operative",
                                    "community benefit", "industrial and provident"]):
            return "UK_company", "high"
        # If we know it's CCOD but category doesn't match known patterns,
        # it's still corporate (CCOD only contains corporate owners)
        if source_type == "ccod":
            return "UK_company", "medium"

    # Fallback: name-based classification for non-CCOD/OCOD sources
    if not n:
        return "unresolved", "low"
    if any(x in n for x in ["ltd", "limited", "plc", "llp", "l.l.p"]):
        return "UK_company", "high"
    if any(x in n for x in ["inc", "corp", "gmbh", "bvi", "cayman", "jersey", "guernsey"]):
        return "overseas_company", "medium"
    if "trust" in n or "trustee" in n or "foundation" in n:
        return "trust_or_other", "medium"
    return "individual", "medium"
```

Note: removed "sa" from the overseas patterns — it was matching names like "Lisa" and "Sarah".

**Step 2: Update `_iter_ownership_rows` to pass source type**

```python
def _iter_ownership_rows(files: list[Path]):
    """Yield (row_dict, source_type) from ownership CSV/ZIP files."""
    for path in files:
        # Determine source type from filename
        fname = path.name.upper()
        if "OCOD" in fname:
            source_type = "ocod"
        elif "CCOD" in fname:
            source_type = "ccod"
        else:
            source_type = ""

        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            reader = csv.DictReader(txt)
                            for r in reader:
                                yield {k.lower().strip(): v for k, v in r.items()}, source_type
        elif path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    yield {k.lower().strip(): v for k, v in r.items()}, source_type
```

**Step 3: Update `prepare_ownership` to use all 4 proprietor slots**

In `prepare_ownership()`, after the postcode filter:

```python
        # Check all 4 proprietor slots
        best_type, best_conf = "unresolved", "low"
        for i in range(1, 5):
            pname = r.get(f"proprietor name ({i})") or ""
            pcat = r.get(f"proprietorship category ({i})") or ""
            if not pname and not pcat:
                continue
            otype, oconf = classify_proprietor(pname, pcat, source_type)
            # Corporate signals dominate: if any proprietor is corporate, use that
            if otype in ("UK_company", "overseas_company", "UK_public_body", "UK_housing_association"):
                best_type, best_conf = otype, oconf
                break  # corporate is definitive
            # Otherwise keep the highest-confidence non-corporate classification
            conf_rank = {"high": 3, "medium": 2, "low": 1}
            if conf_rank.get(oconf, 0) > conf_rank.get(best_conf, 0):
                best_type, best_conf = otype, oconf
```

**Step 4: Verify**

Run: `python3 -c "from prepare_ownership import classify_proprietor; print(classify_proprietor('THE CROWN ESTATE', 'Corporate Body', 'ccod'))"`
Expected: `('UK_company', 'high')`

Run: `python3 -c "from prepare_ownership import classify_proprietor; print(classify_proprietor('JOHN SMITH', '', ''))"`
Expected: `('individual', 'medium')`

**Step 5: Commit**

```bash
git add src/prepare_ownership.py
git commit -m "feat: use CCOD/OCOD structured proprietorship category instead of name matching

Check all 4 proprietor slots. Differentiate CCOD (UK corporate) from OCOD
(overseas). Remove false-positive 'sa' pattern from overseas detection."
```

---

### Task 2: Use EPC `TRANSACTION_TYPE` + deduplicate by latest EPC

**Files:**
- Modify: `src/prepare_epc.py`

**Step 1: Update `map_epc_category` to combine TENURE and TRANSACTION_TYPE**

```python
def map_epc_category(tenure: str, transaction_type: str = "") -> str:
    """Combine TENURE and TRANSACTION_TYPE for strongest signal."""
    t = clean_text(tenure)
    tx = clean_text(transaction_type)

    # TRANSACTION_TYPE is a stronger signal — check first
    if "rental" in tx and "social" in tx:
        return "rented_social"
    if "rental" in tx and "private" in tx:
        return "rented_private"
    if tx == "rental":
        return "rented_private"  # unspecified rental defaults to private

    # TENURE field
    if "owner" in t:
        return "owner_occupied"
    if "private" in t and "rent" in t:
        return "rented_private"
    if "social" in t or "council" in t or "housing association" in t:
        return "rented_social"
    if "rent" in t:
        return "rented_private"

    # TRANSACTION_TYPE fallback for tenure=unknown cases
    if "marketed sale" in tx or "non marketed sale" in tx:
        return "sale_context"  # sold recently, weak owner signal
    return "unknown"
```

**Step 2: Capture `LODGEMENT_DATE` and `BUILDING_REFERENCE_NUMBER`**

In `prepare_epc`, change the row accumulation to capture these fields:

```python
        lodgement = r.get("lodgement_date") or r.get("lodgement_datetime") or ""
        brn = r.get("building_reference_number") or ""
        tenure = r.get("tenure") or ""
        txn_type = r.get("transaction_type") or ""
        out.append({
            "postcode_clean": postcode,
            "address_clean": addr,
            "epc_source_field": str(tenure),
            "epc_transaction_type": str(txn_type),
            "epc_category": map_epc_category(str(tenure), str(txn_type)),
            "lodgement_date": str(lodgement)[:10],
            "building_reference_number": brn,
        })
```

**Step 3: Deduplicate — keep latest EPC per property**

After the scan loop in `prepare_epc`, before writing:

```python
    # Deduplicate: keep latest EPC per property (by building_reference_number or postcode+address)
    dedup: dict[str, dict] = {}
    for rec in out:
        brn = rec.get("building_reference_number", "")
        key = brn if brn else f"{rec['postcode_clean']}|{rec['address_clean']}"
        existing = dedup.get(key)
        if not existing or rec.get("lodgement_date", "") > existing.get("lodgement_date", ""):
            dedup[key] = rec
    out = list(dedup.values())
    print(f"EPC: {len(out):,} unique properties after deduplication.")
```

**Step 4: Verify**

Run: `python3 -c "from prepare_epc import map_epc_category; print(map_epc_category('unknown', 'rental (private)'))"`
Expected: `rented_private`

Run: `python3 -c "from prepare_epc import map_epc_category; print(map_epc_category('', 'marketed sale'))"`
Expected: `sale_context`

**Step 5: Commit**

```bash
git add src/prepare_epc.py
git commit -m "feat: use EPC TRANSACTION_TYPE field and deduplicate to latest per property

Combines TENURE and TRANSACTION_TYPE for stronger occupancy signal.
Deduplicates by BUILDING_REFERENCE_NUMBER, keeping most recent lodgement."
```

---

### Task 3: Improve address matching — penalise building number mismatches

**Files:**
- Modify: `src/io_utils.py`

**Step 1: Update `score_similarity` to penalise number mismatches**

The current Jaccard similarity treats all words equally, so "1 park lane" vs "2 park lane" scores 67%. Building numbers must match for a valid property match.

```python
def score_similarity(a: str, b: str) -> int:
    """Word-level Jaccard similarity with building-number guard.

    If both addresses start with a number and those numbers differ,
    return 0 — the properties are different even if the street matches.
    """
    a_words = clean_text(a).split()
    b_words = clean_text(b).split()
    if not a_words and not b_words:
        return 100
    if not a_words or not b_words:
        return 0

    # Building number guard: if first token of each is numeric and they differ, no match
    a_first, b_first = a_words[0], b_words[0]
    if a_first.isdigit() and b_first.isdigit() and a_first != b_first:
        return 0

    a_set = set(a_words)
    b_set = set(b_words)
    overlap = len(a_set & b_set)
    total = len(a_set | b_set)
    return int((overlap / total) * 100)
```

**Step 2: Verify**

Run: `python3 -c "from io_utils import score_similarity; print(score_similarity('1 park lane', '2 park lane'))"`
Expected: `0`

Run: `python3 -c "from io_utils import score_similarity; print(score_similarity('18 ormond yard london', 'flat 2 18 ormond yard london'))"`
Expected: `67` (4/6 words overlap — still matches at 60% cutoff)

**Step 3: Commit**

```bash
git add src/io_utils.py
git commit -m "fix: penalise building number mismatches in address similarity

Addresses starting with different house numbers now score 0,
preventing false matches like '1 park lane' ↔ '2 park lane'."
```

---

### Task 4: Use PPD property_type and tenure_type in classification

**Files:**
- Modify: `src/classify_owner_occupation.py`

PPD `property_type`: D=Detached, S=Semi, T=Terraced, F=Flat, O=Other
PPD `tenure_type`: F=Freehold, L=Leasehold

Leasehold flats are more likely investment/rental. Freehold detached houses are more likely owner-occupied. Use these as tiebreakers for uncertain cases.

**Step 1: Update `classify_row`**

```python
def classify_row(r: dict) -> tuple[str, str, str, bool]:
    ownership = r.get("ownership_type", "unresolved")
    epc = r.get("epc_category", "unknown")
    prop_type = r.get("property_type", "")   # D/S/T/F/O from PPD
    ppd_tenure = r.get("tenure_type", "")     # F=Freehold, L=Leasehold from PPD

    company = ownership in {"UK_company", "overseas_company", "UK_public_body", "UK_housing_association"}
    rental = epc in {"rented_private", "rented_social"}
    owner_sig = epc == "owner_occupied"
    sale_context = epc == "sale_context"
    conflicting = owner_sig and company

    # --- High confidence ---
    if company or rental:
        return "not_owner_occupied_likely", "high", "direct_or_strong_proxy", conflicting
    if owner_sig and not conflicting:
        return "owner_occupied_likely", "high", "epc_owner_signal", conflicting

    # --- Medium confidence ---
    if ownership == "individual" and not rental:
        # Upgrade to high if we also have sale_context EPC (individual + sold = likely owner)
        if sale_context:
            return "owner_occupied_likely", "high", "individual_plus_sale_epc", conflicting
        return "owner_occupied_likely", "medium", "individual_no_conflict", conflicting
    if ownership == "trust_or_other":
        return "not_owner_occupied_likely", "medium", "ownership_proxy", conflicting

    # --- Low confidence with PPD signal tiebreakers ---
    # Freehold detached/semi with sale-context EPC: weak owner signal
    if sale_context and ppd_tenure == "F" and prop_type in ("D", "S"):
        return "owner_occupied_likely", "low", "sale_context_freehold_house", conflicting
    # Leasehold flat with no other signal: weak investment signal
    if ppd_tenure == "L" and prop_type == "F" and ownership == "unresolved" and epc == "unknown":
        return "not_owner_occupied_likely", "low", "leasehold_flat_no_signal", conflicting

    return "uncertain", "low", "sparse_or_conflicting", conflicting
```

**Step 2: Update `build_headline_range` — keep weight map unchanged**

The new `UK_public_body` and `UK_housing_association` types are already handled by the `company` check in `classify_row`, so they flow through as `not_owner_occupied_likely/high`. No changes needed to the range builder.

**Step 3: Verify**

Run: `python3 -c "from classify_owner_occupation import classify_row; print(classify_row({'ownership_type': 'UK_public_body', 'epc_category': 'unknown'}))"`
Expected: `('not_owner_occupied_likely', 'high', 'direct_or_strong_proxy', False)`

Run: `python3 -c "from classify_owner_occupation import classify_row; print(classify_row({'ownership_type': 'unresolved', 'epc_category': 'unknown', 'tenure_type': 'L', 'property_type': 'F'}))"`
Expected: `('not_owner_occupied_likely', 'low', 'leasehold_flat_no_signal', False)`

**Step 4: Commit**

```bash
git add src/classify_owner_occupation.py
git commit -m "feat: use PPD property_type and tenure_type as classification signals

Leasehold flats with no other signal → weak not-owner-occupied.
Freehold houses with sale-context EPC → weak owner-occupied.
Individual + sale EPC upgraded to high confidence."
```

---

### Task 5: Classify V2 candidates and report both populations

**Files:**
- Modify: `src/link_properties.py`
- Modify: `src/classify_owner_occupation.py`
- Modify: `src/run_pipeline.py`

**Step 1: Add `link_properties_v2` function in `link_properties.py`**

After the existing `link_properties()` function, add:

```python
def link_properties_v2(cfg: PipelineConfig) -> list[dict]:
    """Link and return V2 candidates (HPI-uplifted). V2 is a superset of V1."""
    candidates = read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet")
    if not candidates:
        return []

    epc_rows = read_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet")
    own_rows = read_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet")

    epc_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in epc_rows}
    own_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in own_rows}
    epc_by_postcode = _build_postcode_index(epc_rows)
    own_by_postcode = _build_postcode_index(own_rows)

    linked = []
    for c in candidates:
        pc = c.get("postcode_clean", "")
        addr = clean_text(c.get("address_clean", ""))
        key = (pc, addr)
        rec = dict(c)
        rec["address_norm"] = addr
        rec["match_stage"] = "unmatched"

        e = epc_idx.get(key)
        if e:
            rec["epc_category"] = e.get("epc_category", "unknown")
            rec["epc_transaction_type"] = e.get("epc_transaction_type", "")
            rec["match_stage"] = "exact_postcode_address"
        else:
            same_pc = epc_by_postcode.get(pc, [])
            if same_pc:
                scored = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc],
                    reverse=True, key=lambda z: z[0],
                )
                if scored and scored[0][0] >= cfg.fuzzy_match_cutoff:
                    rec["epc_category"] = scored[0][1].get("epc_category", "unknown")
                    rec["epc_transaction_type"] = scored[0][1].get("epc_transaction_type", "")
                    rec["match_stage"] = "postcode_fuzzy_address"

        o = own_idx.get(key)
        if not o:
            same_pc_own = own_by_postcode.get(pc, [])
            if same_pc_own:
                scored_own = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc_own],
                    reverse=True, key=lambda z: z[0],
                )
                if scored_own and scored_own[0][0] >= cfg.fuzzy_match_cutoff:
                    o = scored_own[0][1]
        if o:
            rec["ownership_type"] = o.get("ownership_type", "unresolved")
            rec["ownership_type_confidence"] = o.get("ownership_type_confidence", "low")
        linked.append(rec)

    write_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population_v2.parquet", linked)
    return linked
```

**Step 2: Also pass `epc_transaction_type` through in existing `link_properties`**

In the existing `link_properties()` EPC matching blocks, also copy `epc_transaction_type`:

```python
        # Where EPC match is found (both exact and fuzzy blocks):
        rec["epc_transaction_type"] = e.get("epc_transaction_type", "")
        # (for fuzzy: scored[0][1].get("epc_transaction_type", ""))
```

**Step 3: Add V2 classification + reporting in `classify_owner_occupation.py`**

After `classify_owner_occupation()`, add:

```python
def classify_v2(cfg: PipelineConfig) -> list[dict]:
    """Classify V2 (HPI-uplifted) population."""
    rows = read_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population_v2.parquet")
    if not rows:
        return []
    out = []
    for r in rows:
        status, tier, evidence, flag = classify_row(r)
        rr = dict(r)
        rr.update({
            "owner_occupation_status": status,
            "confidence_tier": tier,
            "evidence_basis": evidence,
            "conflicting_signals_flag": flag,
        })
        out.append(rr)
    write_parquet_placeholder(DATA_PROCESSED / "classified_v2.parquet", out)
    return out
```

**Step 4: Update `run_pipeline.py` to call V2 linking and classification**

After the existing V1 linking and classification:

```python
    linked_v2 = link_properties_v2(cfg); stage_counts["linked_v2"] = len(linked_v2)
    classified_v2 = classify_v2(cfg); stage_counts["classified_v2"] = len(classified_v2)
    metrics_v2 = build_headline_range(classified_v2)
    write_csv(OUTPUTS / "headline_metrics_v2.csv", metrics_v2, ["estimate_type", "owner_occupation_share"])
```

**Step 5: Commit**

```bash
git add src/link_properties.py src/classify_owner_occupation.py src/run_pipeline.py
git commit -m "feat: classify V2 (HPI-uplifted) candidates and report both populations

V1 = sold >= £2M. V2 = uplifted value >= £2M (superset).
V2 includes longer-held properties likely biased toward owner-occupation."
```

---

### Task 6: Download VOA CTSOP Band H counts for population calibration

**Files:**
- Modify: `src/config.py` — add CTSOP URL
- Modify: `src/download_data.py` — add CTSOP download
- Create: `src/prepare_voa.py` — parse Band H counts from CTSOP

**Step 1: Add CTSOP URL to config**

In `PipelineConfig.source_urls`, add:

```python
        "voa_ctsop": "https://assets.publishing.service.gov.uk/media/6685468cab5fc5929851b928/CTSOP1-0-1993-2024.zip",
```

**Step 2: Add CTSOP download to `download_data.py`**

In `run_downloads()`, add after the existing downloads:

```python
    # Download VOA CTSOP (council tax stock of properties by band)
    ctsop_dir = DATA_RAW / "voa"
    ctsop_dir.mkdir(parents=True, exist_ok=True)
    ctsop_files = list(ctsop_dir.glob("*.csv")) + list(ctsop_dir.glob("*.zip"))
    if not ctsop_files:
        ctsop_url = cfg.source_urls.get("voa_ctsop", "")
        if ctsop_url:
            print(f"Downloading VOA CTSOP from {ctsop_url}")
            _download_single_file(ctsop_url, ctsop_dir / "CTSOP1-0.zip")
```

**Step 3: Create `src/prepare_voa.py`**

```python
from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


def prepare_voa_band_h(cfg: PipelineConfig) -> dict:
    """Parse CTSOP to extract Band H counts by local authority.

    Returns dict with keys: total_band_h, by_district (dict of district→count),
    and band_h_share (Band H as share of all dwellings).
    """
    folder = DATA_RAW / "voa"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        print("INFO: No VOA CTSOP files found. Skipping population calibration.")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    # CTSOP CSV structure: columns include local authority name, Band A-H counts
    # We need the latest year's Band H column
    rows = []
    for path in files:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            reader = csv.DictReader(txt)
                            for r in reader:
                                rows.append({k.lower().strip(): v for k, v in r.items()})
        elif path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append({k.lower().strip(): v for k, v in r.items()})

    if not rows:
        print("INFO: VOA CTSOP files found but no rows parsed.")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    # Find the Band H column — CTSOP uses various column naming conventions
    # Look for columns containing "band_h" or "h" in the header
    sample = rows[0]
    band_h_col = None
    all_bands_col = None
    area_col = None
    year_col = None
    for k in sample:
        kl = k.lower().strip()
        if "band_h" in kl or kl.endswith("_h") or kl == "h":
            band_h_col = k
        if "all" in kl and "band" in kl:
            all_bands_col = k
        if "total" in kl and band_h_col is None:
            pass  # skip
        if "area" in kl or "authority" in kl or "la name" in kl or "district" in kl:
            area_col = k
        if "year" in kl or "date" in kl:
            year_col = k

    if not band_h_col:
        # Try to find it by examining column names
        print(f"INFO: Could not identify Band H column. Available columns: {list(sample.keys())[:20]}")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    # Filter to latest year if year column exists
    if year_col:
        years = {r.get(year_col, "") for r in rows}
        latest = max(years) if years else ""
        rows = [r for r in rows if r.get(year_col, "") == latest]

    total_h = 0
    total_all = 0
    by_district = {}
    for r in rows:
        try:
            h_count = int(float(str(r.get(band_h_col, "0")).replace(",", "").strip() or "0"))
        except (ValueError, TypeError):
            continue
        area = r.get(area_col, "unknown") if area_col else "unknown"
        total_h += h_count
        by_district[clean_text(area)] = h_count
        if all_bands_col:
            try:
                total_all += int(float(str(r.get(all_bands_col, "0")).replace(",", "").strip() or "0"))
            except (ValueError, TypeError):
                pass

    result = {
        "total_band_h": total_h,
        "by_district": by_district,
        "band_h_share": (total_h / total_all) if total_all else 0.0,
    }
    print(f"VOA: {total_h:,} Band H properties across {len(by_district)} areas.")

    # Save for downstream use
    band_rows = [{"district": k, "band_h_count": v} for k, v in by_district.items()]
    write_parquet_placeholder(DATA_INTERIM / "voa_band_h.parquet", band_rows)

    return result


if __name__ == "__main__":
    print(prepare_voa_band_h(PipelineConfig()))
```

**Step 4: Commit**

```bash
git add src/config.py src/download_data.py src/prepare_voa.py
git commit -m "feat: download and parse VOA CTSOP Band H counts for population calibration"
```

---

### Task 7: Improve sensitivity analysis + VOA calibration

**Files:**
- Modify: `src/sensitivity_analysis.py`

**Step 1: Expand scenarios**

Replace current thin sensitivity with richer scenarios:

```python
from __future__ import annotations

import base64
from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder, write_csv
from classify_owner_occupation import build_headline_range


def run_sensitivity(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet")
    base = build_headline_range(rows)
    central = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "central"), 0.0)
    conservative = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "conservative"), 0.0)
    upper = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "upper"), 0.0)

    total = len(rows) or 1
    unmatched = sum(1 for r in rows if r.get("match_stage") == "unmatched")
    uncertain = sum(1 for r in rows if r.get("owner_occupation_status") == "uncertain")

    # V1 and V2 counts
    v1 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet"))
    v2 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet"))

    # V2 headline if available
    v2_rows = read_parquet_placeholder(DATA_PROCESSED / "classified_v2.parquet")
    v2_metrics = build_headline_range(v2_rows) if v2_rows else []
    v2_central = next((x["owner_occupation_share"] for x in v2_metrics if x["estimate_type"] == "central"), 0.0)

    # VOA Band H population calibration
    voa_rows = read_parquet_placeholder(DATA_INTERIM / "voa_band_h.parquet")
    voa_total = sum(int(r.get("band_h_count", 0)) for r in voa_rows)

    scenarios = [
        {"scenario": "v1_conservative", "owner_share": conservative},
        {"scenario": "v1_central", "owner_share": central},
        {"scenario": "v1_upper", "owner_share": upper},
    ]

    # V2 population (includes longer-held properties, likely more owner-occupied)
    if v2_central:
        scenarios.append({"scenario": "v2_central", "owner_share": v2_central})

    # Unmatched sensitivity: what if all unmatched are owner-occupied vs not
    scenarios.extend([
        {"scenario": "unmatched_all_not_owner", "owner_share": max(0.0, central - (unmatched * 0.5) / total)},
        {"scenario": "unmatched_all_owner", "owner_share": min(1.0, central + (unmatched * 0.5) / total)},
    ])

    # Signal-poor sensitivity
    scenarios.append({"scenario": "signal_poor_reclassed", "owner_share": max(0.0, central - 0.05)})

    # VOA population calibration: our V1 sample as share of estimated Band H population
    # Band H ≈ £320K in 1991 ≈ roughly £1.5-2.5M today (wide range)
    if voa_total:
        coverage = v1 / voa_total
        scenarios.extend([
            {"scenario": "voa_band_h_total", "owner_share": float(voa_total)},
            {"scenario": "v1_coverage_of_band_h", "owner_share": coverage},
        ])
        # If we only observe coverage% of the true population, and the unobserved
        # are more likely owner-occupied (long holders who haven't transacted):
        # adjusted estimate weights observed central + assumes 70% owner for unobserved
        if coverage < 1.0:
            adjusted = central * coverage + 0.70 * (1 - coverage)
            scenarios.append({"scenario": "voa_adjusted_central", "owner_share": adjusted})

    # Population counts for reference
    scenarios.extend([
        {"scenario": "candidate_pop_v1_count", "owner_share": float(v1)},
        {"scenario": "candidate_pop_v2_count", "owner_share": float(v2)},
    ])

    write_csv(OUTPUTS / "sensitivity_scenarios.csv", scenarios, ["scenario", "owner_share"])

    # minimal PNG placeholder
    png_pixel = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO6p5wAAAABJRU5ErkJggg==")
    (OUTPUTS / "sensitivity_chart.png").write_bytes(png_pixel)

    share_scenarios = [s for s in scenarios if "count" not in s["scenario"] and "coverage" not in s["scenario"] and "band_h_total" not in s["scenario"]]
    values = [s["owner_share"] for s in share_scenarios]
    low, high = (min(values), max(values)) if values else (0.0, 0.0)
    (OUTPUTS / "sensitivity_note.md").write_text(
        f"# Sensitivity note\n\n"
        f"V1 range (transaction-based): {conservative:.3f} to {upper:.3f} (central: {central:.3f})\n"
        f"V2 central (HPI-uplifted): {v2_central:.3f}\n"
        f"Full sensitivity range: {low:.3f} to {high:.3f}\n"
        f"VOA Band H estimated population: {voa_total:,}\n"
        f"V1 coverage of Band H: {v1/voa_total*100:.1f}%\n" if voa_total else ""
        f"\nLower bound implies tighter targeting may be feasible; "
        f"upper bound implies higher owner-occupier inclusion risk.\n",
        encoding="utf-8",
    )
    return scenarios


if __name__ == "__main__":
    run_sensitivity(PipelineConfig())
```

**Step 2: Commit**

```bash
git add src/sensitivity_analysis.py
git commit -m "feat: richer sensitivity analysis with V2 comparison and VOA calibration"
```

---

### Task 8: Update pipeline orchestration and re-run

**Files:**
- Modify: `src/run_pipeline.py`

**Step 1: Wire in VOA and V2 stages**

Add imports and calls:

```python
from prepare_voa import prepare_voa_band_h
```

In `run_pipeline()`, after contextual sources:

```python
    voa = prepare_voa_band_h(cfg); stage_counts["voa_band_h"] = voa.get("total_band_h", 0)
```

After V1 classification, add V2:

```python
    from classify_owner_occupation import classify_v2
    linked_v2 = link_properties_v2(cfg); stage_counts["linked_v2"] = len(linked_v2)
    classified_v2 = classify_v2(cfg); stage_counts["classified_v2"] = len(classified_v2)
    metrics_v2 = build_headline_range(classified_v2)
    write_csv(OUTPUTS / "headline_metrics_v2.csv", metrics_v2, ["estimate_type", "owner_occupation_share"])
```

Also update imports to include `link_properties_v2`.

**Step 2: Run the full pipeline**

```bash
cd /mnt/c/Users/court/mets/property-sandbox && python3 src/run_pipeline.py
```

**Step 3: Verify outputs**

Check these files exist and contain reasonable values:
- `outputs/headline_metrics.csv` — V1 range (should be tighter than before)
- `outputs/headline_metrics_v2.csv` — V2 range (likely higher owner-occupation)
- `outputs/sensitivity_scenarios.csv` — should have ~12 scenarios
- `outputs/ownership_type_distribution.csv` — should show fewer "individual" (now correctly classified as corporate from CCOD)
- `outputs/linkage_coverage_summary.csv` — match rate should be similar or slightly better
- `outputs/audit_summary.md` — should include voa_band_h and v2 stage counts

**Step 4: Commit**

```bash
git add src/run_pipeline.py
git commit -m "feat: wire VOA calibration and V2 classification into pipeline"
```

---

## Summary of changes by file

| File | Changes |
|------|---------|
| `src/prepare_ownership.py` | Task 1: Structured CCOD/OCOD categories, all 4 proprietor slots |
| `src/prepare_epc.py` | Task 2: TRANSACTION_TYPE, dedup by latest LODGEMENT_DATE |
| `src/io_utils.py` | Task 3: Building number guard in similarity scoring |
| `src/classify_owner_occupation.py` | Task 4+5: PPD signals, sale_context, V2 classification |
| `src/link_properties.py` | Task 5: V2 linking function, pass epc_transaction_type |
| `src/config.py` | Task 6: CTSOP URL |
| `src/download_data.py` | Task 6: CTSOP download |
| `src/prepare_voa.py` | Task 6: New — parse Band H counts |
| `src/sensitivity_analysis.py` | Task 7: Richer scenarios, VOA calibration |
| `src/run_pipeline.py` | Task 8: Wire everything together |

## Datasets

**Auto-downloaded (no action needed):**
- VOA CTSOP Band H counts (~2MB ZIP)

**Already present (from user):**
- PPD yearly files in `data/raw/price_paid/`
- EPC bulk ZIP in `data/raw/epc/`
- CCOD + OCOD CSVs in `data/raw/ownership/`

**Not needed for this iteration:**
- Companies House (lower priority — CCOD structured fields replace most of its value)
- EHS (doesn't have value-band breakdowns)
