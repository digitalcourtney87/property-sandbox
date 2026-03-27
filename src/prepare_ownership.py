from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


# ---------------------------------------------------------------------------
# Proprietorship-category keyword groups (matched against the lowercased
# ``Proprietorship Category (N)`` field from CCOD / OCOD).
# ---------------------------------------------------------------------------
_PUBLIC_BODY_KEYWORDS = ("local authority", "county council")
_HOUSING_ASSOC_KEYWORDS = ("housing association", "housing society")
_COMPANY_KEYWORDS = (
    "limited company",
    "public limited",
    "corporate body",
    "limited liability",
    "unlimited company",
    "registered society",
    "co operative",
    "community benefit",
    "industrial and provident",
)

# Confidence ranking for selecting the "best" classification across
# multiple proprietor slots.
_CONF_RANK = {"high": 3, "medium": 2, "low": 1}

# Types considered corporate for the "corporate dominates" rule.
_CORPORATE_TYPES = {"UK_company", "overseas_company", "UK_public_body", "UK_housing_association"}


def classify_proprietor(
    name: str,
    category: str,
    source_type: str,
) -> tuple[str, str]:
    """Classify a single proprietor using source type, structured category, and name.

    Parameters
    ----------
    name:
        Proprietor name (raw text).
    category:
        Value from the ``Proprietorship Category (N)`` field, or empty string.
    source_type:
        One of ``"ocod"``, ``"ccod"``, or ``""`` (unknown / other).

    Returns
    -------
    tuple of (ownership_type, confidence)
    """
    # ------------------------------------------------------------------
    # 1. OCOD — every row is overseas corporate by definition
    # ------------------------------------------------------------------
    if source_type == "ocod":
        return "overseas_company", "high"

    # ------------------------------------------------------------------
    # 2. CCOD or any row with a populated category field
    # ------------------------------------------------------------------
    cat_lower = (category or "").lower().strip()
    if source_type == "ccod" or cat_lower:
        if cat_lower:
            if any(kw in cat_lower for kw in _PUBLIC_BODY_KEYWORDS):
                return "UK_public_body", "high"
            if any(kw in cat_lower for kw in _HOUSING_ASSOC_KEYWORDS):
                return "UK_housing_association", "high"
            if any(kw in cat_lower for kw in _COMPANY_KEYWORDS):
                return "UK_company", "high"
        # CCOD only contains companies; if the category didn't match a
        # specific bucket we still know it's corporate.
        if source_type == "ccod":
            return "UK_company", "medium"

    # ------------------------------------------------------------------
    # 3. Fallback: name-based heuristic (same as before, minus "sa")
    # ------------------------------------------------------------------
    n = clean_text(name)
    if not n:
        return "unresolved", "low"
    if any(x in n for x in ["ltd", "limited", "plc", "llp"]):
        return "UK_company", "high"
    if any(x in n for x in ["inc", "corp", "gmbh", "bvi", "cayman"]):
        return "overseas_company", "medium"
    if "trust" in n or "trustee" in n or "foundation" in n:
        return "trust_or_other", "medium"
    return "individual", "medium"


def _source_type_from_filename(filename: str) -> str:
    """Derive ``'ocod'``, ``'ccod'``, or ``''`` from a file / zip-entry name."""
    upper = filename.upper()
    if "OCOD" in upper:
        return "ocod"
    if "CCOD" in upper:
        return "ccod"
    return ""


def _iter_ownership_rows(files: list[Path]):
    """Yield ``(row_dict, source_type)`` from ownership CSV/ZIP files,
    normalising headers to lowercase."""
    for path in files:
        # Determine source_type from the outer filename first; zip entries
        # may override if they contain a clearer indicator.
        path_source = _source_type_from_filename(path.name)
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        entry_source = _source_type_from_filename(name) or path_source
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            reader = csv.DictReader(txt)
                            for r in reader:
                                yield ({k.lower().strip(): v for k, v in r.items()}, entry_source)
        elif path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    yield ({k.lower().strip(): v for k, v in r.items()}, path_source)


def _best_classification(
    row: dict,
    source_type: str,
) -> tuple[str, str, str]:
    """Check all four proprietor slots and return the best classification.

    Returns ``(owner_name, ownership_type, confidence)``.

    Strategy:
    * If ANY proprietor is classified as corporate, use that classification
      (corporate dominates).
    * Otherwise, keep the highest-confidence non-corporate classification.
    """
    best_corporate: tuple[str, str, str] | None = None
    best_other: tuple[str, str, str] | None = None

    for i in range(1, 5):
        name_key = f"proprietor name ({i})"
        cat_key = f"proprietorship category ({i})"
        name = row.get(name_key, "") or ""
        category = row.get(cat_key, "") or ""

        if not name.strip() and not category.strip():
            continue

        otype, conf = classify_proprietor(name, category, source_type)

        if otype in _CORPORATE_TYPES:
            if best_corporate is None or _CONF_RANK.get(conf, 0) > _CONF_RANK.get(best_corporate[2], 0):
                best_corporate = (name, otype, conf)
        else:
            if best_other is None or _CONF_RANK.get(conf, 0) > _CONF_RANK.get(best_other[2], 0):
                best_other = (name, otype, conf)

    if best_corporate is not None:
        return best_corporate
    if best_other is not None:
        return best_other

    # No proprietor slots populated — try legacy / generic columns
    fallback_name = (
        row.get("proprietor_name")
        or row.get("owner_name")
        or row.get("owner")
        or row.get("proprietor")
        or row.get("name")
        or ""
    )
    otype, conf = classify_proprietor(fallback_name, "", source_type)
    return fallback_name, otype, conf


def prepare_ownership(cfg: PipelineConfig, candidate_postcodes: set[str] | None = None) -> list[dict]:
    folder = DATA_RAW / "ownership"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        print(
            "INFO: No ownership files found in data/raw/ownership/. "
            "CCOD/OCOD data requires registration at https://use-land-property-data.service.gov.uk/. "
            "See docs/data_acquisition_guide.md for instructions."
        )
        write_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet", [])
        return []

    out = []
    scanned = 0
    for r, source_type in _iter_ownership_rows(files):
        scanned += 1
        postcode_raw = r.get("postcode", "")
        postcode = clean_text(postcode_raw).replace(" ", "")

        # Early postcode filter: skip rows not matching any candidate property
        if candidate_postcodes and postcode not in candidate_postcodes:
            continue

        owner_name, ownership_type, conf = _best_classification(r, source_type)

        address_raw = r.get("property address") or r.get("address") or ""
        # CCOD/OCOD addresses include the postcode at the end — strip it
        # so address matching against PPD (which omits postcode) works
        addr_clean = clean_text(address_raw)
        if postcode and addr_clean.endswith(postcode):
            addr_clean = addr_clean[: -len(postcode)].rstrip()
        out.append({
            "postcode_clean": postcode,
            "address_clean": addr_clean,
            "owner_name_raw": owner_name,
            "ownership_type": ownership_type,
            "ownership_type_confidence": conf,
        })

    filter_desc = f", kept {len(out):,} matching candidate postcodes" if candidate_postcodes else ""
    print(f"Ownership: scanned {scanned:,} rows{filter_desc}.")

    write_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet", out)
    return out


if __name__ == "__main__":
    prepare_ownership(PipelineConfig())
