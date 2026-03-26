from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


def classify_owner_name(name: str) -> tuple[str, str]:
    n = clean_text(name)
    if not n:
        return "unresolved", "low"
    if any(x in n for x in ["ltd", "limited", "plc", "llp"]):
        return "UK_company", "high"
    if any(x in n for x in ["inc", "corp", "gmbh", "sa", "bvi", "cayman"]):
        return "overseas_company", "medium"
    if "trust" in n or "trustee" in n or "foundation" in n:
        return "trust_or_other", "medium"
    return "individual", "medium"


def _iter_ownership_rows(files: list[Path]):
    """Yield rows from ownership CSV/ZIP files one at a time, normalising headers to lowercase."""
    for path in files:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            reader = csv.DictReader(txt)
                            for r in reader:
                                yield {k.lower().strip(): v for k, v in r.items()}
        elif path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    yield {k.lower().strip(): v for k, v in r.items()}


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
    for r in _iter_ownership_rows(files):
        scanned += 1
        postcode_raw = r.get("postcode", "")
        postcode = clean_text(postcode_raw).replace(" ", "")

        # Early postcode filter: skip rows not matching any candidate property
        if candidate_postcodes and postcode not in candidate_postcodes:
            continue

        # CCOD/OCOD uses "proprietor name (1)"; other formats use various keys
        owner_name = (r.get("proprietor name (1)") or r.get("proprietor_name")
                      or r.get("owner_name") or r.get("owner") or r.get("proprietor")
                      or r.get("name") or "")
        ownership_type, conf = classify_owner_name(owner_name)
        address_raw = r.get("property address") or r.get("address") or ""
        out.append({
            "postcode_clean": postcode,
            "address_clean": clean_text(address_raw),
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
