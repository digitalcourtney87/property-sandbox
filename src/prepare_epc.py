from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


def map_epc_category(value: str) -> str:
    t = clean_text(value)
    if "owner" in t:
        return "owner_occupied"
    if "private" in t and "rent" in t:
        return "rented_private"
    if "social" in t or "council" in t or "housing association" in t:
        return "rented_social"
    return "unknown"


def _iter_epc_rows(files: list[Path]):
    """Yield rows from EPC CSV/ZIP files one at a time, normalising headers to lowercase."""
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


def prepare_epc(cfg: PipelineConfig, candidate_postcodes: set[str] | None = None) -> list[dict]:
    folder = DATA_RAW / "epc"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        print(
            "INFO: No EPC files found in data/raw/epc/. "
            "Property-level EPC data requires registration at https://epc.opendatacommunities.org/. "
            "See docs/data_acquisition_guide.md for instructions."
        )
        write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", [])
        return []

    out = []
    scanned = 0
    for r in _iter_epc_rows(files):
        scanned += 1
        postcode = clean_text(r.get("postcode", "")).replace(" ", "")

        # Early postcode filter: skip rows not matching any candidate property
        if candidate_postcodes and postcode not in candidate_postcodes:
            continue

        addr = clean_text(r.get("address") or r.get("address1") or "")
        tenure = r.get("tenure") or r.get("transaction_type") or r.get("tenancy") or ""
        out.append({
            "postcode_clean": postcode,
            "address_clean": addr,
            "epc_source_field": str(tenure),
            "epc_category": map_epc_category(str(tenure)),
        })

    filter_desc = f", kept {len(out):,} matching candidate postcodes" if candidate_postcodes else ""
    print(f"EPC: scanned {scanned:,} rows{filter_desc}.")

    write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", out)
    return out


if __name__ == "__main__":
    prepare_epc(PipelineConfig())
