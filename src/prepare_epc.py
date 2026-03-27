from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


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


def _iter_epc_rows(files: list[Path]):
    """Yield rows from EPC CSV/ZIP files one at a time, normalising headers to lowercase."""
    for path in files:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if not name.lower().endswith(".csv"):
                        continue
                    # Skip recommendations files — only certificates have tenure data
                    if "recommendations" in name.lower():
                        continue
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

    filter_desc = f", kept {len(out):,} matching candidate postcodes" if candidate_postcodes else ""
    print(f"EPC: scanned {scanned:,} rows{filter_desc}.")

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

    write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", out)
    return out


if __name__ == "__main__":
    prepare_epc(PipelineConfig())
