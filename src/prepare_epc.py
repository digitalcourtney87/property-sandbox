from __future__ import annotations

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, read_csv_and_zip_files, write_parquet_placeholder


def map_epc_category(value: str) -> str:
    t = clean_text(value)
    if "owner" in t:
        return "owner_occupied"
    if "private" in t and "rent" in t:
        return "rented_private"
    if "social" in t or "council" in t or "housing association" in t:
        return "rented_social"
    return "unknown"


def prepare_epc(cfg: PipelineConfig) -> list[dict]:
    folder = DATA_RAW / "epc"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", [])
        return []

    rows = read_csv_and_zip_files(files)
    out = []
    for r in rows:
        postcode = clean_text(r.get("postcode", r.get("Postcode", ""))).replace(" ", "")
        addr = clean_text(r.get("address", r.get("Address", r.get("address1", ""))))
        tenure = r.get("tenure", r.get("transaction_type", r.get("tenancy", r.get("Tenure", ""))))
        out.append({
            "postcode_clean": postcode,
            "address_clean": addr,
            "epc_source_field": str(tenure),
            "epc_category": map_epc_category(str(tenure)),
        })

    write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", out)
    return out


if __name__ == "__main__":
    prepare_epc(PipelineConfig())
