from __future__ import annotations

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, read_csv_and_zip_files, write_parquet_placeholder


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


def prepare_ownership(cfg: PipelineConfig) -> list[dict]:
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

    rows = read_csv_and_zip_files(files)
    out = []
    for r in rows:
        # CCOD/OCOD uses "proprietor name (1)"; other formats use various keys
        owner_name = (r.get("proprietor name (1)") or r.get("proprietor_name")
                      or r.get("owner_name") or r.get("owner") or r.get("proprietor")
                      or r.get("name") or "")
        ownership_type, conf = classify_owner_name(owner_name)
        postcode_raw = r.get("postcode", "")
        address_raw = r.get("property address") or r.get("address") or ""
        out.append({
            "postcode_clean": clean_text(postcode_raw).replace(" ", ""),
            "address_clean": clean_text(address_raw),
            "owner_name_raw": owner_name,
            "ownership_type": ownership_type,
            "ownership_type_confidence": conf,
        })

    write_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet", out)
    return out


if __name__ == "__main__":
    prepare_ownership(PipelineConfig())
