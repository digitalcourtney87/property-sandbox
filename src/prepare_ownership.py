from __future__ import annotations

from pathlib import Path
import json
import pandas as pd

from config import DATA_INTERIM, DATA_RAW, PipelineConfig


def classify_owner_name(name: str) -> tuple[str, str]:
    n = (name or "").lower()
    if not n.strip():
        return "unresolved", "low"
    if any(x in n for x in ["ltd", "limited", "plc", "llp"]):
        return "UK_company", "high"
    if any(x in n for x in ["inc", "corp", "sa", "gmbh", "bvi", "cayman"]):
        return "overseas_company", "medium"
    if "trust" in n or "trustee" in n or "foundation" in n:
        return "trust_or_other", "medium"
    if any(ch.isdigit() for ch in n) and len(n.split()) <= 2:
        return "unresolved", "low"
    return "individual", "medium"


def prepare_ownership(cfg: PipelineConfig) -> pd.DataFrame:
    discovery_dir = DATA_RAW / "land_property_api_discovery"
    json_files = sorted(discovery_dir.glob("*.json"))

    # Placeholder structure: if ownership endpoint exports are not available,
    # keep an empty but typed table for downstream reproducibility.
    rows = []

    # Allow local drop-in ownership extracts in CSV format if provided later.
    ownership_dir = DATA_RAW / "land_property_api"
    csv_files = sorted(ownership_dir.glob("*.csv"))
    for f in csv_files:
        try:
            d = pd.read_csv(f, low_memory=False)
        except Exception:
            continue

        cols = {c.lower(): c for c in d.columns}
        owner_col = next((cols[c] for c in cols if "owner" in c or "proprietor" in c or "name" == c), None)
        postcode_col = next((cols[c] for c in cols if "postcode" in c), None)
        addr_col = next((cols[c] for c in cols if "address" in c), None)

        if owner_col is None:
            continue

        for _, r in d.iterrows():
            owner_name = str(r.get(owner_col, ""))
            ownership_type, conf = classify_owner_name(owner_name)
            rows.append(
                {
                    "postcode_clean": str(r.get(postcode_col, "")).replace(" ", "").lower() if postcode_col else "",
                    "address_clean": str(r.get(addr_col, "")).lower() if addr_col else "",
                    "owner_name_raw": owner_name,
                    "ownership_type": ownership_type,
                    "ownership_type_confidence": conf,
                }
            )

    df = pd.DataFrame(rows, columns=[
        "postcode_clean", "address_clean", "owner_name_raw", "ownership_type", "ownership_type_confidence"
    ])
    df.to_parquet(DATA_INTERIM / "ownership_clean.parquet", index=False)
    return df


if __name__ == "__main__":
    prepare_ownership(PipelineConfig())
