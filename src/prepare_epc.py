from __future__ import annotations

from pathlib import Path
import pandas as pd

from config import DATA_INTERIM, DATA_RAW, PipelineConfig


def _clean_text(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9 ]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def _map_epc_category(text: str) -> str:
    t = (text or "").lower()
    if any(x in t for x in ["owner", "occupied by owner", "owner occupied"]):
        return "owner_occupied"
    if any(x in t for x in ["private rent", "private rented", "assured shorthold"]):
        return "rented_private"
    if any(x in t for x in ["social rent", "housing association", "council tenant"]):
        return "rented_social"
    return "unknown"


def prepare_epc(cfg: PipelineConfig) -> pd.DataFrame:
    epc_dir = DATA_RAW / "epc_collection"
    files = sorted(list(epc_dir.glob("*.csv")))
    if not files:
        df = pd.DataFrame(columns=["postcode_clean", "address_clean", "epc_category", "epc_source_field"]) 
        df.to_parquet(DATA_INTERIM / "epc_clean.parquet", index=False)
        return df

    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f, low_memory=False))
        except Exception:
            continue
    if not frames:
        df = pd.DataFrame(columns=["postcode_clean", "address_clean", "epc_category", "epc_source_field"]) 
        df.to_parquet(DATA_INTERIM / "epc_clean.parquet", index=False)
        return df

    raw = pd.concat(frames, ignore_index=True)
    cols = {c.lower(): c for c in raw.columns}
    postcode_col = next((cols[c] for c in cols if "postcode" in c), None)
    addr_col = next((cols[c] for c in cols if "address" in c and "1" not in c), None)
    tenure_col = next((cols[c] for c in cols if "tenure" in c or "transaction_type" in c or "tenancy" in c), None)

    if not postcode_col:
        raw["postcode_clean"] = ""
    else:
        raw["postcode_clean"] = _clean_text(raw[postcode_col]).str.replace(" ", "", regex=False)

    raw["address_clean"] = _clean_text(raw[addr_col]) if addr_col else ""
    raw["epc_source_field"] = raw[tenure_col].astype(str) if tenure_col else ""
    raw["epc_category"] = raw["epc_source_field"].map(_map_epc_category)

    out_cols = ["postcode_clean", "address_clean", "epc_category", "epc_source_field"]
    df = raw[out_cols].drop_duplicates()
    df.to_parquet(DATA_INTERIM / "epc_clean.parquet", index=False)
    return df


if __name__ == "__main__":
    prepare_epc(PipelineConfig())
