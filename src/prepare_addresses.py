from __future__ import annotations

from pathlib import Path
import pandas as pd

from config import DATA_INTERIM, DATA_RAW, DOCS, PipelineConfig


def prepare_addresses(cfg: PipelineConfig) -> pd.DataFrame:
    os_dir = DATA_RAW / "os_gb_address"
    csvs = sorted(os_dir.glob("*.csv"))
    note_path = DOCS / "address_reference_note.md"

    if not csvs:
        note_path.write_text(
            "OS GB Address source not available in this environment (likely licensed/paid). "
            "Fallback mode enabled: address standardisation from source datasets only.\n",
            encoding="utf-8",
        )
        df = pd.DataFrame(columns=["uprn", "postcode_clean", "address_clean", "source"])
        df.to_parquet(DATA_INTERIM / "address_reference.parquet", index=False)
        return df

    frames = []
    for f in csvs:
        try:
            frames.append(pd.read_csv(f, low_memory=False))
        except Exception:
            continue
    if not frames:
        df = pd.DataFrame(columns=["uprn", "postcode_clean", "address_clean", "source"])
        df.to_parquet(DATA_INTERIM / "address_reference.parquet", index=False)
        return df

    raw = pd.concat(frames, ignore_index=True)
    cols = {c.lower(): c for c in raw.columns}
    uprn = next((cols[c] for c in cols if "uprn" in c), None)
    postcode = next((cols[c] for c in cols if "postcode" in c), None)
    address = next((cols[c] for c in cols if "address" in c), None)

    df = pd.DataFrame(
        {
            "uprn": raw[uprn] if uprn else "",
            "postcode_clean": raw[postcode].astype(str).str.lower().str.replace(" ", "", regex=False) if postcode else "",
            "address_clean": raw[address].astype(str).str.lower() if address else "",
            "source": "os_gb_address",
        }
    )
    df.to_parquet(DATA_INTERIM / "address_reference.parquet", index=False)
    return df


if __name__ == "__main__":
    prepare_addresses(PipelineConfig())
