from __future__ import annotations

from config import DATA_INTERIM, DATA_RAW, DOCS, PipelineConfig
from io_utils import clean_text, read_csv_files, write_parquet_placeholder


def prepare_addresses(cfg: PipelineConfig) -> list[dict]:
    files = sorted((DATA_RAW / "os_gb_address").glob("*.csv"))
    if not files:
        (DOCS / "address_reference_note.md").write_text(
            "OS GB Address not accessible in this environment (likely licensed/paid). Using fallback address standardisation only.\n",
            encoding="utf-8",
        )
        write_parquet_placeholder(DATA_INTERIM / "address_reference.parquet", [])
        return []

    rows = read_csv_files(files)
    out = []
    for r in rows:
        out.append({
            "uprn": r.get("uprn", ""),
            "postcode_clean": clean_text(r.get("postcode", "")).replace(" ", ""),
            "address_clean": clean_text(r.get("address", "")),
            "source": "os_gb_address",
        })
    write_parquet_placeholder(DATA_INTERIM / "address_reference.parquet", out)
    return out


if __name__ == "__main__":
    prepare_addresses(PipelineConfig())
