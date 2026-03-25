from __future__ import annotations

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import read_csv_and_zip_files, write_parquet_placeholder


def prepare_ukhpi(cfg: PipelineConfig) -> list[dict]:
    folder = DATA_RAW / "ukhpi"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        msg = f"Missing UKHPI raw files in {folder}. Run download_data.py or place UKHPI CSV/ZIP files manually."
        raise FileNotFoundError(msg)

    rows = read_csv_and_zip_files(files)
    if not rows:
        raise RuntimeError("UKHPI files found but no rows could be parsed.")

    out = []
    by_region_base = {}
    for r in rows:
        region = r.get("region") or r.get("Region") or r.get("area") or r.get("Area") or r.get("geography") or r.get("Geography") or "unknown"
        date = r.get("date") or r.get("Date") or ""
        hpi_raw = (
            r.get("hpi") or r.get("Index") or r.get("index") or r.get("house_price_index") or r.get("House price index") or ""
        )
        try:
            hpi = float(str(hpi_raw).replace(",", ""))
from io_utils import read_csv_files, write_parquet_placeholder


def prepare_ukhpi(cfg: PipelineConfig) -> list[dict]:
    files = sorted((DATA_RAW / "ukhpi").glob("*.csv"))
    if not files:
        write_parquet_placeholder(DATA_INTERIM / "ukhpi_uplift.parquet", [])
        return []

    rows = read_csv_files(files)
    # Flexible column discovery
    out = []
    by_region_base = {}
    for r in rows:
        region = r.get("region") or r.get("Region") or r.get("area") or r.get("Area") or r.get("geography") or "unknown"
        date = r.get("date") or r.get("Date") or ""
        hpi_raw = r.get("hpi") or r.get("Index") or r.get("index") or ""
        try:
            hpi = float(hpi_raw)
        except Exception:
            continue

        if region not in by_region_base:
            by_region_base[region] = hpi
        base = by_region_base[region] or 1.0
        uplift = hpi / base if base else 1.0
        out.append({"region": region, "date": date, "hpi": hpi, "hpi_base": base, "uplift_factor": uplift})

    if not out:
        raise RuntimeError("UKHPI parsing produced zero usable rows.")

    write_parquet_placeholder(DATA_INTERIM / "ukhpi_uplift.parquet", out)
    return out


if __name__ == "__main__":
    prepare_ukhpi(PipelineConfig())
