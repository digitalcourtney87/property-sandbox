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

    # Parse and sort by date so base HPI per region is the earliest observation
    parsed = []
    for r in rows:
        region = (r.get("regionname") or r.get("region_name") or r.get("region name")
                  or r.get("region") or r.get("area") or r.get("geography") or "unknown")
        date = r.get("date") or ""
        hpi_raw = (r.get("index") or r.get("hpi") or r.get("house_price_index")
                   or r.get("house price index") or "")
        try:
            hpi = float(str(hpi_raw).replace(",", ""))
        except Exception:
            continue
        parsed.append({"region": region, "date": date, "hpi": hpi})

    parsed.sort(key=lambda x: x["date"])

    out = []
    by_region_base: dict[str, float] = {}
    for p in parsed:
        region = p["region"]
        if region not in by_region_base:
            by_region_base[region] = p["hpi"]
        base = by_region_base[region] or 1.0
        uplift = p["hpi"] / base if base else 1.0
        out.append({"region": region, "date": p["date"], "hpi": p["hpi"], "hpi_base": base, "uplift_factor": uplift})

    if not out:
        raise RuntimeError("UKHPI parsing produced zero usable rows.")

    write_parquet_placeholder(DATA_INTERIM / "ukhpi_uplift.parquet", out)
    return out


if __name__ == "__main__":
    prepare_ukhpi(PipelineConfig())
