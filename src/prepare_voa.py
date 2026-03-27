from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


def prepare_voa_band_h(cfg: PipelineConfig) -> dict:
    """Parse CTSOP to extract Band H counts by local authority.

    CTSOP ZIP contains one CSV per year (1993-2024) plus a combined file.
    Each CSV has rows at multiple geography levels: ENGWAL, NATL, REGL, LAUA.
    We read only the latest single-year file and only LAUA-level rows
    to avoid double-counting.

    Returns dict with keys: total_band_h, by_district, band_h_share.
    """
    folder = DATA_RAW / "voa"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        print("INFO: No VOA CTSOP files found. Skipping population calibration.")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    # Find the latest single-year CSV inside ZIPs
    # Filenames like CTSOP1_0_2024_03_31.csv — pick the one with the highest year
    target_csv = None
    target_zf = None
    for path in files:
        if path.suffix.lower() != ".zip":
            continue
        zf = zipfile.ZipFile(path)
        best_year = 0
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            # Skip combined multi-year files (contain year ranges like 1993_2024)
            if re.search(r"\d{4}_\d{4}", name):
                continue
            # Extract year from filename
            match = re.search(r"(\d{4})", name)
            if match:
                year = int(match.group(1))
                if year > best_year:
                    best_year = year
                    target_csv = name
                    target_zf = zf

    if not target_csv or not target_zf:
        print("INFO: Could not identify latest CTSOP year file.")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    print(f"VOA: reading {target_csv}")

    # Parse the target CSV
    rows: list[dict] = []
    with target_zf.open(target_csv) as f:
        txt = io.TextIOWrapper(f, encoding="utf-8-sig", errors="ignore", newline="")
        reader = csv.DictReader(txt)
        for r in reader:
            rows.append({k.lower().strip(): v.strip().strip('"') for k, v in r.items()})

    target_zf.close()

    if not rows:
        print("INFO: CTSOP file had no rows.")
        return {"total_band_h": 0, "by_district": {}, "band_h_share": 0.0}

    # Extract national total (ENGWAL row) and LA-level breakdown
    national_h = 0
    national_all = 0
    by_district: dict[str, int] = {}

    for r in rows:
        geo = r.get("geography", "").strip().strip('"')
        area_name = r.get("area_name", "unknown").strip().strip('"')

        try:
            h_val = r.get("band_h", "0").replace(",", "").strip().strip('"')
            h_count = int(float(h_val)) if h_val and h_val != ".." else 0
        except (ValueError, TypeError):
            h_count = 0

        try:
            all_val = r.get("all_properties", "0").replace(",", "").strip().strip('"')
            all_count = int(float(all_val)) if all_val and all_val != ".." else 0
        except (ValueError, TypeError):
            all_count = 0

        if geo == "ENGWAL":
            national_h = h_count
            national_all = all_count
        elif geo == "LAUA":
            by_district[clean_text(area_name)] = h_count

    # If national total wasn't found, sum LA-level
    if not national_h:
        national_h = sum(by_district.values())

    result = {
        "total_band_h": national_h,
        "by_district": by_district,
        "band_h_share": (national_h / national_all) if national_all else 0.0,
    }
    print(f"VOA: {national_h:,} Band H properties across {len(by_district)} local authorities.")

    # Save for downstream use — include national total as a special row
    band_rows = [{"district": "__national_total__", "band_h_count": national_h}]
    band_rows.extend({"district": k, "band_h_count": v} for k, v in by_district.items())
    write_parquet_placeholder(DATA_INTERIM / "voa_band_h.parquet", band_rows)

    return result


if __name__ == "__main__":
    print(prepare_voa_band_h(PipelineConfig()))
