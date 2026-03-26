from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder

PPD_COLUMNS = [
    "transaction_unique_identifier", "price", "transfer_date", "postcode", "property_type",
    "new_build_flag", "tenure_type", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status",
]


def _iter_ppd_rows(files: list[Path]) -> csv.DictReader:
    """Yield rows from PPD CSV/ZIP files one at a time to avoid loading everything into memory."""
    for path in files:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            yield from csv.DictReader(txt, fieldnames=PPD_COLUMNS)
        else:
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                yield from csv.DictReader(f, fieldnames=PPD_COLUMNS)


def prepare_price_paid(cfg: PipelineConfig) -> list[dict]:
    folder = DATA_RAW / "price_paid"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        msg = f"Missing Price Paid raw files in {folder}. Run download_data.py or place CSV/ZIP files manually."
        raise FileNotFoundError(msg)

    # Stream rows and filter early: only keep prices >= threshold_band_floor (1.5M)
    # This reduces memory from ~8GB (all rows) to ~80MB (relevant rows only)
    dedup: dict[str, dict] = {}
    row_count = 0
    kept_count = 0

    for r in _iter_ppd_rows(files):
        row_count += 1
        price_raw = (r.get("price", "0") or "0").strip("'\" ")
        try:
            price = int(float(price_raw))
        except Exception:
            continue

        if price < cfg.threshold_band_floor:
            continue

        kept_count += 1
        transfer_date = (r.get("transfer_date", "") or "").strip("'\" ")
        try:
            dt = datetime.fromisoformat(transfer_date[:10]).date().isoformat()
        except Exception:
            dt = ""

        postcode_clean = clean_text(r.get("postcode", "")).replace(" ", "")
        address_clean = clean_text(" ".join([
            r.get("paon", ""), r.get("saon", ""), r.get("street", ""), r.get("town_city", ""),
        ]))
        key = f"{postcode_clean}|{address_clean}"

        out = dict(r)
        out.update({
            "price": price,
            "transfer_date": dt,
            "postcode_clean": postcode_clean,
            "address_clean": address_clean,
            "property_key": key,
        })

        # Dedup inline: keep latest transaction per property
        if key not in dedup or (dt > dedup[key].get("transfer_date", "")):
            dedup[key] = out

    if not dedup:
        raise RuntimeError(
            f"Price Paid parsing produced zero rows >= £{cfg.threshold_band_floor:,} "
            f"(scanned {row_count:,} rows total)."
        )

    print(f"PPD: scanned {row_count:,} rows, kept {kept_count:,} >= £{cfg.threshold_band_floor:,}, "
          f"{len(dedup):,} unique properties after dedup.")

    final_rows = list(dedup.values())
    write_parquet_placeholder(DATA_INTERIM / "price_paid_clean.parquet", final_rows)
    return final_rows


if __name__ == "__main__":
    prepare_price_paid(PipelineConfig())
