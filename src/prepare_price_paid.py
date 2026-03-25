from __future__ import annotations

from datetime import datetime

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, read_csv_and_zip_files, write_parquet_placeholder
from io_utils import clean_text, read_csv_files, write_parquet_placeholder

PPD_COLUMNS = [
    "transaction_unique_identifier", "price", "transfer_date", "postcode", "property_type",
    "new_build_flag", "tenure_type", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status",
]


def prepare_price_paid(cfg: PipelineConfig) -> list[dict]:
    folder = DATA_RAW / "price_paid"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        msg = f"Missing Price Paid raw files in {folder}. Run download_data.py or place CSV/ZIP files manually."
        raise FileNotFoundError(msg)

    rows = read_csv_and_zip_files(files, fieldnames=PPD_COLUMNS)
    if not rows:
        raise RuntimeError("Price Paid files found but no rows could be parsed.")
    files = sorted((DATA_RAW / "price_paid").glob("*.csv"))
    rows = read_csv_files(files, fieldnames=PPD_COLUMNS) if files else []

    cleaned = []
    for r in rows:
        try:
            price = int(float(r.get("price", "0") or 0))
        except Exception:
            continue
        transfer_date = r.get("transfer_date", "")
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
        cleaned.append(out)

    if not cleaned:
        raise RuntimeError("Price Paid parsing produced zero usable rows.")

    dedup = {}
    for r in cleaned:
        k = r["property_key"]
        if k not in dedup or (r.get("transfer_date", "") > dedup[k].get("transfer_date", "")):
            dedup[k] = r

    final_rows = list(dedup.values())
    write_parquet_placeholder(DATA_INTERIM / "price_paid_clean.parquet", final_rows)
    return final_rows


if __name__ == "__main__":
    prepare_price_paid(PipelineConfig())
