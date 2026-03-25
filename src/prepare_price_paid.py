from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

from config import DATA_INTERIM, DATA_RAW, PipelineConfig


PPD_COLUMNS = [
    "transaction_unique_identifier",
    "price",
    "transfer_date",
    "postcode",
    "property_type",
    "new_build_flag",
    "tenure_type",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
]


def _clean_text(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9 ]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def _load_latest_price_paid_csv() -> pd.DataFrame:
    ppd_dir = DATA_RAW / "price_paid"
    files = sorted(ppd_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame(columns=PPD_COLUMNS)
    frames = []
    for fp in files:
        df = pd.read_csv(fp, header=None, names=PPD_COLUMNS, low_memory=False)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def prepare_price_paid(cfg: PipelineConfig) -> pd.DataFrame:
    df = _load_latest_price_paid_csv()
    if df.empty:
        out = DATA_INTERIM / "price_paid_clean.parquet"
        df.to_parquet(out, index=False)
        return df

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["transfer_date"] = pd.to_datetime(df["transfer_date"], errors="coerce")
    df["postcode_clean"] = _clean_text(df["postcode"]).str.replace(" ", "", regex=False)

    for c in ["paon", "saon", "street", "locality", "town_city", "district", "county"]:
        df[f"{c}_clean"] = _clean_text(df[c])

    df["address_clean"] = (
        df[["paon_clean", "saon_clean", "street_clean", "town_city_clean"]]
        .fillna("")
        .agg(" ".join, axis=1)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    key = df["postcode_clean"].fillna("") + "|" + df["address_clean"].fillna("")
    df["property_key"] = key
    df = df.sort_values(["property_key", "transfer_date"], ascending=[True, False])
    dedup = df.drop_duplicates(subset=["property_key"], keep="first")

    if cfg.max_rows_per_source:
        dedup = dedup.head(cfg.max_rows_per_source)

    out = DATA_INTERIM / "price_paid_clean.parquet"
    dedup.to_parquet(out, index=False)
    return dedup


if __name__ == "__main__":
    prepare_price_paid(PipelineConfig())
