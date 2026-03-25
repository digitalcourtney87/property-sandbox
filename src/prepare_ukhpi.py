from __future__ import annotations

from pathlib import Path
import pandas as pd

from config import DATA_INTERIM, DATA_RAW, PipelineConfig


def _find_ukhpi_csv() -> list[Path]:
    return sorted((DATA_RAW / "ukhpi").glob("*.csv"))


def prepare_ukhpi(cfg: PipelineConfig) -> pd.DataFrame:
    files = _find_ukhpi_csv()
    if not files:
        df = pd.DataFrame(columns=["region", "date", "hpi", "hpi_base", "uplift_factor"])
        df.to_parquet(DATA_INTERIM / "ukhpi_uplift.parquet", index=False)
        return df

    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            continue
    if not frames:
        df = pd.DataFrame(columns=["region", "date", "hpi", "hpi_base", "uplift_factor"])
        df.to_parquet(DATA_INTERIM / "ukhpi_uplift.parquet", index=False)
        return df

    raw = pd.concat(frames, ignore_index=True)

    col_map = {c.lower(): c for c in raw.columns}
    date_col = next((col_map[c] for c in col_map if "date" in c), None)
    region_col = next((col_map[c] for c in col_map if "region" in c or "area" in c or "geography" in c), None)
    hpi_col = next((col_map[c] for c in col_map if "index" in c or c == "hpi"), None)

    if not all([date_col, region_col, hpi_col]):
        df = pd.DataFrame(columns=["region", "date", "hpi", "hpi_base", "uplift_factor"])
        df.to_parquet(DATA_INTERIM / "ukhpi_uplift.parquet", index=False)
        return df

    df = raw[[region_col, date_col, hpi_col]].copy()
    df.columns = ["region", "date", "hpi"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["hpi"] = pd.to_numeric(df["hpi"], errors="coerce")
    df = df.dropna(subset=["date", "hpi"])

    df = df.sort_values(["region", "date"])
    df["hpi_base"] = df.groupby("region")["hpi"].transform("first")
    df["uplift_factor"] = df["hpi"] / df["hpi_base"]

    out = DATA_INTERIM / "ukhpi_uplift.parquet"
    df.to_parquet(out, index=False)
    return df


if __name__ == "__main__":
    prepare_ukhpi(PipelineConfig())
