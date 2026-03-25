from __future__ import annotations

import pandas as pd
from config import DATA_INTERIM, DATA_RAW, PipelineConfig


CONTEXTUAL_DIR_NAMES = [
    "planning_data",
    "dwelling_stock",
    "rents_lettings",
    "ehs_tables",
    "house_building",
    "fire_stats",
]


def prepare_contextual_sources(cfg: PipelineConfig) -> pd.DataFrame:
    rows = []
    for d in CONTEXTUAL_DIR_NAMES:
        folder = DATA_RAW / d
        for f in sorted(folder.glob("*")):
            rows.append({
                "dataset": d,
                "file_name": f.name,
                "file_size_bytes": f.stat().st_size,
                "used_in_classifier": False,
                "note": "Contextual only unless explicit defensible property-level use emerges.",
            })

    df = pd.DataFrame(rows)
    df.to_csv(DATA_INTERIM / "contextual_inventory.csv", index=False)
    return df


if __name__ == "__main__":
    prepare_contextual_sources(PipelineConfig())
