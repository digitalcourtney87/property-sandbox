from __future__ import annotations

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import write_csv

CONTEXTUAL_DIR_NAMES = [
    "planning_data",
    "dwelling_stock",
    "rents_lettings",
    "ehs_tables",
    "house_building",
    "fire_stats",
]


def prepare_contextual_sources(cfg: PipelineConfig) -> list[dict]:
    rows: list[dict] = []
    for name in CONTEXTUAL_DIR_NAMES:
        folder = DATA_RAW / name
        for fp in sorted(folder.glob("*")):
            if fp.is_file():
                rows.append({
                    "dataset": name,
                    "file_name": fp.name,
                    "file_size_bytes": fp.stat().st_size,
                    "used_in_classifier": False,
                    "note": "Contextual only.",
                })

    write_csv(
        DATA_INTERIM / "contextual_inventory.csv",
        rows,
        ["dataset", "file_name", "file_size_bytes", "used_in_classifier", "note"],
    )
    return rows


if __name__ == "__main__":
    prepare_contextual_sources(PipelineConfig())
