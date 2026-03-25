from __future__ import annotations

import base64
from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder, write_csv
from classify_owner_occupation import build_headline_range


def run_sensitivity(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet")
    base = build_headline_range(rows)
    central = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "central"), 0.0)

    unmatched = [r for r in rows if r.get("match_stage") == "unmatched"]
    unmatched_count = len(unmatched)
    total = len(rows) or 1

    scenarios = [
        {"scenario": "central_base", "owner_share": central},
        {"scenario": "unmatched_pessimistic", "owner_share": max(0.0, central - unmatched_count / total)},
        {"scenario": "unmatched_optimistic", "owner_share": min(1.0, central + unmatched_count / total)},
        {"scenario": "signal_poor_individual_uncertain", "owner_share": max(0.0, central - 0.05)},
    ]

    v1 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet"))
    v2 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet"))
    scenarios.extend([
        {"scenario": "candidate_pop_v1_count", "owner_share": float(v1)},
        {"scenario": "candidate_pop_v2_count", "owner_share": float(v2)},
    ])

    write_csv(OUTPUTS / "sensitivity_scenarios.csv", scenarios, ["scenario", "owner_share"])

    # minimal PNG placeholder (1x1 transparent pixel)
    png_pixel = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO6p5wAAAABJRU5ErkJggg==")
    (OUTPUTS / "sensitivity_chart.png").write_bytes(png_pixel)

    values = [s["owner_share"] for s in scenarios if "count" not in s["scenario"]]
    low, high = (min(values), max(values)) if values else (0.0, 0.0)
    (OUTPUTS / "sensitivity_note.md").write_text(
        f"# Sensitivity note\n\nPolicy sensitivity range in this run: {low:.3f} to {high:.3f}.\n"
        "Lower bound implies tighter targeting may be feasible; upper bound implies higher owner-occupier inclusion risk.\n",
        encoding="utf-8",
    )
    return scenarios


if __name__ == "__main__":
    run_sensitivity(PipelineConfig())
