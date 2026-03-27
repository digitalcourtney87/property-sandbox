from __future__ import annotations

import base64
from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder, write_csv
from classify_owner_occupation import build_headline_range


def run_sensitivity(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet")
    base = build_headline_range(rows)
    central = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "central"), 0.0)
    conservative = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "conservative"), 0.0)
    upper = next((x["owner_occupation_share"] for x in base if x["estimate_type"] == "upper"), 0.0)

    total = len(rows) or 1
    unmatched = sum(1 for r in rows if r.get("match_stage") == "unmatched")
    uncertain = sum(1 for r in rows if r.get("owner_occupation_status") == "uncertain")

    # V1 and V2 counts
    v1 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet"))
    v2 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet"))

    # V2 headline if available
    v2_rows = read_parquet_placeholder(DATA_PROCESSED / "classified_v2.parquet")
    v2_metrics = build_headline_range(v2_rows) if v2_rows else []
    v2_central = next((x["owner_occupation_share"] for x in v2_metrics if x["estimate_type"] == "central"), 0.0)
    v2_conservative = next((x["owner_occupation_share"] for x in v2_metrics if x["estimate_type"] == "conservative"), 0.0)
    v2_upper = next((x["owner_occupation_share"] for x in v2_metrics if x["estimate_type"] == "upper"), 0.0)

    # VOA Band H population calibration
    voa_rows = read_parquet_placeholder(DATA_INTERIM / "voa_band_h.parquet")
    # Use national total row if present, else sum LA-level
    national_row = [r for r in voa_rows if r.get("district") == "__national_total__"]
    if national_row:
        voa_total = int(national_row[0].get("band_h_count", 0))
    else:
        voa_total = sum(int(r.get("band_h_count", 0)) for r in voa_rows)

    scenarios = [
        {"scenario": "v1_conservative", "owner_share": conservative},
        {"scenario": "v1_central", "owner_share": central},
        {"scenario": "v1_upper", "owner_share": upper},
    ]

    # V2 population (includes longer-held properties, likely more owner-occupied)
    if v2_central:
        scenarios.extend([
            {"scenario": "v2_conservative", "owner_share": v2_conservative},
            {"scenario": "v2_central", "owner_share": v2_central},
            {"scenario": "v2_upper", "owner_share": v2_upper},
        ])

    # Unmatched sensitivity: what if all unmatched are owner-occupied vs not
    # Scale by 0.5 because unmatched contribute 0.5 to central already
    scenarios.extend([
        {"scenario": "unmatched_all_not_owner", "owner_share": max(0.0, central - (unmatched * 0.5) / total)},
        {"scenario": "unmatched_all_owner", "owner_share": min(1.0, central + (unmatched * 0.5) / total)},
    ])

    # Signal-poor sensitivity
    scenarios.append({"scenario": "signal_poor_reclassed", "owner_share": max(0.0, central - 0.05)})

    # VOA population calibration
    if voa_total:
        coverage = v1 / voa_total
        scenarios.extend([
            {"scenario": "voa_band_h_total", "owner_share": float(voa_total)},
            {"scenario": "v1_coverage_of_band_h", "owner_share": coverage},
        ])
        # If we only observe coverage% of the true population, and the unobserved
        # are more likely owner-occupied (long holders who haven't transacted):
        if coverage < 1.0:
            adjusted = central * coverage + 0.70 * (1 - coverage)
            scenarios.append({"scenario": "voa_adjusted_central", "owner_share": adjusted})

    # Population counts for reference
    scenarios.extend([
        {"scenario": "candidate_pop_v1_count", "owner_share": float(v1)},
        {"scenario": "candidate_pop_v2_count", "owner_share": float(v2)},
    ])

    write_csv(OUTPUTS / "sensitivity_scenarios.csv", scenarios, ["scenario", "owner_share"])

    # minimal PNG placeholder
    png_pixel = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO6p5wAAAABJRU5ErkJggg==")
    (OUTPUTS / "sensitivity_chart.png").write_bytes(png_pixel)

    # Build sensitivity note
    share_scenarios = [s for s in scenarios
                       if "count" not in s["scenario"]
                       and "coverage" not in s["scenario"]
                       and "band_h_total" not in s["scenario"]]
    values = [s["owner_share"] for s in share_scenarios]
    low, high = (min(values), max(values)) if values else (0.0, 0.0)

    note_lines = [
        "# Sensitivity note",
        "",
        f"V1 range (transaction-based, n={v1:,}): {conservative:.3f} to {upper:.3f} (central: {central:.3f})",
    ]
    if v2_central:
        note_lines.append(f"V2 range (HPI-uplifted, n={v2:,}): {v2_conservative:.3f} to {v2_upper:.3f} (central: {v2_central:.3f})")
    note_lines.append(f"Full sensitivity range: {low:.3f} to {high:.3f}")
    if voa_total:
        note_lines.append(f"VOA Band H estimated population: {voa_total:,}")
        note_lines.append(f"V1 coverage of Band H: {v1/voa_total*100:.1f}%")
        if v1 / voa_total < 1.0:
            adjusted = central * (v1 / voa_total) + 0.70 * (1 - v1 / voa_total)
            note_lines.append(f"VOA-adjusted central (assuming 70% owner-occ for unobserved): {adjusted:.3f}")
    note_lines.extend([
        "",
        "Lower bound implies tighter targeting may be feasible;",
        "upper bound implies higher owner-occupier inclusion risk.",
    ])

    (OUTPUTS / "sensitivity_note.md").write_text("\n".join(note_lines) + "\n", encoding="utf-8")
    return scenarios


if __name__ == "__main__":
    run_sensitivity(PipelineConfig())
