from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig
from classify_owner_occupation import build_headline_range


def run_sensitivity(cfg: PipelineConfig) -> pd.DataFrame:
    classified_fp = DATA_PROCESSED / "classified_owner_occupation.parquet"
    if not classified_fp.exists():
        raise FileNotFoundError("classified_owner_occupation.parquet missing")
    df = pd.read_parquet(classified_fp)

    scenarios = []

    base = build_headline_range(df)
    scenarios.append({"scenario": "central_base", "owner_share": float(base.loc[base["estimate_type"] == "central", "owner_occupation_share"].iloc[0])})

    # unmatched pessimistic
    d1 = df.copy()
    unmatched = d1.get("match_stage", pd.Series(index=d1.index, dtype=str)).eq("unmatched")
    d1.loc[unmatched, "owner_occupation_status"] = "not_owner_occupied_likely"
    scenarios.append({"scenario": "unmatched_pessimistic", "owner_share": float((d1["owner_occupation_status"] == "owner_occupied_likely").mean())})

    # unmatched optimistic
    d2 = df.copy()
    d2.loc[unmatched, "owner_occupation_status"] = "owner_occupied_likely"
    scenarios.append({"scenario": "unmatched_optimistic", "owner_share": float((d2["owner_occupation_status"] == "owner_occupied_likely").mean())})

    # signal-poor individual records
    d3 = df.copy()
    mask = d3.get("ownership_type", "").eq("individual") & d3.get("epc_category", "unknown").eq("unknown")
    d3.loc[mask, "owner_occupation_status"] = "uncertain"
    scenarios.append({"scenario": "signal_poor_individual_uncertain", "owner_share": float((d3["owner_occupation_status"] == "owner_occupied_likely").mean())})

    # v1 vs v2 population size comparison (impact marker)
    v1_n = len(pd.read_parquet(DATA_INTERIM / "candidate_population_v1.parquet")) if (DATA_INTERIM / "candidate_population_v1.parquet").exists() else 0
    v2_n = len(pd.read_parquet(DATA_INTERIM / "candidate_population_v2.parquet")) if (DATA_INTERIM / "candidate_population_v2.parquet").exists() else 0
    scenarios.append({"scenario": "candidate_pop_v1_count", "owner_share": float(v1_n)})
    scenarios.append({"scenario": "candidate_pop_v2_count", "owner_share": float(v2_n)})

    s = pd.DataFrame(scenarios)
    s.to_csv(OUTPUTS / "sensitivity_scenarios.csv", index=False)

    chart_df = s[~s["scenario"].str.contains("count")]
    plt.figure(figsize=(10, 5))
    plt.bar(chart_df["scenario"], chart_df["owner_share"])
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Owner-occupation share")
    plt.title("Sensitivity of owner-occupation estimate")
    plt.tight_layout()
    plt.savefig(OUTPUTS / "sensitivity_chart.png", dpi=150)
    plt.close()

    low = chart_df["owner_share"].min() if not chart_df.empty else 0
    high = chart_df["owner_share"].max() if not chart_df.empty else 0
    note = f"""# Sensitivity note

Policy sensitivity is material when the plausible owner-occupation share ranges from {low:.3f} to {high:.3f} under tested assumptions.

If true owner-occupation is at the lower bound, policies targeting non-owner occupancy can be tighter with lower exclusion risk.
If true owner-occupation is at the upper bound, aggressive targeting could capture more owner-occupiers and requires stronger safeguards.

Most influential assumptions in this first pass:
1. Treatment of unmatched records.
2. Treatment of signal-poor individual-owned records.
3. Candidate population definition (transaction-only vs uplifted current value).
"""
    (OUTPUTS / "sensitivity_note.md").write_text(note, encoding="utf-8")

    return s


if __name__ == "__main__":
    run_sensitivity(PipelineConfig())
