from __future__ import annotations

import traceback
import pandas as pd

from config import OUTPUTS, PipelineConfig, ensure_directories
from download_data import run_downloads
from prepare_price_paid import prepare_price_paid
from prepare_ukhpi import prepare_ukhpi
from prepare_epc import prepare_epc
from prepare_ownership import prepare_ownership
from prepare_addresses import prepare_addresses
from prepare_contextual_sources import prepare_contextual_sources
from link_properties import build_candidate_populations, link_properties
from classify_owner_occupation import classify_owner_occupation, build_headline_range
from sensitivity_analysis import run_sensitivity


def write_policy_brief(metrics: pd.DataFrame, classified: pd.DataFrame) -> None:
    if metrics.empty or classified.empty:
        content = """# Policy brief note

1. Research question
- What is the defensible range of owner-occupation among £2m+ properties?

2. Why it matters for HVCTS
- Targeting effectiveness and fairness depend on true occupancy mix.

3. What data was used
- Pipeline scaffolding is implemented; accessible raw datasets were attempted.

4. What is directly observed
- Transactions and any matched EPC/ownership fields where available.

5. What is inferred
- Occupancy classification and range estimates.

6. What we can say with confidence
- Insufficient data loaded to produce robust estimates in this run.

7. The defensible range of owner-occupation for £2m+ properties
- Not available from this run due to missing upstream inputs.

8. How sensitive policy design is to that range
- Sensitivity framework prepared; requires populated data.

9. Key caveats
- Open-data coverage and linkage uncertainty.

10. Recommendations for improving the evidence base
- Ingest full PPD/EPC extracts and ownership endpoint exports; improve identifier linkage.
"""
    else:
        m = {r["estimate_type"]: r["owner_occupation_share"] for _, r in metrics.iterrows()}
        content = f"""# Policy brief note

1. Research question
- Defensible range of owner-occupation for £2m+ properties.

2. Why it matters for HVCTS
- Incidence, exemptions, and behavioural impacts depend on occupancy composition.

3. What data was used
- Price Paid, UKHPI uplift proxy, EPC/ownership matches where available.

4. What is directly observed
- Transaction and matched ownership/tenure signals.

5. What is inferred
- Occupancy class and confidence tier.

6. What we can say with confidence
- High-confidence subset is reported in classification outputs.

7. The defensible range of owner-occupation for £2m+ properties
- Conservative: {m.get('conservative', 0):.3f}
- Central: {m.get('central', 0):.3f}
- Upper: {m.get('upper', 0):.3f}

8. How sensitive policy design is to that range
- See outputs/sensitivity_note.md and sensitivity_scenarios.csv.

9. Key caveats
- Missing matches and proxy assumptions affect estimates.

10. Recommendations for improving the evidence base
- Improve identifier-level linkage and refresh occupancy-signaling sources.
"""

    (OUTPUTS / "policy_brief_note.md").write_text(content, encoding="utf-8")


def write_audit_summary(stage_counts: dict, classified: pd.DataFrame) -> None:
    top_assumptions = [
        "Candidate population definition (v1 vs v2).",
        "Uplift factor choice for current-value proxy.",
        "Treatment of unmatched records.",
        "Company ownership interpreted as non-owner-occupation proxy.",
        "Interpretation and coverage of EPC tenure/occupancy fields.",
    ]
    weak_source = "Ownership linkage feed (often sparse/endpoint-dependent in open environment)."
    false_confidence_risk = "Classifying signal-poor records as owner-occupied by default."

    lines = [
        "# Audit summary",
        "",
        "## Stage row counts",
    ]
    for k, v in stage_counts.items():
        lines.append(f"- {k}: {v}")

    match_rates = "N/A"
    if not classified.empty and "match_stage" in classified.columns:
        rates = classified["match_stage"].value_counts(normalize=True).round(3).to_dict()
        match_rates = str(rates)

    lines += [
        "",
        "## Match rates",
        f"- {match_rates}",
        "",
        "## Confidence tiers",
    ]

    if not classified.empty and "confidence_tier" in classified.columns:
        for tier, cnt in classified["confidence_tier"].value_counts().to_dict().items():
            lines.append(f"- {tier}: {cnt}")

    lines += [
        "",
        "## Top five assumptions likely to bias estimate",
    ]
    lines += [f"- {x}" for x in top_assumptions]
    lines += [
        "",
        "## Weakest join or data source",
        f"- {weak_source}",
        "",
        "## Biggest risk of false confidence",
        f"- {false_confidence_risk}",
    ]

    (OUTPUTS / "audit_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_pipeline(cfg: PipelineConfig) -> None:
    ensure_directories()
    stage_counts = {}

    run_downloads(cfg)

    ppd = prepare_price_paid(cfg)
    stage_counts["price_paid_clean"] = len(ppd)

    ukhpi = prepare_ukhpi(cfg)
    stage_counts["ukhpi_uplift"] = len(ukhpi)

    epc = prepare_epc(cfg)
    stage_counts["epc_clean"] = len(epc)

    own = prepare_ownership(cfg)
    stage_counts["ownership_clean"] = len(own)

    addrs = prepare_addresses(cfg)
    stage_counts["address_reference"] = len(addrs)

    contextual = prepare_contextual_sources(cfg)
    stage_counts["contextual_inventory"] = len(contextual)

    v1, v2 = build_candidate_populations(cfg)
    stage_counts["candidate_population_v1"] = len(v1)
    stage_counts["candidate_population_v2"] = len(v2)

    linked = link_properties(cfg)
    stage_counts["linked_candidate_population"] = len(linked)

    classified = classify_owner_occupation(cfg)
    stage_counts["classified_owner_occupation"] = len(classified)

    metrics = build_headline_range(classified)
    metrics.to_csv(OUTPUTS / "headline_metrics.csv", index=False)

    run_sensitivity(cfg)
    write_policy_brief(metrics, classified)
    write_audit_summary(stage_counts, classified)


if __name__ == "__main__":
    try:
        run_pipeline(PipelineConfig())
    except Exception:
        traceback.print_exc()
        raise
