from __future__ import annotations

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
from io_utils import write_csv


def write_policy_brief(metrics: list[dict], classified: list[dict]) -> None:
    metric_map = {m["estimate_type"]: m["owner_occupation_share"] for m in metrics}
    if not classified:
        text = """# Policy brief note

1. Research question
- What is the defensible range of owner-occupation among £2m+ properties?

2. Why it matters for HVCTS
- Policy incidence and fairness depend on occupancy mix.

3. What data was used
- Pipeline ran, but core raw datasets were not populated.

4. What is directly observed
- None in this run.

5. What is inferred
- No robust property-level inference possible in this run.

6. What we can say with confidence
- Current run is a dry-run scaffold.

7. The defensible range of owner-occupation for £2m+ properties
- Conservative: 0.000
- Central: 0.000
- Upper: 0.000

8. How sensitive policy design is to that range
- Framework available; data required for substantive results.

9. Key caveats
- Missing raw core inputs.

10. Recommendations for improving the evidence base
- Ingest PPD/EPC/ownership extracts and rerun.
"""
    else:
        text = f"""# Policy brief note

1. Research question
- Defensible range of owner-occupation for £2m+ properties.

2. Why it matters for HVCTS
- Drives targeting/exemption trade-offs.

3. What data was used
- PPD, UKHPI, EPC, ownership where matched.

4. What is directly observed
- Transactions and matched signals.

5. What is inferred
- Occupancy class with confidence tiers.

6. What we can say with confidence
- High-confidence subset reported in outputs.

7. The defensible range of owner-occupation for £2m+ properties
- Conservative: {metric_map.get('conservative', 0):.3f}
- Central: {metric_map.get('central', 0):.3f}
- Upper: {metric_map.get('upper', 0):.3f}

8. How sensitive policy design is to that range
- See outputs/sensitivity_note.md.

9. Key caveats
- Coverage and linkage limitations.

10. Recommendations for improving the evidence base
- Improve identifier-level linkage and coverage.
"""
    (OUTPUTS / "policy_brief_note.md").write_text(text, encoding="utf-8")


def write_audit_summary(stage_counts: dict, classified: list[dict]) -> None:
    confidence = {}
    for r in classified:
        tier = r.get("confidence_tier", "unknown")
        confidence[tier] = confidence.get(tier, 0) + 1

    lines = ["# Audit summary", "", "## Stage row counts"]
    for k, v in stage_counts.items():
        lines.append(f"- {k}: {v}")

    lines += ["", "## Confidence tiers"]
    for k, v in confidence.items():
        lines.append(f"- {k}: {v}")

    lines += [
        "",
        "## Top five assumptions most likely to bias estimate",
        "- Candidate population definition (v1 vs v2)",
        "- Uplift factor assumption",
        "- Unmatched record treatment",
        "- Company-ownership interpretation",
        "- EPC coverage and interpretation",
        "",
        "## Weakest join or data source",
        "- Ownership linkage feed in open environment",
        "",
        "## Biggest risk of false confidence",
        "- Treating signal-poor records as owner-occupied",
    ]
    (OUTPUTS / "audit_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_pipeline(cfg: PipelineConfig) -> None:
    ensure_directories()
    stage_counts = {}

    run_downloads(cfg)
    ppd = prepare_price_paid(cfg); stage_counts["price_paid_clean"] = len(ppd)
    ukhpi = prepare_ukhpi(cfg); stage_counts["ukhpi_uplift"] = len(ukhpi)

    # Build candidate postcode set for early filtering of large EPC/ownership files
    candidate_postcodes = {r.get("postcode_clean", "") for r in ppd} - {""}
    print(f"Candidate postcodes for EPC/ownership filter: {len(candidate_postcodes):,}")

    epc = prepare_epc(cfg, candidate_postcodes=candidate_postcodes); stage_counts["epc_clean"] = len(epc)
    own = prepare_ownership(cfg, candidate_postcodes=candidate_postcodes); stage_counts["ownership_clean"] = len(own)
    addr = prepare_addresses(cfg); stage_counts["address_reference"] = len(addr)
    ctx = prepare_contextual_sources(cfg); stage_counts["contextual_inventory"] = len(ctx)

    v1, v2 = build_candidate_populations(cfg)
    stage_counts["candidate_population_v1"] = len(v1)
    stage_counts["candidate_population_v2"] = len(v2)

    linked = link_properties(cfg); stage_counts["linked_candidate_population"] = len(linked)
    classified = classify_owner_occupation(cfg); stage_counts["classified_owner_occupation"] = len(classified)

    metrics = build_headline_range(classified)
    write_csv(OUTPUTS / "headline_metrics.csv", metrics, ["estimate_type", "owner_occupation_share"])

    run_sensitivity(cfg)
    write_policy_brief(metrics, classified)
    write_audit_summary(stage_counts, classified)


if __name__ == "__main__":
    run_pipeline(PipelineConfig())
