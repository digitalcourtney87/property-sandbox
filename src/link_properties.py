from __future__ import annotations

import random
import statistics

from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import clean_text, read_parquet_placeholder, score_similarity, write_csv, write_parquet_placeholder


def _build_postcode_index(rows: list[dict]) -> dict[str, list[dict]]:
    """Group rows by postcode_clean for efficient lookup."""
    idx: dict[str, list[dict]] = {}
    for r in rows:
        pc = r.get("postcode_clean", "")
        idx.setdefault(pc, []).append(r)
    return idx


def build_candidate_populations(cfg: PipelineConfig) -> tuple[list[dict], list[dict]]:
    ppd = read_parquet_placeholder(DATA_INTERIM / "price_paid_clean.parquet")
    ukhpi = read_parquet_placeholder(DATA_INTERIM / "ukhpi_uplift.parquet")

    if cfg.strict_core_inputs and not ppd:
        raise RuntimeError("price_paid_clean.parquet has no rows. Core candidate population cannot be built.")
    if cfg.strict_core_inputs and not ukhpi:
        raise RuntimeError("ukhpi_uplift.parquet has no rows. Core current-value uplift cannot be built.")

    # Use median uplift as a more robust national estimate than max
    uplift_values = [float(r.get("uplift_factor", 1.15)) for r in ukhpi if float(r.get("uplift_factor", 1.0)) > 0]
    national_uplift = statistics.median(uplift_values) if uplift_values else 1.15

    # Build per-region latest uplift for region-aware V2
    latest_by_region: dict[str, float] = {}
    for r in ukhpi:
        region = r.get("region", "unknown")
        latest_by_region[region] = float(r.get("uplift_factor", 1.15))

    v1, v2, proximity = [], [], []
    for r in ppd:
        price = float(r.get("price", 0) or 0)
        if price >= cfg.min_price_threshold:
            rr = dict(r)
            rr["candidate_version"] = "v1_transaction"
            v1.append(rr)
        elif cfg.threshold_band_floor <= price < cfg.min_price_threshold:
            proximity.append({"property_key": r.get("property_key", ""), "latest_price": price})

        # Use region-specific uplift if available, else national median
        district = clean_text(r.get("district", "") or r.get("county", "") or "")
        uplift = latest_by_region.get(district, national_uplift)
        est = price * uplift
        if est >= cfg.min_price_threshold:
            rr2 = dict(r)
            rr2["candidate_version"] = "v2_uplift"
            rr2["uplift_factor"] = uplift
            rr2["estimated_current_value"] = est
            v2.append(rr2)

    if cfg.strict_core_inputs and not v1 and not v2:
        raise RuntimeError("Candidate populations are empty after applying £2m threshold. Check source coverage/date range.")

    write_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet", v1)
    write_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet", v2)
    write_csv(OUTPUTS / "candidate_population_comparison.csv", [{"metric": "v1_count", "value": len(v1)}, {"metric": "v2_count", "value": len(v2)}], ["metric", "value"])
    write_csv(OUTPUTS / "threshold_proximity_distribution.csv", proximity, ["property_key", "latest_price"])
    return v1, v2


def link_properties(cfg: PipelineConfig) -> list[dict]:
    candidates = read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet")
    epc_rows = read_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet")
    own_rows = read_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet")

    if cfg.strict_core_inputs and not candidates:
        raise RuntimeError("candidate_population_v1.parquet has no rows; cannot link properties.")

    # Exact-match indexes
    epc_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in epc_rows}
    own_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in own_rows}

    # Postcode indexes for fuzzy fallback
    epc_by_postcode = _build_postcode_index(epc_rows)
    own_by_postcode = _build_postcode_index(own_rows)

    linked = []
    for c in candidates:
        pc = c.get("postcode_clean", "")
        addr = clean_text(c.get("address_clean", ""))
        key = (pc, addr)
        rec = dict(c)
        rec["address_norm"] = addr
        rec["match_stage"] = "unmatched"

        # EPC matching: exact then fuzzy
        e = epc_idx.get(key)
        if e:
            rec["epc_category"] = e.get("epc_category", "unknown")
            rec["epc_transaction_type"] = e.get("epc_transaction_type", "")
            rec["match_stage"] = "exact_postcode_address"
        else:
            same_pc = epc_by_postcode.get(pc, [])
            if same_pc:
                scored = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc],
                    reverse=True, key=lambda z: z[0],
                )
                if scored and scored[0][0] >= cfg.fuzzy_match_cutoff:
                    rec["epc_category"] = scored[0][1].get("epc_category", "unknown")
                    rec["epc_transaction_type"] = scored[0][1].get("epc_transaction_type", "")
                    rec["match_stage"] = "postcode_fuzzy_address"

        # Ownership matching: exact then fuzzy
        o = own_idx.get(key)
        if not o:
            same_pc_own = own_by_postcode.get(pc, [])
            if same_pc_own:
                scored_own = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc_own],
                    reverse=True, key=lambda z: z[0],
                )
                if scored_own and scored_own[0][0] >= cfg.fuzzy_match_cutoff:
                    o = scored_own[0][1]
        if o:
            rec["ownership_type"] = o.get("ownership_type", "unresolved")
            rec["ownership_type_confidence"] = o.get("ownership_type_confidence", "low")
        linked.append(rec)

    write_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population.parquet", linked)

    counts = {}
    for r in linked:
        counts[r.get("match_stage", "unmatched")] = counts.get(r.get("match_stage", "unmatched"), 0) + 1
    summary = [{"match_stage": k, "count": v, "share": (v / len(linked) if linked else 0)} for k, v in counts.items()]
    write_csv(OUTPUTS / "linkage_coverage_summary.csv", summary, ["match_stage", "count", "share"])

    random.seed(cfg.random_seed)
    sample_n = min(cfg.manual_review_sample_size, len(linked))
    sample = random.sample(linked, sample_n) if sample_n else []
    sample_rows = [{
        "property_key": r.get("property_key", ""),
        "postcode_clean": r.get("postcode_clean", ""),
        "address_norm": r.get("address_norm", ""),
        "match_stage": r.get("match_stage", ""),
        "ownership_type": r.get("ownership_type", ""),
        "epc_category": r.get("epc_category", ""),
    } for r in sample]
    write_csv(OUTPUTS / "manual_review_sample.csv", sample_rows, ["property_key", "postcode_clean", "address_norm", "match_stage", "ownership_type", "epc_category"])
    return linked


def link_properties_v2(cfg: PipelineConfig) -> list[dict]:
    """Link V2 (HPI-uplifted) candidates against EPC and ownership data."""
    candidates = read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet")
    if not candidates:
        return []

    epc_rows = read_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet")
    own_rows = read_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet")

    epc_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in epc_rows}
    own_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in own_rows}
    epc_by_postcode = _build_postcode_index(epc_rows)
    own_by_postcode = _build_postcode_index(own_rows)

    linked = []
    for c in candidates:
        pc = c.get("postcode_clean", "")
        addr = clean_text(c.get("address_clean", ""))
        key = (pc, addr)
        rec = dict(c)
        rec["address_norm"] = addr
        rec["match_stage"] = "unmatched"

        e = epc_idx.get(key)
        if e:
            rec["epc_category"] = e.get("epc_category", "unknown")
            rec["epc_transaction_type"] = e.get("epc_transaction_type", "")
            rec["match_stage"] = "exact_postcode_address"
        else:
            same_pc = epc_by_postcode.get(pc, [])
            if same_pc:
                scored = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc],
                    reverse=True, key=lambda z: z[0],
                )
                if scored and scored[0][0] >= cfg.fuzzy_match_cutoff:
                    rec["epc_category"] = scored[0][1].get("epc_category", "unknown")
                    rec["epc_transaction_type"] = scored[0][1].get("epc_transaction_type", "")
                    rec["match_stage"] = "postcode_fuzzy_address"

        o = own_idx.get(key)
        if not o:
            same_pc_own = own_by_postcode.get(pc, [])
            if same_pc_own:
                scored_own = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc_own],
                    reverse=True, key=lambda z: z[0],
                )
                if scored_own and scored_own[0][0] >= cfg.fuzzy_match_cutoff:
                    o = scored_own[0][1]
        if o:
            rec["ownership_type"] = o.get("ownership_type", "unresolved")
            rec["ownership_type_confidence"] = o.get("ownership_type_confidence", "low")
        linked.append(rec)

    write_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population_v2.parquet", linked)
    print(f"V2 linking: {len(linked):,} candidates linked.")
    return linked


if __name__ == "__main__":
    cfg = PipelineConfig()
    build_candidate_populations(cfg)
    link_properties(cfg)
    link_properties_v2(cfg)
