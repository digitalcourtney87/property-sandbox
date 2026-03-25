from __future__ import annotations

from config import DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder, write_csv, write_parquet_placeholder


def classify_row(r: dict) -> tuple[str, str, str, bool]:
    ownership = r.get("ownership_type", "unresolved")
    epc = r.get("epc_category", "unknown")

    company = ownership in {"UK_company", "overseas_company"}
    rental = epc in {"rented_private", "rented_social"}
    owner_sig = epc == "owner_occupied"
    conflicting = owner_sig and company

    if company or rental:
        return "not_owner_occupied_likely", "high", "direct_or_strong_proxy", conflicting
    if owner_sig and not conflicting:
        return "owner_occupied_likely", "high", "epc_owner_signal", conflicting
    if ownership == "individual" and not rental:
        return "owner_occupied_likely", "medium", "individual_no_conflict", conflicting
    if ownership == "trust_or_other":
        return "not_owner_occupied_likely", "medium", "ownership_proxy", conflicting
    return "uncertain", "low", "sparse_or_conflicting", conflicting


def classify_owner_occupation(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population.parquet")
    out = []
    for r in rows:
        status, tier, evidence, flag = classify_row(r)
        rr = dict(r)
        rr.update({
            "owner_occupation_status": status,
            "confidence_tier": tier,
            "evidence_basis": evidence,
            "conflicting_signals_flag": flag,
        })
        out.append(rr)

    write_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet", out)

    conf_counts = {}
    own_counts = {}
    for r in out:
        ck = (r["owner_occupation_status"], r["confidence_tier"])
        conf_counts[ck] = conf_counts.get(ck, 0) + 1
        ok = r.get("ownership_type", "unresolved") or "unresolved"
        own_counts[ok] = own_counts.get(ok, 0) + 1

    conf_rows = [{"owner_occupation_status": k[0], "confidence_tier": k[1], "count": v, "share": (v/len(out) if out else 0)} for k, v in conf_counts.items()]
    write_csv(OUTPUTS / "classification_confidence_summary.csv", conf_rows, ["owner_occupation_status", "confidence_tier", "count", "share"])

    own_rows = [{"ownership_type": k, "count": v, "share": (v/len(out) if out else 0)} for k, v in own_counts.items()]
    write_csv(OUTPUTS / "ownership_type_distribution.csv", own_rows, ["ownership_type", "count", "share"])

    region_counts = {}
    for r in out:
        region = r.get("district", "unknown")
        region_counts.setdefault(region, {"count": 0, "owner": 0})
        region_counts[region]["count"] += 1
        if r["owner_occupation_status"] == "owner_occupied_likely":
            region_counts[region]["owner"] += 1
    region_rows = []
    for region, vals in region_counts.items():
        region_rows.append({"region": region, "count": vals["count"], "owner_occupied_share_central_proxy": (vals["owner"] / vals["count"] if vals["count"] else 0)})
    write_csv(OUTPUTS / "owner_occupation_range_by_region.csv", region_rows, ["region", "count", "owner_occupied_share_central_proxy"])

    return out


def build_headline_range(rows: list[dict]) -> list[dict]:
    n = len(rows)
    if n == 0:
        return [
            {"estimate_type": "conservative", "owner_occupation_share": 0.0},
            {"estimate_type": "central", "owner_occupation_share": 0.0},
            {"estimate_type": "upper", "owner_occupation_share": 0.0},
        ]

    owner = sum(1 for r in rows if r["owner_occupation_status"] == "owner_occupied_likely")
    uncertain = sum(1 for r in rows if r["owner_occupation_status"] == "uncertain")
    conservative = owner / n
    upper = (owner + uncertain) / n

    weight_map = {"high": 1.0, "medium": 0.7, "low": 0.4}
    central_score = 0.0
    for r in rows:
        if r["owner_occupation_status"] == "owner_occupied_likely":
            central_score += weight_map.get(r.get("confidence_tier", "low"), 0.5)
        elif r["owner_occupation_status"] == "uncertain":
            central_score += 0.5
    central = central_score / n

    return [
        {"estimate_type": "conservative", "owner_occupation_share": conservative},
        {"estimate_type": "central", "owner_occupation_share": central},
        {"estimate_type": "upper", "owner_occupation_share": upper},
    ]


if __name__ == "__main__":
    cfg = PipelineConfig()
    metrics = build_headline_range(classify_owner_occupation(cfg))
    write_csv(OUTPUTS / "headline_metrics.csv", metrics, ["estimate_type", "owner_occupation_share"])
