from __future__ import annotations

import pandas as pd

from config import DATA_PROCESSED, OUTPUTS, PipelineConfig


def classify_row(r: pd.Series) -> tuple[str, str, str, bool]:
    ownership = str(r.get("ownership_type", "unresolved"))
    epc = str(r.get("epc_category", "unknown"))

    company = ownership in {"UK_company", "overseas_company"}
    rental = epc in {"rented_private", "rented_social"}
    owner_sig = epc == "owner_occupied"

    conflicting = owner_sig and company

    if company or rental:
        return "not_owner_occupied_likely", "high", "direct_or_strong_proxy", conflicting

    if owner_sig and not company and not rental and not conflicting:
        return "owner_occupied_likely", "high", "epc_owner_signal", conflicting

    if ownership in {"individual"} and not rental and not conflicting:
        return "owner_occupied_likely", "medium", "individual_no_conflict", conflicting

    if ownership in {"trust_or_other"}:
        return "not_owner_occupied_likely", "medium", "ownership_proxy", conflicting

    return "uncertain", "low", "sparse_or_conflicting", conflicting


def classify_owner_occupation(cfg: PipelineConfig) -> pd.DataFrame:
    fp = DATA_PROCESSED / "linked_candidate_population.parquet"
    if not fp.exists():
        raise FileNotFoundError("linked_candidate_population.parquet missing")

    df = pd.read_parquet(fp)
    results = df.apply(classify_row, axis=1, result_type="expand")
    results.columns = ["owner_occupation_status", "confidence_tier", "evidence_basis", "conflicting_signals_flag"]
    out = pd.concat([df, results], axis=1)

    out.to_parquet(DATA_PROCESSED / "classified_owner_occupation.parquet", index=False)

    conf = out.groupby(["owner_occupation_status", "confidence_tier"]).size().reset_index(name="count")
    conf["share"] = conf["count"] / len(out) if len(out) else 0
    conf.to_csv(OUTPUTS / "classification_confidence_summary.csv", index=False)

    own_dist = out["ownership_type"].fillna("unresolved").value_counts().rename_axis("ownership_type").reset_index(name="count")
    own_dist["share"] = own_dist["count"] / len(out) if len(out) else 0
    own_dist.to_csv(OUTPUTS / "ownership_type_distribution.csv", index=False)

    region_col = "district" if "district" in out.columns else None
    if region_col:
        grp = out.groupby(region_col)
        res = []
        for reg, g in grp:
            n = len(g)
            owner_share = (g["owner_occupation_status"] == "owner_occupied_likely").mean() if n else 0
            res.append({"region": reg, "count": n, "owner_occupied_share_central_proxy": owner_share})
        pd.DataFrame(res).to_csv(OUTPUTS / "owner_occupation_range_by_region.csv", index=False)
    else:
        pd.DataFrame(columns=["region", "count", "owner_occupied_share_central_proxy"]).to_csv(
            OUTPUTS / "owner_occupation_range_by_region.csv", index=False
        )

    return out


def build_headline_range(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame([
            {"estimate_type": "conservative", "owner_occupation_share": 0.0},
            {"estimate_type": "central", "owner_occupation_share": 0.0},
            {"estimate_type": "upper", "owner_occupation_share": 0.0},
        ])

    owner = df["owner_occupation_status"] == "owner_occupied_likely"
    not_owner = df["owner_occupation_status"] == "not_owner_occupied_likely"
    uncertain = df["owner_occupation_status"] == "uncertain"

    conservative = owner.mean()
    upper = (owner | uncertain).mean()

    weights = df["confidence_tier"].map({"high": 1.0, "medium": 0.7, "low": 0.4}).fillna(0.5)
    central = (owner.astype(float) * weights + uncertain.astype(float) * 0.5).sum() / n

    return pd.DataFrame([
        {"estimate_type": "conservative", "owner_occupation_share": conservative},
        {"estimate_type": "central", "owner_occupation_share": central},
        {"estimate_type": "upper", "owner_occupation_share": upper},
    ])


if __name__ == "__main__":
    cfg = PipelineConfig()
    d = classify_owner_occupation(cfg)
    build_headline_range(d).to_csv(OUTPUTS / "headline_metrics.csv", index=False)
