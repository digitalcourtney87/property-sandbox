from __future__ import annotations

import re
import pandas as pd
from rapidfuzz import fuzz

from config import DATA_INTERIM, DATA_PROCESSED, OUTPUTS, PipelineConfig


def normalize_address(s: str) -> str:
    s = (s or "").lower()
    replacements = {
        " road ": " rd ",
        " street ": " st ",
        " avenue ": " ave ",
        " lane ": " ln ",
    }
    s = f" {s} "
    for k, v in replacements.items():
        s = s.replace(k, v)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_candidate_populations(cfg: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    ppd_fp = DATA_INTERIM / "price_paid_clean.parquet"
    uk_fp = DATA_INTERIM / "ukhpi_uplift.parquet"
    if not ppd_fp.exists():
        raise FileNotFoundError("price_paid_clean.parquet missing")

    ppd = pd.read_parquet(ppd_fp)
    ppd["latest_price"] = pd.to_numeric(ppd.get("price"), errors="coerce")
    ppd["address_clean"] = ppd.get("address_clean", "").astype(str).map(normalize_address)

    v1 = ppd[ppd["latest_price"] >= cfg.min_price_threshold].copy()
    v1["candidate_version"] = "v1_transaction"
    v1.to_parquet(DATA_INTERIM / "candidate_population_v1.parquet", index=False)

    if uk_fp.exists() and uk_fp.stat().st_size > 0:
        uk = pd.read_parquet(uk_fp)
        default_uplift = uk["uplift_factor"].dropna().max() if "uplift_factor" in uk.columns and not uk.empty else 1.15
    else:
        default_uplift = 1.15

    v2 = ppd.copy()
    v2["uplift_factor"] = float(default_uplift) if pd.notna(default_uplift) else 1.15
    v2["estimated_current_value"] = v2["latest_price"] * v2["uplift_factor"]
    v2 = v2[v2["estimated_current_value"] >= cfg.min_price_threshold].copy()
    v2["candidate_version"] = "v2_uplift"
    v2.to_parquet(DATA_INTERIM / "candidate_population_v2.parquet", index=False)

    comp = pd.DataFrame(
        [
            {"metric": "v1_count", "value": len(v1)},
            {"metric": "v2_count", "value": len(v2)},
        ]
    )
    comp.to_csv(OUTPUTS / "candidate_population_comparison.csv", index=False)

    proximity = ppd[(ppd["latest_price"] >= cfg.threshold_band_floor) & (ppd["latest_price"] < cfg.min_price_threshold)].copy()
    proximity[["property_key", "latest_price"]].to_csv(OUTPUTS / "threshold_proximity_distribution.csv", index=False)

    return v1, v2


def link_properties(cfg: PipelineConfig) -> pd.DataFrame:
    v1_fp = DATA_INTERIM / "candidate_population_v1.parquet"
    if not v1_fp.exists():
        raise FileNotFoundError("candidate_population_v1.parquet missing")
    cand = pd.read_parquet(v1_fp)

    epc_fp = DATA_INTERIM / "epc_clean.parquet"
    own_fp = DATA_INTERIM / "ownership_clean.parquet"
    epc = pd.read_parquet(epc_fp) if epc_fp.exists() else pd.DataFrame(columns=["postcode_clean", "address_clean", "epc_category"])
    own = pd.read_parquet(own_fp) if own_fp.exists() else pd.DataFrame(columns=["postcode_clean", "address_clean", "ownership_type"])

    cand["postcode_clean"] = cand.get("postcode_clean", "").astype(str).str.replace(" ", "", regex=False)
    cand["address_norm"] = cand.get("address_clean", "").astype(str).map(normalize_address)
    epc["postcode_clean"] = epc.get("postcode_clean", "").astype(str).str.replace(" ", "", regex=False)
    epc["address_norm"] = epc.get("address_clean", "").astype(str).map(normalize_address)
    own["postcode_clean"] = own.get("postcode_clean", "").astype(str).str.replace(" ", "", regex=False)
    own["address_norm"] = own.get("address_clean", "").astype(str).map(normalize_address)

    linked = cand.merge(
        epc[["postcode_clean", "address_norm", "epc_category"]].drop_duplicates(),
        on=["postcode_clean", "address_norm"],
        how="left",
    )
    linked = linked.merge(
        own[["postcode_clean", "address_norm", "ownership_type", "ownership_type_confidence"]].drop_duplicates(),
        on=["postcode_clean", "address_norm"],
        how="left",
    )

    linked["match_stage"] = linked["epc_category"].notna().map({True: "exact_postcode_address", False: "unmatched"})

    if not epc.empty:
        unmatched_idx = linked[linked["epc_category"].isna()].index.tolist()
        epc_by_postcode = {k: g["address_norm"].tolist() for k, g in epc.groupby("postcode_clean")}
        for i in unmatched_idx[:5000]:
            pc = linked.at[i, "postcode_clean"]
            addr = linked.at[i, "address_norm"]
            candidates = epc_by_postcode.get(pc, [])
            if not candidates:
                continue
            scores = [(c, fuzz.token_sort_ratio(addr, c)) for c in candidates]
            best_addr, best_score = max(scores, key=lambda x: x[1])
            if best_score >= cfg.fuzzy_match_cutoff:
                r = epc[(epc["postcode_clean"] == pc) & (epc["address_norm"] == best_addr)].head(1)
                if not r.empty:
                    linked.at[i, "epc_category"] = r.iloc[0]["epc_category"]
                    linked.at[i, "match_stage"] = "postcode_fuzzy_address"

    linked.to_parquet(DATA_PROCESSED / "linked_candidate_population.parquet", index=False)

    summary = linked["match_stage"].value_counts(dropna=False).rename_axis("match_stage").reset_index(name="count")
    summary["share"] = summary["count"] / len(linked) if len(linked) else 0
    summary.to_csv(OUTPUTS / "linkage_coverage_summary.csv", index=False)

    sample_n = min(cfg.manual_review_sample_size, len(linked))
    manual = linked.sample(sample_n, random_state=cfg.random_seed) if sample_n else linked
    manual_cols = [c for c in ["property_key", "postcode_clean", "address_norm", "match_stage", "ownership_type", "epc_category"] if c in manual.columns]
    manual[manual_cols].to_csv(OUTPUTS / "manual_review_sample.csv", index=False)

    return linked


if __name__ == "__main__":
    cfg = PipelineConfig()
    build_candidate_populations(cfg)
    link_properties(cfg)
