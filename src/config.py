from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_INTERIM = ROOT / "data" / "interim"
DATA_PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "outputs"
DOCS = ROOT / "docs"


@dataclass
class PipelineConfig:
    analysis_date: str = datetime.now(timezone.utc).date().isoformat()
    min_price_threshold: int = 2_000_000
    threshold_band_floor: int = 1_500_000
    fuzzy_match_cutoff: float = 90.0
    manual_review_sample_size: int = 120
    max_rows_per_source: int | None = None
    random_seed: int = 42
    source_urls: dict = field(default_factory=lambda: {
        "price_paid": "https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads",
        "land_property_api": "https://use-land-property-data.service.gov.uk/api/v1/",
        "land_property_api_info": "https://use-land-property-data.service.gov.uk/api-information",
        "land_property_portal": "https://use-land-property-data.service.gov.uk/",
        "epc_collection": "https://www.gov.uk/government/collections/energy-performance-of-buildings-certificates",
        "ukhpi": "https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads-december-2025",
        "os_gb_address": "https://www.ordnancesurvey.co.uk/products/os-gb-address",
        "planning_data": "https://www.planning.data.gov.uk/dataset/",
        "dwelling_stock": "https://www.gov.uk/government/statistical-data-sets/live-tables-on-dwelling-stock-including-vacants",
        "rents_lettings": "https://www.gov.uk/government/statistical-data-sets/live-tables-on-rents-lettings-and-tenancies",
        "ehs_tables": "https://www.gov.uk/government/collections/english-housing-survey-live-tables",
        "house_building": "https://www.gov.uk/government/statistical-data-sets/live-tables-on-house-building",
        "fire_stats": "https://www.gov.uk/government/statistical-data-sets/fire-statistics-data-tables",
    })


def ensure_directories() -> None:
    for p in [DATA_RAW, DATA_INTERIM, DATA_PROCESSED, OUTPUTS, DOCS]:
        p.mkdir(parents=True, exist_ok=True)
