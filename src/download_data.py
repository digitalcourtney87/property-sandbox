from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories


LOG_PATH = OUTPUTS / "download_log.csv"


def _safe_name(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_").lower()


def _log_download(rows: Iterable[dict]) -> None:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dataset",
        "source_url",
        "download_timestamp_utc",
        "status",
        "file_path",
        "file_size_bytes",
        "http_status",
        "content_type",
        "note",
    ]
    exists = LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


def fetch_url_to_raw(dataset: str, url: str, timeout: int = 60) -> dict:
    dataset_dir = DATA_RAW / _safe_name(dataset)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = dataset_dir / f"{_safe_name(dataset)}_{ts}.bin"
    row = {
        "dataset": dataset,
        "source_url": url,
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": "",
        "file_size_bytes": "",
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    try:
        r = requests.get(url, timeout=timeout)
        row["http_status"] = r.status_code
        row["content_type"] = r.headers.get("content-type", "")
        if r.status_code != 200:
            row["note"] = f"non-200 response ({r.status_code})"
            return row

        suffix = ".html" if "text/html" in row["content_type"] else ".dat"
        out_path = out_path.with_suffix(suffix)
        out_path.write_bytes(r.content)

        row["status"] = "ok"
        row["file_path"] = str(out_path)
        row["file_size_bytes"] = out_path.stat().st_size
    except Exception as e:
        row["note"] = str(e)
    return row


def discover_land_property_api(cfg: PipelineConfig) -> dict:
    dataset = "land_property_api_discovery"
    dataset_dir = DATA_RAW / _safe_name(dataset)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_json = dataset_dir / f"api_root_{ts}.json"

    row = {
        "dataset": dataset,
        "source_url": cfg.source_urls["land_property_api"],
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": "",
        "file_size_bytes": "",
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    try:
        r = requests.get(cfg.source_urls["land_property_api"], timeout=60)
        row["http_status"] = r.status_code
        row["content_type"] = r.headers.get("content-type", "")
        if r.status_code == 200:
            payload = r.json() if "json" in row["content_type"] else {"raw": r.text[:5000]}
            out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            row["status"] = "ok"
            row["file_path"] = str(out_json)
            row["file_size_bytes"] = out_json.stat().st_size
        else:
            row["note"] = f"non-200 response ({r.status_code})"
    except Exception as e:
        row["note"] = str(e)

    return row


def run_downloads(cfg: PipelineConfig) -> None:
    ensure_directories()
    rows = []

    for name, url in cfg.source_urls.items():
        if name == "land_property_api":
            continue
        rows.append(fetch_url_to_raw(name, url))

    rows.append(discover_land_property_api(cfg))
    _log_download(rows)


if __name__ == "__main__":
    run_downloads(PipelineConfig())
