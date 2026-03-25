from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories

LOG_PATH = OUTPUTS / "download_log.csv"
ALLOWED_EXTENSIONS = (".csv", ".zip", ".xls", ".xlsx", ".json")


class DownloadDiscoveryError(RuntimeError):
    pass


def _safe_name(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_").lower()


def _fetch_bytes(url: str) -> tuple[bytes, str, int]:
    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    with urlopen(req, timeout=90) as resp:
        return resp.read(), resp.headers.get("Content-Type", ""), getattr(resp, "status", 200)


def _extract_links(html: str, base_url: str) -> list[str]:
    links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    out = []
    for href in links:
        if href.startswith("mailto:"):
            continue
        out.append(urljoin(base_url, href))

    seen, dedup = set(), []
    for u in out:
        if u not in seen:
            dedup.append(u)
            seen.add(u)
    return dedup


def _is_download_candidate(url: str, keywords: tuple[str, ...]) -> bool:
    low = url.lower()
    if any(low.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return True
    parsed = urlparse(low)
    query = parse_qs(parsed.query)
    if any(k in parsed.path for k in ["download", "attachment", "dataset"]):
        return any(term in low for term in keywords)
    if "file" in query or "format" in query:
        return any(term in low for term in keywords)
    return False


def _filename_from_url(url: str, fallback_prefix: str) -> str:
    path_name = Path(urlparse(url).path).name
    if path_name and any(path_name.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return path_name
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return f"{fallback_prefix}_{digest}.dat"


def _load_manifest(folder: Path) -> dict[str, str]:
    manifest_path = folder / "download_manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    return {}


def _save_manifest(folder: Path, manifest: dict[str, str]) -> None:
    (folder / "download_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _log_rows(rows: list[dict]) -> None:
    with LOG_PATH.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "dataset", "entry_type", "source_url", "download_timestamp_utc", "status", "file_path",
            "file_size_bytes", "http_status", "content_type", "note",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _fetch_landing_page(dataset: str, url: str, folder: Path) -> tuple[str, list[dict]]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    landing_path = folder / f"{ts}_landing.html"
    row = {
        "dataset": dataset,
        "entry_type": "landing_page",
        "source_url": url,
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": str(landing_path),
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    body, ctype, status = _fetch_bytes(url)
    landing_path.write_bytes(body)
    row.update({
        "status": "ok",
        "file_size_bytes": landing_path.stat().st_size,
        "http_status": status,
        "content_type": ctype,
    })
    html = body.decode("utf-8", errors="ignore")
    return html, [row]


def _download_discovered_files(dataset: str, links: list[str], folder: Path) -> list[dict]:
    manifest = _load_manifest(folder)
    rows = []

    for idx, link in enumerate(links, start=1):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = _filename_from_url(link, f"{dataset}_{idx:03d}")
        out_path = folder / filename
        row = {
            "dataset": dataset,
            "entry_type": "dataset_file",
            "source_url": link,
            "download_timestamp_utc": ts,
            "status": "failed",
            "file_path": str(out_path),
            "file_size_bytes": 0,
            "http_status": "",
            "content_type": "",
            "note": "",
        }

        prior = manifest.get(link)
        if prior and (folder / prior).exists():
            row.update({
                "status": "skipped_duplicate",
                "file_path": str(folder / prior),
                "file_size_bytes": (folder / prior).stat().st_size,
                "note": "URL previously downloaded; skipped duplicate fetch.",
            })
            rows.append(row)
            continue

        try:
            body, ctype, status = _fetch_bytes(link)
            out_path.write_bytes(body)
            manifest[link] = out_path.name
            row.update({
                "status": "ok",
                "file_size_bytes": out_path.stat().st_size,
                "http_status": status,
                "content_type": ctype,
            })
        except HTTPError as exc:
            row["http_status"] = exc.code
            row["note"] = f"HTTPError: {exc.reason}"
        except URLError as exc:
            row["note"] = f"URLError: {exc.reason}"
        except Exception as exc:
            row["note"] = str(exc)
        rows.append(row)

    _save_manifest(folder, manifest)
    return rows


def _process_dataset_page(dataset: str, page_url: str, keywords: tuple[str, ...], limit: int, strict: bool = True) -> list[dict]:
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    try:
        html, landing_rows = _fetch_landing_page(dataset, page_url, folder)
        rows.extend(landing_rows)
        links = _extract_links(html, page_url)
        download_links = [u for u in links if _is_download_candidate(u, keywords)]

        discovered_note = f"Discovered {len(download_links)} downloadable links."
        rows[-1]["note"] = discovered_note

        if not download_links:
            if strict:
                raise DownloadDiscoveryError(f"No downloadable file links found for {dataset} at {page_url}")
            return rows

        selected = download_links[:limit]
        rows.extend(_download_discovered_files(dataset, selected, folder))
    except Exception as exc:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rows.append({
            "dataset": dataset,
            "entry_type": "discovery_error",
            "source_url": page_url,
            "download_timestamp_utc": ts,
            "status": "failed",
            "file_path": "",
            "file_size_bytes": 0,
            "http_status": "",
            "content_type": "",
            "note": str(exc),
        })
        if strict:
            _log_rows(rows)
            raise

    return rows


def _download_land_property_api(cfg: PipelineConfig) -> list[dict]:
    folder = DATA_RAW / "land_property_api"
    folder.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_json = folder / f"{ts}_api_root.json"
    row = {
        "dataset": "land_property_api",
        "entry_type": "dataset_file",
        "source_url": cfg.source_urls["land_property_api"],
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": str(out_json),
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }
    try:
        body, ctype, status = _fetch_bytes(cfg.source_urls["land_property_api"])
        out_json.write_bytes(body)
        row.update({"status": "ok", "file_size_bytes": out_json.stat().st_size, "http_status": status, "content_type": ctype})
    except Exception as exc:
        row["note"] = str(exc)
    return [row]


def run_downloads(cfg: PipelineConfig) -> list[dict]:
    ensure_directories()
    rows: list[dict] = []

    rows.extend(_process_dataset_page(
        dataset="price_paid",
        page_url=cfg.source_urls["price_paid"],
        keywords=("price", "paid", "pp-", "landregistry", "hmlandregistry"),
        limit=cfg.ppd_download_limit,
        strict=True,
    ))
    rows.extend(_process_dataset_page(
        dataset="ukhpi",
        page_url=cfg.source_urls["ukhpi"],
        keywords=("hpi", "house", "price", "ukhpi"),
        limit=cfg.ukhpi_download_limit,
        strict=True,
    ))
    rows.extend(_process_dataset_page(
        dataset="epc",
        page_url=cfg.source_urls["epc_collection"],
        keywords=("epc", "energy", "performance", "certificate"),
        limit=cfg.epc_download_limit,
        strict=False,
    ))
    rows.extend(_download_land_property_api(cfg))

    # contextual landing pages only (logged but not parsed for file downloads)
    for name in ["land_property_api_info", "land_property_portal", "os_gb_address", "planning_data", "dwelling_stock", "rents_lettings", "ehs_tables", "house_building", "fire_stats"]:
        rows.extend(_process_dataset_page(name, cfg.source_urls[name], keywords=("csv", "zip", "xls", "xlsx", "json"), limit=1, strict=False))

    _log_rows(rows)
    return rows


if __name__ == "__main__":
    run_downloads(PipelineConfig())
