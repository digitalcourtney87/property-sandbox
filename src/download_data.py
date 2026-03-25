from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories

LOG_PATH = OUTPUTS / "download_log.csv"


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
    # preserve order, dedupe
    seen, dedup = set(), []
    for u in out:
        if u not in seen:
            dedup.append(u)
            seen.add(u)
    return dedup


def _filename_from_url(url: str, fallback: str) -> str:
    name = Path(urlparse(url).path).name or fallback
    if len(name) < 4:
        name = fallback
    return name


def _download_urls(dataset: str, urls: list[str]) -> list[dict]:
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)
    rows = []

    for idx, url in enumerate(urls, start=1):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{ts}_{idx:03d}_{_filename_from_url(url, f'{dataset}.dat')}"
        out_path = folder / filename
        row = {
            "dataset": dataset,
            "source_url": url,
            "download_timestamp_utc": ts,
            "status": "failed",
            "file_path": str(out_path),
            "file_size_bytes": 0,
            "http_status": "",
            "content_type": "",
            "note": "",
        }
        try:
            body, ctype, status = _fetch_bytes(url)
            out_path.write_bytes(body)
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
    return rows


def _discover_and_download(price_url: str, include_terms: tuple[str, ...], extensions: tuple[str, ...], limit: int, dataset: str) -> list[dict]:
    # Save source page for traceability
    page_rows = _download_urls(f"{dataset}_page", [price_url])
    candidates: list[str] = []
    try:
        body, _, _ = _fetch_bytes(price_url)
        html = body.decode("utf-8", errors="ignore")
        links = _extract_links(html, price_url)
        for link in links:
            low = link.lower()
            if any(low.endswith(ext) for ext in extensions) and any(term in low for term in include_terms):
                candidates.append(link)
        if not candidates:
            for link in links:
                low = link.lower()
                if any(low.endswith(ext) for ext in extensions):
                    candidates.append(link)
    except Exception:
        pass

    data_rows = _download_urls(dataset, candidates[:limit]) if candidates else []
    return page_rows + data_rows


def _download_land_property_api(cfg: PipelineConfig) -> list[dict]:
    dataset = "land_property_api"
    folder = DATA_RAW / dataset
    folder.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_json = folder / f"{ts}_api_root.json"
    row = {
        "dataset": dataset,
        "source_url": cfg.source_urls["land_property_api"],
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": str(out_json),
def fetch_url_to_raw(dataset: str, url: str) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / f"{_safe_name(dataset)}_{ts}.html"

    row = {
        "dataset": dataset,
        "source_url": url,
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": "",
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


def _download_context_pages(cfg: PipelineConfig) -> list[dict]:
    rows = []
    for name, url in cfg.source_urls.items():
        if name in {"price_paid", "ukhpi", "epc_collection", "land_property_api"}:
            continue
        rows.extend(_download_urls(name, [url]))
    return rows

    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    try:
        with urlopen(req, timeout=60) as resp:
            body = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type.lower():
                out = out.with_suffix(".json")
            out.write_bytes(body)
            row.update({
                "status": "ok",
                "file_path": str(out),
                "file_size_bytes": out.stat().st_size,
                "http_status": getattr(resp, "status", 200),
                "content_type": content_type,
            })
    except HTTPError as exc:
        row["http_status"] = exc.code
        row["note"] = f"HTTPError: {exc.reason}"
    except URLError as exc:
        row["note"] = f"URLError: {exc.reason}"
    except Exception as exc:
        row["note"] = str(exc)

    return row


def run_downloads(cfg: PipelineConfig) -> list[dict]:
    ensure_directories()
    rows: list[dict] = []

    rows.extend(_discover_and_download(
        cfg.source_urls["price_paid"],
        include_terms=("price", "paid", "pp-", "landregistry", "hmlandregistry"),
        extensions=(".csv", ".zip"),
        limit=cfg.ppd_download_limit,
        dataset="price_paid",
    ))
    rows.extend(_discover_and_download(
        cfg.source_urls["ukhpi"],
        include_terms=("hpi", "house", "price", "ukhpi"),
        extensions=(".csv", ".zip"),
        limit=cfg.ukhpi_download_limit,
        dataset="ukhpi",
    ))
    rows.extend(_discover_and_download(
        cfg.source_urls["epc_collection"],
        include_terms=("epc", "energy", "performance", "certificate"),
        extensions=(".csv", ".zip"),
        limit=cfg.epc_download_limit,
        dataset="epc",
    ))
    rows.extend(_download_land_property_api(cfg))
    rows.extend(_download_context_pages(cfg))
    rows = [fetch_url_to_raw(name, url) for name, url in cfg.source_urls.items()]

    exists = LOG_PATH.exists()
    with LOG_PATH.open("a", encoding="utf-8", newline="") as f:
        fields = [
            "dataset", "source_url", "download_timestamp_utc", "status", "file_path",
            "file_size_bytes", "http_status", "content_type", "note",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return rows


if __name__ == "__main__":
    run_downloads(PipelineConfig())
