from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories

LOG_PATH = OUTPUTS / "download_log.csv"
ALLOWED_EXTENSIONS = (".csv", ".zip", ".xls", ".xlsx", ".json")
PRIORITY_FILE_HOST_HINTS = (
    "assets.publishing.service.gov.uk",
    "publicdata.landregistry.gov.uk",
    "use-land-property-data.service.gov.uk",
    "opendatacommunities",
    "files.digital",
)


class DownloadDiscoveryError(RuntimeError):
    """Raised when a strict dataset page has no downloadable links."""


def _safe_name(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_").lower()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fetch_bytes(url: str) -> tuple[bytes, str, int]:
    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    with urlopen(req, timeout=90) as resp:
        return resp.read(), resp.headers.get("Content-Type", ""), getattr(resp, "status", 200)


def _stream_to_file(url: str, out_path: Path, chunk_size: int = 65536) -> tuple[str, int]:
    """Download a URL directly to disk in chunks to avoid OOM on large files."""
    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    with urlopen(req, timeout=300) as resp:
        content_type = resp.headers.get("Content-Type", "")
        status = getattr(resp, "status", 200)
        if "text/html" in (content_type or "").lower():
            return content_type, status
        with open(out_path, "wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
    return content_type, status


def _extract_links(html: str, base_url: str) -> list[str]:
    raw_links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    links = []
    for href in raw_links:
        if href.startswith("mailto:") or href.startswith("#"):
            continue
        absolute = urljoin(base_url, href)
        parsed_abs = urlparse(absolute)
        parsed_base = urlparse(base_url)
        # Skip same-page anchors (same URL + fragment only)
        if parsed_abs.scheme == parsed_base.scheme and parsed_abs.netloc == parsed_base.netloc and parsed_abs.path == parsed_base.path and parsed_abs.fragment:
            continue
        links.append(absolute)

    # Deduplicate while preserving order
    seen, deduped = set(), []
    for link in links:
        if link not in seen:
            deduped.append(link)
            seen.add(link)
    return deduped


def _looks_like_download(url: str, keywords: tuple[str, ...], strict_files_only: bool = False) -> bool:
    low = url.lower()
    parsed = urlparse(low)

    # Never treat fragment links as files
    if parsed.fragment and not parsed.path:
        return False

    if any(low.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return True

    query = parse_qs(parsed.query)

    path_hint = any(token in parsed.path for token in ("download", "attachment", "dataset", "file"))
    query_hint = any(token in query for token in ("file", "format", "download"))
    keyword_hint = any(k in low for k in keywords)
    host_hint = any(h in parsed.netloc for h in PRIORITY_FILE_HOST_HINTS)

    if strict_files_only:
        # For core sources, only allow direct file URLs or likely asset-file endpoints.
        return host_hint and (path_hint or query_hint) and keyword_hint

    return (host_hint or path_hint or query_hint) and keyword_hint


def _candidate_rejection_reason(url: str, keywords: tuple[str, ...], strict_files_only: bool) -> str:
    parsed = urlparse(url)
    if url.startswith("#"):
        return "fragment-only link"
    if parsed.fragment and not parsed.path:
        return "fragment-only link"
    if _looks_like_download(url, keywords, strict_files_only=strict_files_only):
        return ""
    return "not a file-like download candidate"


def _prioritize_links(links: list[str]) -> list[str]:
    def score(u: str) -> tuple[int, int]:
        low = u.lower()
        host_priority = 0 if any(h in urlparse(low).netloc for h in PRIORITY_FILE_HOST_HINTS) else 1
        ext_priority = 0 if any(low.endswith(ext) for ext in ALLOWED_EXTENSIONS) else 1
        return (host_priority, ext_priority)

    return sorted(links, key=score)


def _filename_for_url(url: str, fallback_prefix: str) -> str:
    path_name = Path(urlparse(url).path).name
    if path_name and any(path_name.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return path_name
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return f"{fallback_prefix}_{digest}.dat"


def _manifest_path(folder: Path) -> Path:
    return folder / "download_manifest.json"


def _load_manifest(folder: Path) -> dict[str, str]:
    path = _manifest_path(folder)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _save_manifest(folder: Path, manifest: dict[str, str]) -> None:
    _manifest_path(folder).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _landing_row(dataset: str, source_url: str, file_path: str) -> dict:
    return {
        "dataset": dataset,
        "entry_type": "landing_page",
        "source_url": source_url,
        "download_timestamp_utc": _utc_timestamp(),
        "status": "failed",
        "file_path": file_path,
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }


def _fetch_and_save_landing_page(dataset: str, page_url: str, folder: Path) -> tuple[str, dict]:
    landing_path = folder / f"{_utc_timestamp()}_landing.html"
    row = _landing_row(dataset, page_url, str(landing_path))

    body, content_type, http_status = _fetch_bytes(page_url)
    landing_path.write_bytes(body)
    row.update(
        {
            "status": "ok",
            "file_size_bytes": landing_path.stat().st_size,
            "http_status": http_status,
            "content_type": content_type,
        }
    )
    return body.decode("utf-8", errors="ignore"), row


_SKIP_PATTERNS: dict[str, tuple[str, ...]] = {
    "price_paid": ("pp-complete",),
}


def _download_discovered_files(dataset: str, links: list[str], folder: Path) -> list[dict]:
    manifest = _load_manifest(folder)
    rows: list[dict] = []
    skip_patterns = _SKIP_PATTERNS.get(dataset, ())

    for idx, link in enumerate(links, start=1):
        # Skip known oversized aggregate files
        if skip_patterns and any(p in link.lower() for p in skip_patterns):
            rows.append({
                "dataset": dataset,
                "entry_type": "candidate_reject",
                "source_url": link,
                "download_timestamp_utc": _utc_timestamp(),
                "status": "rejected",
                "file_path": "",
                "file_size_bytes": 0,
                "http_status": "",
                "content_type": "",
                "note": f"Skipped oversized aggregate file (matches {skip_patterns}).",
            })
            continue

        filename = _filename_for_url(link, f"{dataset}_{idx:03d}")
        out_path = folder / filename
        row = {
            "dataset": dataset,
            "entry_type": "dataset_file",
            "source_url": link,
            "download_timestamp_utc": _utc_timestamp(),
            "status": "failed",
            "file_path": str(out_path),
            "file_size_bytes": 0,
            "http_status": "",
            "content_type": "",
            "note": "",
        }

        # URL-level dedupe
        prior_name = manifest.get(link)
        if prior_name and (folder / prior_name).exists():
            prior_path = folder / prior_name
            row.update(
                {
                    "status": "skipped_duplicate",
                    "file_path": str(prior_path),
                    "file_size_bytes": prior_path.stat().st_size,
                    "note": "URL already downloaded in previous run.",
                }
            )
            rows.append(row)
            continue

        try:
            content_type, http_status = _stream_to_file(link, out_path)
            if "text/html" in (content_type or "").lower():
                row.update(
                    {
                        "status": "rejected_html",
                        "http_status": http_status,
                        "content_type": content_type,
                        "note": "Rejected candidate link: returned HTML, not a dataset file.",
                    }
                )
                rows.append(row)
                continue
            manifest[link] = out_path.name
            row.update(
                {
                    "status": "ok",
                    "file_size_bytes": out_path.stat().st_size,
                    "http_status": http_status,
                    "content_type": content_type,
                }
            )
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


def _process_dataset_page(
    dataset: str,
    page_url: str,
    keywords: tuple[str, ...],
    limit: int,
    strict: bool,
) -> list[dict]:
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    try:
        html, landing_row = _fetch_and_save_landing_page(dataset, page_url, folder)
        rows.append(landing_row)

        all_links = _extract_links(html, page_url)
        strict_files_only = dataset in {"price_paid", "ukhpi"}

        data_links: list[str] = []
        for candidate in all_links:
            rejection = _candidate_rejection_reason(candidate, keywords, strict_files_only=strict_files_only)
            if rejection:
                rows.append(
                    {
                        "dataset": dataset,
                        "entry_type": "candidate_reject",
                        "source_url": candidate,
                        "download_timestamp_utc": _utc_timestamp(),
                        "status": "rejected",
                        "file_path": "",
                        "file_size_bytes": 0,
                        "http_status": "",
                        "content_type": "",
                        "note": f"Rejected candidate link: {rejection}",
                    }
                )
                continue
            data_links.append(candidate)

        data_links = _prioritize_links(data_links)

        landing_row["note"] = f"Discovered {len(data_links)} downloadable links; limit={limit}."

        if not data_links:
            message = f"No downloadable links found for {dataset} ({page_url})."
            rows.append(
                {
                    "dataset": dataset,
                    "entry_type": "discovery_error",
                    "source_url": page_url,
                    "download_timestamp_utc": _utc_timestamp(),
                    "status": "failed",
                    "file_path": "",
                    "file_size_bytes": 0,
                    "http_status": "",
                    "content_type": "",
                    "note": message,
                }
            )
            if strict:
                raise DownloadDiscoveryError(message)
            return rows

        selected_links = data_links[:limit]
        rows.extend(_download_discovered_files(dataset, selected_links, folder))

    except Exception as exc:
        rows.append(
            {
                "dataset": dataset,
                "entry_type": "discovery_error",
                "source_url": page_url,
                "download_timestamp_utc": _utc_timestamp(),
                "status": "failed",
                "file_path": "",
                "file_size_bytes": 0,
                "http_status": "",
                "content_type": "",
                "note": str(exc),
            }
        )
        if strict:
            raise

    return rows


def _download_land_property_api(cfg: PipelineConfig) -> list[dict]:
    dataset = "land_property_api"
    folder = DATA_RAW / dataset
    folder.mkdir(parents=True, exist_ok=True)

    out_path = folder / f"{_utc_timestamp()}_api_root.json"
    row = {
        "dataset": dataset,
        "entry_type": "dataset_file",
        "source_url": cfg.source_urls["land_property_api"],
        "download_timestamp_utc": _utc_timestamp(),
        "status": "failed",
        "file_path": str(out_path),
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    try:
        body, content_type, http_status = _fetch_bytes(cfg.source_urls["land_property_api"])
        out_path.write_bytes(body)
        row.update(
            {
                "status": "ok",
                "file_size_bytes": out_path.stat().st_size,
                "http_status": http_status,
                "content_type": content_type,
            }
        )
    except Exception as exc:
        row["note"] = str(exc)

    return [row]


def _write_log(rows: list[dict]) -> None:
    fields = [
        "dataset",
        "entry_type",
        "source_url",
        "download_timestamp_utc",
        "status",
        "file_path",
        "file_size_bytes",
        "http_status",
        "content_type",
        "note",
    ]
    with LOG_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_downloads(cfg: PipelineConfig) -> list[dict]:
    """Run two-stage downloader for core and contextual sources.

    Stage 1: fetch and save landing page.
    Stage 2: parse and download actual data links.
    """
    ensure_directories()
    rows: list[dict] = []

    # Core sources (strict for PPD + UKHPI, best-effort for EPC)
    rows.extend(
        _process_dataset_page(
            dataset="price_paid",
            page_url=cfg.source_urls["price_paid"],
            keywords=("price", "paid", "landregistry", "pp-"),
            limit=cfg.ppd_download_limit,
            strict=True,
        )
    )
    rows.extend(
        _process_dataset_page(
            dataset="ukhpi",
            page_url=cfg.source_urls["ukhpi"],
            keywords=("hpi", "house", "price", "ukhpi"),
            limit=cfg.ukhpi_download_limit,
            strict=True,
        )
    )
    rows.extend(
        _process_dataset_page(
            dataset="epc",
            page_url=cfg.source_urls["epc_collection"],
            keywords=("epc", "energy", "performance", "certificate"),
            limit=cfg.epc_download_limit,
            strict=False,
        )
    )

    # API handled separately
    rows.extend(_download_land_property_api(cfg))

    # VOA CTSOP (council tax stock of properties by band) — direct URL download
    voa_dir = DATA_RAW / "voa"
    voa_dir.mkdir(parents=True, exist_ok=True)
    voa_files = list(voa_dir.glob("*.csv")) + list(voa_dir.glob("*.zip"))
    if not voa_files:
        ctsop_url = cfg.source_urls.get("voa_ctsop", "")
        if ctsop_url:
            out_path = voa_dir / "CTSOP1-0.zip"
            voa_row = {
                "dataset": "voa_ctsop",
                "entry_type": "dataset_file",
                "source_url": ctsop_url,
                "download_timestamp_utc": _utc_timestamp(),
                "status": "failed",
                "file_path": str(out_path),
                "file_size_bytes": 0,
                "http_status": "",
                "content_type": "",
                "note": "",
            }
            try:
                print(f"Downloading VOA CTSOP from {ctsop_url}")
                content_type, http_status = _stream_to_file(ctsop_url, out_path)
                voa_row.update({
                    "status": "ok",
                    "file_size_bytes": out_path.stat().st_size,
                    "http_status": http_status,
                    "content_type": content_type,
                })
            except Exception as exc:
                voa_row["note"] = str(exc)
            rows.append(voa_row)

    # Contextual pages (best-effort one file each)
    contextual_names = [
        "land_property_api_info",
        "land_property_portal",
        "os_gb_address",
        "planning_data",
        "dwelling_stock",
        "rents_lettings",
        "ehs_tables",
        "house_building",
        "fire_stats",
    ]
    for name in contextual_names:
        rows.extend(
            _process_dataset_page(
                dataset=name,
                page_url=cfg.source_urls[name],
                keywords=("csv", "zip", "xls", "xlsx", "json"),
                limit=1,
                strict=False,
            )
        )

    _write_log(rows)
    return rows


if __name__ == "__main__":
    run_downloads(PipelineConfig())
