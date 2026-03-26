from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from pathlib import Path


def clean_text(value: str) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9 ]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _normalise_row(row: dict) -> dict:
    """Lowercase all dict keys so field lookups are case-insensitive."""
    return {k.lower().strip(): v for k, v in row.items()}


def read_csv_files(paths: list[Path], fieldnames: list[str] | None = None) -> list[dict]:
    rows: list[dict] = []
    normalise = fieldnames is None  # only normalise when headers come from the CSV itself
    for path in paths:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f, fieldnames=fieldnames) if fieldnames else csv.DictReader(f)
            if normalise:
                rows.extend(_normalise_row(r) for r in reader)
            else:
                rows.extend(dict(r) for r in reader)
    return rows


def read_csv_and_zip_files(paths: list[Path], fieldnames: list[str] | None = None) -> list[dict]:
    rows: list[dict] = []
    normalise = fieldnames is None
    for path in paths:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            txt = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                            reader = csv.DictReader(txt, fieldnames=fieldnames) if fieldnames else csv.DictReader(txt)
                            if normalise:
                                rows.extend(_normalise_row(r) for r in reader)
                            else:
                                rows.extend(dict(r) for r in reader)
        else:
            rows.extend(read_csv_files([path], fieldnames=fieldnames))
    return rows


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def write_parquet_placeholder(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_parquet_placeholder(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def score_similarity(a: str, b: str) -> int:
    a_set = set(clean_text(a).split())
    b_set = set(clean_text(b).split())
    if not a_set and not b_set:
        return 100
    if not a_set or not b_set:
        return 0
    overlap = len(a_set & b_set)
    total = len(a_set | b_set)
    return int((overlap / total) * 100)
