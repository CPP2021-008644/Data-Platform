#!/usr/bin/env python3
"""
Download exactly the NACE crosswalk and guide the user to manually fetch taxonomy.xlsx.

- Saves/uses:
  - <out-dir>/ec_updated_mapping.xlsx  (auto-download)
  - <out-dir>/taxonomy.xlsx            (manual download by the user)
- Writes <out-dir>/SOURCE.md with timestamp and SHA256 for files present.
- If destination files exist, they are skipped unless --force is provided.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import sys
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

# ----------------------------------------------------------------------
# Hardcoded endpoints
#   taxonomy.xlsx: user must download manually from this URL (not fetched by script)
#   mapping xlsx: auto-downloaded
# ----------------------------------------------------------------------
TAXONOMY_URL = "https://ec.europa.eu/sustainable-finance-taxonomy/home/"
MAPPING_URL = (
    "https://finance.ec.europa.eu/system/files/2023-06/"
    "sustainable-finance-taxonomy-nace-alternate-classification-mapping_en.xlsx"
)


def sha256_of_file(path: Path) -> str:
    """Compute SHA256 of a file using 64 KiB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def http_download(url: str, dest: Path) -> None:
    """HTTP(S) download with a basic User-Agent and streamed write."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "project-x-platform/compass-downloader"},
    )
    with urllib.request.urlopen(req, timeout=120) as r, dest.open("wb") as f:
        while True:
            chunk = r.read(65536)
            if not chunk:
                break
            f.write(chunk)


def write_source_md(
    out_dir: Path, rows: Iterable[tuple[str, Path]], manual_note: str | None
) -> None:
    """Write SOURCE.md with timestamp and SHA256 for the downloaded/present files, plus a manual note if needed."""
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    lines = [
        "# EU Taxonomy Compass — Download provenance",
        "",
        f"Generated at: {now}",
        "",
        "| File | URL | SHA256 |",
        "|------|-----|--------|",
    ]
    for url, path in rows:
        lines.append(f"| {path.name} | {url} | {sha256_of_file(path)} |")
    if manual_note:
        lines += [
            "",
            "## Manual step required",
            manual_note,
        ]
    (out_dir / "SOURCE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] Written {(out_dir / 'SOURCE.md')}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Download mapping file and print manual URL for taxonomy.xlsx"
    )
    ap.add_argument(
        "--out-dir", default="data", help="Destination directory (default: ./data)"
    )
    ap.add_argument(
        "--force", action="store_true", help="Overwrite existing files if present"
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    dst_tax = out_dir / "taxonomy.xlsx"
    dst_map = out_dir / "ec_updated_mapping.xlsx"

    written: list[tuple[str, Path]] = []
    manual_note: str | None = None

    # 1) Taxonomy.xlsx: manual
    if dst_tax.exists():
        print(f"[keep] taxonomy.xlsx already present at: {dst_tax}")
        written.append((TAXONOMY_URL, dst_tax))
    else:
        print(
            "[manual] taxonomy.xlsx not found.\n"
            f"          Please download it manually from:\n"
            f"          {TAXONOMY_URL}\n"
            f"          and save it as:\n"
            f"          {dst_tax}\n"
        )
        manual_note = (
            f"- **taxonomy.xlsx** must be downloaded manually from:\n"
            f"  {TAXONOMY_URL}\n"
            f"  Save the file to: `{dst_tax}`"
        )

    # 2) Mapping: auto-download
    try:
        if dst_map.exists() and not args.force:
            print(f"[skip] {dst_map} already exists (use --force to overwrite)")
        else:
            print(f"[download] {MAPPING_URL} -> {dst_map}")
            http_download(MAPPING_URL, dst_map)
        written.append((MAPPING_URL, dst_map))
    except Exception as exc:
        print(f"[error] downloading mapping: {exc}", file=sys.stderr)
        # seguimos generando SOURCE.md con lo que haya
        pass

    # 3) SOURCE.md
    write_source_md(out_dir, written, manual_note)

    print("[ok] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
