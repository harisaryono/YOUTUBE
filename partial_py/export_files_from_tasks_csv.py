#!/usr/bin/env python3
"""
Generate a file list (for rsync/tar) from a tasks CSV.

Input CSV columns (any of):
- transcript_file_path
- transcript_path

Output: one path per line (default stdout), suitable for rsync --files-from
"""

import argparse
import csv
from pathlib import Path


def _normalize(p: str) -> str:
    p = str(p or "").strip()
    if not p:
        return ""
    # Prefer relative 'uploads/..' so the list can be used from repo root.
    idx = p.find("/uploads/")
    if idx != -1:
        p = p[idx + 1 :]
    if p.startswith("./"):
        p = p[2:]
    return p


def main() -> int:
    ap = argparse.ArgumentParser(description="Export transcript file paths from tasks CSV")
    ap.add_argument("--tasks-csv", required=True, help="Tasks CSV path")
    ap.add_argument("--out", default="-", help="Output file list path or '-' for stdout")
    args = ap.parse_args()

    paths: set[str] = set()
    with open(args.tasks_csv, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            p = _normalize(row.get("transcript_file_path") or row.get("transcript_path") or "")
            if p:
                paths.add(p)

    out_lines = "\n".join(sorted(paths)) + ("\n" if paths else "")
    if args.out == "-":
        print(out_lines, end="")
        return 0

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out_lines, encoding="utf-8")
    print(f"Wrote {len(paths)} paths -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

