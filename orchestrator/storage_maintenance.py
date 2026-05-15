"""
Storage maintenance utilities for orchestrator growth control.

This module answers: "what is making orchestrator large?" and provides safe
maintenance actions that can be run manually:

  python -m orchestrator.storage_maintenance usage
  python -m orchestrator.storage_maintenance vacuum --dry-run
  python -m orchestrator.storage_maintenance trim-db --older-than-days 30 --dry-run

It intentionally does not delete run directories. Raw run logs are handled by
orchestrator.log_compact and archived-run pruning is handled by
orchestrator.log_archive.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from .state import DEFAULT_DB_PATH, OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs" / "orchestrator"
LOGS_DIR = PROJECT_ROOT / "logs"
DB_DIR = PROJECT_ROOT / "db"


def _size_bytes(path: Path) -> int:
    try:
        if path.is_file():
            return int(path.stat().st_size)
        if path.is_dir():
            total = 0
            for root, _dirs, files in os.walk(path):
                for filename in files:
                    try:
                        total += int((Path(root) / filename).stat().st_size)
                    except OSError:
                        continue
            return total
    except OSError:
        return 0
    return 0


def _human_size(value: int) -> str:
    size = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _top_files(base: Path, *, limit: int = 20) -> list[dict[str, Any]]:
    if not base.exists():
        return []
    items: list[tuple[int, Path]] = []
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        try:
            items.append((int(path.stat().st_size), path))
        except OSError:
            continue
    items.sort(reverse=True, key=lambda item: item[0])
    return [
        {"size_bytes": size, "size": _human_size(size), "path": str(path.relative_to(PROJECT_ROOT))}
        for size, path in items[: max(1, int(limit or 20))]
    ]


def _top_run_dirs(*, limit: int = 20) -> list[dict[str, Any]]:
    if not RUNS_DIR.exists():
        return []
    items: list[tuple[int, Path]] = []
    for path in RUNS_DIR.iterdir():
        if not path.is_dir() or path.name == "reports":
            continue
        items.append((_size_bytes(path), path))
    items.sort(reverse=True, key=lambda item: item[0])
    return [
        {"size_bytes": size, "size": _human_size(size), "path": str(path.relative_to(PROJECT_ROOT))}
        for size, path in items[: max(1, int(limit or 20))]
    ]


def _db_table_sizes(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = sqlite3.connect(str(db_path))
    try:
        con.row_factory = sqlite3.Row
        # dbstat may not be enabled in every SQLite build, so fallback gracefully.
        try:
            rows = con.execute(
                """
                SELECT name, SUM(pgsize) AS size_bytes
                FROM dbstat
                GROUP BY name
                ORDER BY size_bytes DESC
                """
            ).fetchall()
            return [
                {"name": str(row["name"]), "size_bytes": int(row["size_bytes"] or 0), "size": _human_size(int(row["size_bytes"] or 0))}
                for row in rows
            ]
        except sqlite3.Error:
            names = con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            result: list[dict[str, Any]] = []
            for row in names:
                name = str(row[0])
                try:
                    count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                except sqlite3.Error:
                    count = 0
                result.append({"name": name, "row_count": int(count), "size_bytes": 0, "size": "unknown"})
            return result
    finally:
        con.close()


def build_usage_report(*, top: int = 20) -> dict[str, Any]:
    db_path = DEFAULT_DB_PATH
    paths = {
        "runs_orchestrator": RUNS_DIR,
        "logs": LOGS_DIR,
        "logs_archive": LOGS_DIR / "archive",
        "db": DB_DIR,
        "orchestrator_db": db_path,
        "orchestrator_db_wal": Path(str(db_path) + "-wal"),
        "orchestrator_db_shm": Path(str(db_path) + "-shm"),
    }
    sizes = {key: {"path": str(path.relative_to(PROJECT_ROOT)) if path.is_absolute() and PROJECT_ROOT in path.parents or path == PROJECT_ROOT else str(path), "size_bytes": _size_bytes(path), "size": _human_size(_size_bytes(path)), "exists": path.exists()} for key, path in paths.items()}
    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sizes": sizes,
        "top_run_dirs": _top_run_dirs(limit=top),
        "top_files_runs": _top_files(RUNS_DIR, limit=top),
        "top_files_logs": _top_files(LOGS_DIR, limit=top),
        "db_tables": _db_table_sizes(db_path),
    }


def vacuum_orchestrator_db(*, dry_run: bool = False) -> dict[str, Any]:
    db_path = DEFAULT_DB_PATH
    before = {
        "db": _size_bytes(db_path),
        "wal": _size_bytes(Path(str(db_path) + "-wal")),
        "shm": _size_bytes(Path(str(db_path) + "-shm")),
    }
    if dry_run:
        return {"success": True, "dry_run": True, "before": before, "after": before}
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        con.execute("VACUUM")
        con.execute("PRAGMA optimize")
    finally:
        con.close()
    after = {
        "db": _size_bytes(db_path),
        "wal": _size_bytes(Path(str(db_path) + "-wal")),
        "shm": _size_bytes(Path(str(db_path) + "-shm")),
    }
    return {"success": True, "dry_run": False, "before": before, "after": after, "saved_bytes": sum(before.values()) - sum(after.values())}


def trim_db_finished_job_text(*, older_than_days: int = 30, dry_run: bool = False) -> dict[str, Any]:
    """Trim large stored error_text/log snippets from old finished jobs.

    Safe intent: raw log evidence should be in run dirs/archives. The DB should
    keep metadata, not large repeated log fragments forever.
    """
    older_than_days = max(1, int(older_than_days or 30))
    con = sqlite3.connect(str(DEFAULT_DB_PATH))
    try:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT job_id, LENGTH(COALESCE(error_text, '')) AS n
            FROM orchestrator_active_jobs
            WHERE status != 'running'
              AND COALESCE(error_text, '') != ''
              AND datetime(COALESCE(finished_at, updated_at, started_at)) < datetime('now', '-' || ? || ' days')
            """,
            (older_than_days,),
        ).fetchall()
        total_bytes = sum(int(row["n"] or 0) for row in rows)
        if not dry_run and rows:
            con.execute(
                """
                UPDATE orchestrator_active_jobs
                SET error_text = substr(error_text, 1, 500) || '\n...[trimmed by storage_maintenance after archival window]...',
                    updated_at = datetime('now')
                WHERE status != 'running'
                  AND COALESCE(error_text, '') != ''
                  AND datetime(COALESCE(finished_at, updated_at, started_at)) < datetime('now', '-' || ? || ' days')
                """,
                (older_than_days,),
            )
            con.commit()
        return {"success": True, "dry_run": dry_run, "older_than_days": older_than_days, "rows_matched": len(rows), "stored_error_text_bytes_before": total_bytes}
    finally:
        con.close()


def _cmd_usage(args: argparse.Namespace) -> int:
    report = build_usage_report(top=int(args.top or 20))
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0


def _cmd_vacuum(args: argparse.Namespace) -> int:
    result = vacuum_orchestrator_db(dry_run=bool(args.dry_run))
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    return 0 if result.get("success") else 1


def _cmd_trim_db(args: argparse.Namespace) -> int:
    result = trim_db_finished_job_text(older_than_days=int(args.older_than_days), dry_run=bool(args.dry_run))
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    return 0 if result.get("success") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose and reduce orchestrator storage growth")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("usage", help="Show size breakdown for orchestrator storage")
    p.add_argument("--top", type=int, default=20)
    p.set_defaults(func=_cmd_usage)

    p = sub.add_parser("vacuum", help="Checkpoint WAL and VACUUM db/orchestrator.db")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_vacuum)

    p = sub.add_parser("trim-db", help="Trim old stored error_text snippets from finished job rows")
    p.add_argument("--older-than-days", type=int, default=30)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_trim_db)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
