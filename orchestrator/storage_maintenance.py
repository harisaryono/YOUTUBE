"""
Storage maintenance utilities for orchestrator growth control.

This module answers: "what is making orchestrator large?" and provides safe
maintenance actions that can be run manually:

  python -m orchestrator.storage_maintenance usage
  python -m orchestrator.storage_maintenance vacuum --dry-run
  python -m orchestrator.storage_maintenance trim-db --older-than-days 30 --dry-run
  python -m orchestrator.storage_maintenance trim-snapshots --keep-days 7 --dry-run
  python -m orchestrator.storage_maintenance trim-events --info-days 7 --important-days 14 --dry-run

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

# Event types considered low-value that can be trimmed early
LOW_VALUE_EVENT_TYPES = {
    "sleep",
    "plan",
    "dispatch_success",
    "cleanup",
    "report",
    "inventory",
    "health",
    "control.retry_queue_drain",
}


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
    """Trim large stored error_text/log snippets from old finished jobs."""
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


def trim_inventory_snapshots(*, keep_days: int = 7, keep_latest: int = 500, dry_run: bool = False) -> dict[str, Any]:
    """Remove old inventory snapshots beyond retention limits.

    Inventory snapshots contain full cluster state (cooldowns, locks, jobs, etc.)
    and are taken every few seconds. Keeping unlimited snapshots is wasteful.

    Safe intent: latest N snapshots within keep_days are preserved.
    Older snapshots beyond both thresholds are deleted.
    """
    keep_days = max(1, int(keep_days or 7))
    keep_latest = max(10, int(keep_latest or 500))

    con = sqlite3.connect(str(DEFAULT_DB_PATH))
    try:
        con.row_factory = sqlite3.Row
        before_count = con.execute("SELECT COUNT(*) FROM orchestrator_inventory_snapshots").fetchone()[0]

        # Find IDs to delete: rows older than keep_days, excluding the latest keep_latest
        rows = con.execute(
            """
            SELECT id, created_at, LENGTH(COALESCE(payload_json, '')) AS payload_bytes
            FROM orchestrator_inventory_snapshots
            WHERE created_at < datetime('now', '-' || ? || ' days')
            ORDER BY id ASC
            """,
            (keep_days,),
        ).fetchall()

        # Also find IDs beyond keep_latest (if total exceeds that)
        total = con.execute("SELECT COUNT(*) FROM orchestrator_inventory_snapshots").fetchone()[0]
        extra_rows = []
        if total > keep_latest:
            threshold_id_rows = con.execute(
                "SELECT id FROM orchestrator_inventory_snapshots ORDER BY id DESC LIMIT 1 OFFSET ?",
                (keep_latest - 1,),
            ).fetchall()
            if threshold_id_rows:
                threshold_id = threshold_id_rows[0]["id"]
                extra_rows = con.execute(
                    """
                    SELECT id, created_at, LENGTH(COALESCE(payload_json, '')) AS payload_bytes
                    FROM orchestrator_inventory_snapshots
                    WHERE id < ?
                    """,
                    (threshold_id,),
                ).fetchall()

        # Combine and deduplicate
        delete_ids = set()
        total_bytes = 0
        for r in rows:
            delete_ids.add(int(r["id"]))
            total_bytes += int(r["payload_bytes"] or 0)
        for r in extra_rows:
            did = int(r["id"])
            if did not in delete_ids:
                delete_ids.add(did)
                total_bytes += int(r["payload_bytes"] or 0)

        if not dry_run and delete_ids:
            ids_list = sorted(delete_ids)
            # Delete in batches to avoid too many SQL variables
            batch_size = 500
            for i in range(0, len(ids_list), batch_size):
                batch = ids_list[i : i + batch_size]
                placeholders = ",".join("?" for _ in batch)
                con.execute(
                    f"DELETE FROM orchestrator_inventory_snapshots WHERE id IN ({placeholders})",
                    batch,
                )
            con.commit()

        return {
            "success": True,
            "dry_run": dry_run,
            "keep_days": keep_days,
            "keep_latest": keep_latest,
            "total_before": before_count,
            "rows_to_delete": len(delete_ids),
            "estimated_bytes": total_bytes,
            "after_delete": {
                "estimated_remaining": max(0, before_count - len(delete_ids)),
            },
        }
    finally:
        con.close()


def trim_events(*, info_days: int = 7, important_days: int = 14, dry_run: bool = False) -> dict[str, Any]:
    """Remove low-value events older than info_days and important events older than important_days.

    Low-value event types (sleep, dispatch_success, etc.) are trimmed aggressively.
    Important event types (error, dispatch_failure, timeout, etc.) are kept longer.
    """
    info_days = max(1, int(info_days or 7))
    important_days = max(1, int(important_days or 14))

    con = sqlite3.connect(str(DEFAULT_DB_PATH))
    try:
        con.row_factory = sqlite3.Row

        before_count = con.execute("SELECT COUNT(*) FROM orchestrator_events").fetchone()[0]
        before_bytes = con.execute("SELECT SUM(LENGTH(COALESCE(message,'')) + LENGTH(COALESCE(payload_json,''))) FROM orchestrator_events").fetchone()[0] or 0

        # Low-value events older than info_days
        low_value_rows = con.execute(
            """
            SELECT id, LENGTH(COALESCE(message,'')) + LENGTH(COALESCE(payload_json,'')) AS row_bytes
            FROM orchestrator_events
            WHERE event_type IN ({})
              AND created_at < datetime('now', '-' || ? || ' days')
            """.format(",".join("?" for _ in LOW_VALUE_EVENT_TYPES)),
            list(LOW_VALUE_EVENT_TYPES) + [info_days],
        ).fetchall()

        # Important events older than important_days (excluding low-value types)
        important_rows = con.execute(
            """
            SELECT id, LENGTH(COALESCE(message,'')) + LENGTH(COALESCE(payload_json,'')) AS row_bytes
            FROM orchestrator_events
            WHERE event_type NOT IN ({})
              AND created_at < datetime('now', '-' || ? || ' days')
            """.format(",".join("?" for _ in LOW_VALUE_EVENT_TYPES)),
            list(LOW_VALUE_EVENT_TYPES) + [important_days],
        ).fetchall()

        all_rows = low_value_rows + important_rows
        delete_ids = sorted(set(int(r["id"]) for r in all_rows))
        total_bytes = sum(int(r["row_bytes"] or 0) for r in all_rows)

        if not dry_run and delete_ids:
            batch_size = 500
            for i in range(0, len(delete_ids), batch_size):
                batch = delete_ids[i : i + batch_size]
                placeholders = ",".join("?" for _ in batch)
                con.execute(
                    f"DELETE FROM orchestrator_events WHERE id IN ({placeholders})",
                    batch,
                )
            con.commit()

        return {
            "success": True,
            "dry_run": dry_run,
            "info_days": info_days,
            "important_days": important_days,
            "total_before": before_count,
            "rows_to_delete": len(delete_ids),
            "estimated_bytes": total_bytes,
            "low_value_rows": len(low_value_rows),
            "important_rows": len(important_rows),
        }
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


def _cmd_trim_snapshots(args: argparse.Namespace) -> int:
    result = trim_inventory_snapshots(
        keep_days=int(args.keep_days),
        keep_latest=int(args.keep_latest),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    return 0 if result.get("success") else 1


def _cmd_trim_events(args: argparse.Namespace) -> int:
    result = trim_events(
        info_days=int(args.info_days),
        important_days=int(args.important_days),
        dry_run=bool(args.dry_run),
    )
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

    p = sub.add_parser("trim-snapshots", help="Remove old inventory snapshots beyond retention limits")
    p.add_argument("--keep-days", type=int, default=7, help="Keep snapshots newer than this many days (default: 7)")
    p.add_argument("--keep-latest", type=int, default=500, help="Keep at most this many latest snapshots (default: 500)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_trim_snapshots)

    p = sub.add_parser("trim-events", help="Remove old events beyond retention limits (low-value events trimmed more aggressively)")
    p.add_argument("--info-days", type=int, default=7, help="Keep low-value events (sleep/dispatch_success) newer than this many days (default: 7)")
    p.add_argument("--important-days", type=int, default=14, help="Keep important events (error/failure) newer than this many days (default: 14)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_trim_events)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
