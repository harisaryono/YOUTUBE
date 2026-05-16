"""
Guard and repair tools for oversized orchestrator_events rows.

The orchestrator DB is for operational state, not raw logs. Large repeated
`deferred` info events should be compacted or removed after we have enough
summary signal in log files.

Examples:
  python -m orchestrator.event_bloat_guard report --top 30
  python -m orchestrator.event_bloat_guard trim-deferred --dry-run --keep-latest 1000
  python -m orchestrator.event_bloat_guard trim-deferred --keep-latest 1000
  python -m orchestrator.event_bloat_guard compact-events --dry-run
  python -m orchestrator.event_bloat_guard compact-events
  sqlite3 db/orchestrator.db "PRAGMA wal_checkpoint(TRUNCATE); VACUUM;"
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from .state import DEFAULT_DB_PATH

MAX_MESSAGE_CHARS = 1000
MAX_INFO_PAYLOAD_CHARS = 1500
MAX_WARNING_PAYLOAD_CHARS = 4000
MAX_BLOCKING_PAYLOAD_CHARS = 8000


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _limit_for_severity(severity: str) -> int:
    severity = str(severity or "").strip().lower()
    if severity in {"blocking", "error", "critical"}:
        return MAX_BLOCKING_PAYLOAD_CHARS
    if severity in {"warning", "warn"}:
        return MAX_WARNING_PAYLOAD_CHARS
    return MAX_INFO_PAYLOAD_CHARS


def _safe_json_loads(raw: str) -> Any:
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {"raw_payload_prefix": str(raw or "")[:500]}


def _compact_payload(raw: str, *, severity: str, event_type: str) -> str:
    limit = _limit_for_severity(severity)
    raw = str(raw or "{}")
    if len(raw) <= limit:
        return raw
    payload = _safe_json_loads(raw)
    compact: dict[str, Any] = {
        "_compacted": True,
        "_original_bytes": len(raw),
        "event_type": event_type,
    }
    if isinstance(payload, dict):
        for key in (
            "reason_code",
            "reason",
            "stage",
            "scope",
            "status",
            "verdict",
            "recommendation",
            "cooldown_seconds",
            "policy_blockers",
            "blockers",
        ):
            if key in payload:
                value = payload.get(key)
                if isinstance(value, str):
                    compact[key] = value[:500]
                elif isinstance(value, (int, float, bool)) or value is None:
                    compact[key] = value
                else:
                    compact[key] = str(value)[:500]
        compact["payload_keys"] = sorted(str(k) for k in payload.keys())[:50]
    else:
        compact["raw_type"] = type(payload).__name__
    text = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
    if len(text) > limit:
        compact["_truncated_again"] = True
        compact.pop("payload_keys", None)
        text = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))[:limit]
    return text


def report(db_path: Path = DEFAULT_DB_PATH, *, top: int = 30) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        by_type = [dict(row) for row in conn.execute(
            """
            SELECT event_type,
                   severity,
                   COUNT(*) AS rows,
                   ROUND(SUM(LENGTH(message)+LENGTH(payload_json))/1024.0/1024.0,2) AS mb,
                   ROUND(AVG(LENGTH(message)+LENGTH(payload_json)),2) AS avg_bytes
            FROM orchestrator_events
            GROUP BY event_type, severity
            ORDER BY SUM(LENGTH(message)+LENGTH(payload_json)) DESC
            LIMIT ?
            """,
            (int(top),),
        ).fetchall()]
        biggest_rows = [dict(row) for row in conn.execute(
            """
            SELECT id, event_type, severity, created_at,
                   LENGTH(message) AS message_bytes,
                   LENGTH(payload_json) AS payload_bytes
            FROM orchestrator_events
            ORDER BY LENGTH(message)+LENGTH(payload_json) DESC
            LIMIT ?
            """,
            (int(top),),
        ).fetchall()]
        return {"by_type": by_type, "biggest_rows": biggest_rows}
    finally:
        conn.close()


def trim_deferred(db_path: Path = DEFAULT_DB_PATH, *, keep_latest: int = 1000, dry_run: bool = False) -> dict[str, Any]:
    keep_latest = max(0, int(keep_latest or 0))
    conn = _connect(db_path)
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM orchestrator_events WHERE event_type='deferred' AND severity='info'").fetchone()[0] or 0)
        bytes_total = int(conn.execute("SELECT COALESCE(SUM(LENGTH(message)+LENGTH(payload_json)),0) FROM orchestrator_events WHERE event_type='deferred' AND severity='info'").fetchone()[0] or 0)
        rows_to_delete = max(0, total - keep_latest)
        if not dry_run and rows_to_delete > 0:
            conn.execute(
                """
                DELETE FROM orchestrator_events
                WHERE id IN (
                    SELECT id
                    FROM orchestrator_events
                    WHERE event_type='deferred' AND severity='info'
                    ORDER BY id ASC
                    LIMIT ?
                )
                """,
                (rows_to_delete,),
            )
            conn.commit()
        return {
            "dry_run": bool(dry_run),
            "event_type": "deferred",
            "severity": "info",
            "total_rows": total,
            "keep_latest": keep_latest,
            "rows_to_delete": rows_to_delete,
            "bytes_total_before": bytes_total,
            "mb_total_before": round(bytes_total / 1024 / 1024, 2),
        }
    finally:
        conn.close()


def compact_events(db_path: Path = DEFAULT_DB_PATH, *, dry_run: bool = False, batch_size: int = 1000) -> dict[str, Any]:
    conn = _connect(db_path)
    changed = 0
    bytes_before = 0
    bytes_after = 0
    try:
        rows = conn.execute(
            """
            SELECT id, event_type, severity, message, payload_json,
                   LENGTH(message) AS message_bytes,
                   LENGTH(payload_json) AS payload_bytes
            FROM orchestrator_events
            WHERE LENGTH(message) > ? OR LENGTH(payload_json) > ?
            ORDER BY id ASC
            """,
            (MAX_MESSAGE_CHARS, MAX_INFO_PAYLOAD_CHARS),
        ).fetchall()
        for row in rows:
            severity = str(row["severity"] or "info")
            event_type = str(row["event_type"] or "")
            message = str(row["message"] or "")
            payload = str(row["payload_json"] or "{}")
            new_message = message if len(message) <= MAX_MESSAGE_CHARS else message[:MAX_MESSAGE_CHARS] + " ...[trimmed]"
            new_payload = _compact_payload(payload, severity=severity, event_type=event_type)
            if new_message == message and new_payload == payload:
                continue
            changed += 1
            bytes_before += len(message) + len(payload)
            bytes_after += len(new_message) + len(new_payload)
            if not dry_run:
                conn.execute(
                    "UPDATE orchestrator_events SET message=?, payload_json=? WHERE id=?",
                    (new_message, new_payload, int(row["id"])),
                )
                if changed % max(1, int(batch_size)) == 0:
                    conn.commit()
        if not dry_run:
            conn.commit()
        return {
            "dry_run": bool(dry_run),
            "rows_to_compact": changed,
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
            "estimated_saved_bytes": max(0, bytes_before - bytes_after),
            "estimated_saved_mb": round(max(0, bytes_before - bytes_after) / 1024 / 1024, 2),
        }
    finally:
        conn.close()


def vacuum(db_path: Path = DEFAULT_DB_PATH, *, dry_run: bool = False) -> dict[str, Any]:
    before = db_path.stat().st_size if db_path.exists() else 0
    wal = Path(str(db_path) + "-wal")
    before_wal = wal.stat().st_size if wal.exists() else 0
    if not dry_run:
        conn = _connect(db_path)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("VACUUM")
            conn.execute("PRAGMA optimize")
        finally:
            conn.close()
    after = db_path.stat().st_size if db_path.exists() else 0
    after_wal = wal.stat().st_size if wal.exists() else 0
    return {"dry_run": bool(dry_run), "before_bytes": before, "before_wal_bytes": before_wal, "after_bytes": after, "after_wal_bytes": after_wal}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose and reduce orchestrator_events bloat")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("report")
    p.add_argument("--top", type=int, default=30)
    p = sub.add_parser("trim-deferred")
    p.add_argument("--keep-latest", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true")
    p = sub.add_parser("compact-events")
    p.add_argument("--dry-run", action="store_true")
    p = sub.add_parser("vacuum")
    p.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    db_path = Path(args.db)
    if args.command == "report":
        payload = report(db_path, top=int(args.top))
    elif args.command == "trim-deferred":
        payload = trim_deferred(db_path, keep_latest=int(args.keep_latest), dry_run=bool(args.dry_run))
    elif args.command == "compact-events":
        payload = compact_events(db_path, dry_run=bool(args.dry_run))
    elif args.command == "vacuum":
        payload = vacuum(db_path, dry_run=bool(args.dry_run))
    else:
        raise SystemExit(f"unsupported command: {args.command}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
