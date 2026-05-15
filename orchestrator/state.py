"""
Orchestrator State Store
SQLite-based persistent state for cooldowns, events, and key-value store.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "db" / "orchestrator.db"


class OrchestratorState:
    """Persistent state store for the orchestrator daemon."""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS orchestrator_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS orchestrator_cooldowns (
                scope TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                cooldown_until TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'normal',
                recommendation TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS orchestrator_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                stage TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT '',
                severity TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                recommendation TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_events_created
                ON orchestrator_events(created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_events_type
                ON orchestrator_events(event_type, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_cooldowns_until
                ON orchestrator_cooldowns(cooldown_until);

            CREATE TABLE IF NOT EXISTS orchestrator_locks (
                lock_key TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS orchestrator_inventory_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_created
                ON orchestrator_inventory_snapshots(created_at DESC);

            CREATE TABLE IF NOT EXISTS orchestrator_active_jobs (
                job_id TEXT PRIMARY KEY,
                stage TEXT NOT NULL,
                scope TEXT NOT NULL DEFAULT '',
                group_name TEXT NOT NULL DEFAULT '',
                slot_index INTEGER NOT NULL DEFAULT 0,
                lock_key TEXT NOT NULL DEFAULT '',
                pid INTEGER NOT NULL,
                command TEXT NOT NULL DEFAULT '',
                run_dir TEXT NOT NULL DEFAULT '',
                log_path TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'running',
                returncode INTEGER,
                error_text TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_active_jobs_status_stage
                ON orchestrator_active_jobs(status, stage);

            CREATE INDEX IF NOT EXISTS idx_active_jobs_status_group
                ON orchestrator_active_jobs(status, group_name);

            CREATE TABLE IF NOT EXISTS orchestrator_retry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_job_id TEXT NOT NULL UNIQUE,
                stage TEXT NOT NULL,
                scope TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                requested_by TEXT NOT NULL DEFAULT '',
                requested_at TEXT NOT NULL DEFAULT (datetime('now')),
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 1,
                claimed_by TEXT NOT NULL DEFAULT '',
                claimed_at TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                error_text TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_retry_queue_status_stage
                ON orchestrator_retry_queue(status, stage);

            CREATE INDEX IF NOT EXISTS idx_retry_queue_requested
                ON orchestrator_retry_queue(requested_at DESC);
        """)
        for ddl in (
            "ALTER TABLE orchestrator_retry_queue ADD COLUMN claimed_by TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE orchestrator_retry_queue ADD COLUMN claimed_at TEXT",
        ):
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
        conn.commit()

    # --- Key-Value State ---

    def get(self, key: str, default: str = "") -> str:
        conn = self._connect()
        row = conn.execute(
            "SELECT value FROM orchestrator_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def set(self, key: str, value: str) -> None:
        conn = self._connect()
        conn.execute(
            """INSERT INTO orchestrator_state (key, value, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')""",
            (key, value),
        )
        conn.commit()

    def get_int(self, key: str, default: int = 0) -> int:
        val = self.get(key)
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def set_int(self, key: str, value: int) -> None:
        self.set(key, str(value))

    def record_event(
        self,
        event_type: str,
        message: str,
        stage: str = "",
        scope: str = "",
        severity: str = "info",
        recommendation: str = "",
        reason_code: str = "",
        payload: dict[str, Any] | None = None,
    ) -> int:
        """Compatibility wrapper for add_event()."""
        return self.add_event(
            event_type=event_type,
            message=message,
            stage=stage,
            scope=scope,
            severity=severity,
            recommendation=recommendation,
            reason_code=reason_code,
            payload=payload,
        )

    # --- Cooldowns ---

    def set_cooldown(
        self,
        scope: str,
        reason: str,
        duration_seconds: int,
        severity: str = "normal",
        recommendation: str = "",
    ) -> None:
        """Set a cooldown for a scope that expires after duration_seconds."""
        until = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn = self._connect()
        conn.execute(
            """INSERT INTO orchestrator_cooldowns (scope, reason, cooldown_until, severity, recommendation, updated_at)
               VALUES (?, ?, datetime('now', '+' || ? || ' seconds'), ?, ?, datetime('now'))
               ON CONFLICT(scope) DO UPDATE SET
                   reason = excluded.reason,
                   cooldown_until = excluded.cooldown_until,
                   severity = excluded.severity,
                   recommendation = excluded.recommendation,
                   updated_at = datetime('now')""",
            (scope, reason, int(duration_seconds), severity, recommendation),
        )
        conn.commit()

    def get_cooldown(self, scope: str) -> dict[str, Any] | None:
        """Return cooldown info if active, None if expired or not set."""
        conn = self._connect()
        row = conn.execute(
            """SELECT scope, reason, cooldown_until, severity, recommendation
               FROM orchestrator_cooldowns
               WHERE scope = ? AND cooldown_until > datetime('now')""",
            (scope,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def is_cooldown_active(self, scope: str) -> bool:
        return self.get_cooldown(scope) is not None

    def clear_cooldown(self, scope: str) -> None:
        conn = self._connect()
        conn.execute("DELETE FROM orchestrator_cooldowns WHERE scope = ?", (scope,))
        conn.commit()

    def clear_expired_cooldowns(self) -> int:
        """Remove expired cooldowns. Returns count removed."""
        conn = self._connect()
        cursor = conn.execute(
            "DELETE FROM orchestrator_cooldowns WHERE cooldown_until <= datetime('now')"
        )
        conn.commit()
        return cursor.rowcount

    def list_active_cooldowns(self) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            """SELECT scope, reason, cooldown_until, severity, recommendation
               FROM orchestrator_cooldowns
               WHERE cooldown_until > datetime('now')
               ORDER BY cooldown_until ASC""",
        ).fetchall()
        return [dict(r) for r in rows]

    def _state_rows_like(self, pattern: str) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            """SELECT key, value, updated_at
               FROM orchestrator_state
               WHERE key LIKE ?
               ORDER BY key ASC""",
            (pattern,),
        ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _decode_control_payload(raw: str) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {"reason": text, "raw_value": text}
        if isinstance(payload, dict):
            payload.setdefault("raw_value", text)
            return payload
        return {"reason": text, "raw_value": text}

    # --- Events ---

    def add_event(
        self,
        event_type: str,
        message: str,
        stage: str = "",
        scope: str = "",
        severity: str = "info",
        recommendation: str = "",
        reason_code: str = "",
        payload: dict[str, Any] | None = None,
    ) -> int:
        """Add an event. Returns event ID."""
        conn = self._connect()
        payload_obj = dict(payload or {})
        if reason_code:
            payload_obj.setdefault("reason_code", reason_code)
        cursor = conn.execute(
            """INSERT INTO orchestrator_events
               (event_type, stage, scope, severity, message, recommendation, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                event_type,
                stage,
                scope,
                severity,
                message,
                recommendation,
                json.dumps(payload_obj),
            ),
        )
        conn.commit()
        return cursor.lastrowid or 0

    def get_recent_events(
        self,
        limit: int = 50,
        event_type: str | None = None,
        severity: str | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        query = "SELECT * FROM orchestrator_events WHERE 1=1"
        params: list[Any] = []
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_blocking_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent blocking/warning events."""
        return self.get_recent_events(
            limit=limit, severity="blocking"
        ) + self.get_recent_events(
            limit=limit, severity="warning"
        )

    # --- Cleanup ---

    def cleanup_old_events(self, days: int = 30) -> int:
        """Remove events older than N days. Returns count removed."""
        conn = self._connect()
        cursor = conn.execute(
            "DELETE FROM orchestrator_events WHERE created_at < datetime('now', '-' || ? || ' days')",
            (days,),
        )
        conn.commit()
        return cursor.rowcount

    # --- Locks ---

    def acquire_lock(self, lock_key: str, owner: str = "", ttl_seconds: int = 7200) -> bool:
        """
        Acquire a named lock. Returns True if acquired, False if already locked.
        Locks auto-expire after ttl_seconds.
        """
        conn = self._connect()
        # Clear expired locks first
        conn.execute(
            "DELETE FROM orchestrator_locks WHERE expires_at <= datetime('now')"
        )
        try:
            conn.execute(
                """INSERT INTO orchestrator_locks (lock_key, owner, expires_at)
                   VALUES (?, ?, datetime('now', '+' || ? || ' seconds'))""",
                (lock_key, owner or f"pid:{os.getpid()}", int(ttl_seconds)),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def release_lock(self, lock_key: str) -> None:
        """Release a named lock."""
        conn = self._connect()
        conn.execute("DELETE FROM orchestrator_locks WHERE lock_key = ?", (lock_key,))
        conn.commit()

    def is_locked(self, lock_key: str) -> bool:
        """Check if a lock is currently held (not expired)."""
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM orchestrator_locks WHERE lock_key = ? AND expires_at > datetime('now')",
            (lock_key,),
        ).fetchone()
        return row is not None

    def clear_expired_locks(self) -> int:
        """Remove expired locks. Returns count removed."""
        conn = self._connect()
        cursor = conn.execute(
            "DELETE FROM orchestrator_locks WHERE expires_at <= datetime('now')"
        )
        conn.commit()
        return cursor.rowcount

    def clear_stale_pid_locks(self) -> int:
        """
        Remove locks owned by dead pid:* owners.

        This helps recover from crashes or force-killed daemon/worker processes
        that would otherwise keep a lock around until TTL expiry.
        """
        conn = self._connect()
        rows = conn.execute(
            "SELECT lock_key, owner FROM orchestrator_locks"
        ).fetchall()
        removed = 0
        for row in rows:
            owner = str(row["owner"] or "").strip()
            if not owner.startswith("pid:"):
                continue
            try:
                pid = int(owner.split(":", 1)[1])
            except (ValueError, IndexError):
                continue
            if self._pid_is_alive(pid):
                continue
            conn.execute("DELETE FROM orchestrator_locks WHERE lock_key = ?", (row["lock_key"],))
            removed += 1
        if removed > 0:
            conn.commit()
        return removed

    # --- Pause / Resume ---

    def set_pause(self, key: str, reason: str = "") -> None:
        """Pause a stage/scope using an orchestrator_state key."""
        pause_key = self._pause_key(key)
        self.set(pause_key, reason or "1")

    def set_pause_details(
        self,
        key: str,
        reason: str = "",
        *,
        until: str = "",
        actor: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Persist a pause entry with structured metadata."""
        pause_key = self._pause_key(key)
        payload: dict[str, Any] = {
            "reason": reason or "",
            "until": until or "",
            "actor": actor or "",
            "active": True,
            "pause_key": pause_key,
        }
        if metadata:
            payload.update(metadata)
        self.set(pause_key, json.dumps(payload, ensure_ascii=False))

    def clear_pause(self, key: str) -> None:
        """Clear a pause key."""
        pause_key = self._pause_key(key)
        conn = self._connect()
        conn.execute("DELETE FROM orchestrator_state WHERE key = ?", (pause_key,))
        conn.commit()

    def is_paused(self, key: str) -> bool:
        """Return True if a pause key is currently set."""
        pause_key = self._pause_key(key)
        value = self.get(pause_key, "").strip()
        return bool(value)

    def list_pauses(self) -> list[dict[str, Any]]:
        """List all active pause keys."""
        result = []
        for row in self._state_rows_like("pause:%"):
            item = dict(row)
            payload = self._decode_control_payload(str(item.get("value", "") or ""))
            pause_key = str(item.get("key", "")).replace("pause:", "", 1)
            item["pause_key"] = pause_key
            item["scope"] = pause_key
            item["reason"] = str(payload.get("reason") or item.get("value") or "").strip()
            item["actor"] = str(payload.get("actor") or "").strip()
            item["until"] = str(payload.get("until") or "").strip()
            item["active"] = bool(payload.get("active", True))
            item["payload"] = payload
            result.append(item)
        return [item for item in result if item.get("active", True)]

    @staticmethod
    def _pause_key(key: str) -> str:
        value = str(key or "").strip()
        if not value:
            return "pause:scope:all"
        if value.startswith("pause:"):
            return value
        return f"pause:{value}"

    def quarantine_channel(
        self,
        channel_id: str,
        reason: str = "",
        *,
        actor: str = "",
        until: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        channel_id = str(channel_id or "").strip()
        if not channel_id:
            return
        key = f"quarantine:channel:{channel_id}"
        payload: dict[str, Any] = {
            "channel_id": channel_id,
            "reason": reason or "",
            "actor": actor or "",
            "until": until or "",
            "active": True,
            "quarantine_key": key,
        }
        if metadata:
            payload.update(metadata)
        self.set(key, json.dumps(payload, ensure_ascii=False))

    def unquarantine_channel(
        self,
        channel_id: str,
        *,
        actor: str = "",
        reason: str = "",
    ) -> None:
        channel_id = str(channel_id or "").strip()
        if not channel_id:
            return
        key = f"quarantine:channel:{channel_id}"
        existing = self.get(key, "")
        payload = self._decode_control_payload(existing)
        payload.update(
            {
                "channel_id": channel_id,
                "reason": reason or str(payload.get("reason") or ""),
                "actor": actor or str(payload.get("actor") or ""),
                "active": False,
                "released_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "quarantine_key": key,
            }
        )
        self.set(key, json.dumps(payload, ensure_ascii=False))

    def is_quarantined_channel(self, channel_id: str) -> bool:
        channel_id = str(channel_id or "").strip()
        if not channel_id:
            return False
        key = f"quarantine:channel:{channel_id}"
        payload = self._decode_control_payload(self.get(key, ""))
        return bool(payload.get("active", True)) and bool(self.get(key, "").strip())

    def list_quarantined_channels(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for row in self._state_rows_like("quarantine:channel:%"):
            item = dict(row)
            payload = self._decode_control_payload(str(item.get("value", "") or ""))
            channel_id = str(payload.get("channel_id") or "").strip()
            if not channel_id:
                channel_id = str(item.get("key", "")).rsplit(":", 1)[-1]
            item["channel_id"] = channel_id
            item["reason"] = str(payload.get("reason") or "").strip()
            item["actor"] = str(payload.get("actor") or "").strip()
            item["until"] = str(payload.get("until") or "").strip()
            item["active"] = bool(payload.get("active", True))
            item["payload"] = payload
            item["quarantine_key"] = str(item.get("key", "")).replace("quarantine:", "", 1)
            if item["active"]:
                result.append(item)
        return result

    # --- Retry Queue ---

    @staticmethod
    def _decode_json_payload(raw: str) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def enqueue_retry_queue_item(
        self,
        source_job: dict[str, Any],
        *,
        requested_by: str = "",
        reason: str = "",
        max_attempts: int = 1,
    ) -> dict[str, Any]:
        source_job_id = str(source_job.get("job_id") or "").strip()
        stage = str(source_job.get("stage") or "").strip()
        scope = str(source_job.get("scope") or "").strip()
        if not source_job_id or not stage:
            raise ValueError("source_job must include job_id and stage")

        payload = {
            "source_job": dict(source_job),
            "job": self._decode_json_payload(str(source_job.get("payload_json") or "")).get("job", {}),
            "requested_by": str(requested_by or "").strip(),
            "reason": str(reason or "").strip(),
            "source_job_id": source_job_id,
            "retry_queue_status": "pending",
        }
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO orchestrator_retry_queue (
                source_job_id, stage, scope, reason, requested_by, requested_at,
                status, attempts, max_attempts, claimed_by, claimed_at, payload_json, error_text, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), 'pending', 1, ?, '', NULL, ?, '', datetime('now'), NULL)
            ON CONFLICT(source_job_id) DO UPDATE SET
                stage = excluded.stage,
                scope = excluded.scope,
                reason = excluded.reason,
                requested_by = excluded.requested_by,
                requested_at = datetime('now'),
                status = 'pending',
                attempts = COALESCE(attempts, 0) + 1,
                max_attempts = excluded.max_attempts,
                claimed_by = '',
                claimed_at = NULL,
                payload_json = excluded.payload_json,
                error_text = '',
                updated_at = datetime('now'),
                finished_at = NULL
            """,
            (
                source_job_id,
                stage,
                scope,
                reason,
                requested_by,
                int(max_attempts or 1),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()
        return self.get_retry_queue_item(source_job_id) or {
            "source_job_id": source_job_id,
            "stage": stage,
            "scope": scope,
            "status": "pending",
        }

    def get_retry_queue_item(self, source_job_id: str) -> dict[str, Any] | None:
        source_job_id = str(source_job_id or "").strip()
        if not source_job_id:
            return None
        conn = self._connect()
        row = conn.execute(
            """
            SELECT *
            FROM orchestrator_retry_queue
            WHERE source_job_id = ?
            """,
            (source_job_id,),
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["payload"] = self._decode_json_payload(str(item.get("payload_json") or ""))
        return item

    def claim_retry_queue_item(
        self,
        source_job_id: str,
        *,
        claimed_by: str = "",
    ) -> bool:
        source_job_id = str(source_job_id or "").strip()
        if not source_job_id:
            return False
        conn = self._connect()
        cursor = conn.execute(
            """
            UPDATE orchestrator_retry_queue
            SET status = 'claimed',
                claimed_by = ?,
                claimed_at = datetime('now'),
                updated_at = datetime('now'),
                error_text = ''
            WHERE source_job_id = ? AND status = 'pending'
            """,
            (str(claimed_by or "").strip(), source_job_id),
        )
        conn.commit()
        return cursor.rowcount > 0

    def release_retry_queue_item(
        self,
        source_job_id: str,
        *,
        status: str = "pending",
        error_text: str = "",
    ) -> None:
        source_job_id = str(source_job_id or "").strip()
        if not source_job_id:
            return
        conn = self._connect()
        conn.execute(
            """
            UPDATE orchestrator_retry_queue
            SET status = ?,
                claimed_by = '',
                claimed_at = NULL,
                error_text = ?,
                updated_at = datetime('now'),
                finished_at = CASE WHEN ? = 'pending' THEN NULL ELSE finished_at END
            WHERE source_job_id = ?
            """,
            (str(status or "pending"), str(error_text or ""), str(status or "pending"), source_job_id),
        )
        conn.commit()

    def list_retry_queue(
        self,
        *,
        status: str | None = None,
        stage: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        query = "SELECT * FROM orchestrator_retry_queue WHERE 1=1"
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if stage:
            query += " AND stage = ?"
            params.append(stage)
        query += " ORDER BY requested_at ASC, id ASC LIMIT ?"
        params.append(max(1, int(limit or 50)))
        rows = conn.execute(query, params).fetchall()
        items = [dict(r) for r in rows]
        for item in items:
            item["payload"] = self._decode_json_payload(str(item.get("payload_json") or ""))
        return items

    def count_retry_queue(self, status: str | None = None) -> int:
        conn = self._connect()
        query = "SELECT COUNT(*) AS cnt FROM orchestrator_retry_queue WHERE 1=1"
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        row = conn.execute(query, params).fetchone()
        return int(row["cnt"] if row else 0)

    def mark_retry_queue_running(self, source_job_id: str, *, launched_job_id: str = "") -> None:
        source_job_id = str(source_job_id or "").strip()
        if not source_job_id:
            return
        conn = self._connect()
        item = self.get_retry_queue_item(source_job_id) or {}
        payload = dict(item.get("payload") or {})
        if launched_job_id:
            payload["launched_job_id"] = launched_job_id
            payload["launched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE orchestrator_retry_queue
            SET status = 'running',
                payload_json = ?,
                updated_at = datetime('now')
            WHERE source_job_id = ?
            """,
            (json.dumps(payload, ensure_ascii=False), source_job_id),
        )
        conn.commit()

    def mark_retry_queue_finished(
        self,
        source_job_id: str,
        *,
        status: str,
        error_text: str = "",
    ) -> None:
        source_job_id = str(source_job_id or "").strip()
        if not source_job_id:
            return
        conn = self._connect()
        conn.execute(
            """
            UPDATE orchestrator_retry_queue
            SET status = ?,
                error_text = ?,
                claimed_by = '',
                claimed_at = NULL,
                updated_at = datetime('now'),
                finished_at = datetime('now')
            WHERE source_job_id = ?
            """,
            (status, error_text, source_job_id),
        )
        conn.commit()

    # --- Inventory snapshots ---

    def record_inventory_snapshot(self, payload: dict[str, Any]) -> int:
        """Persist a work inventory snapshot."""
        conn = self._connect()
        cursor = conn.execute(
            """INSERT INTO orchestrator_inventory_snapshots (payload_json)
               VALUES (?)""",
            (json.dumps(payload),),
        )
        conn.commit()
        return cursor.lastrowid or 0

    def get_latest_inventory_snapshot(self) -> dict[str, Any] | None:
        """Return the latest inventory snapshot payload, if any."""
        conn = self._connect()
        row = conn.execute(
            """SELECT payload_json, created_at
               FROM orchestrator_inventory_snapshots
               ORDER BY id DESC
               LIMIT 1""",
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            payload.setdefault("created_at", row["created_at"])
        return payload

    # --- Active Jobs ---

    def register_active_job(
        self,
        job_id: str,
        stage: str,
        scope: str,
        group_name: str,
        pid: int,
        command: str,
        run_dir: str,
        log_path: str,
        *,
        slot_index: int = 0,
        lock_key: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO orchestrator_active_jobs (
                job_id, stage, scope, group_name, slot_index, lock_key,
                pid, command, run_dir, log_path, status, payload_json,
                started_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, datetime('now'), datetime('now'))
            """,
            (
                job_id,
                stage,
                scope,
                group_name,
                int(slot_index or 0),
                lock_key,
                int(pid),
                command,
                run_dir,
                log_path,
                json.dumps(payload or {}),
            ),
        )
        conn.commit()

    def mark_active_job_finished(
        self,
        job_id: str,
        returncode: int,
        status: str,
        error_text: str = "",
    ) -> None:
        conn = self._connect()
        conn.execute(
            """
            UPDATE orchestrator_active_jobs
            SET status = ?,
                returncode = ?,
                error_text = ?,
                updated_at = datetime('now'),
                finished_at = datetime('now')
            WHERE job_id = ?
            """,
            (status, int(returncode), error_text, job_id),
        )
        conn.commit()

    def list_running_jobs(self) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT *
            FROM orchestrator_active_jobs
            WHERE status = 'running'
            ORDER BY started_at ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT *
            FROM orchestrator_active_jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def list_jobs(
        self,
        *,
        status: str | None = None,
        stage: str | None = None,
        group_name: str | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        query = "SELECT * FROM orchestrator_active_jobs WHERE 1=1"
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if stage:
            query += " AND stage = ?"
            params.append(stage)
        if group_name:
            query += " AND group_name = ?"
            params.append(group_name)
        query += " ORDER BY started_at ASC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def count_running_total(self) -> int:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orchestrator_active_jobs
            WHERE status = 'running'
            """
        ).fetchone()
        return int(row["cnt"] if row else 0)

    def count_running_by_stage(self, stage: str) -> int:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orchestrator_active_jobs
            WHERE status = 'running' AND stage = ?
            """,
            (stage,),
        ).fetchone()
        return int(row["cnt"] if row else 0)

    def count_running_by_group(self, group_name: str) -> int:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orchestrator_active_jobs
            WHERE status = 'running' AND group_name = ?
            """,
            (group_name,),
        ).fetchone()
        return int(row["cnt"] if row else 0)

    # --- Adaptive batch state ---

    def get_stage_batch_limit(self, stage: str, default: int) -> int:
        """Get the current adaptive batch limit for a stage."""
        value = self.get_int(f"adaptive_batch_limit:{stage}", default)
        return value if value > 0 else default

    def set_stage_batch_limit(self, stage: str, value: int) -> None:
        """Set the adaptive batch limit for a stage."""
        self.set_int(f"adaptive_batch_limit:{stage}", int(value))

    def record_stage_batch_outcome(
        self,
        stage: str,
        *,
        success: bool,
        blocked: bool,
        min_batch: int,
        max_batch: int,
        step: int,
        increase_after_success_batches: int,
        decrease_on_block: bool = True,
    ) -> dict[str, Any]:
        """Adjust adaptive batch state for a stage and return the new values."""
        stage = str(stage or "").strip()
        if not stage:
            return {"stage": "", "batch_limit": 0, "success_streak": 0, "blocked_streak": 0}

        min_batch = max(int(min_batch), 1)
        max_batch = max(int(max_batch), min_batch)
        step = max(int(step), 1)
        increase_after_success_batches = max(int(increase_after_success_batches), 1)

        limit_key = f"adaptive_batch_limit:{stage}"
        success_key = f"adaptive_success_streak:{stage}"
        blocked_key = f"adaptive_blocked_streak:{stage}"

        current_limit = self.get_stage_batch_limit(stage, min_batch)
        success_streak = self.get_int(success_key, 0)
        blocked_streak = self.get_int(blocked_key, 0)

        if blocked:
            success_streak = 0
            blocked_streak += 1
            if decrease_on_block:
                current_limit = max(min_batch, min(current_limit, max_batch) // 2)
                current_limit = max(current_limit, min_batch)
        elif success:
            blocked_streak = 0
            success_streak += 1
            if success_streak >= increase_after_success_batches:
                current_limit = min(max_batch, max(current_limit, min_batch) + step)
                success_streak = 0
        else:
            # Non-blocking failure: keep the current batch size but reset streaks gently.
            success_streak = 0
            blocked_streak = 0

        self.set_int(limit_key, int(current_limit))
        self.set_int(success_key, int(success_streak))
        self.set_int(blocked_key, int(blocked_streak))
        return {
            "stage": stage,
            "batch_limit": int(current_limit),
            "success_streak": int(success_streak),
            "blocked_streak": int(blocked_streak),
        }

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False

    def list_active_locks(self) -> list[dict[str, Any]]:
        """List all currently active (non-expired) locks."""
        conn = self._connect()
        rows = conn.execute(
            """SELECT lock_key, owner, expires_at, created_at
               FROM orchestrator_locks
               WHERE expires_at > datetime('now')
               ORDER BY created_at ASC""",
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Safety flags (Stage 15) ---

    _SAFETY_ES_KEY = "safety:emergency_stop"
    _SAFETY_READONLY_KEY = "safety:readonly"

    def set_emergency_stop(self, reason: str = "", actor: str = "cli") -> None:
        """Activate emergency stop with a reason."""
        payload = json.dumps({
            "active": True,
            "reason": str(reason or "").strip(),
            "actor": str(actor or "").strip(),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        self.set(self._SAFETY_ES_KEY, payload)
        self.add_event(
            event_type="safety.emergency_stop.enabled",
            message=f"Emergency stop activated by {actor}: {reason}" if reason else f"Emergency stop activated by {actor}",
            stage="control",
            severity="blocking",
            payload={"actor": actor, "reason": reason},
        )

    def clear_emergency_stop(self, reason: str = "", actor: str = "cli") -> None:
        """Clear emergency stop."""
        payload = json.dumps({
            "active": False,
            "reason": str(reason or "").strip(),
            "actor": str(actor or "").strip(),
            "cleared_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        self.set(self._SAFETY_ES_KEY, payload)
        self.add_event(
            event_type="safety.emergency_stop.cleared",
            message=f"Emergency stop cleared by {actor}: {reason}" if reason else f"Emergency stop cleared by {actor}",
            stage="control",
            severity="info",
            payload={"actor": actor, "reason": reason},
        )

    def is_emergency_stop_active(self) -> bool:
        """Return True if emergency stop is active."""
        raw = self.get(self._SAFETY_ES_KEY, "").strip()
        if not raw:
            return False
        try:
            payload = json.loads(raw)
        except Exception:
            return False
        return bool(payload.get("active", False))

    def get_safety_status(self) -> dict[str, Any]:
        """Return full safety status dict."""
        es_raw = self.get(self._SAFETY_ES_KEY, "").strip()
        es_payload: dict[str, Any] = {}
        if es_raw:
            try:
                es_payload = json.loads(es_raw)
            except Exception:
                es_payload = {"raw": es_raw}
        ro_raw = self.get(self._SAFETY_READONLY_KEY, "").strip()
        ro_payload: dict[str, Any] = {}
        if ro_raw:
            try:
                ro_payload = json.loads(ro_raw)
            except Exception:
                ro_payload = {"raw": ro_raw}
        return {
            "emergency_stop": {
                "active": bool(es_payload.get("active", False)),
                "reason": str(es_payload.get("reason") or "").strip(),
                "actor": str(es_payload.get("actor") or "").strip(),
                "updated_at": str(es_payload.get("updated_at") or "").strip(),
                "cleared_at": str(es_payload.get("cleared_at") or "").strip(),
            },
            "readonly": {
                "active": bool(ro_payload.get("active", False)),
                "reason": str(ro_payload.get("reason") or "").strip(),
            },
        }

    def list_safety_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return recent safety-related events."""
        return self.get_recent_events(
            limit=limit,
            event_type="safety.emergency_stop.enabled,safety.emergency_stop.cleared,safety.launch_blocked,safety.drain_blocked",
        )

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
