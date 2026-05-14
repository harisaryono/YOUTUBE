"""
Orchestrator State Store
SQLite-based persistent state for cooldowns, events, and key-value store.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
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
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
        return self._conn

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
        """)
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

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
