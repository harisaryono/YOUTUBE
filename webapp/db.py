import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Tuple


_VALID_JOURNAL_MODES = {"DELETE", "WAL", "TRUNCATE", "PERSIST", "MEMORY", "OFF"}
_SCHEMA_READY = False
_LAST_SCHEMA_CHECK_TS = 0.0


def _preferred_journal_mode() -> str:
    raw = (os.getenv("DB_JOURNAL_MODE") or "DELETE").strip().upper()
    if raw in _VALID_JOURNAL_MODES:
        return raw
    return "DELETE"


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or ("1" if default else "0")).strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _busy_timeout_ms() -> int:
    raw = (os.getenv("DB_BUSY_TIMEOUT_MS") or "5000").strip()
    if raw.isdigit():
        v = int(raw)
        if v > 0:
            return v
    return 5000


def _schema_check_interval_s() -> int:
    raw = (os.getenv("DB_SCHEMA_CHECK_INTERVAL_S") or "300").strip()
    if raw.isdigit():
        v = int(raw)
        if v >= 0:
            return v
    return 300


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    if _env_flag("DB_SET_JOURNAL_ON_CONNECT", False):
        preferred = _preferred_journal_mode()
        try:
            con.execute(f"PRAGMA journal_mode={preferred};")
        except sqlite3.OperationalError:
            # Some hosting/filesystem setups fail on locking protocol.
            try:
                con.execute("PRAGMA journal_mode=DELETE;")
            except sqlite3.OperationalError:
                pass
    con.execute(f"PRAGMA busy_timeout={_busy_timeout_ms()};")
    return con


def integrity_check(db_path: Path, *, quick: bool = False) -> Tuple[bool, str]:
    con: Optional[sqlite3.Connection] = None
    pragma = "quick_check" if quick else "integrity_check"
    try:
        con = sqlite3.connect(str(db_path), timeout=10, check_same_thread=False)
        rows = con.execute(f"PRAGMA {pragma};").fetchall()
        msgs = [str(r[0]) for r in rows if len(r) > 0]
        if msgs and all(m.lower() == "ok" for m in msgs):
            return True, "ok"
        if not msgs:
            return False, f"{pragma} returned empty result"
        return False, "; ".join(msgs[:5])
    except Exception as ex:
        return False, str(ex)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [r["name"] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def _ensure_column(con: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    if _has_column(con, table, column):
        return
    try:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl};")
    except sqlite3.OperationalError:
        # Table might not exist yet, or another process added it concurrently.
        pass


def _needs_video_categories_fk_fix(con: sqlite3.Connection) -> bool:
    try:
        rows = con.execute("PRAGMA foreign_key_list(video_categories)").fetchall()
    except sqlite3.OperationalError:
        return False
    if not rows:
        return False
    tables = {str(r["table"]) for r in rows if "table" in r.keys()}
    return "videos" not in tables and "videos_bad" in tables


def _fix_video_categories_fk(con: sqlite3.Connection) -> None:
    if not _needs_video_categories_fk_fix(con):
        return
    con.execute("PRAGMA foreign_keys=OFF;")
    try:
        con.execute("ALTER TABLE video_categories RENAME TO video_categories_old;")
        con.execute(
            """
            CREATE TABLE video_categories (
              video_pk INTEGER NOT NULL,
              category_id INTEGER NOT NULL,
              PRIMARY KEY(video_pk, category_id),
              FOREIGN KEY(video_pk) REFERENCES videos(id) ON DELETE CASCADE,
              FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
            );
            """
        )
        con.execute(
            """
            INSERT OR IGNORE INTO video_categories(video_pk, category_id)
            SELECT video_pk, category_id
            FROM video_categories_old
            """
        )
        con.execute("DROP TABLE video_categories_old;")
        con.execute("CREATE INDEX IF NOT EXISTS idx_vc_category ON video_categories(category_id);")
    finally:
        con.execute("PRAGMA foreign_keys=ON;")


def ensure_schema(con: sqlite3.Connection) -> None:
    # Resume is stored as a Markdown file on disk; DB only keeps `videos.link_resume`.

    # Optional fields on existing DBs (created by scrap_all_channel.py).
    _ensure_column(con, "videos", "read_at", "TEXT")
    _ensure_column(con, "channels", "category_id", "INTEGER")
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_channels_category ON channels(category_id);")
    except sqlite3.OperationalError:
        pass

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          color TEXT,
          created_at TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS video_categories (
          video_pk INTEGER NOT NULL,
          category_id INTEGER NOT NULL,
          PRIMARY KEY(video_pk, category_id),
          FOREIGN KEY(video_pk) REFERENCES videos(id) ON DELETE CASCADE,
          FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_vc_category ON video_categories(category_id);")
    _fix_video_categories_fk(con)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          title TEXT NOT NULL,
          channel_id INTEGER,
          status TEXT NOT NULL,            -- queued|running|stopping|stopped|done|error
          pid INTEGER,
          returncode INTEGER,
          log_path TEXT,
          params_json TEXT,
          created_at TEXT,
          started_at TEXT,
          finished_at TEXT,
          error_msg TEXT,
          FOREIGN KEY(channel_id) REFERENCES channels(id)
        );
        """
    )
    _ensure_column(con, "jobs", "params_json", "TEXT")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);")


def wal_checkpoint_truncate(con: sqlite3.Connection) -> None:
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchall()
    except sqlite3.OperationalError:
        pass


def cleanup_wal_shm_files(db_path: Path) -> None:
    for p in (
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def maintenance_blocker_reason(db_path: Path) -> Optional[str]:
    con: Optional[sqlite3.Connection] = None
    try:
        con = sqlite3.connect(str(db_path), timeout=0.2, check_same_thread=False)
        con.execute("PRAGMA busy_timeout=200;")
        con.execute("BEGIN IMMEDIATE;")
        con.execute("ROLLBACK;")
        return None
    except sqlite3.OperationalError as ex:
        msg = str(ex).lower()
        if "locked" in msg or "busy" in msg:
            return "Database sedang sibuk (ada proses lain yang menulis)."
        return f"Database maintenance tidak tersedia: {ex}"
    except Exception as ex:
        return f"Database maintenance tidak tersedia: {ex}"
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


@contextmanager
def session(db_path: Path) -> Iterator[sqlite3.Connection]:
    global _SCHEMA_READY, _LAST_SCHEMA_CHECK_TS
    con = connect(db_path)
    try:
        now = time.time()
        interval_s = _schema_check_interval_s()
        need_schema_check = (not _SCHEMA_READY) or (
            interval_s == 0 or (now - _LAST_SCHEMA_CHECK_TS) >= interval_s
        )
        if need_schema_check:
            with con:
                ensure_schema(con)
            _SCHEMA_READY = True
            _LAST_SCHEMA_CHECK_TS = now
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass
