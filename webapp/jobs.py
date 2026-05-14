from __future__ import annotations

import os
import secrets
import sqlite3
import subprocess
import sys
import signal
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from . import db


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Stored like "YYYY-MM-DDTHH:MM:SSZ"
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _pid_state(pid: int) -> Optional[str]:
    """
    Return process state from /proc/<pid>/stat (Linux), e.g. 'R', 'S', 'Z', etc.
    None if not available.
    """
    try:
        with open(f"/proc/{int(pid)}/stat", "r", encoding="utf-8", errors="ignore") as f:
            parts = f.read().split()
        if len(parts) >= 3:
            return parts[2]
        return None
    except Exception:
        return None


def _pid_alive(pid: int) -> bool:
    st = _pid_state(pid)
    if st == "Z":
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


@dataclass(frozen=True)
class JobRow:
    id: str
    kind: str
    title: str
    channel_id: Optional[int]
    status: str
    pid: Optional[int]
    returncode: Optional[int]
    log_path: Optional[str]
    params_json: Optional[str]
    created_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    error_msg: Optional[str]


def _job_id() -> str:
    return secrets.token_hex(12)


def _logs_dir(base_dir: Path) -> Path:
    p = base_dir / ".webapp_jobs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_jobs(con: sqlite3.Connection, limit: int = 50) -> List[JobRow]:
    rows = con.execute(
        """
        SELECT * FROM jobs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [JobRow(**dict(r)) for r in rows]


def get_job(con: sqlite3.Connection, job_id: str) -> Optional[JobRow]:
    r = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not r:
        return None
    return JobRow(**dict(r))


def channel_running_job(con: sqlite3.Connection, channel_id: int) -> Optional[str]:
    r = con.execute(
        """
        SELECT id FROM jobs
        WHERE channel_id=? AND status IN ('running','stopping')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    return str(r[0]) if r else None


def _active_running_jobs(con: sqlite3.Connection) -> List[JobRow]:
    rows = con.execute(
        """
        SELECT * FROM jobs
        WHERE status IN ('running','stopping')
        ORDER BY created_at ASC
        """
    ).fetchall()
    return [JobRow(**dict(r)) for r in rows]


def _has_running_job(con: sqlite3.Connection) -> Optional[JobRow]:
    running = _active_running_jobs(con)
    for job in running:
        if job.pid and _pid_alive(int(job.pid)):
            return job
        # Stale runner: mark finished so it doesn't block the queue forever.
        status2 = "stopped" if job.status == "stopping" else "error"
        msg2 = "stopped by user" if status2 == "stopped" else "runner process exited"
        con.execute(
            "UPDATE jobs SET status=?, finished_at=?, returncode=COALESCE(returncode, 1), error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (status2, now_iso(), msg2, job.id),
        )
    return None


def _spawn_job(
    *,
    con: sqlite3.Connection,
    job_id: str,
    log_path: Path,
    params: dict,
) -> str:
    db_path = Path(params["db_path"])
    out_root = Path(params["out_root"])
    script_path = Path(params["script_path"])
    channel_url = str(params["channel_url"])
    stop_at_known = bool(params.get("stop_at_known"))
    stop_at_known_after = int(params.get("stop_at_known_after", 25))
    stop_at_known_min_scan = int(params.get("stop_at_known_min_scan", 200))
    pending_only = bool(params.get("pending_only"))
    video_id = (params.get("video_id") or "").strip()

    cmd = [
        sys.executable,
        "-m",
        "webapp.job_runner",
        "--job-id",
        job_id,
        "--db",
        str(db_path),
        "--out",
        str(out_root),
        "--script",
        str(script_path),
        "--channel-url",
        channel_url,
        "--log-path",
        str(log_path),
        "--update",
    ]
    if stop_at_known:
        cmd += [
            "--stop-at-known",
            "--stop-at-known-after",
            str(stop_at_known_after),
            "--stop-at-known-min-scan",
            str(stop_at_known_min_scan),
        ]
    if pending_only:
        cmd.append("--pending-only")
    if video_id:
        cmd += ["--video-id", video_id]

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[{now_iso()}] [spawn] $ {' '.join(cmd)}\n")
        logf.flush()
        p = subprocess.Popen(
            cmd,
            cwd=str(script_path.resolve().parent),
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )

    con.execute(
        "UPDATE jobs SET status='running', pid=?, started_at=? WHERE id=?",
        (int(p.pid), now_iso(), job_id),
    )
    try:
        con.commit()
    except Exception:
        pass
    return job_id


def _start_next_queued(con: sqlite3.Connection) -> Optional[str]:
    if _has_running_job(con):
        return None
    r = con.execute(
        """
        SELECT * FROM jobs
        WHERE status='queued' AND (pid IS NULL OR pid=0)
        ORDER BY created_at ASC
        LIMIT 1
        """
    ).fetchone()
    if not r:
        return None
    job = JobRow(**dict(r))
    if not job.log_path or not job.params_json:
        con.execute(
            "UPDATE jobs SET status='error', finished_at=?, returncode=1, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), "missing job params/log_path", job.id),
        )
        try:
            con.commit()
        except Exception:
            pass
        return None
    try:
        params = json.loads(job.params_json)
    except Exception:
        con.execute(
            "UPDATE jobs SET status='error', finished_at=?, returncode=1, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), "invalid job params", job.id),
        )
        try:
            con.commit()
        except Exception:
            pass
        return None
    return _spawn_job(con=con, job_id=job.id, log_path=Path(job.log_path), params=params)


def start_next_queued_job(db_path: Path) -> Optional[str]:
    with db.session(db_path) as con:
        with con:
            return _start_next_queued(con)


def start_channel_update(
    *,
    con: sqlite3.Connection,
    channel_id: int,
    channel_url: str,
    db_path: Path,
    out_root: Path,
    script_path: Path,
    stop_at_known: bool,
    stop_at_known_after: int = 25,
    stop_at_known_min_scan: int = 200,
    pending_only: bool = False,
    video_id: Optional[str] = None,
    logs_base: Path,
) -> str:
    existing = channel_running_job(con, channel_id)
    if existing:
        job = get_job(con, existing)
        if job:
            stale = False
            if job.pid and not _pid_alive(int(job.pid)):
                stale = True

            # Clear stale running/queued job so a new one can start.
            if stale:
                status2 = "stopped" if job.status == "stopping" else "error"
                msg2 = "stopped by user" if status2 == "stopped" else "runner process exited"
                con.execute(
                    "UPDATE jobs SET status=?, finished_at=?, returncode=COALESCE(returncode, 1), error_msg=COALESCE(error_msg, ?) WHERE id=?",
                    (status2, now_iso(), msg2, job.id),
                )

    job_id = _job_id()
    log_path = (_logs_dir(logs_base.resolve()) / f"{job_id}.log").resolve()
    params = {
        "channel_id": int(channel_id),
        "channel_url": channel_url,
        "db_path": str(db_path.resolve()),
        "out_root": str(out_root.resolve()),
        "script_path": str(script_path.resolve()),
        "stop_at_known": bool(stop_at_known),
        "stop_at_known_after": int(stop_at_known_after),
        "stop_at_known_min_scan": int(stop_at_known_min_scan),
        "pending_only": bool(pending_only),
        "video_id": (video_id or "").strip(),
    }
    params_json = json.dumps(params, ensure_ascii=False)

    con.execute(
        """
        INSERT INTO jobs(
          id, kind, title, channel_id, status, pid, returncode, log_path,
          params_json, created_at, started_at, finished_at, error_msg
        )
        VALUES(?, 'update_channel', ?, ?, 'queued', NULL, NULL, ?, ?, ?, NULL, NULL, NULL)
        """,
        (job_id, f"Update channel {channel_id}", channel_id, str(log_path), params_json, now_iso()),
    )
    # Important: commit before spawning the subprocess.
    # The runner reads its log_path from the DB using a separate SQLite connection;
    # if the row isn't committed yet, it can exit immediately with "job not found".
    try:
        con.commit()
    except Exception:
        pass

    running = _has_running_job(con)
    if running:
        with log_path.open("a", encoding="utf-8") as logf:
            logf.write(
                f"[{now_iso()}] [queue] waiting for active job {running.id} to finish\n"
            )
        try:
            con.commit()
        except Exception:
            pass
        return job_id

    return _spawn_job(con=con, job_id=job_id, log_path=log_path, params=params)


def request_stop(con: sqlite3.Connection, job_id: str, *, reason: str = "stop requested by user") -> tuple[bool, str]:
    job = get_job(con, job_id)
    if not job:
        return False, "job not found"

    if job.status in ("done", "error", "stopped"):
        return True, f"job already finished ({job.status})"

    if job.status == "queued" and not job.pid:
        con.execute(
            "UPDATE jobs SET status='stopped', finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), reason, job_id),
        )
        return True, "job stopped"

    if not job.pid:
        # Unexpected, but avoid crashing.
        con.execute(
            "UPDATE jobs SET status='stopped', finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), reason, job_id),
        )
        return True, "job stopped (missing pid)"

    # If PID already exited (or is zombie), mark as stopped immediately.
    if not _pid_alive(int(job.pid)):
        con.execute(
            "UPDATE jobs SET status='stopped', finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), reason, job_id),
        )
        return True, "job already exited"

    # Mark stopping first so UI can reflect it.
    con.execute(
        "UPDATE jobs SET status='stopping', error_msg=COALESCE(error_msg, ?) WHERE id=?",
        (reason, job_id),
    )

    try:
        pgid = os.getpgid(int(job.pid))
        os.killpg(pgid, signal.SIGTERM)
        # Best effort: if it exits quickly, mark it as stopped to avoid leaving "stopping" forever.
        for _ in range(10):
            try:
                if not _pid_alive(int(job.pid)):
                    raise ProcessLookupError
                os.kill(int(job.pid), 0)
                time.sleep(0.2)
            except ProcessLookupError:
                con.execute(
                    "UPDATE jobs SET status='stopped', finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
                    (now_iso(), reason, job_id),
                )
                break
        return True, "stop signal sent (SIGTERM)"
    except ProcessLookupError:
        con.execute(
            "UPDATE jobs SET status='stopped', finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
            (now_iso(), reason, job_id),
        )
        return True, "job already exited"
    except PermissionError:
        return False, "no permission to stop job process"


def _tail_lines(path: Path, max_lines: int = 400) -> List[str]:
    if max_lines <= 0:
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            block = 8192
            data = b""
            pos = end
            while pos > 0 and data.count(b"\n") <= max_lines:
                pos = max(0, pos - block)
                f.seek(pos)
                data = f.read(end - pos) + data
                end = pos
            lines = data.splitlines()[-max_lines:]
            return [l.decode("utf-8", errors="replace") for l in lines]
    except Exception:
        return []


def job_log_tail(job: JobRow, max_lines: int = 400) -> List[str]:
    if not job.log_path:
        return []
    return _tail_lines(Path(job.log_path), max_lines=max_lines)
