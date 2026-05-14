from __future__ import annotations

import os
import re
import signal
import subprocess
import json
import sys
import time
import markdown
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

import scrap_all_channel
import shard_storage

from . import db, jobs


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def nvidia_chat_completion(
    messages: List[Dict[str, str]],
    *,
    model: str = "moonshotai/kimi-k2.5",
    max_tokens: int = 1024,
    temperature: float = 0.2,
    top_p: float = 1.0,
    stream: bool = False,
    timeout_s: int = 300,
) -> Any:
    api_key = (os.getenv("NVIDIA_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("NVIDIA_API_KEY belum di-set di environment (.env).")

    invoke_url = "https://integrate.api.nvidia.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/event-stream" if stream else "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "stream": stream,
        "chat_template_kwargs": {"thinking": False},
    }

    response = requests.post(invoke_url, headers=headers, json=payload, timeout=timeout_s)
    response.raise_for_status()
    if stream:
        return response.iter_lines()
    return response.json()


def video_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET", "dev")

    # Avoid zombie job runner processes when we intentionally don't wait() on them.
    try:
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)  # type: ignore[attr-defined]
    except Exception:
        pass

    # Resolve important paths relative to the project root (not the process CWD).
    # This avoids "file not found" when the app is started via WSGI/Passenger with a different working directory.
    project_root = Path(__file__).resolve().parents[1]

    def _path_from_env(env_key: str, default_rel: str) -> Path:
        raw = (os.getenv(env_key) or "").strip()
        p = Path(raw) if raw else Path(default_rel)
        if not p.is_absolute():
            p = project_root / p
        return p.resolve()

    app.config["DB_PATH"] = _path_from_env("CHANNELS_DB", "channels.db")
    app.config["OUT_ROOT"] = _path_from_env("OUT_ROOT", "out")
    app.config["SCRIPT_PATH"] = _path_from_env("SCRAPER_SCRIPT", "scrap_all_channel.py")
    app.config["LOGS_BASE"] = _path_from_env("WEBAPP_LOGS_BASE", ".")
    app.config["RESUME_AGENTS_SCRIPT"] = _path_from_env("RESUME_AGENTS_SCRIPT", "run_resume_agents.sh")
    app.config["COMPACTOR_SCRIPT"] = _path_from_env("COMPACTOR_SCRIPT", "compact_out_to_shards.py")

    def _env_flag(name: str, default: bool) -> bool:
        raw = (os.getenv(name) or ("1" if default else "0")).strip().lower()
        return raw not in ("0", "false", "no", "off", "")

    resume_cache_limit_raw = (os.getenv("WEBAPP_RESUME_READ_CACHE_SIZE") or "256").strip()
    resume_cache_ttl_raw = (os.getenv("WEBAPP_RESUME_READ_CACHE_TTL_S") or "900").strip()
    try:
        resume_cache_limit = max(16, min(int(resume_cache_limit_raw), 2000))
    except Exception:
        resume_cache_limit = 256
    try:
        resume_cache_ttl_s = max(10.0, float(resume_cache_ttl_raw))
    except Exception:
        resume_cache_ttl_s = 900.0
    _resume_render_cache: Dict[str, Tuple[float, str, bool]] = {}
    _resume_render_cache_order: List[str] = []

    def _resume_cache_get(key: str) -> Optional[Tuple[str, bool]]:
        row = _resume_render_cache.get(key)
        if not row:
            return None
        ts, html, is_empty = row
        if (time.time() - ts) > resume_cache_ttl_s:
            _resume_render_cache.pop(key, None)
            return None
        return html, is_empty

    def _resume_cache_put(key: str, html: str, is_empty: bool) -> None:
        _resume_render_cache[key] = (time.time(), html, is_empty)
        _resume_render_cache_order.append(key)
        if len(_resume_render_cache_order) > (resume_cache_limit * 3):
            # Compact stale keys and keep insertion order roughly bounded.
            seen: set[str] = set()
            compact: List[str] = []
            for k in reversed(_resume_render_cache_order):
                if k in seen:
                    continue
                seen.add(k)
                if k in _resume_render_cache:
                    compact.append(k)
                if len(compact) >= resume_cache_limit:
                    break
            compact.reverse()
            _resume_render_cache_order[:] = compact
            keep = set(compact)
            for k in list(_resume_render_cache.keys()):
                if k not in keep:
                    _resume_render_cache.pop(k, None)

    def _parse_seq_from_name_startup(name: Optional[str]) -> Optional[int]:
        if not name:
            return None
        base = Path(str(name)).name
        m = re.match(r"^(\d+)_", base)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _default_resume_link_startup(
        video_id: str, seq_num: Optional[int], link_file: Optional[str]
    ) -> Optional[str]:
        seq: Optional[int] = None
        try:
            if seq_num is not None:
                seq = int(seq_num)
        except Exception:
            seq = None
        if not seq:
            seq = _parse_seq_from_name_startup(link_file)
        if not seq:
            return None
        return str(Path("resume") / f"{int(seq):04d}_{video_id}.md")

    def _channel_base(channel_slug: str) -> Path:
        return (app.config["OUT_ROOT"] / str(channel_slug)).resolve()

    def _link_exists(channel_slug: str, rel_path: Optional[str]) -> bool:
        return shard_storage.link_exists(_channel_base(channel_slug), rel_path)

    def _link_read_text(channel_slug: str, rel_path: Optional[str]) -> Optional[str]:
        raw = _link_read_bytes(channel_slug, rel_path)
        if raw is None:
            return None
        try:
            return raw.decode("utf-8", errors="ignore")
        except Exception:
            return None

    def _link_read_bytes(channel_slug: str, rel_path: Optional[str]) -> Optional[bytes]:
        base = _channel_base(channel_slug)
        tries = 3
        for i in range(tries):
            blob = shard_storage.read_link_bytes(base, rel_path)
            if blob is not None:
                return blob
            if not shard_storage.link_exists(base, rel_path):
                return None
            # When index/shard is being touched concurrently, a short retry helps.
            if i + 1 < tries:
                time.sleep(0.08 * (i + 1))
        return None

    def _link_mtime(channel_slug: str, rel_path: Optional[str]) -> Optional[float]:
        return shard_storage.link_mtime(_channel_base(channel_slug), rel_path)

    def _link_source(channel_slug: str, rel_path: Optional[str]) -> str:
        return shard_storage.link_source_label(_channel_base(channel_slug), rel_path)

    def _legacy_manual_transcript_retry_available(video: Any) -> bool:
        if not video:
            return False
        if bool(video.get("is_short")):
            return False
        if bool(video.get("is_member_only")):
            return False
        transcript_downloaded = int(video.get("transcript_downloaded") or 0)
        transcript_language = str(video.get("transcript_language") or "").strip().lower()
        return transcript_downloaded != 1 and transcript_language != "blocked"

    def _startup_prepare_db() -> None:
        db_path: Path = app.config["DB_PATH"]
        startup_profile = (os.getenv("WEBAPP_STARTUP_PROFILE") or "fast").strip().lower()
        profile_safe = startup_profile in {"safe", "full"}
        run_integrity = _env_flag("WEBAPP_STARTUP_INTEGRITY", profile_safe)
        run_cleanup = _env_flag("WEBAPP_STARTUP_CLEANUP_WAL", profile_safe)
        run_reconcile = _env_flag("WEBAPP_STARTUP_RECONCILE_FILES", profile_safe)
        integrity_quick = _env_flag("WEBAPP_STARTUP_INTEGRITY_QUICK", True)
        reconcile_limit_raw = (
            os.getenv("WEBAPP_STARTUP_RECONCILE_LIMIT")
            or ("0" if profile_safe else "1200")
        ).strip()
        reconcile_limit = int(reconcile_limit_raw) if reconcile_limit_raw.isdigit() else 0
        reconcile_batch_raw = (os.getenv("WEBAPP_STARTUP_RECONCILE_BATCH") or "250").strip()
        reconcile_batch = int(reconcile_batch_raw) if reconcile_batch_raw.isdigit() else 250
        reconcile_batch = max(50, min(reconcile_batch, 2000))
        reconcile_max_seconds_raw = (
            os.getenv("WEBAPP_STARTUP_RECONCILE_MAX_SECONDS")
            or ("0" if profile_safe else "6")
        ).strip()
        try:
            reconcile_max_seconds = float(reconcile_max_seconds_raw)
        except Exception:
            reconcile_max_seconds = 0.0

        if run_integrity:
            ok, msg = db.integrity_check(db_path, quick=integrity_quick)
            if not ok:
                raise RuntimeError(f"DB integrity_check gagal: {msg}")
            app.logger.info("DB integrity_check(%s): ok", "quick" if integrity_quick else "full")

        if run_cleanup:
            blocker = db.maintenance_blocker_reason(db_path)
            if blocker:
                app.logger.warning("Startup DB cleanup dilewati: %s", blocker)
            else:
                with db.session(db_path) as con:
                    try:
                        con.execute("PRAGMA journal_mode=DELETE;")
                    except Exception:
                        pass
                    db.wal_checkpoint_truncate(con)
                db.cleanup_wal_shm_files(db_path)
                app.logger.info("Startup DB cleanup selesai (checkpoint + hapus -wal/-shm).")

        if not run_reconcile:
            return

        checked = 0
        relinked = 0
        unlinked_missing = 0
        unlinked_no_transcript = 0
        to_link: List[Tuple[str, int]] = []
        to_unlink: List[Tuple[int]] = []
        truncated_by_time = False
        started_at = time.perf_counter()

        with db.session(db_path) as con:
            def _flush_updates() -> None:
                if not to_unlink and not to_link:
                    return
                with con:
                    if to_unlink:
                        con.executemany(
                            "UPDATE videos SET link_resume=NULL WHERE id=? AND IFNULL(link_resume,'') != ''",
                            to_unlink,
                        )
                    if to_link:
                        con.executemany(
                            "UPDATE videos SET link_resume=? WHERE id=? AND IFNULL(link_resume,'')=''",
                            to_link,
                        )
                to_unlink.clear()
                to_link.clear()

            last_id = 0
            remaining = reconcile_limit
            while True:
                if remaining == 0:
                    break
                step_limit = reconcile_batch
                if remaining > 0:
                    step_limit = min(step_limit, remaining)

                rows = con.execute(
                    """
                    SELECT v.id, v.video_id, v.seq_num, v.link_file, v.link_resume, c.slug AS channel_slug
                    FROM videos v
                    JOIN channels c ON c.id=v.channel_id
                    WHERE v.id > ?
                    ORDER BY v.id ASC
                    LIMIT ?
                    """,
                    (last_id, step_limit),
                ).fetchall()
                if not rows:
                    break

                for r in rows:
                    checked += 1
                    has_text = _link_exists(r["channel_slug"], r["link_file"])

                    resume_link = (r["link_resume"] or "").strip()
                    if resume_link:
                        has_resume = _link_exists(r["channel_slug"], resume_link)
                        if not has_resume:
                            to_unlink.append((int(r["id"]),))
                            unlinked_missing += 1
                        elif not has_text:
                            to_unlink.append((int(r["id"]),))
                            unlinked_no_transcript += 1
                        continue

                    if not has_text:
                        continue
                    default_link = _default_resume_link_startup(r["video_id"], r["seq_num"], r["link_file"])
                    if not default_link:
                        continue
                    if _link_exists(r["channel_slug"], default_link):
                        to_link.append((default_link, int(r["id"])))
                        relinked += 1

                last_id = int(rows[-1]["id"])
                if remaining > 0:
                    remaining = max(0, remaining - len(rows))

                if len(to_unlink) >= 200 or len(to_link) >= 200:
                    _flush_updates()

                if reconcile_max_seconds > 0 and (time.perf_counter() - started_at) >= reconcile_max_seconds:
                    truncated_by_time = True
                    break

            _flush_updates()

        app.logger.info(
            "Startup reconcile selesai: checked=%d relinked=%d unlinked_missing_resume=%d "
            "unlinked_no_transcript=%d limit=%d batch=%d max_seconds=%.1f truncated=%s elapsed=%.2fs",
            checked,
            relinked,
            unlinked_missing,
            unlinked_no_transcript,
            reconcile_limit,
            reconcile_batch,
            reconcile_max_seconds,
            int(truncated_by_time),
            (time.perf_counter() - started_at),
        )

    _startup_prepare_db()

    def _pid_alive(pid: Optional[int]) -> bool:
        if not pid:
            return False
        # Linux: treat zombie as not alive (otherwise os.kill(pid, 0) still succeeds).
        try:
            with open(f"/proc/{int(pid)}/stat", "r", encoding="utf-8", errors="ignore") as f:
                parts = f.read().split()
            if len(parts) >= 3 and parts[2] == "Z":
                return False
        except Exception:
            pass
        try:
            os.kill(int(pid), 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but we don't have permission to signal it.
            return True

    def _resume_lock_active() -> bool:
        out_root = app.config["OUT_ROOT"]
        lock_ttl = max(600, int(os.getenv("RESUME_LOCK_TTL") or "21600"))
        now = time.time()
        try:
            for p in out_root.glob("*/resume/*.lock"):
                try:
                    age = now - p.stat().st_mtime
                except Exception:
                    continue
                if age > lock_ttl:
                    try:
                        p.unlink()
                    except Exception:
                        pass
                    continue
                return True
        except Exception:
            return False
        return False

    def _cleanup_block_reason(con: Any) -> Optional[str]:
        active = con.execute(
            """
            SELECT id, status
            FROM jobs
            WHERE status IN ('queued', 'running', 'stopping')
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if active:
            return f"Ada job aktif ({active['status']}). Tunggu job selesai."
        if _resume_lock_active():
            return "Ada proses resume aktif (file lock terdeteksi)."
        return None

    def _run_resume_agents(args: List[str], *, timeout_s: int = 120) -> tuple[int, str]:
        script_path = Path(app.config["RESUME_AGENTS_SCRIPT"])
        if not script_path.exists():
            return 127, f"Script tidak ditemukan: {script_path}"
        cmd = ["bash", str(script_path), *args]
        try:
            p = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                timeout=timeout_s,
                check=False,
            )
            out = ((p.stdout or "").strip() + "\n" + (p.stderr or "").strip()).strip()
            if len(out) > 30000:
                out = "[truncated]\n" + out[-30000:]
            return int(p.returncode), out
        except subprocess.TimeoutExpired as ex:
            out = (((ex.stdout or "") + "\n" + (ex.stderr or "")).strip())[-8000:]
            return 124, (f"Timeout: {timeout_s}s\n{out}").strip()
        except Exception as ex:
            return 1, str(ex)

    def _ops_state_dir() -> Path:
        p = (app.config["LOGS_BASE"] / ".webapp_ops").resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _compactor_state_path() -> Path:
        return _ops_state_dir() / "compactor_state.json"

    def _read_compactor_state() -> Dict[str, Any]:
        p = _compactor_state_path()
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        return {}

    def _write_compactor_state(state: Dict[str, Any]) -> None:
        p = _compactor_state_path()
        tmp = p.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(state, ensure_ascii=True, sort_keys=True), encoding="utf-8")
            tmp.replace(p)
        except Exception:
            pass

    def _compact_log_tail(log_path: Optional[Path], max_lines: int = 120) -> str:
        if not log_path:
            return ""
        try:
            with log_path.open("r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
        except Exception:
            return ""
        return "".join(lines[-max_lines:]).strip()

    def _compact_parse_rc(log_text: str) -> Optional[int]:
        for line in reversed((log_text or "").splitlines()):
            s = line.strip()
            m = re.match(r"^__COMPACTOR_RC__=(\-?\d+)$", s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
        return None

    def _compact_status() -> Dict[str, Any]:
        state = _read_compactor_state()
        pid = None
        try:
            pid = int(state.get("pid")) if state.get("pid") is not None else None
        except Exception:
            pid = None
        log_path = Path(state.get("log_path")).resolve() if state.get("log_path") else None
        running = bool(pid and _pid_alive(pid))
        status = str(state.get("status") or "idle")

        if running:
            status = "running"
        elif status == "running":
            status = "finished"
            state["status"] = status
            state["ended_at"] = now_iso()

        tail = _compact_log_tail(log_path)
        rc = state.get("returncode")
        if rc is None:
            rc = _compact_parse_rc(tail)
            if rc is not None:
                state["returncode"] = int(rc)
                if not running:
                    state["status"] = "done" if int(rc) == 0 else "error"
                    status = str(state["status"])

        _write_compactor_state(state)
        return {
            "running": running,
            "status": status,
            "pid": pid,
            "started_at": state.get("started_at"),
            "ended_at": state.get("ended_at"),
            "returncode": state.get("returncode"),
            "kind": state.get("kind") or "both",
            "min_age_minutes": state.get("min_age_minutes") if state.get("min_age_minutes") is not None else 1,
            "max_shard_mb": state.get("max_shard_mb") if state.get("max_shard_mb") is not None else 128,
            "log_path": str(log_path) if log_path else "",
            "log_tail": tail,
        }

    def _compact_start(*, kind: str, min_age_minutes: int, max_shard_mb: int) -> tuple[bool, str]:
        cur = _compact_status()
        if cur.get("running"):
            return False, "Compactor sedang running."

        script_path = Path(app.config["COMPACTOR_SCRIPT"])
        if not script_path.exists():
            return False, f"Script compactor tidak ditemukan: {script_path}"

        kind = kind if kind in {"text", "resume", "both"} else "both"
        min_age_minutes = max(0, int(min_age_minutes))
        max_shard_mb = max(8, min(1024, int(max_shard_mb)))

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        log_path = (_ops_state_dir() / f"compactor_{stamp}.log").resolve()
        cmd = [
            "bash",
            "-lc",
            (
                f"'{sys.executable}' '{script_path}' "
                f"--db '{app.config['DB_PATH']}' "
                f"--out-root '{app.config['OUT_ROOT']}' "
                f"--kind '{kind}' "
                f"--min-age-minutes '{min_age_minutes}' "
                f"--max-shard-mb '{max_shard_mb}' ; "
                "echo __COMPACTOR_RC__=$?"
            ),
        ]

        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as logf:
                logf.write(f"[{now_iso()}] [spawn] $ {' '.join(cmd)}\n")
                logf.flush()
                p = subprocess.Popen(
                    cmd,
                    cwd=str(project_root),
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
        except Exception as ex:
            return False, f"Gagal start compactor: {ex}"

        _write_compactor_state(
            {
                "status": "running",
                "pid": int(p.pid),
                "started_at": now_iso(),
                "ended_at": None,
                "returncode": None,
                "kind": kind,
                "min_age_minutes": int(min_age_minutes),
                "max_shard_mb": int(max_shard_mb),
                "log_path": str(log_path),
            }
        )
        return True, f"Compactor started (pid={p.pid})"

    def _compact_stop() -> tuple[bool, str]:
        state = _read_compactor_state()
        pid_raw = state.get("pid")
        if pid_raw is None:
            state["status"] = "stopped"
            state["ended_at"] = now_iso()
            _write_compactor_state(state)
            return True, "Compactor tidak sedang running."
        try:
            pid = int(pid_raw)
        except Exception:
            pid = 0
        if pid <= 0 or not _pid_alive(pid):
            state["status"] = "stopped"
            state["ended_at"] = now_iso()
            _write_compactor_state(state)
            return True, "Compactor sudah berhenti."

        try:
            os.killpg(pid, signal.SIGTERM)
        except Exception:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception as ex:
                return False, f"Gagal stop compactor: {ex}"

        deadline = time.time() + 8.0
        while time.time() < deadline:
            if not _pid_alive(pid):
                break
            time.sleep(0.2)
        if _pid_alive(pid):
            try:
                os.killpg(pid, signal.SIGKILL)
            except Exception:
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass

        state["status"] = "stopped"
        state["ended_at"] = now_iso()
        _write_compactor_state(state)
        return True, "Compactor stop signal dikirim."

    def _queue_stale_updates(con: Any) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        queued_count = 0
        stale = con.execute(
            """
            SELECT id, url, last_scanned FROM channels
            WHERE last_scanned < ? OR last_scanned IS NULL
            """,
            (cutoff_iso,),
        ).fetchall()
        for ch in stale:
            cid = int(ch["id"])
            if jobs.channel_running_job(con, cid):
                continue
            is_queued = con.execute(
                "SELECT 1 FROM jobs WHERE channel_id=? AND status='queued' LIMIT 1",
                (cid,),
            ).fetchone()
            if is_queued:
                continue
            with con:
                jobs.start_channel_update(
                    con=con,
                    channel_id=cid,
                    channel_url=str(ch["url"]),
                    db_path=app.config["DB_PATH"],
                    out_root=app.config["OUT_ROOT"],
                    script_path=app.config["SCRIPT_PATH"],
                    stop_at_known=True,
                    logs_base=app.config["LOGS_BASE"],
                )
            queued_count += 1
        return queued_count

    def _ops_dashboard_data(*, status_rc: Optional[int] = None, status_output: Optional[str] = None) -> Dict[str, Any]:
        if status_rc is None or status_output is None:
            status_rc, status_output = _run_resume_agents(["status"], timeout_s=45)
        compact_info = _compact_status()
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        with db.session(app.config["DB_PATH"]) as con:
            active_rows = con.execute(
                """
                SELECT * FROM jobs
                WHERE status IN ('queued','running','stopping')
                ORDER BY created_at ASC
                LIMIT 200
                """
            ).fetchall()
            active_jobs = [jobs.JobRow(**dict(r)) for r in active_rows]
            recent_jobs = jobs.list_jobs(con, limit=30)
            channels_small = con.execute(
                """
                SELECT id, slug, last_scanned
                FROM channels
                ORDER BY id DESC
                LIMIT 500
                """
            ).fetchall()
            stale_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM channels WHERE last_scanned < ? OR last_scanned IS NULL",
                    (cutoff_iso,),
                ).fetchone()[0]
                or 0
            )
        return {
            "resume_status_rc": status_rc,
            "resume_status_output": status_output or "",
            "active_jobs": active_jobs,
            "recent_jobs": recent_jobs,
            "channels_small": channels_small,
            "stale_count": stale_count,
            "compact_info": compact_info,
        }

    @app.template_filter("fmt_yyyymmdd")
    def fmt_yyyymmdd(v: Any) -> str:
        s = (v or "").strip()
        if not re.fullmatch(r"\d{8}", s):
            return s
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

    @app.template_filter("fmt_iso")
    def fmt_iso(v: Any) -> str:
        s = (v or "").strip()
        if not s:
            return ""
        # Keep it simple; works with "YYYY-MM-DDTHH:MM:SSZ".
        return s.replace("T", " ").replace("Z", " UTC")

    @app.route("/")
    def home() -> Any:
        return redirect(url_for("channels"))

    @app.route("/videos")
    def videos_redirect() -> Any:
        return redirect(url_for("channels"))

    @app.route("/channel/<path:channel_slug>")
    def channel_slug_redirect(channel_slug: str) -> Any:
        slug = (channel_slug or "").strip().strip("/")
        if not slug:
            abort(404)
        slug = slug.lstrip("@")
        with db.session(app.config["DB_PATH"]) as con:
            row = con.execute(
                """
                SELECT id
                FROM channels
                WHERE lower(slug)=lower(?)
                   OR lower(url)=lower(?)
                   OR lower(url)=lower(?)
                LIMIT 1
                """
                ,
                (slug, f"https://www.youtube.com/@{slug}", f"https://www.youtube.com/channel/{slug}"),
            ).fetchone()
        if not row:
            abort(404)
        return redirect(url_for("channel_detail", channel_id=int(row["id"])))

    @app.route("/channels")
    def channels() -> Any:
        q = (request.args.get("q") or "").strip()
        category_id = (request.args.get("category") or "").strip()
        page_raw = (request.args.get("page") or "1").strip()
        per_page_raw = (request.args.get("per_page") or "50").strip()
        page = int(page_raw) if page_raw.isdigit() and int(page_raw) > 0 else 1
        per_page = int(per_page_raw) if per_page_raw.isdigit() and int(per_page_raw) > 0 else 50
        per_page = min(max(per_page, 10), 200)
        cleanup_disabled_reason: Optional[str] = None
        with db.session(app.config["DB_PATH"]) as con:
            categories = con.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()

            where = ["1=1"]
            params: List[Any] = []
            if q:
                like = f"%{q}%"
                where.append("(c.slug LIKE ? OR c.url LIKE ?)")
                params.extend([like, like])
            if category_id == "uncategorized":
                where.append("c.category_id IS NULL")
            elif category_id.isdigit():
                where.append("c.category_id=?")
                params.append(int(category_id))

            where_sql = " AND ".join(where)
            total_channels = int(
                con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM channels c
                    WHERE {where_sql}
                    """,
                    tuple(params),
                ).fetchone()[0]
                or 0
            )
            page_count = max(1, (total_channels + per_page - 1) // per_page)
            if page > page_count:
                page = page_count
            offset = (page - 1) * per_page

            rows = con.execute(
                f"""
                SELECT
                  c.chan_seq, c.id, c.url, c.slug, c.last_scanned, c.category_id,
                  cat.name AS category_name,
                  cat.color AS category_color,
                  COALESCE(vs.video_count, 0) AS video_count,
                  COALESCE(vs.ok_count, 0) AS ok_count,
                  COALESCE(vs.pending_count, 0) AS pending_count,
                  COALESCE(vs.error_count, 0) AS error_count
                FROM (
                  SELECT
                    ROW_NUMBER() OVER (ORDER BY id DESC) AS chan_seq,
                    id, url, slug, last_scanned, category_id
                  FROM channels
                ) c
                LEFT JOIN categories cat ON cat.id = c.category_id
                LEFT JOIN (
                  SELECT
                    channel_id,
                    COUNT(*) AS video_count,
                    SUM(CASE WHEN status_download='downloaded' THEN 1 ELSE 0 END) AS ok_count,
                    SUM(CASE WHEN status_download='pending' THEN 1 ELSE 0 END) AS pending_count,
                    SUM(CASE WHEN status_download='error' THEN 1 ELSE 0 END) AS error_count
                  FROM videos
                  GROUP BY channel_id
                ) vs ON vs.channel_id = c.id
                WHERE {where_sql}
                ORDER BY c.id DESC
                LIMIT ?
                OFFSET ?
                """,
                tuple([*params, per_page, offset]),
            ).fetchall()
            # Clean up stale jobs (pid died/zombie) so UI doesn't show stuck running/stopping.
            touched = False
            active_rows = con.execute(
                "SELECT id, status, pid FROM jobs WHERE status IN ('queued','running','stopping') AND pid IS NOT NULL"
            ).fetchall()
            for r in active_rows:
                pid = r["pid"]
                if pid and not _pid_alive(int(pid)):
                    if r["status"] == "stopping":
                        status2 = "stopped"
                        msg2 = "stopped by user"
                    else:
                        status2 = "error"
                        msg2 = "runner process exited"
                    with con:
                        con.execute(
                            "UPDATE jobs SET status=?, finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
                            (status2, now_iso(), msg2, r["id"]),
                        )
                    touched = True

            job_rows = jobs.list_jobs(con, limit=5)
            if touched:
                job_rows = jobs.list_jobs(con, limit=5)
            channel_seq_by_id = {int(r["id"]): int(r["chan_seq"]) for r in rows}
            missing_seq_ids = sorted(
                {
                    int(j.channel_id)
                    for j in job_rows
                    if j.channel_id and int(j.channel_id) not in channel_seq_by_id
                }
            )
            if missing_seq_ids:
                placeholders = ",".join("?" for _ in missing_seq_ids)
                seq_rows = con.execute(
                    f"""
                    SELECT id, chan_seq
                    FROM (
                      SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS chan_seq
                      FROM channels
                    )
                    WHERE id IN ({placeholders})
                    """,
                    tuple(missing_seq_ids),
                ).fetchall()
                for r in seq_rows:
                    channel_seq_by_id[int(r["id"])] = int(r["chan_seq"])
            cleanup_disabled_reason = _cleanup_block_reason(con)
        if not cleanup_disabled_reason:
            cleanup_disabled_reason = db.maintenance_blocker_reason(app.config["DB_PATH"])
        return render_template(
            "channels.html",
            channels=rows,
            categories=categories,
            jobs=job_rows,
            channel_seq_by_id=channel_seq_by_id,
            cleanup_disabled_reason=cleanup_disabled_reason,
            q=q,
            category_id=category_id,
            page=page,
            per_page=per_page,
            total_channels=total_channels,
            page_count=page_count,
        )

    @app.route("/search")
    def search() -> Any:
        q = (request.args.get("q") or "").strip()
        status = (request.args.get("status") or "").strip()
        category_id = (request.args.get("category") or "").strip()
        include_resume = (request.args.get("resume") or "") == "1"

        with db.session(app.config["DB_PATH"]) as con:
            categories = con.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()

            where = ["1=1"]
            params: List[Any] = []
            if status:
                where.append("v.status_download=?")
                params.append(status)
            if q:
                where.append("(v.title LIKE ? OR v.video_id LIKE ?)")
                like = f"%{q}%"
                params.extend([like, like])
            if category_id.isdigit():
                where.append(
                    "EXISTS (SELECT 1 FROM video_categories vc WHERE vc.video_pk=v.id AND vc.category_id=?)"
                )
                params.append(int(category_id))

            vids = con.execute(
                f"""
                SELECT v.*, c.slug AS channel_slug
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE {' AND '.join(where)}
                ORDER BY v.upload_date DESC, v.seq_num DESC
                LIMIT 250
                """,
                tuple(params),
            ).fetchall()

            if include_resume and q:
                scan_cap = 2000
                ql = q.lower()
                where2 = ["v.link_resume IS NOT NULL AND v.link_resume != ''"]
                params2: List[Any] = []
                if status:
                    where2.append("v.status_download=?")
                    params2.append(status)
                if category_id.isdigit():
                    where2.append(
                        "EXISTS (SELECT 1 FROM video_categories vc WHERE vc.video_pk=v.id AND vc.category_id=?)"
                    )
                    params2.append(int(category_id))

                extra = con.execute(
                    f"""
                    SELECT v.*, c.slug AS channel_slug
                    FROM videos v
                    JOIN channels c ON c.id=v.channel_id
                    WHERE {' AND '.join(where2)}
                    ORDER BY v.upload_date DESC, v.seq_num DESC
                    LIMIT ?
                    """,
                    tuple(params2 + [scan_cap]),
                ).fetchall()

                already = {r["id"] for r in vids}
                found = []
                for v in extra:
                    if v["id"] in already:
                        continue
                    txt = _link_read_text(v["channel_slug"], v["link_resume"])
                    if txt is None:
                        continue
                    if ql in txt.lower():
                        found.append(v)
                        already.add(v["id"])
                        if len(vids) + len(found) >= 250:
                            break
                vids = list(vids) + found

        return render_template(
            "search.html",
            q=q,
            status=status,
            category_id=category_id,
            categories=categories,
            videos=vids,
            include_resume=include_resume,
        )

    @app.route("/channels/add", methods=["GET", "POST"])
    def add_channel() -> Any:
        if request.method == "POST":
            url = (request.form.get("url") or "").strip()
            start_update = (request.form.get("start_update") or "") == "1"
            category_raw = (request.form.get("category_id") or "").strip()
            category_id: Optional[int] = None
            if category_raw:
                if not category_raw.isdigit():
                    flash("Kategori channel tidak valid.", "danger")
                    return redirect(url_for("add_channel"))
                category_id = int(category_raw)
            if not url:
                flash("URL channel wajib diisi.", "danger")
                return redirect(url_for("add_channel"))

            url = url.rstrip("/")
            if "youtube.com" not in url:
                flash("URL harus dari youtube.com.", "danger")
                return redirect(url_for("add_channel"))

            videos_url = scrap_all_channel.ensure_videos_url(url)
            slug = scrap_all_channel.parse_channel_slug(url)
            channel_id: Optional[int] = None
            with db.session(app.config["DB_PATH"]) as con:
                if category_id is not None:
                    cat = con.execute("SELECT id FROM categories WHERE id=?", (category_id,)).fetchone()
                    if not cat:
                        flash("Kategori channel tidak ditemukan.", "danger")
                        return redirect(url_for("add_channel"))
                with con:
                    con.execute(
                        "INSERT INTO channels(url, slug, last_scanned, category_id) VALUES (?, ?, ?, ?) "
                        "ON CONFLICT(url) DO UPDATE SET slug=excluded.slug, category_id=excluded.category_id;",
                        (videos_url, slug, now_iso(), category_id),
                    )
                    channel_id = con.execute("SELECT id FROM channels WHERE url=?", (videos_url,)).fetchone()[0]
            flash(f"Channel tersimpan: {slug}", "success")
            if start_update and channel_id is not None:
                job = None
                with db.session(app.config["DB_PATH"]) as con:
                    with con:
                        job_id = jobs.start_channel_update(
                            con=con,
                            channel_id=int(channel_id),
                            channel_url=videos_url,
                            db_path=app.config["DB_PATH"],
                            out_root=app.config["OUT_ROOT"],
                            script_path=app.config["SCRIPT_PATH"],
                            stop_at_known=True,
                            logs_base=app.config["LOGS_BASE"],
                        )
                    job = jobs.get_job(con, job_id)
                if job and job.status == "queued":
                    flash("Update masuk antrian. Menunggu job lain selesai.", "warning")
                else:
                    flash("Update dimulai di background.", "info")
                return redirect(url_for("job_detail", job_id=job_id))
            return redirect(url_for("channels"))

        with db.session(app.config["DB_PATH"]) as con:
            categories = con.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()
        return render_template("channel_add.html", categories=categories)

    @app.route("/channels/<int:channel_id>/category", methods=["POST"])
    def channel_category_set(channel_id: int) -> Any:
        category_raw = (request.form.get("category_id") or "").strip()
        next_url = (request.form.get("next") or "").strip()
        category_id: Optional[int] = None
        if category_raw:
            if not category_raw.isdigit():
                flash("Kategori channel tidak valid.", "danger")
                return redirect(request.referrer or url_for("channels"))
            category_id = int(category_raw)

        with db.session(app.config["DB_PATH"]) as con:
            ch = con.execute("SELECT id FROM channels WHERE id=?", (channel_id,)).fetchone()
            if not ch:
                abort(404)
            if category_id is not None:
                cat = con.execute("SELECT id FROM categories WHERE id=?", (category_id,)).fetchone()
                if not cat:
                    flash("Kategori channel tidak ditemukan.", "danger")
                    return redirect(request.referrer or url_for("channels"))
            with con:
                con.execute("UPDATE channels SET category_id=? WHERE id=?", (category_id, channel_id))

        flash("Kategori channel diperbarui.", "success")
        if next_url.startswith("/"):
            return redirect(next_url)
        return redirect(request.referrer or url_for("channels"))

    @app.route("/channels/<int:channel_id>")
    def channel_detail(channel_id: int) -> Any:
        q = (request.args.get("q") or "").strip()
        status = (request.args.get("status") or "").strip()
        category_id = (request.args.get("category") or "").strip()
        page_raw = (request.args.get("page") or "1").strip()
        per_page_raw = (request.args.get("per_page") or "100").strip()
        page = int(page_raw) if page_raw.isdigit() and int(page_raw) > 0 else 1
        per_page = int(per_page_raw) if per_page_raw.isdigit() and int(per_page_raw) > 0 else 100
        per_page = min(max(per_page, 25), 500)
        show_nosub = (request.args.get("show_nosub") or "0") == "1"

        with db.session(app.config["DB_PATH"]) as con:
            ch = con.execute(
                """
                SELECT c.*, cat.name AS category_name, cat.color AS category_color
                FROM channels c
                LEFT JOIN categories cat ON cat.id = c.category_id
                WHERE c.id=?
                """,
                (channel_id,),
            ).fetchone()
            if not ch:
                abort(404)
            channel_seq = con.execute(
                """
                SELECT chan_seq FROM (
                  SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS chan_seq
                  FROM channels
                )
                WHERE id=?
                """,
                (channel_id,),
            ).fetchone()
            channel_seq = int(channel_seq[0]) if channel_seq else None

            categories = con.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()

            where = ["v.channel_id=?"]
            params: List[Any] = [channel_id]
            if status:
                where.append("v.status_download=?")
                params.append(status)
            elif not show_nosub:
                where.append("v.status_download != 'no_subtitle'")
            if q:
                where.append("(v.title LIKE ? OR v.video_id LIKE ?)")
                like = f"%{q}%"
                params.extend([like, like])
            if category_id.isdigit():
                where.append(
                    "EXISTS (SELECT 1 FROM video_categories vc WHERE vc.video_pk=v.id AND vc.category_id=?)"
                )
                params.append(int(category_id))

            sql_count = f"SELECT COUNT(*) FROM videos v WHERE {' AND '.join(where)}"
            total_filtered = int(con.execute(sql_count, tuple(params)).fetchone()[0] or 0)
            page_count = max(1, (total_filtered + per_page - 1) // per_page)
            if page > page_count:
                page = page_count
            offset = (page - 1) * per_page

            sql = f"""
                SELECT
                  v.*,
                  (
                    SELECT GROUP_CONCAT(vc.category_id || ':' || c.name, '|')
                    FROM video_categories vc
                    JOIN categories c ON c.id = vc.category_id
                    WHERE vc.video_pk=v.id
                    ORDER BY vc.category_id
                  ) AS cat_pairs
                FROM videos v
                WHERE {' AND '.join(where)}
                ORDER BY
                  (v.seq_num IS NULL) ASC,
                  v.seq_num DESC,
                  v.upload_date DESC,
                  v.id DESC
                LIMIT ?
                OFFSET ?
            """
            vids = con.execute(sql, tuple([*params, per_page, offset])).fetchall()

            stats = con.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN status_download='downloaded' THEN 1 ELSE 0 END) AS ok,
                  SUM(CASE WHEN status_download='pending' THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN status_download='no_subtitle' THEN 1 ELSE 0 END) AS no_sub,
                  SUM(CASE WHEN status_download='error' THEN 1 ELSE 0 END) AS err
                FROM videos
                WHERE channel_id=?
                """,
                (channel_id,),
            ).fetchone()
            running_job_id = jobs.channel_running_job(con, channel_id)
            if running_job_id:
                j = jobs.get_job(con, running_job_id)
                if j and j.status in ("queued", "running", "stopping") and j.pid and not _pid_alive(j.pid):
                    if j.status == "stopping":
                        status2 = "stopped"
                        msg2 = "stopped by user"
                    else:
                        status2 = "error"
                        msg2 = "runner process exited"
                    with con:
                        con.execute(
                            "UPDATE jobs SET status=?, finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
                            (status2, now_iso(), msg2, j.id),
                        )
                    running_job_id = jobs.channel_running_job(con, channel_id)

        return render_template(
            "channel_detail.html",
            channel=ch,
            channel_seq=channel_seq,
            videos=vids,
            stats=stats,
            q=q,
            status=status,
            categories=categories,
            category_id=category_id,
            running_job_id=running_job_id,
            page=page,
            per_page=per_page,
            total_filtered=total_filtered,
            page_count=page_count,
            show_nosub=show_nosub,
        )

    @app.route("/channels/<int:channel_id>/update", methods=["POST"])
    def channel_update(channel_id: int) -> Any:
        mode = (request.form.get("mode") or "quick").strip()
        stop_at_known = mode != "full"
        with db.session(app.config["DB_PATH"]) as con:
            ch = con.execute("SELECT * FROM channels WHERE id=?", (channel_id,)).fetchone()
            if not ch:
                abort(404)
            video_count = int(
                con.execute("SELECT COUNT(*) FROM videos WHERE channel_id=?", (channel_id,)).fetchone()[0] or 0
            )
            # Seed safety: kalau DB masih kecil, jangan stop-at-known karena bisa berhenti terlalu cepat
            # (playlist order tidak selalu newest->oldest).
            if stop_at_known and video_count < 50:
                stop_at_known = False
                flash("DB channel masih sedikit; jalankan full scan agar semua video ke-scrape.", "info")
            job = None
            with con:
                job_id = jobs.start_channel_update(
                    con=con,
                    channel_id=channel_id,
                    channel_url=ch["url"],
                    db_path=app.config["DB_PATH"],
                    out_root=app.config["OUT_ROOT"],
                    script_path=app.config["SCRIPT_PATH"],
                    stop_at_known=stop_at_known,
                    stop_at_known_after=25,
                    stop_at_known_min_scan=200,
                    logs_base=app.config["LOGS_BASE"],
                )
            job = jobs.get_job(con, job_id)
        if job and job.status == "queued":
            flash("Update masuk antrian. Menunggu job lain selesai.", "warning")
        else:
            flash("Update dimulai di background.", "info")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/channels/<int:channel_id>/update-pending", methods=["POST"])
    def channel_update_pending(channel_id: int) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            ch = con.execute("SELECT * FROM channels WHERE id=?", (channel_id,)).fetchone()
            if not ch:
                abort(404)
            with con:
                job_id = jobs.start_channel_update(
                    con=con,
                    channel_id=channel_id,
                    channel_url=ch["url"],
                    db_path=app.config["DB_PATH"],
                    out_root=app.config["OUT_ROOT"],
                    script_path=app.config["SCRIPT_PATH"],
                    stop_at_known=False,
                    pending_only=True,
                    video_id=None,
                    logs_base=app.config["LOGS_BASE"],
                )
            job = jobs.get_job(con, job_id)
        if job and job.status == "queued":
            flash("Pending-only masuk antrian. Menunggu job lain selesai.", "warning")
        else:
            flash("Pending-only dimulai di background.", "info")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/channels/update-stale", methods=["POST"])
    def update_stale_channels() -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            queued_count = _queue_stale_updates(con)
        if queued_count > 0:
            flash(f"Berhasil queue update untuk {queued_count} channel yang sudah lama tidak di-scan (>7 hari).", "success")
        else:
            flash("Semua channel masih fresh (atau sudah ada di antrian/running).", "info")
        
        return redirect(url_for("channels"))

    @app.route("/ops/dashboard")
    def ops_dashboard() -> Any:
        ctx = _ops_dashboard_data()
        return render_template("ops_dashboard.html", **ctx)

    @app.route("/ops/dashboard/action", methods=["POST"])
    def ops_dashboard_action() -> Any:
        action = (request.form.get("action") or "").strip()
        status_rc: Optional[int] = None
        status_output: Optional[str] = None

        if action in {"resume_start", "resume_stop", "resume_restart", "resume_status"}:
            targets_raw = (request.form.get("targets") or "").strip()
            targets: List[str] = []
            invalid_targets: List[str] = []
            if targets_raw:
                for part in re.split(r"[\s,]+", targets_raw):
                    p = part.strip()
                    if not p:
                        continue
                    if re.fullmatch(r"[A-Za-z0-9._:-]+", p):
                        targets.append(p)
                    else:
                        invalid_targets.append(p)
            if invalid_targets:
                flash(f"Target tidak valid (diabaikan): {', '.join(invalid_targets[:6])}", "warning")

            if action == "resume_start":
                cmd = ["start", *targets] if targets else ["start"]
                rc, out = _run_resume_agents(cmd, timeout_s=90)
                flash(f"Resume start rc={rc} (targets={len(targets)})", "success" if rc == 0 else "warning")
                status_rc, status_output = _run_resume_agents(["status"], timeout_s=45)
                if rc != 0 and out:
                    flash((out.splitlines() or [out])[0][:240], "danger")
            elif action == "resume_stop":
                rc, out = _run_resume_agents(["stop", "all"], timeout_s=90)
                flash(f"Resume stop rc={rc}", "success" if rc == 0 else "warning")
                status_rc, status_output = _run_resume_agents(["status"], timeout_s=45)
                if rc != 0 and out:
                    flash((out.splitlines() or [out])[0][:240], "danger")
            elif action == "resume_restart":
                rc1, out1 = _run_resume_agents(["stop", "all"], timeout_s=90)
                cmd2 = ["start", *targets] if targets else ["start"]
                rc2, out2 = _run_resume_agents(cmd2, timeout_s=90)
                flash(
                    f"Resume restart stop_rc={rc1} start_rc={rc2} (targets={len(targets)})",
                    "success" if rc2 == 0 else "warning",
                )
                status_rc, status_output = _run_resume_agents(["status"], timeout_s=45)
                if rc1 != 0 and out1:
                    flash((out1.splitlines() or [out1])[0][:240], "danger")
                if rc2 != 0 and out2:
                    flash((out2.splitlines() or [out2])[0][:240], "danger")
            else:
                status_rc, status_output = _run_resume_agents(["status"], timeout_s=45)
                flash("Resume status diperbarui.", "info")
        elif action == "scrape_queue_stale":
            with db.session(app.config["DB_PATH"]) as con:
                queued_count = _queue_stale_updates(con)
            if queued_count > 0:
                flash(f"Queued update stale: {queued_count} channel.", "success")
            else:
                flash("Tidak ada channel stale yang perlu di-queue.", "info")
        elif action == "scrape_queue_channel":
            channel_id_raw = (request.form.get("channel_id") or "").strip()
            mode = (request.form.get("mode") or "quick").strip().lower()
            target_video_id = (request.form.get("video_id") or "").strip()
            if not channel_id_raw.isdigit():
                flash("channel_id tidak valid.", "danger")
            else:
                channel_id = int(channel_id_raw)
                with db.session(app.config["DB_PATH"]) as con:
                    ch = con.execute("SELECT * FROM channels WHERE id=?", (channel_id,)).fetchone()
                    if not ch:
                        flash("Channel tidak ditemukan.", "danger")
                    else:
                        stop_at_known = (mode == "quick")
                        pending_only = (mode in {"pending", "video"})
                        if mode == "video":
                            if not re.fullmatch(r"[A-Za-z0-9_-]{11}", target_video_id):
                                flash("video_id harus 11 karakter.", "danger")
                                return redirect(url_for("ops_dashboard"))
                        else:
                            target_video_id = ""
                        with con:
                            job_id = jobs.start_channel_update(
                                con=con,
                                channel_id=channel_id,
                                channel_url=ch["url"],
                                db_path=app.config["DB_PATH"],
                                out_root=app.config["OUT_ROOT"],
                                script_path=app.config["SCRIPT_PATH"],
                                stop_at_known=stop_at_known,
                                pending_only=pending_only,
                                video_id=(target_video_id or None),
                                logs_base=app.config["LOGS_BASE"],
                            )
                        j = jobs.get_job(con, job_id)
                        state = j.status if j else "queued"
                        flash(
                            f"Queue channel #{channel_id} mode={mode} -> job {job_id} ({state}).",
                            "success" if state != "error" else "warning",
                        )
        elif action == "scrape_stop_all":
            stopped = 0
            failed = 0
            with db.session(app.config["DB_PATH"]) as con:
                active = con.execute(
                    "SELECT id FROM jobs WHERE status IN ('queued','running','stopping') ORDER BY created_at ASC"
                ).fetchall()
                for row in active:
                    jid = str(row["id"])
                    with con:
                        ok, _msg = jobs.request_stop(con, jid, reason="stop requested from ops dashboard")
                    if ok:
                        stopped += 1
                    else:
                        failed += 1
            flash(f"Scrape stop-all: stopped={stopped}, failed={failed}", "warning" if failed else "success")
        elif action == "scrape_stop_one":
            job_id = (request.form.get("job_id") or "").strip()
            if not job_id:
                flash("job_id kosong.", "danger")
            else:
                with db.session(app.config["DB_PATH"]) as con:
                    with con:
                        ok, msg = jobs.request_stop(con, job_id, reason="stop requested from ops dashboard")
                flash(msg, "info" if ok else "danger")
        elif action == "compact_start":
            kind = (request.form.get("compact_kind") or "both").strip().lower()
            min_age_raw = (request.form.get("compact_min_age_minutes") or "1").strip()
            max_shard_raw = (request.form.get("compact_max_shard_mb") or "128").strip()
            try:
                min_age = int(min_age_raw)
            except Exception:
                min_age = 1
            try:
                max_shard = int(max_shard_raw)
            except Exception:
                max_shard = 128
            ok, msg = _compact_start(kind=kind, min_age_minutes=min_age, max_shard_mb=max_shard)
            flash(msg, "success" if ok else "danger")
        elif action == "compact_stop":
            ok, msg = _compact_stop()
            flash(msg, "info" if ok else "danger")
        elif action == "compact_status":
            flash("Status compactor diperbarui.", "info")
        else:
            flash("Aksi tidak dikenal.", "danger")

        ctx = _ops_dashboard_data(status_rc=status_rc, status_output=status_output)
        return render_template("ops_dashboard.html", **ctx)

    @app.route("/videos/<int:video_pk>/update-pending", methods=["POST"])
    def video_update_pending(video_pk: int) -> Any:
        nxt = (request.form.get("next") or "").strip()
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.id, v.video_id, v.status_download, v.channel_id, c.url AS channel_url
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)
            if (v["status_download"] or "") != "pending":
                flash("Video ini bukan pending, tapi tetap dicoba update sekali.", "warning")
            with con:
                job_id = jobs.start_channel_update(
                    con=con,
                    channel_id=int(v["channel_id"]),
                    channel_url=str(v["channel_url"]),
                    db_path=app.config["DB_PATH"],
                    out_root=app.config["OUT_ROOT"],
                    script_path=app.config["SCRIPT_PATH"],
                    stop_at_known=False,
                    pending_only=True,
                    video_id=str(v["video_id"]),
                    logs_base=app.config["LOGS_BASE"],
                )
            job = jobs.get_job(con, job_id)
        if job and job.status == "queued":
            flash("Retry video pending masuk antrian. Menunggu job lain selesai.", "warning")
        else:
            flash("Retry video pending dimulai di background.", "info")
        if nxt.startswith("/"):
            return redirect(nxt)
        return redirect(url_for("channel_detail", channel_id=int(v["channel_id"])))

    @app.route("/jobs/<job_id>")
    def job_detail(job_id: str) -> Any:
        channel_seq = None
        with db.session(app.config["DB_PATH"]) as con:
            job = jobs.get_job(con, job_id)
            if not job:
                abort(404)
            if job.status in ("queued", "running", "stopping") and job.pid and not _pid_alive(job.pid):
                # Runner died before updating status; mark so UI doesn't look stuck forever.
                if job.status == "stopping":
                    status2 = "stopped"
                    msg2 = "stopped by user"
                else:
                    status2 = "error"
                    msg2 = "runner process exited"
                with con:
                    con.execute(
                        "UPDATE jobs SET status=?, finished_at=?, error_msg=COALESCE(error_msg, ?) WHERE id=?",
                        (status2, now_iso(), msg2, job_id),
                    )
                job = jobs.get_job(con, job_id) or job
            if job.channel_id:
                r = con.execute(
                    """
                    SELECT chan_seq FROM (
                      SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS chan_seq
                      FROM channels
                    )
                    WHERE id=?
                    """,
                    (int(job.channel_id),),
                ).fetchone()
                channel_seq = int(r[0]) if r else None
        lines = jobs.job_log_tail(job, max_lines=500)
        return render_template("job_detail.html", job=job, lines=lines, channel_seq=channel_seq)

    @app.route("/jobs/<job_id>/stop", methods=["POST"])
    def job_stop(job_id: str) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            job = jobs.get_job(con, job_id)
            if not job:
                abort(404)
            with con:
                ok, msg = jobs.request_stop(con, job_id)
        flash(msg, "info" if ok else "danger")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/videos/<int:video_pk>")
    def video_detail(video_pk: int) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.*, c.slug AS channel_slug, c.id AS channel_id
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)

            cats = con.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()
            selected = {
                r["category_id"]
                for r in con.execute(
                    "SELECT category_id FROM video_categories WHERE video_pk=?", (video_pk,)
                ).fetchall()
            }

        # Optional preview of transcript (fast; reads at most ~50KB).
        text_preview: Optional[str] = None
        text_unreadable = False
        try:
            raw = _link_read_text(v["channel_slug"], v["link_file"])
            if raw is not None:
                text_preview = raw[:50_000]
            elif v["link_file"] and _link_exists(v["channel_slug"], v["link_file"]):
                text_unreadable = True
        except Exception:
            text_preview = None

        resume_text = ""
        resume_mtime = None
        resume_unreadable = False
        resume_link = v["link_resume"] or _default_resume_link(v["video_id"], v["seq_num"], v["link_file"])
        try:
            txt = _link_read_text(v["channel_slug"], resume_link)
            if txt is not None:
                resume_text = txt
                mtime_ts = _link_mtime(v["channel_slug"], resume_link)
                if mtime_ts:
                    resume_mtime = datetime.fromtimestamp(mtime_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            elif resume_link and _link_exists(v["channel_slug"], resume_link):
                resume_unreadable = True
        except Exception:
            resume_text = ""
            resume_mtime = None

        return render_template(
            "video_detail.html",
            v=v,
            watch_url=video_watch_url(v["video_id"]),
            cats=cats,
            selected=selected,
            text_preview=text_preview,
            text_unreadable=text_unreadable,
            resume_text=resume_text,
            resume_mtime=resume_mtime,
            resume_unreadable=resume_unreadable,
            transcript="",
            formatted_transcript="",
            summary="",
            manual_transcript_job="",
            manual_transcript_message="",
            manual_transcript_retry_available=_legacy_manual_transcript_retry_available(v),
            transcript_source="",
            upload_date_status="",
            upload_date_reason="",
            upload_date_source="",
        )

    def _parse_seq_from_name(name: Optional[str]) -> Optional[int]:
        if not name:
            return None
        base = Path(str(name)).name
        m = re.match(r"^(\d+)_", base)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _parse_seq_from_link_file(link_file: Optional[str]) -> Optional[int]:
        return _parse_seq_from_name(link_file)

    def _derive_seq(
        seq_num: Optional[int], link_file: Optional[str], link_resume: Optional[str]
    ) -> Optional[int]:
        if seq_num is not None:
            try:
                return int(seq_num)
            except Exception:
                pass
        seq = _parse_seq_from_name(link_file)
        if seq is not None:
            return seq
        return _parse_seq_from_name(link_resume)

    def _default_resume_link(
        video_id: str, seq_num: Optional[int] = None, link_file: Optional[str] = None
    ) -> Optional[str]:
        seq = seq_num if seq_num else _parse_seq_from_link_file(link_file)
        if not seq:
            return None
        return str(Path("resume") / f"{int(seq):04d}_{video_id}.md")

    def _resolve_resume_path(channel_slug: str, link_resume: Optional[str]) -> Optional[Path]:
        if not link_resume:
            return None
        rel = Path(str(link_resume))
        base = app.config["OUT_ROOT"] / channel_slug
        p = (base / rel).resolve()
        try:
            p.relative_to(base.resolve())
        except Exception:
            return None
        return p

    @app.route("/videos/<int:video_pk>/text")
    def video_text(video_pk: int) -> Any:
        raw = request.args.get("raw") == "1"
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.id, v.video_id, v.title, v.link_file, c.slug AS channel_slug, c.id AS channel_id
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)
        blob = _link_read_bytes(v["channel_slug"], v["link_file"])
        if blob is None:
            if _link_exists(v["channel_slug"], v["link_file"]):
                flash("Transcript ada tapi gagal dibaca dari shard. Coba restart web app.", "warning")
            else:
                flash("File TXT tidak ditemukan.", "warning")
            return redirect(url_for("video_detail", video_pk=video_pk))

        if raw:
            headers = {"Content-Disposition": f'attachment; filename="{v["video_id"]}.txt"'}
            return Response(blob, headers=headers, mimetype="text/plain; charset=utf-8")

        txt = blob.decode("utf-8", errors="ignore")
        src = _link_source(v["channel_slug"], v["link_file"])
        text_empty = len(txt.strip()) == 0
        return render_template("video_text.html", v=v, text=txt, path=src, text_empty=text_empty)

    @app.route("/videos/<int:video_pk>/resume", methods=["POST"])
    def video_resume_save(video_pk: int) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.id, v.video_id, v.seq_num, v.link_resume, v.link_file, c.slug AS channel_slug
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)

            resume_text = (request.form.get("resume_text") or "").rstrip() + "\n"
            link_resume = v["link_resume"] or _default_resume_link(v["video_id"], v["seq_num"], v["link_file"])
            if not link_resume:
                flash("Gagal menyimpan resume: seq_num belum tersedia.", "danger")
                return redirect(url_for("video_detail", video_pk=video_pk))
            p = _resolve_resume_path(v["channel_slug"], link_resume)
            if not p:
                flash("Gagal resolve path resume.", "danger")
                return redirect(url_for("video_detail", video_pk=video_pk))

            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(resume_text, encoding="utf-8")
            with con:
                con.execute(
                    "UPDATE videos SET link_resume=? WHERE id=?",
                    (str(Path(link_resume)), video_pk),
                )
        flash("Resume tersimpan.", "success")
        return redirect(url_for("video_detail", video_pk=video_pk))

    @app.route("/videos/<int:video_pk>/resume.md")
    def video_resume_raw(video_pk: int) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.id, v.video_id, v.seq_num, v.link_resume, v.link_file, c.slug AS channel_slug
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)
        link_resume = v["link_resume"] or _default_resume_link(v["video_id"], v["seq_num"], v["link_file"])
        if not link_resume:
            abort(404)
        blob = _link_read_bytes(v["channel_slug"], link_resume)
        if blob is None:
            if _link_exists(v["channel_slug"], link_resume):
                flash("Resume ada tapi gagal dibaca dari shard. Coba restart web app.", "warning")
                return redirect(url_for("video_detail", video_pk=video_pk))
            abort(404)
        headers = {"Content-Disposition": f'attachment; filename="{v["video_id"]}.md"'}
        return Response(blob, headers=headers, mimetype="text/markdown; charset=utf-8")

    @app.route("/videos/<int:video_pk>/resume/read")
    def video_resume_read(video_pk: int) -> Any:
        prev_v: Optional[Dict[str, Any]] = None
        next_v: Optional[Dict[str, Any]] = None
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute(
                """
                SELECT v.id, v.video_id, v.title, v.seq_num, v.link_resume, v.link_file,
                       v.channel_id, c.slug AS channel_slug
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE v.id=?
                """,
                (video_pk,),
            ).fetchone()
            if not v:
                abort(404)

            current_seq = _derive_seq(v["seq_num"], v["link_file"], v["link_resume"])
            if current_seq is not None:
                prev_row = con.execute(
                    """
                    SELECT v.id, v.video_id, v.title, v.seq_num
                    FROM videos v
                    WHERE v.channel_id=?
                      AND IFNULL(v.link_resume,'') != ''
                      AND v.seq_num IS NOT NULL
                      AND (v.seq_num < ? OR (v.seq_num = ? AND v.id < ?))
                    ORDER BY v.seq_num DESC, v.id DESC
                    LIMIT 1
                    """,
                    (v["channel_id"], int(current_seq), int(current_seq), v["id"]),
                ).fetchone()
                next_row = con.execute(
                    """
                    SELECT v.id, v.video_id, v.title, v.seq_num
                    FROM videos v
                    WHERE v.channel_id=?
                      AND IFNULL(v.link_resume,'') != ''
                      AND v.seq_num IS NOT NULL
                      AND (v.seq_num > ? OR (v.seq_num = ? AND v.id > ?))
                    ORDER BY v.seq_num ASC, v.id ASC
                    LIMIT 1
                    """,
                    (v["channel_id"], int(current_seq), int(current_seq), v["id"]),
                ).fetchone()
            else:
                # Fallback when seq_num cannot be derived.
                prev_row = con.execute(
                    """
                    SELECT v.id, v.video_id, v.title, v.seq_num
                    FROM videos v
                    WHERE v.channel_id=?
                      AND IFNULL(v.link_resume,'') != ''
                      AND v.id < ?
                    ORDER BY v.id DESC
                    LIMIT 1
                    """,
                    (v["channel_id"], v["id"]),
                ).fetchone()
                next_row = con.execute(
                    """
                    SELECT v.id, v.video_id, v.title, v.seq_num
                    FROM videos v
                    WHERE v.channel_id=?
                      AND IFNULL(v.link_resume,'') != ''
                      AND v.id > ?
                    ORDER BY v.id ASC
                    LIMIT 1
                    """,
                    (v["channel_id"], v["id"]),
                ).fetchone()

            if prev_row:
                prev_v = {
                    "id": int(prev_row["id"]),
                    "video_id": prev_row["video_id"],
                    "title": prev_row["title"],
                    "seq_num": int(prev_row["seq_num"]) if prev_row["seq_num"] is not None else None,
                }
            if next_row:
                next_v = {
                    "id": int(next_row["id"]),
                    "video_id": next_row["video_id"],
                    "title": next_row["title"],
                    "seq_num": int(next_row["seq_num"]) if next_row["seq_num"] is not None else None,
                }

        link_resume = v["link_resume"] or _default_resume_link(v["video_id"], v["seq_num"], v["link_file"])
        if not link_resume:
            abort(404)
        mtime_ts = _link_mtime(v["channel_slug"], link_resume)
        cache_key = ""
        if mtime_ts is not None:
            cache_key = f"{v['channel_slug']}|{link_resume}|{int(mtime_ts)}"
        cached = _resume_cache_get(cache_key) if cache_key else None
        if cached:
            html, resume_empty = cached
        else:
            txt = _link_read_text(v["channel_slug"], link_resume)
            if txt is None:
                if _link_exists(v["channel_slug"], link_resume):
                    flash("Resume ada tapi gagal dibaca dari shard. Coba restart web app.", "warning")
                    return redirect(url_for("video_detail", video_pk=video_pk))
                abort(404)
            resume_empty = len(txt.strip()) == 0
            if resume_empty:
                html = "<p class='text-light-emphasis mb-0'>Resume masih kosong.</p>"
            else:
                html = markdown.markdown(txt, extensions=["extra", "nl2br", "sane_lists"])
            if cache_key:
                _resume_cache_put(cache_key, html, resume_empty)
        mtime = ""
        if mtime_ts:
            mtime = datetime.fromtimestamp(mtime_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        return render_template(
            "video_resume_read.html",
            v=v,
            content=html,
            resume_empty=resume_empty,
            mtime=mtime,
            prev_v=prev_v,
            next_v=next_v,
        )

    @app.route("/videos/<int:video_pk>/read", methods=["POST"])
    def video_read_set(video_pk: int) -> Any:
        is_fetch = (request.headers.get("X-Requested-With") or "").lower() == "fetch"
        nxt = (request.form.get("next") or request.referrer or "").strip()
        read = (request.form.get("read") or "").strip() == "1"
        with db.session(app.config["DB_PATH"]) as con:
            v = con.execute("SELECT id FROM videos WHERE id=?", (video_pk,)).fetchone()
            if not v:
                abort(404)
            with con:
                if read:
                    con.execute("UPDATE videos SET read_at=? WHERE id=?", (now_iso(), video_pk))
                else:
                    con.execute("UPDATE videos SET read_at=NULL WHERE id=?", (video_pk,))
        if is_fetch:
            return ("", 204)
        flash("Ditandai sudah dibaca." if read else "Ditandai belum dibaca.", "success")
        if nxt.startswith("/"):
            return redirect(nxt)
        return redirect(url_for("video_detail", video_pk=video_pk))

    @app.route("/videos/<int:video_pk>/categories", methods=["POST"])
    def video_categories_save(video_pk: int) -> Any:
        ids = request.form.getlist("category_ids")
        ids_int = [int(x) for x in ids if x.isdigit()]
        with db.session(app.config["DB_PATH"]) as con:
            with con:
                con.execute("DELETE FROM video_categories WHERE video_pk=?", (video_pk,))
                for cid in sorted(set(ids_int)):
                    con.execute(
                        "INSERT OR IGNORE INTO video_categories(video_pk, category_id) VALUES (?, ?)",
                        (video_pk, cid),
                    )
        flash("Kategori diperbarui.", "success")
        return redirect(url_for("video_detail", video_pk=video_pk))

    @app.route("/videos/categories/bulk", methods=["POST"])
    def video_categories_bulk() -> Any:
        video_pks_raw = request.form.getlist("video_pks")
        cat_ids_raw = request.form.getlist("category_ids")
        mode = (request.form.get("mode") or "add").strip().lower()

        video_pks = sorted({int(x) for x in video_pks_raw if str(x).isdigit()})
        cat_ids = sorted({int(x) for x in cat_ids_raw if str(x).isdigit()})

        if not video_pks:
            flash("Pilih minimal 1 video.", "danger")
            return redirect(request.referrer or url_for("channels"))

        if mode not in ("add", "replace"):
            mode = "add"

        with db.session(app.config["DB_PATH"]) as con:
            def chunks(xs: List[int], n: int = 900) -> List[List[int]]:
                return [xs[i : i + n] for i in range(0, len(xs), n)]

            # Validate video ids exist (and avoid operating on arbitrary ids).
            valid_set = set()
            for part in chunks(video_pks, 900):
                placeholders = ",".join("?" for _ in part)
                rows = con.execute(
                    f"SELECT id FROM videos WHERE id IN ({placeholders})", tuple(part)
                ).fetchall()
                valid_set.update(int(r[0]) for r in rows)
            valid = sorted(valid_set)
            if not valid:
                flash("Video tidak ditemukan.", "danger")
                return redirect(request.referrer or url_for("channels"))

            with con:
                if mode == "replace":
                    for part in chunks(valid, 900):
                        placeholders2 = ",".join("?" for _ in part)
                        con.execute(
                            f"DELETE FROM video_categories WHERE video_pk IN ({placeholders2})",
                            tuple(part),
                        )
                if cat_ids:
                    for part in chunks(valid, 300):
                        pairs = [(vp, cid) for vp in part for cid in cat_ids]
                        con.executemany(
                            "INSERT OR IGNORE INTO video_categories(video_pk, category_id) VALUES (?, ?)",
                            pairs,
                        )

        if mode == "replace" and not cat_ids:
            flash(f"Kategori dihapus untuk {len(valid)} video.", "success")
        else:
            action = "ditambahkan ke" if mode == "add" else "di-set untuk"
            flash(f"{len(cat_ids)} kategori {action} {len(valid)} video.", "success")

        nxt = (request.form.get("next") or "").strip()
        if nxt.startswith("/"):
            return redirect(nxt)
        return redirect(request.referrer or url_for("channels"))

    @app.route("/categories", methods=["GET", "POST"])
    def categories() -> Any:
        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            color = (request.form.get("color") or "").strip()
            if not name:
                flash("Nama kategori wajib.", "danger")
                return redirect(url_for("categories"))
            if len(name) > 64:
                flash("Nama kategori terlalu panjang (maks 64).", "danger")
                return redirect(url_for("categories"))
            if color and not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
                flash("Color harus format hex seperti #1a2b3c.", "danger")
                return redirect(url_for("categories"))
            with db.session(app.config["DB_PATH"]) as con:
                with con:
                    con.execute(
                        "INSERT INTO categories(name, color, created_at) VALUES(?, ?, ?) "
                        "ON CONFLICT(name) DO UPDATE SET color=excluded.color;",
                        (name, color or None, now_iso()),
                    )
            flash("Kategori tersimpan.", "success")
            return redirect(url_for("categories"))

        with db.session(app.config["DB_PATH"]) as con:
            rows = con.execute(
                """
                SELECT
                  c.*,
                  (SELECT COUNT(*) FROM video_categories vc WHERE vc.category_id=c.id) AS usage_count
                FROM categories c
                ORDER BY c.name ASC
                """
            ).fetchall()
        return render_template("categories.html", categories=rows)

    @app.route("/categories/<int:category_id>/delete", methods=["POST"])
    def category_delete(category_id: int) -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            with con:
                con.execute("UPDATE channels SET category_id=NULL WHERE category_id=?", (category_id,))
                con.execute("DELETE FROM categories WHERE id=?", (category_id,))
        flash("Kategori dihapus.", "info")
        return redirect(url_for("categories"))

    @app.route("/db/cleanup", methods=["POST"])
    def db_cleanup() -> Any:
        with db.session(app.config["DB_PATH"]) as con:
            reason = _cleanup_block_reason(con)
        if not reason:
            reason = db.maintenance_blocker_reason(app.config["DB_PATH"])
        if reason:
            flash(f"DB cleanup diblokir: {reason}", "warning")
            return redirect(url_for("channels"))

        with db.session(app.config["DB_PATH"]) as con:
            db.wal_checkpoint_truncate(con)
        db.cleanup_wal_shm_files(app.config["DB_PATH"])
        flash("DB checkpoint + cleanup (-wal/-shm) selesai.", "success")
        return redirect(url_for("channels"))

    @app.route("/db/migrate-resumes", methods=["POST"])
    def db_migrate_resumes() -> Any:
        migrated = 0
        with db.session(app.config["DB_PATH"]) as con:
            cols = [r["name"] for r in con.execute("PRAGMA table_info(videos)").fetchall()]
            if "resume_text" not in cols:
                flash("Tidak ada kolom resume_text di DB (tidak perlu migrasi).", "info")
                return redirect(url_for("channels"))

            rows = con.execute(
                """
                SELECT v.id, v.video_id, v.seq_num, v.link_file, v.resume_text, c.slug AS channel_slug
                FROM videos v
                JOIN channels c ON c.id=v.channel_id
                WHERE IFNULL(v.resume_text,'') != ''
                  AND (v.link_resume IS NULL OR v.link_resume = '')
                ORDER BY v.id ASC
                """
            ).fetchall()

            with con:
                for r in rows:
                    link_resume = _default_resume_link(r["video_id"], r["seq_num"], r["link_file"])
                    if not link_resume:
                        continue
                    p = _resolve_resume_path(r["channel_slug"], link_resume)
                    if not p:
                        continue
                    try:
                        p.parent.mkdir(parents=True, exist_ok=True)
                        txt = (r["resume_text"] or "").rstrip() + "\n"
                        p.write_text(txt, encoding="utf-8")
                    except Exception:
                        continue
                    con.execute(
                        "UPDATE videos SET link_resume=?, resume_text=NULL, resume_updated_at=NULL WHERE id=?",
                        (str(Path(link_resume)), int(r["id"])),
                    )
                    migrated += 1

        flash(f"Migrasi resume ke file .md selesai: {migrated} video.", "success")
        return redirect(url_for("channels"))

    return app


app = create_app()


if __name__ == "__main__":
    # Local dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
