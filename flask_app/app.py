#!/usr/bin/env python3
"""
Flask Application untuk YouTube Transcript Manager
Web interface untuk menampilkan dan mengelola transkrip YouTube
"""

import os
import sys
import re
import json
import gzip
import time
import threading
import hashlib
import tempfile
import html
from pathlib import Path
from datetime import datetime
from flask import Flask, make_response, Response, render_template, render_template_string, request, jsonify, redirect, url_for, send_from_directory, g
from werkzeug.middleware.proxy_fix import ProxyFix
import requests

try:
    import markdown  # type: ignore
except ModuleNotFoundError:
    markdown = None

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database_optimized import OptimizedDatabase
from orchestrator.config import load_config
from orchestrator.doctor import build_doctor_report
from orchestrator.state import OrchestratorState

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

# Database configuration
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'youtube_transcripts.db')
BASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'uploads')
REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / 'logs'
ADMIN_JOB_LIST_LIMIT = 20
ADMIN_RECENT_JOB_DAYS = int(os.getenv("WEBAPP_ADMIN_RECENT_JOB_DAYS", "2"))
PUBLIC_PAGE_CACHE_TTL_S = float(os.getenv("WEBAPP_PUBLIC_PAGE_CACHE_TTL_S", "3600"))
PUBLIC_STATS_CACHE_TTL_S = float(os.getenv("WEBAPP_PUBLIC_STATS_CACHE_TTL_S", "3600"))
VIDEO_PAGE_CACHE_TTL_S = float(os.getenv("WEBAPP_VIDEO_PAGE_CACHE_TTL_S", "3600"))
ADMIN_DATA_CACHE_TTL_S = float(os.getenv("WEBAPP_ADMIN_DATA_CACHE_TTL_S", "60"))
ADMIN_DATA_JOBS_CACHE_TTL_S = float(os.getenv("WEBAPP_ADMIN_DATA_JOBS_CACHE_TTL_S", "30"))
ADMIN_STALE_QUEUED_JOB_SECONDS = float(os.getenv("WEBAPP_ADMIN_STALE_QUEUED_JOB_SECONDS", "900"))
ADMIN_STALE_RUNNING_JOB_SECONDS = float(os.getenv("WEBAPP_ADMIN_STALE_RUNNING_JOB_SECONDS", "1800"))
_PUBLIC_CACHE: dict[tuple, tuple[float, object]] = {}
PAGE_HTML_CACHE_DIR = REPO_ROOT / "tmp" / "page_cache"
PAGE_HTML_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_DB_SINGLETON: OptimizedDatabase | None = None
_PREWARM_STARTED = False

def get_db():
    """
    Get or create database connection for current request
    Flask g object is request-scoped, so this ensures thread safety
    """
    global _DB_SINGLETON
    if _DB_SINGLETON is None:
        _DB_SINGLETON = OptimizedDatabase(DB_PATH, BASE_DIR)
    if 'db' not in g:
        g.db = _DB_SINGLETON
    return g.db


def _normalize_channel_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _video_is_member_only(video: dict) -> bool:
    if video.get('is_member_only'):
        return True
    metadata_raw = video.get('metadata')
    if not metadata_raw:
        return False
    try:
        metadata = json.loads(metadata_raw)
    except Exception:
        return False
    flags = metadata.get('flags') or {}
    return bool(
        metadata.get('member_only')
        or flags.get('member_only')
        or metadata.get('upload_date_reason') == 'member_only'
        or flags.get('upload_date_reason') == 'member_only'
    )


def _manual_transcript_retry_available(video: dict) -> bool:
    if not video:
        return False
    if video.get('is_short'):
        return False
    if _video_is_member_only(video):
        return False
    transcript_language = str(video.get('transcript_language') or '').strip().lower()
    return not bool(video.get('transcript_downloaded')) and transcript_language != 'blocked'


def _manual_transcript_job_running(video_id: str) -> bool:
    try:
        return bool(_manual_transcript_active_job(video_id))
    except Exception:
        return False


def _manual_transcript_active_job(video_id: str) -> dict | None:
    try:
        return get_db().get_active_admin_job(target_video_id=str(video_id or "").strip(), job_type="transcript")
    except Exception:
        return None


def _launch_manual_transcript_retry(video: dict) -> tuple[str, str]:
    video_id = str(video.get('video_id') or '').strip()
    if not video_id:
        raise ValueError("video_id is required")

    safe_video_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", video_id) or "video"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = f"manual_transcript_{ts}_{_secrets.token_hex(3)}"
    run_dir = str(Path(__file__).resolve().parents[1] / "runs" / f"manual_transcript_{ts}_{safe_video_id}")
    log_path = str(Path(__file__).resolve().parents[1] / "logs" / f"{job_id}.log")
    script = str(Path(__file__).resolve().parents[1] / "scripts" / "manual_transcript_then_resume_format.sh")
    cmd = ["bash", script, "--video-id", video_id, "--run-dir", run_dir]

    _record_admin_job(
        job_id,
        "transcript",
        "queued",
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source="video_page",
        target_video_id=video_id,
    )
    _start_admin_job_process(
        job_id=job_id,
        job_type="transcript",
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source="video_page",
        target_video_id=video_id,
    )
    return job_id, run_dir


def _resolve_channel_record(db: OptimizedDatabase, channel_id: str) -> dict | None:
    """
    Resolve a channel from its canonical id or a looser slug/name variant.

    This keeps public URLs resilient when the incoming path is a channel slug
    instead of the exact stored `channel_id`.
    """
    channel = db.get_channel_by_id(channel_id)
    if channel:
        return channel

    wanted = _normalize_channel_token(channel_id)
    if not wanted:
        return None

    for row in db.get_all_channels():
        candidate_id = _normalize_channel_token(row.get('channel_id'))
        candidate_name = _normalize_channel_token(row.get('channel_name'))
        if wanted in {candidate_id, candidate_name}:
            return row
    return None


def _channel_detail_payload(channel_id: str, page: int, per_page: int, offset: int, stats_version: int | None = None):
    db = get_db()
    channel = _resolve_channel_record(db, channel_id)
    if channel is None:
        return render_template(
            'error.html',
            error=f"Channel dengan ID '{channel_id}' tidak ditemukan",
        ), 404

    resolved_channel_id = str(channel.get('channel_id') or channel_id)
    stats_version = int(stats_version if stats_version is not None else _public_data_cache_version())
    videos = _cache_get(
        ("channel_videos", stats_version, resolved_channel_id, page, per_page, offset),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: db.get_videos_by_channel(resolved_channel_id, limit=per_page, offset=offset),
    )
    total_videos = _cache_get(
        ("channel_total_videos", stats_version, resolved_channel_id),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: db.get_total_videos_by_channel(resolved_channel_id),
    )
    transcript_total = _cache_get(
        ("channel_transcript_total", stats_version, resolved_channel_id),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: db.get_transcript_count_by_channel(resolved_channel_id),
    )

    return render_template(
        'channel_detail.html',
        channel=channel,
        videos=videos,
        page=page,
        total_videos=total_videos,
        transcript_total=transcript_total,
        transcript_page_count=sum(
            1 for video in videos
            if video.get('transcript_downloaded')
        ),
        total_pages=(total_videos + per_page - 1) // per_page,
    )


def _index_payload():
    stats_version = _public_data_cache_version()
    stats_start = time.perf_counter()
    stats = _cache_get(("stats", stats_version), PUBLIC_STATS_CACHE_TTL_S, lambda: get_db().get_statistics())
    _record_server_timing("stats", time.perf_counter() - stats_start)

    latest_start = time.perf_counter()
    latest_videos = _cache_get(
        ("latest_videos_global", stats_version, 12),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: get_db().get_latest_videos(limit=12),
    )
    _record_server_timing("latest", time.perf_counter() - latest_start)

    channels_start = time.perf_counter()
    home_channels = _cache_get(
        ("home_channels", stats_version, 6),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: get_db().get_all_channels(limit=6),
    )
    _record_server_timing("channels", time.perf_counter() - channels_start)

    render_start = time.perf_counter()
    html_output = render_template(
        'index.html',
        stats=stats,
        latest_videos=latest_videos,
        channels=home_channels,
    )
    _record_server_timing("render", time.perf_counter() - render_start)
    return html_output


def _video_detail_payload(video_id: str):
    db = get_db()
    video = db.get_video_by_id(video_id)

    if not video:
        return render_template('error.html',
                            error=f"Video dengan ID '{video_id}' tidak ditemukan"), 404

    if video.get('is_short'):
        return render_template('error.html',
                            error=f"Video ini adalah Shorts dan tidak ditampilkan sesuai kebijakan."), 403
    if _video_is_member_only(video):
        return render_template('error.html',
                            error=f"Video ini member-only dan tidak ditampilkan sesuai kebijakan."), 403

    transcript_content = db.read_transcript(video_id)
    summary_content = db.read_summary(video_id)
    formatted_transcript_content = db.read_formatted_transcript(video_id)
    stats_version = _public_data_cache_version()
    adjacent_videos = _cache_get(
        ("video_adjacent", stats_version, video_id),
        VIDEO_PAGE_CACHE_TTL_S,
        lambda: db.get_adjacent_videos_by_video_id(video_id),
    )
    manual_transcript_retry_available = _manual_transcript_retry_available(video)
    active_manual_job = _manual_transcript_active_job(video_id)
    manual_transcript_running = bool(active_manual_job)
    manual_transcript_job = request.args.get("manual_transcript_job", "").strip() or (
        str(active_manual_job.get("job_id", "")).strip() if active_manual_job else ""
    )
    manual_transcript_message = request.args.get("manual_transcript_message", "").strip() or (
        "Manual download sedang berjalan. Resume dan format akan dilanjutkan otomatis setelah selesai."
        if active_manual_job
        else ""
    )
    age_restricted = False
    transcript_source = ""
    upload_date_status = ""
    upload_date_reason = ""
    upload_date_source = ""
    metadata_raw = video.get('metadata')
    if metadata_raw:
        try:
            metadata = json.loads(metadata_raw)
            age_restricted = bool(
                metadata.get('age_restricted')
                or (metadata.get('flags') or {}).get('age_restricted')
            )
            transcript_source = str(
                metadata.get('transcript_source')
                or (metadata.get('flags') or {}).get('transcript_source')
                or ''
            ).strip()
            upload_date_status = str(metadata.get('upload_date_status') or '').strip()
            upload_date_reason = str(metadata.get('upload_date_reason') or '').strip()
            upload_date_source = str(metadata.get('upload_date_source') or '').strip()
        except Exception:
            age_restricted = False
            transcript_source = ""
            upload_date_status = ""
            upload_date_reason = ""
            upload_date_source = ""

    summary_html = None
    if summary_content:
        summary_html = render_summary_html(summary_content)

    return render_template('video_detail.html',
                         video=video,
                         transcript=transcript_content,
                         formatted_transcript=formatted_transcript_content,
                         summary=summary_html,
                         age_restricted=age_restricted,
                         transcript_source=transcript_source,
                         manual_transcript_retry_available=manual_transcript_retry_available,
                         manual_transcript_job=manual_transcript_job,
                         manual_transcript_message=manual_transcript_message,
                         upload_date_status=upload_date_status,
                         upload_date_reason=upload_date_reason,
                         upload_date_source=upload_date_source,
                         adjacent_videos=adjacent_videos,
                         previous_video=adjacent_videos.get('previous'),
                         next_video=adjacent_videos.get('next'),
                         manual_transcript_running=manual_transcript_running)


@app.route('/video/<video_id>')
def video_detail_cached(video_id):
    """Halaman detail video dengan cache halaman penuh."""
    if (
        (request.args.get("manual_transcript_job") or "").strip()
        or (request.args.get("manual_transcript_message") or "").strip()
        or _manual_transcript_job_running(video_id)
    ):
        # Jangan cache state transien manual transcript. Kalau tidak, halaman
        # yang selesai sukses bisa tetap memakai HTML lama dan memicu polling ulang.
        return _video_detail_payload(video_id)
    stats_version = _public_data_cache_version()
    return _render_cached_page(
        ("video_detail", stats_version, video_id),
        VIDEO_PAGE_CACHE_TTL_S,
        lambda: _video_detail_payload(video_id),
    )


@app.route('/video/<video_id>/manual-transcript', methods=['POST'])
def manual_transcript_retry(video_id):
    db = get_db()
    video = db.get_video_by_id(video_id)

    if not video:
        return render_template('error.html',
                            error=f"Video dengan ID '{video_id}' tidak ditemukan"), 404
    if video.get('is_short') or _video_is_member_only(video):
        return render_template('error.html',
                            error=f"Video ini tidak memenuhi syarat untuk retry transcript."), 403
    if not _manual_transcript_retry_available(video):
        return redirect(url_for(
            'video_detail_cached',
            video_id=video_id,
            manual_transcript_message='Manual download hanya tersedia untuk video publik yang belum punya transcript.',
        ))
    if _manual_transcript_job_running(video_id):
        return redirect(url_for(
            'video_detail_cached',
            video_id=video_id,
            manual_transcript_message='Manual download sedang berjalan. Resume dan format akan dilanjutkan otomatis setelah selesai.',
        ))

    try:
        job_id, _run_dir = _launch_manual_transcript_retry(video)
    except Exception as exc:
        return redirect(url_for(
            'video_detail_cached',
            video_id=video_id,
            manual_transcript_message=f'Gagal menjalankan manual download: {exc}',
        ))

    return redirect(url_for(
        'video_detail_cached',
        video_id=video_id,
        manual_transcript_job=job_id,
        manual_transcript_message=f'Manual download dimulai (job: {job_id})',
    ))


def _public_page_prewarm():
    """Warm the hottest public pages once per process start."""
    try:
        targets = ['/', '/channels', '/videos']
        for path in targets:
            try:
                with app.test_request_context(path, method='GET'):
                    if path == '/':
                        index()
                    elif path == '/channels':
                        channels()
                    elif path == '/videos':
                        videos()
            except Exception:
                # Prewarm is best-effort only; do not block startup.
                continue
    except Exception:
        pass


def start_public_page_prewarm():
    global _PREWARM_STARTED
    if _PREWARM_STARTED:
        return
    _PREWARM_STARTED = True
    # Run prewarm in a daemon background thread so the first live
    # request is NOT blocked waiting for DB queries to complete.
    # A small delay gives the WSGI process time to finish startup.
    def _run():
        import time as _time
        _time.sleep(2)  # let Passenger finish initializing
        _public_page_prewarm()
    threading.Thread(target=_run, daemon=True).start()


def _cache_get(key: tuple, ttl_s: float, producer):
    now = time.monotonic()
    cached = _PUBLIC_CACHE.get(key)
    if cached is not None:
        cached_at, value = cached
        if now - cached_at <= ttl_s:
            return value
    value = producer()
    _PUBLIC_CACHE[key] = (now, value)
    return value


def _invalidate_admin_job_caches() -> None:
    for key in list(_PUBLIC_CACHE.keys()):
        if not key:
            continue
        head = key[0]
        if isinstance(head, str) and (
            head.startswith("admin_")
            or head.startswith("job_")
        ):
            _PUBLIC_CACHE.pop(key, None)


def _record_server_timing(label: str, duration_s: float):
    parts = getattr(g, 'server_timing_parts', None)
    if parts is None:
        parts = []
        g.server_timing_parts = parts
    parts.append(f'{label};dur={duration_s * 1000.0:.1f}')


def _public_data_cache_version() -> int:
    try:
        return int(get_db().get_statistics_version())
    except Exception:
        return 0


def _cache_key_to_path(key: tuple) -> Path:
    payload = repr(key).encode("utf-8", "replace")
    digest = hashlib.sha256(payload).hexdigest()
    return PAGE_HTML_CACHE_DIR / f"{digest}.html"


def _render_cached_page(key: tuple, ttl_s: float, producer):
    cache_path = _cache_key_to_path(key)
    try:
        stat = cache_path.stat()
        age = time.time() - stat.st_mtime
        if age <= ttl_s:
            return cache_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        pass
    except OSError:
        pass

    produced = producer()
    if isinstance(produced, tuple):
        return produced

    html_text = produced
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=PAGE_HTML_CACHE_DIR,
        delete=False,
        prefix=f"{cache_path.stem}.",
        suffix=".tmp",
    ) as tmp_file:
        tmp_file.write(html_text)
        tmp_path = Path(tmp_file.name)
    tmp_path.replace(cache_path)
    return html_text


PUBLIC_CACHE_ENDPOINT_TTLS = {
    'index': 60,
    'channels': 60,
    'channel_detail': 60,
    'videos': 60,
    'video_detail_cached': 300,
    'video_detail': 60,
    'search': 30,
}

ADMIN_PRIVATE_CACHE_ENDPOINT_TTLS = {
    'admin_data_page': 10,
    'admin_data_fragment_channels': 10,
    'admin_data_fragment_jobs': 10,
    'admin_data_jobs': 5,
    'admin_data_logs': 5,
}


@app.after_request
def add_public_cache_headers(response):
    if (
        request.method in ('GET', 'HEAD')
        and response.status_code == 200
        and request.endpoint in PUBLIC_CACHE_ENDPOINT_TTLS
    ):
        ttl = PUBLIC_CACHE_ENDPOINT_TTLS[request.endpoint]
        response.headers['Cache-Control'] = f'public, max-age={ttl}, stale-while-revalidate={ttl * 5}, no-transform'
        vary = response.headers.get('Vary', '')
        if 'Accept-Encoding' not in vary:
            response.headers['Vary'] = (vary + ', Accept-Encoding').strip(', ')
    if (
        request.method in ('GET', 'HEAD')
        and response.status_code == 200
        and request.endpoint in ADMIN_PRIVATE_CACHE_ENDPOINT_TTLS
    ):
        ttl = ADMIN_PRIVATE_CACHE_ENDPOINT_TTLS[request.endpoint]
        response.headers['Cache-Control'] = f'private, max-age={ttl}, stale-while-revalidate={ttl * 5}, no-transform'
        vary = response.headers.get('Vary', '')
        if 'Cookie' not in vary:
            response.headers['Vary'] = (vary + ', Cookie').strip(', ')
    if (
        request.method == 'GET'
        and 'gzip' in request.headers.get('Accept-Encoding', '').lower()
        and response.headers.get('Content-Encoding') is None
        and response.mimetype == 'text/html'
    ):
        body = response.get_data()
        if len(body) >= 4096:
            compressed = gzip.compress(body, compresslevel=6)
            if len(compressed) < len(body):
                response.set_data(compressed)
                response.headers['Content-Encoding'] = 'gzip'
                response.headers['Content-Length'] = str(len(compressed))
    return response


@app.before_request
def _block_bots_before_request():
    ua = request.headers.get("User-Agent", "").lower()
    for bot in {"gptbot","chatgpt-user","google-extended","ccbot","claudebot","anthropic-ai","facebookbot","bytespider","petalbot","ahrefsbot","semrushbot","dotbot","mj12bot","baiduspider","megaindex"}:
        if bot in ua:
            return ("Forbidden", 403)

def mark_request_start():
    g.request_started_at = time.perf_counter()


@app.after_request
def add_server_timing(response):
    if (
        request.endpoint in PUBLIC_CACHE_ENDPOINT_TTLS
        or request.endpoint in ADMIN_PRIVATE_CACHE_ENDPOINT_TTLS
    ) and response.status_code == 200:
        started_at = getattr(g, 'request_started_at', None)
        parts = list(getattr(g, 'server_timing_parts', []))
        if started_at is not None:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            parts.append(f'app;dur={duration_ms:.1f}')
        else:
            parts.append('app')
        if parts:
            response.headers['Server-Timing'] = ', '.join(parts)
    return response

@app.teardown_appcontext
def close_db(exception):
    """
    Close database connection after request is complete
    """
    g.pop('db', None)


def render_summary_html(summary_content: str) -> str:
    """
    Render summary Markdown when the optional dependency is installed.
    Fall back to escaped plain text so the app still starts without markdown.
    """
    if markdown is not None:
        return markdown.markdown(
            summary_content,
            extensions=['extra', 'codehilite', 'toc']
        )
    return html.escape(summary_content).replace("\n", "<br>\n")


def _resolve_admin_log_path(raw_path: str) -> Path | None:
    raw = str(raw_path or '').strip()
    if not raw:
        return None

    candidates = []
    path = Path(raw)
    if path.is_absolute():
        candidates.append(path)
    else:
        candidates.append(REPO_ROOT / path)
        if path.parts and path.parts[0] == 'logs':
            candidates.append(LOGS_DIR / Path(*path.parts[1:]))
        if path.parts and path.parts[0] == 'runs':
            candidates.append(REPO_ROOT / path)

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            continue
        if not resolved.exists() or not resolved.is_file():
            continue
        try:
            resolved.relative_to(REPO_ROOT)
        except ValueError:
            continue
        return resolved
    return None


def _admin_job_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _admin_job_has_terminal_log(log_path: Path | None) -> bool:
    if log_path is None:
        return False
    try:
        if not log_path.exists() or not log_path.is_file():
            return False
        tail = log_path.read_text(encoding='utf-8', errors='ignore')[-4096:]
    except Exception:
        return False
    markers = (
        '❌',
        'failed',
        'error',
        'traceback',
        'exit code',
        'status=failed',
        'status failed',
    )
    lowered = tail.lower()
    return any(marker in lowered for marker in markers)


def _reconcile_stale_admin_jobs() -> bool:
    db = get_db()
    cutoff = time.time() - max(ADMIN_STALE_QUEUED_JOB_SECONDS, ADMIN_STALE_RUNNING_JOB_SECONDS)
    stale_jobs = db.list_admin_jobs(limit=500)
    changed = False
    for job in stale_jobs:
        status = str(job.get('status') or '').strip().lower()
        if status not in {'queued', 'running', 'in_progress'}:
            continue

        pid = job.get('pid')
        try:
            pid_int = int(pid) if pid not in (None, '') else None
        except Exception:
            pid_int = None

        started_at_raw = str(job.get('started_at') or '').strip()
        updated_at_raw = str(job.get('updated_at') or '').strip()
        age_ref = updated_at_raw or started_at_raw
        
        # Perlindungan: Jika job baru dimulai (< 5 menit), jangan tandai stale dulu
        # Ini memberi waktu bagi sistem untuk menuliskan PID/update awal.
        job_timestamp = 0
        if age_ref:
            try:
                # SQLite CURRENT_TIMESTAMP is UTC 'YYYY-MM-DD HH:MM:SS'
                # Ensure it's treated as UTC for timestamp comparison
                age_ref_norm = age_ref
                if ' ' in age_ref_norm and '+' not in age_ref_norm and 'Z' not in age_ref_norm:
                    age_ref_norm = age_ref_norm.replace(' ', 'T') + '+00:00'
                
                parsed = datetime.fromisoformat(age_ref_norm.replace('Z', '+00:00'))
                job_timestamp = parsed.timestamp()
                if (time.time() - job_timestamp) < 300:
                    continue
            except Exception:
                pass

        age_ok = True
        if age_ref:
            try:
                age_ref_norm = age_ref
                if ' ' in age_ref_norm and '+' not in age_ref_norm and 'Z' not in age_ref_norm:
                    age_ref_norm = age_ref_norm.replace(' ', 'T') + '+00:00'
                parsed = datetime.fromisoformat(age_ref_norm.replace('Z', '+00:00'))
                age_ok = parsed.timestamp() >= cutoff
            except Exception:
                age_ok = True

        alive = _admin_job_is_alive(pid_int)
        if alive and age_ok:
            continue

        log_path = _resolve_admin_log_path(str(job.get('log_path') or ''))
        terminal_log = _admin_job_has_terminal_log(log_path)
        
        # FIX: Jika proses masih ALIVE, jangan ditandai failed meskipun ada error di log.
        # Bisa jadi satu video gagal tapi proses batch masih berlanjut.
        if alive:
            continue

        error_message = str(job.get('error_message') or '').strip()
        if not error_message:
            if terminal_log:
                error_message = 'stale job marked failed from terminal log'
            else:
                error_message = 'job stalled or process exited'

        db.upsert_admin_job(
            str(job.get('job_id') or ''),
            str(job.get('job_type') or 'job'),
            'failed',
            source=str(job.get('source') or 'wrapper'),
            pid=pid_int,
            command=str(job.get('command') or ''),
            log_path=str(job.get('log_path') or ''),
            run_dir=str(job.get('run_dir') or ''),
            target_channel_id=str(job.get('target_channel_id') or ''),
            target_video_id=str(job.get('target_video_id') or ''),
            exit_code=int(job.get('exit_code') or 1) if str(job.get('exit_code') or '').strip() else 1,
            error_message=error_message,
            started_at=str(job.get('started_at') or ''),
            finished_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )
        changed = True
    if changed:
        _invalidate_admin_job_caches()
    return changed

def _job_log_entries(limit: int = ADMIN_JOB_LIST_LIMIT) -> list[dict[str, object]]:
    jobs = get_db().list_admin_jobs(limit=limit, since_days=ADMIN_RECENT_JOB_DAYS)
    entries: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for job in jobs:
        log_path = _resolve_admin_log_path(str(job.get('log_path') or ''))
        if log_path is None:
            continue
        path_key = str(log_path)
        if path_key in seen_paths:
            continue
        seen_paths.add(path_key)
        stat = log_path.stat()
        entries.append(
            {
                'job_id': str(job.get('job_id') or ''),
                'job_type': str(job.get('job_type') or ''),
                'status': str(job.get('status') or ''),
                'source': str(job.get('source') or ''),
                'log_path': str(log_path.relative_to(REPO_ROOT)),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            }
        )
    return entries


def _admin_channel_stats_cached():
    return _cache_get(("admin_channel_stats",), ADMIN_DATA_CACHE_TTL_S, _admin_channel_stats)


def _admin_recent_jobs_cached(limit: int = ADMIN_JOB_LIST_LIMIT):
    _reconcile_stale_admin_jobs()
    return _cache_get(
        ("admin_recent_jobs", limit, ADMIN_RECENT_JOB_DAYS),
        ADMIN_DATA_CACHE_TTL_S,
        lambda: get_db().list_admin_jobs(limit=limit, since_days=ADMIN_RECENT_JOB_DAYS),
    )


def _job_log_entries_cached(limit: int = ADMIN_JOB_LIST_LIMIT) -> list[dict[str, object]]:
    _reconcile_stale_admin_jobs()
    return _cache_get(
        ("job_log_entries", limit, ADMIN_RECENT_JOB_DAYS),
        ADMIN_DATA_JOBS_CACHE_TTL_S,
        lambda: _job_log_entries(limit=limit),
    )


def _render_admin_channels_fragment():
    return _render_cached_page(
        ("admin_channels_fragment",),
        ADMIN_DATA_CACHE_TTL_S,
        lambda: render_template('admin_data_channels_fragment.html', channels=_admin_channel_stats_cached()),
    )


def _render_admin_jobs_fragment():
    return _render_cached_page(
        ("admin_jobs_fragment", ADMIN_RECENT_JOB_DAYS),
        ADMIN_DATA_CACHE_TTL_S,
        lambda: render_template(
            'admin_data_jobs_fragment.html',
            recent_jobs=_admin_recent_jobs_cached(limit=ADMIN_JOB_LIST_LIMIT),
            job_log_entries=_job_log_entries_cached(limit=ADMIN_JOB_LIST_LIMIT),
        ),
    )


def _orchestrator_dashboard_report() -> dict:
    config = load_config()
    return build_doctor_report(config)


def _run_orchestrator_command(args: list[str], timeout: int = 180) -> tuple[int, str, str]:
    script = REPO_ROOT / 'scripts' / 'orchestrator.sh'
    cmd = ['bash', str(script)] + [str(arg) for arg in args]
    result = _subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=timeout,
    )
    return result.returncode, result.stdout or '', result.stderr or ''


def _shorten_control_output(text: str, limit: int = 900) -> str:
    value = str(text or '').strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3] + '...'


def _resolve_orchestrator_job_log_path(value: str) -> Path | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    path = Path(raw)
    if path.is_absolute():
        return path if path.exists() else None
    candidates = [
        REPO_ROOT / path,
        REPO_ROOT / 'logs' / path.name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None

# Configuration
UPLOAD_FOLDER = BASE_DIR
ALLOWED_EXTENSIONS = {'txt', 'json'}

# Use the current interpreter for subprocess calls.
SCRIPTS_ROOT = Path(__file__).parent.parent
PYTHON_BIN = Path(sys.executable)


@app.route('/')
def index():
    """Homepage dengan statistik dan video terbaru"""
    stats_version = _public_data_cache_version()
    return _render_cached_page(
        ("index", stats_version),
        PUBLIC_PAGE_CACHE_TTL_S,
        _index_payload,
    )


@app.route('/channels')
def channels():
    """Halaman list semua channels"""
    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page
    stats_version = _public_data_cache_version()

    return _render_cached_page(
        ("channels", stats_version, page, per_page, offset),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: render_template(
            'channels.html',
            channels=_cache_get(
                ("channels_page", stats_version, page, per_page, offset),
                PUBLIC_PAGE_CACHE_TTL_S,
                lambda: get_db().get_all_channels(limit=per_page, offset=offset),
            ),
            page=page,
            total_pages=(
                _cache_get(
                    ("channel_count", stats_version),
                    PUBLIC_PAGE_CACHE_TTL_S,
                    lambda: get_db().count_all_channels(),
                ) + per_page - 1
            ) // per_page,
        ),
    )


@app.route('/channel/')
def channel_list_redirect():
    """Redirect /channel/ to /channels"""
    return redirect(url_for('channels'))


@app.route('/channel/<channel_id>')
def channel_detail(channel_id):
    """Halaman detail channel dengan list video"""
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 12
    offset = (page - 1) * per_page
    stats_version = _public_data_cache_version()

    return _render_cached_page(
        ("channel_detail", stats_version, channel_id, page, per_page, offset),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: _channel_detail_payload(channel_id, page, per_page, offset, stats_version),
    )


@app.route('/videos')
def videos():
    """Halaman list semua videos"""
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 20
    transcript_filter = request.args.get('transcript', 'all')
    
    # Convert filter to boolean
    transcript_downloaded = None
    if transcript_filter == 'with':
        transcript_downloaded = True
    elif transcript_filter == 'without':
        transcript_downloaded = False
    offset = (page - 1) * per_page
    stats_version = _public_data_cache_version()

    return _render_cached_page(
        ("videos", stats_version, transcript_filter, page, per_page, offset),
        PUBLIC_PAGE_CACHE_TTL_S,
        lambda: (
            lambda total: render_template(
                'videos.html',
                videos=_cache_get(
                    ("videos_page", stats_version, transcript_downloaded, page, per_page, offset),
                    PUBLIC_PAGE_CACHE_TTL_S,
                    lambda: get_db().get_all_videos(
                        transcript_downloaded=transcript_downloaded,
                        limit=per_page,
                        offset=offset,
                    ),
                ),
                page=min(page, max((total + per_page - 1) // per_page, 1) if total else 0),
                transcript_filter=transcript_filter,
                total=total,
                total_pages=max((total + per_page - 1) // per_page, 1) if total else 0,
                per_page=per_page,
            )
        )(
            total
            if False
            else _cache_get(
                ("videos_count", stats_version, transcript_downloaded),
                PUBLIC_PAGE_CACHE_TTL_S,
                lambda: get_db().count_all_videos(transcript_downloaded=transcript_downloaded),
            )
        ),
    )


@app.route('/video/_legacy/<video_id>')
def video_detail(video_id):
    """Halaman detail video dengan transkrip dan ringkasan"""
    db = get_db()
    video = db.get_video_by_id(video_id)

    if not video:
        return render_template('error.html',
                            error=f"Video dengan ID '{video_id}' tidak ditemukan"), 404

    if video and video.get('is_short'):
        return render_template('error.html',
                            error=f"Video ini adalah Shorts dan tidak ditampilkan sesuai kebijakan."), 403
    if video and _video_is_member_only(video):
        return render_template('error.html',
                            error=f"Video ini member-only dan tidak ditampilkan sesuai kebijakan."), 403

    # Baca transkrip dan ringkasan dari file
    transcript_content = db.read_transcript(video_id)
    summary_content = db.read_summary(video_id)
    formatted_transcript_content = db.read_formatted_transcript(video_id)
    adjacent_videos = db.get_adjacent_videos_by_video_id(video_id)
    age_restricted = False
    transcript_source = ""
    upload_date_status = ""
    upload_date_reason = ""
    upload_date_source = ""
    metadata_raw = video.get('metadata')
    if metadata_raw:
        try:
            metadata = json.loads(metadata_raw)
            age_restricted = bool(
                metadata.get('age_restricted')
                or (metadata.get('flags') or {}).get('age_restricted')
            )
            transcript_source = str(
                metadata.get('transcript_source')
                or (metadata.get('flags') or {}).get('transcript_source')
                or ''
            ).strip()
            upload_date_status = str(metadata.get('upload_date_status') or '').strip()
            upload_date_reason = str(metadata.get('upload_date_reason') or '').strip()
            upload_date_source = str(metadata.get('upload_date_source') or '').strip()
        except Exception:
            age_restricted = False
            transcript_source = ""
            upload_date_status = ""
            upload_date_reason = ""
            upload_date_source = ""
    
    # Render summary Markdown to HTML
    summary_html = None
    if summary_content:
        summary_html = render_summary_html(summary_content)

    manual_transcript_retry_available = _manual_transcript_retry_available(video)

    return render_template('video_detail.html',
                         video=video,
                         transcript=transcript_content,
                         formatted_transcript=formatted_transcript_content,
                         summary=summary_html,
                         age_restricted=age_restricted,
                         transcript_source=transcript_source,
                         upload_date_status=upload_date_status,
                         upload_date_reason=upload_date_reason,
                         upload_date_source=upload_date_source,
                         manual_transcript_retry_available=manual_transcript_retry_available,
                         previous_video=adjacent_videos.get('previous'),
                         next_video=adjacent_videos.get('next'))


def _search_payload(query: str, page: int, per_page: int):
    db = get_db()
    total = db.count_search_videos(query)
    total_pages = max((total + per_page - 1) // per_page, 1)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    results = db.search_videos(query, limit=per_page, offset=offset)
    
    return render_template('search.html',
                         query=query,
                         results=results,
                         page=page,
                         total=total,
                         total_pages=total_pages,
                         per_page=per_page)


@app.route('/search')
def search():
    """Halaman pencarian dengan cache halaman penuh."""
    query = request.args.get('q', '')
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 20
    stats_version = _public_data_cache_version()
    
    if not query:
        return render_template('search.html', query='', results=[], page=1, total=0, total_pages=0, per_page=per_page)

    return _render_cached_page(
        ("search", stats_version, query, page, per_page),
        PUBLIC_PAGE_CACHE_TTL_S if query else 0,
        lambda: _search_payload(query, page, per_page),
    )


@app.route('/video_thumbnail/<video_id>')
def proxy_thumbnail(video_id):
    """
    Proxy thumbnail dari i.ytimg.com ke domain kita sendiri.
    Menghilangkan delay DNS dan TLS handshake ke domain Google di browser.
    """
    if not re.match(r'^[A-Za-z0-9_-]{11}$', video_id):
        return "Invalid Video ID", 400
        
    # Urutan resolusi thumbnail: maxresdefault -> hqdefault -> default
    urls = [
        f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/default.jpg"
    ]
    
    for url in urls:
        try:
            resp = requests.get(url, timeout=5, stream=True)
            if resp.status_code == 200:
                # Ambil response headers terpilih
                headers = {
                    'Content-Type': resp.headers.get('Content-Type', 'image/jpeg'),
                    'Cache-Control': 'public, max-age=86400', # Cache 1 hari
                    'Access-Control-Allow-Origin': '*'
                }
                return Response(resp.content, headers=headers)
        except Exception:
            continue
            
    return "Thumbnail not found", 404


@app.route('/api/videos')
def api_videos():
    """API endpoint untuk videos (JSON)"""
    db = get_db()
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 20
    transcript_filter = request.args.get('transcript', 'all')

    transcript_downloaded = None
    if transcript_filter == 'with':
        transcript_downloaded = True
    elif transcript_filter == 'without':
        transcript_downloaded = False

    total = db.count_all_videos(transcript_downloaded=transcript_downloaded)
    total_pages = max((total + per_page - 1) // per_page, 1) if total else 0
    if total_pages:
        page = min(page, total_pages)
    offset = (page - 1) * per_page

    videos = db.get_all_videos(
        transcript_downloaded=transcript_downloaded,
        limit=per_page,
        offset=offset,
    )
    return jsonify({
        'success': True,
        'videos': videos,
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
        'transcript_filter': transcript_filter
    })


@app.route('/api/video/<video_id>')
def api_video_detail(video_id):
    """API endpoint untuk detail video (JSON)"""
    db = get_db()
    video = db.get_video_by_id(video_id)
    
    if not video:
        return jsonify({
            'success': False,
            'error': 'Video not found'
        }), 404

    if video.get('is_short'):
        return jsonify({
            'success': False,
            'error': 'Video is shorts and not publicly available'
        }), 403
    if _video_is_member_only(video):
        return jsonify({
            'success': False,
            'error': 'Video is member-only and not publicly available'
        }), 403
    
    return jsonify({
        'success': True,
        'video': video
    })


@app.route('/api/transcript/<video_id>')
def api_transcript(video_id):
    """API endpoint untuk membaca transkrip (JSON)"""
    db = get_db()
    transcript = db.read_transcript(video_id)
    
    if not transcript:
        return jsonify({
            'success': False,
            'error': 'Transcript not found'
        }), 404
    
    return jsonify({
        'success': True,
        'transcript': transcript
    })


@app.route('/api/summary/<video_id>')
def api_summary(video_id):
    """API endpoint untuk membaca ringkasan (JSON)"""
    db = get_db()
    summary = db.read_summary(video_id)

    if not summary:
        return jsonify({
            'success': False,
            'error': 'Summary not found'
        }), 404

    return jsonify({
        'success': True,
        'summary': summary
    })


@app.route('/api/formatted/<video_id>')
def api_formatted_transcript(video_id):
    """API endpoint untuk membaca transkrip terformat (JSON)"""
    db = get_db()
    formatted = db.read_formatted_transcript(video_id)

    if not formatted:
        return jsonify({
            'success': False,
            'error': 'Formatted transcript not found'
        }), 404

    return jsonify({
        'success': True,
        'formatted': formatted
    })


@app.route('/api/statistics')
def api_statistics():
    """API endpoint untuk statistik (JSON)"""
    db = get_db()
    stats = db.get_statistics()
    file_stats = db.get_file_paths()
    
    return jsonify({
        'success': True,
        'statistics': stats,
        'file_statistics': file_stats
    })


@app.route('/api/search')
def api_search():
    """API endpoint untuk pencarian (JSON)"""
    db = get_db()
    query = request.args.get('q', '')
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 20
    total = 0
    results = []
    if query:
        total = db.count_search_videos(query)
        total_pages = max((total + per_page - 1) // per_page, 1)
        page = min(page, total_pages)
        offset = (page - 1) * per_page
        results = db.search_videos(query, limit=per_page, offset=offset)
    else:
        total_pages = 0
    
    return jsonify({
        'success': True,
        'query': query,
        'results': results,
        'count': len(results),
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages
    })


@app.route('/api/channels/search')
def api_channel_search():
    """API endpoint untuk pencarian channel by id, handle, alias, atau nama."""
    db = get_db()
    query = (request.args.get('q') or '').strip()
    limit = min(max(int(request.args.get('limit', 20) or 20), 1), 100)
    if not query:
        return jsonify({
            'success': True,
            'query': '',
            'results': [],
            'count': 0,
        })
    results = db.search_channels(query, limit=limit)
    return jsonify({
        'success': True,
        'query': query,
        'results': results,
        'count': len(results),
        'limit': limit,
    })


@app.route("/robots.txt")
def robots_txt():
    """Serve robots.txt to block bots."""
    body = (
        "User-agent: GPTBot\n"
        "Disallow: /\n\n"
        "User-agent: ChatGPT-User\n"
        "Disallow: /\n\n"
        "User-agent: Google-Extended\n"
        "Disallow: /\n\n"
        "User-agent: CCBot\n"
        "Disallow: /\n\n"
        "User-agent: ClaudeBot\n"
        "Disallow: /\n\n"
        "User-agent: anthropic-ai\n"
        "Disallow: /\n\n"
        "User-agent: FacebookBot\n"
        "Disallow: /\n\n"
        "User-agent: Bytespider\n"
        "Disallow: /\n\n"
        "User-agent: PetalBot\n"
        "Disallow: /\n\n"
        "User-agent: AhrefsBot\n"
        "Disallow: /\n\n"
        "User-agent: SemrushBot\n"
        "Disallow: /\n\n"
        "User-agent: DotBot\n"
        "Disallow: /\n\n"
        "User-agent: MJ12bot\n"
        "Disallow: /\n\n"
        "User-agent: Baiduspider\n"
        "Disallow: /\n\n"
        "User-agent: megaindex\n"
        "Disallow: /\n\n"
        "User-agent: *\n"
        "Allow: /\n"
        "Crawl-delay: 2\n"
    )
    resp = make_response(body, 200)
    resp.headers["Content-Type"] = "text/plain"
    return resp


@app.route('/uploads/<path:filename>')
def serve_uploads(filename):
    """Serve files dari uploads directory"""
    return send_from_directory(BASE_DIR, filename)


@app.route('/channel_files/<channel_id>')
def channel_files(channel_id):
    """Halaman untuk browse files dalam channel"""
    db = get_db()
    try:
        channel = db.get_channel_by_id(channel_id)
        
        if not channel:
            return render_template('error.html', 
                                error=f"Channel dengan ID '{channel_id}' tidak ditemukan")
        
        # Get channel directories
        transcripts_dir = db.get_channel_transcripts_dir(channel_id)
        summaries_dir = db.get_channel_summaries_dir(channel_id)
        
        # Get files in directories
        transcript_files = []
        summary_files = []
        
        if transcripts_dir.exists():
            for file_path in sorted(transcripts_dir.glob('*.txt')):
                file_info = {
                    'name': file_path.name,
                    'size': file_path.stat().st_size,
                    'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                }
                transcript_files.append(file_info)
        
        if summaries_dir.exists():
            for file_path in sorted(summaries_dir.glob('*.txt')):
                file_info = {
                    'name': file_path.name,
                    'size': file_path.stat().st_size,
                    'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                }
                summary_files.append(file_info)
        
        return render_template('channel_files.html',
                             channel=channel,
                             channel_id=channel_id,
                             transcripts_dir=transcripts_dir,
                             summaries_dir=summaries_dir,
                             transcript_files=transcript_files,
                             summary_files=summary_files)
        
    except Exception as e:
        return render_template('error.html', error=str(e))


@app.route('/file/<channel_id>/<file_type>/<filename>')
def serve_channel_file(channel_id, file_type, filename):
    """Serve files dari channel directories"""
    db = get_db()
    try:
        # Sanitize inputs
        if '..' in filename or '..' in file_type:
            return "Invalid path", 400
        
        # Get correct directory
        if file_type == 'text':
            file_dir = db.get_channel_transcripts_dir(channel_id)
        elif file_type == 'resume':
            file_dir = db.get_channel_summaries_dir(channel_id)
        else:
            return "Invalid file type", 400
        
        # Serve file
        return send_from_directory(file_dir, filename)
        
    except Exception as e:
        return f"Error serving file: {str(e)}", 404


@app.template_filter('format_number')
def format_number(number):
    """Format large numbers dengan K, M, B suffixes"""
    if number is None:
        return '0'
    
    number = int(number)
    if number >= 1_000_000_000:
        return f'{number / 1_000_000_000:.1f}B'
    elif number >= 1_000_000:
        return f'{number / 1_000_000:.1f}M'
    elif number >= 1_000:
        return f'{number / 1_000:.1f}K'
    else:
        return str(number)


@app.template_filter('format_duration')
def format_duration(seconds):
    """Format duration dalam detik ke HH:MM:SS"""
    if not seconds:
        return '0:00:00'
    
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    if hours > 0:
        return f'{hours}:{minutes:02d}:{seconds:02d}'
    else:
        return f'{minutes}:{seconds:02d}'


@app.template_filter('format_date')
def format_date(date_str):
    """Format date string ke format yang lebih readable"""
    if not date_str:
        return 'Unknown'
    try:
        raw = str(date_str).strip()
        if len(raw) == 8 and raw.isdigit():
            year = raw[:4]
            month = raw[4:6]
            day = raw[6:8]
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            month_name = months[int(month) - 1]
            return f'{day} {month_name} {year}'
        if len(raw) >= 10 and raw[4] == '-' and raw[7] == '-':
            return datetime.strptime(raw[:10], '%Y-%m-%d').strftime('%d %b %Y')
        if len(raw) >= 19 and raw[10] == ' ':
            return datetime.strptime(raw[:19], '%Y-%m-%d %H:%M:%S').strftime('%d %b %Y')
        return raw
    except:
        return 'Unknown'


@app.template_filter('format_file_size')
def format_file_size(bytes_size):
    """Format file size ke KB, MB, GB"""
    if not bytes_size:
        return '0 B'
    
    bytes_size = int(bytes_size)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f'{bytes_size:.1f} {unit}'
        bytes_size /= 1024.0
    
    return f'{bytes_size:.1f} TB'


@app.template_filter('first_is_alpha')
def first_is_alpha(s):
    """Check jika karakter pertama dari string adalah alphabetic"""
    if not s:
        return False
    return s[0].isalpha()


@app.template_filter('regex_match')
def regex_match(s, pattern):
    """Regex match filter for Jinja2"""
    if not s:
        return None
    return re.search(pattern, s)


@app.template_filter('linebreaks')
def linebreaks(value):
    """
    Converts newlines into <p> and <br /> tags.
    """
    import html
    if not value:
        return ""
    value = html.escape(value)
    paragraphs = re.split(r'\n\n+', value)
    paragraphs = ["<p>{}</p>".format(p.replace("\n", "<br />")) for p in paragraphs]
    return '\n'.join(paragraphs)


@app.template_filter('clean_description')
def clean_description(value):
    """
    Clean description by unescaping HTML entities and rendering safely.
    Use this for descriptions that already contain HTML.
    """
    import html
    if not value:
        return ""
    # Unescape HTML entities
    value = html.unescape(value)
    # Remove any script tags for security
    value = re.sub(r'<script[^>]*>.*?</script>', '', value, flags=re.IGNORECASE | re.DOTALL)
    return value


@app.template_filter('regex_replace')
def regex_replace(s, pattern, replacement):
    """Regex replace filter for Jinja2"""
    if not s:
        return ""
    return re.sub(pattern, replacement, s)


@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors"""
    return render_template('error.html', error='Page not found'), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return render_template('error.html', error='Internal server error'), 500


# ---------------------------------------------------------------------------
# Admin data management (no link in public pages)
# ---------------------------------------------------------------------------
import sqlite3 as _sqlite3
import subprocess as _subprocess
import secrets as _secrets
import threading as _threading

ADMIN_DATA_TOKEN = os.environ.get('ADMIN_DATA_TOKEN', '')
if not ADMIN_DATA_TOKEN:
    ADMIN_DATA_TOKEN = 'admin123'

_bg_jobs: dict = {}


def _admin_authenticated() -> bool:
    raw = request.headers.get('Cookie') or ''
    return f'admin_data_token={ADMIN_DATA_TOKEN}' in raw


def _job_command_text(cmd) -> str:
    if isinstance(cmd, (list, tuple)):
        return json.dumps(list(cmd), ensure_ascii=False)
    return str(cmd)


def _render_autopost_page(*, title: str, action_url: str, fields: dict[str, str], message: str) -> str:
    inputs = "\n".join(
        f'<input type="hidden" name="{html.escape(name)}" value="{html.escape(value)}">'
        for name, value in fields.items()
    )
    return render_template_string(
        """<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <meta http-equiv="refresh" content="0;url={{ action_url }}">
</head>
<body>
  <p>{{ message }}</p>
  <form id="autopost" method="post" action="{{ action_url }}">
    {{ inputs|safe }}
  </form>
  <script>document.getElementById('autopost').submit();</script>
</body>
</html>""",
        title=title,
        action_url=action_url,
        message=message,
        inputs=inputs,
    )


def _record_admin_job(
    job_id: str,
    job_type: str,
    status: str,
    *,
    command=None,
    log_path: str = '',
    run_dir: str = '',
    source: str = 'admin_ui',
    target_channel_id: str = '',
    target_video_id: str = '',
    pid: int = None,
    exit_code: int = None,
    error_message: str = '',
) -> None:
    db = get_db()
    db.upsert_admin_job(
        job_id,
        job_type,
        status,
        source=source,
        pid=pid,
        command=_job_command_text(command) if command is not None else '',
        log_path=log_path or '',
        run_dir=run_dir or '',
        target_channel_id=target_channel_id or '',
        target_video_id=target_video_id or '',
        exit_code=exit_code,
        error_message=error_message or '',
    )


def _build_job_env(
    *,
    job_id: str,
    job_type: str,
    command,
    log_path: str = '',
    run_dir: str = '',
    source: str = 'admin_ui',
    target_channel_id: str = '',
    target_video_id: str = '',
) -> dict:
    env = os.environ.copy()
    env['JOB_ID'] = job_id
    env['JOB_TYPE'] = job_type
    env['JOB_SOURCE'] = source
    env['JOB_COMMAND'] = _job_command_text(command)
    env['JOB_LOG_PATH'] = log_path or ''
    env['JOB_RUN_DIR'] = run_dir or ''
    if target_channel_id:
        env['JOB_TARGET_CHANNEL_ID'] = target_channel_id
    if target_video_id:
        env['JOB_TARGET_VIDEO_ID'] = target_video_id
    return env


def _start_admin_job_process(
    *,
    job_id: str,
    job_type: str,
    command,
    log_path: str,
    run_dir: str = '',
    source: str = 'admin_ui',
    target_channel_id: str = '',
    target_video_id: str = '',
) -> None:
    env = _build_job_env(
        job_id=job_id,
        job_type=job_type,
        command=command,
        log_path=log_path,
        run_dir=run_dir,
        source=source,
        target_channel_id=target_channel_id,
        target_video_id=target_video_id,
    )

    def _run():
        with open(log_path, 'w') as f:
            proc = _subprocess.Popen(
                command,
                stdout=f,
                stderr=_subprocess.STDOUT,
                cwd=str(SCRIPTS_ROOT),
                env=env,
            )
            _bg_jobs[job_id] = {'pid': proc.pid, 'type': job_type, 'log': log_path}
            proc.wait()
            _bg_jobs.pop(job_id, None)

    _threading.Thread(target=_run, daemon=True).start()


def _launch_new_channel_pipeline(channel_id: str, channel_name: str = "") -> tuple[str, str]:
    safe_channel_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(channel_id or "").strip()) or "channel"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = f'pipeline_{ts}_{_secrets.token_hex(3)}'
    run_dir = str(REPO_ROOT / 'runs' / f'pipeline_one_channel_{ts}_{safe_channel_id}')
    log_path = str(REPO_ROOT / 'logs' / f'{job_id}.log')
    script = str(REPO_ROOT / 'partial_ops' / 'run_pipeline_one_channel.sh')
    cmd = ['bash', script, '--channel-id', channel_id, '--run-dir', run_dir]

    _record_admin_job(
        job_id,
        'pipeline',
        'queued',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=channel_id,
    )
    _start_admin_job_process(
        job_id=job_id,
        job_type='pipeline',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=channel_id,
    )
    return job_id, run_dir


def _admin_channel_stats():
    """Return channel rows with transcript/resume/formatted counts."""
    db = get_db()
    con = db.conn
    cur = con.execute('''
        SELECT c.channel_id, c.channel_name, c.channel_handle,
               COALESCE(alias_counts.alias_count, 0) as alias_count,
               COUNT(v.id) as total_videos,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0 THEN 1 ELSE 0 END), 0) as public_videos,
               COALESCE(SUM(CASE WHEN v.is_short = 1 THEN 1 ELSE 0 END), 0) as shorts_videos,
               COALESCE(SUM(CASE WHEN v.is_member_only = 1 THEN 1 ELSE 0 END), 0) as member_only_videos,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0 AND v.transcript_downloaded = 1 THEN 1 ELSE 0 END), 0) as has_transcript,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0
                                  AND v.transcript_downloaded = 1
                                  AND v.transcript_language != 'no_subtitle'
                                  AND v.summary_file_path IS NOT NULL
                                  AND v.summary_file_path != '' THEN 1 ELSE 0 END), 0) as has_summary,
               COALESCE(SUM(CASE WHEN v.transcript_formatted_path IS NOT NULL
                                  AND v.transcript_formatted_path != '' THEN 1 ELSE 0 END), 0) as has_formatted,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0
                                  AND v.transcript_downloaded = 0
                                  AND v.transcript_language = 'no_subtitle' THEN 1 ELSE 0 END), 0) as no_subtitle,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0
                                  AND v.transcript_downloaded = 0
                                  AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
                                  AND v.transcript_retry_after IS NOT NULL
                                  AND LOWER(COALESCE(v.transcript_retry_reason, '')) NOT LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) as retry_later,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0
                                  AND v.transcript_downloaded = 0
                                  AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
                                  AND LOWER(COALESCE(v.transcript_retry_reason, '')) LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) as proxy_block,
               COALESCE(SUM(CASE WHEN v.is_short = 0 AND v.is_member_only = 0
                                  AND v.transcript_downloaded = 0
                                  AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
                                  AND v.transcript_retry_after IS NULL
                                  AND LOWER(COALESCE(v.transcript_retry_reason, '')) NOT LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) as pending_other
        FROM channels c
        LEFT JOIN videos v ON v.channel_id = c.id
        LEFT JOIN (
            SELECT channel_db_id, COUNT(*) AS alias_count
            FROM channel_aliases
            GROUP BY channel_db_id
        ) alias_counts ON alias_counts.channel_db_id = c.id
        GROUP BY c.id
        ORDER BY total_videos DESC
    ''')
    cols = [d[0] for d in cur.description]
    channels = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Attach runtime state
    for ch in channels:
        ch['scan_enabled'] = 1
        ch['skip_reason'] = None
        rcur = con.execute(
            'SELECT scan_enabled, skip_reason FROM channel_runtime_state WHERE channel_id = ?',
            (ch['channel_id'],)
        )
        rr = rcur.fetchone()
        if rr:
            ch['scan_enabled'] = rr[0]
            ch['skip_reason'] = rr[1]
    return channels


def _admin_summary(channels):
    total_videos = sum(c['total_videos'] for c in channels)
    public_videos = sum(c['public_videos'] for c in channels)
    transcript_ok = sum(c['has_transcript'] for c in channels)
    resume_ok = sum(c['has_summary'] for c in channels)
    formatted_ok = sum(c['has_formatted'] for c in channels)
    no_subtitle = sum(c['no_subtitle'] for c in channels)
    retry_later = sum(c['retry_later'] for c in channels)
    proxy_block = sum(c['proxy_block'] for c in channels)
    pending_other = sum(c['pending_other'] for c in channels)
    shorts = sum(c['shorts_videos'] for c in channels)
    member_only = sum(c['member_only_videos'] for c in channels)
    return {
        'total_channels': len(channels),
        'total_videos': total_videos,
        'public_videos': public_videos,
        'transcript_ok': transcript_ok,
        'resume_ok': resume_ok,
        'formatted_ok': formatted_ok,
        'no_subtitle': no_subtitle,
        'retry_later': retry_later,
        'proxy_block': proxy_block,
        'pending_other': pending_other,
        'shorts': shorts,
        'member_only': member_only,
        'pending_transcript': max(public_videos - transcript_ok - no_subtitle - retry_later - proxy_block, 0),
        'without_transcript': max(public_videos - transcript_ok, 0),
    }


def _extract_channel_id(raw_input: str) -> str:
    """Extract channel ID from URL or raw string."""
    raw = raw_input.strip()
    if not raw:
        return ''
    # https://youtube.com/@name or /c/name or /channel/ID
    import re as _re
    m = _re.search(r'youtube\.com/(?:@|c/|channel/)([\w.-]+)', raw)
    if m:
        return m.group(1)
    # Remove @ prefix
    if raw.startswith('@'):
        return raw[1:]
    return raw


def _build_channel_url(raw_input: str, channel_id: str) -> str:
    """Build a stable YouTube channel URL from pasted input."""
    raw = raw_input.strip()
    token = channel_id.strip().lstrip('@')
    if raw:
        normalized = raw
        if normalized.startswith(('youtube.com/', 'www.youtube.com/')):
            normalized = f'https://{normalized}'
        if normalized.startswith(('http://', 'https://')):
            normalized = normalized.rstrip('/')
            match = re.search(r'youtube\.com/(?P<path>(?:@|c/|channel/)[^/?#]+)', normalized)
            if match:
                return f"https://www.youtube.com/{match.group('path')}"
            return normalized
        if raw.startswith('@'):
            return f'https://www.youtube.com/{raw}'
        if raw.startswith('UC') and re.fullmatch(r'UC[\w-]+', raw):
            return f'https://www.youtube.com/channel/{raw}'
    if token.startswith('UC') and re.fullmatch(r'UC[\w-]+', token):
        return f'https://www.youtube.com/channel/{token}'
    return f'https://www.youtube.com/@{token}'


@app.route('/admin/data', methods=['GET'], strict_slashes=False)
def admin_data_page():
    if not _admin_authenticated():
        return render_template('admin_data.html', authed=False)
    _reconcile_stale_admin_jobs()
    action = (request.args.get('action') or '').strip().lower()
    channel_id = (request.args.get('channel_id') or '').strip()
    channel_name = (request.args.get('channel_name') or '').strip()
    video_id = (request.args.get('video_id') or '').strip()
    if action in {'discover', 'transcript'} and (channel_id or channel_name or video_id):
        if action == 'discover':
            fields = {
                'channel_id': channel_id,
                'channel_name': channel_name,
                'scan_all': request.args.get('scan_all', 'false'),
                'limit': request.args.get('limit', '0'),
                'full_pipeline': request.args.get('full_pipeline', 'false'),
            }
            return _render_autopost_page(
                title='Redirecting discovery...',
                action_url=url_for('admin_data_action_discover'),
                fields={k: v for k, v in fields.items() if str(v or '').strip()},
                message='Menjalankan discovery, silakan tunggu...',
            )
        if action == 'transcript':
            fields = {
                'channel_id': channel_id,
                'video_id': video_id,
                'limit': request.args.get('limit', '0'),
            }
            return _render_autopost_page(
                title='Redirecting transcript...',
                action_url=url_for('admin_data_action_transcript'),
                fields={k: v for k, v in fields.items() if str(v or '').strip()},
                message='Menjalankan transcript, silakan tunggu...',
            )
    channels = _admin_channel_stats_cached()
    summary = _admin_summary(channels)
    flash = request.args.get('flash', '')
    error_flash = request.args.get('error', '')
    
    # Create channels_json for combobox
    channels_json = json.dumps(
        [{'id': ch['channel_id'], 'name': ch['channel_name']} for ch in channels]
    )
    
    html_output = _render_cached_page(
        ("admin_data_page", flash, error_flash),
        ADMIN_DATA_CACHE_TTL_S,
        lambda: render_template(
            'admin_data.html',
            authed=True,
            channels=channels,
            summary=summary,
            flash=flash,
            error_flash=error_flash,
            channels_json=channels_json,
        ),
    )
    response = make_response(html_output)
    response.headers['Cache-Control'] = 'private, max-age=10, stale-while-revalidate=50'
    vary = response.headers.get('Vary', '')
    if 'Cookie' not in vary:
        response.headers['Vary'] = (vary + ', Cookie').strip(', ')
    return response


@app.route('/admin/orchestrator', methods=['GET'], strict_slashes=False)
def admin_orchestrator_page():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))

    report = _orchestrator_dashboard_report()
    if (request.args.get('format') or '').strip().lower() == 'json':
        return jsonify(report)

    return render_template(
        'admin_orchestrator.html',
        report=report,
        flash=request.args.get('flash', ''),
        error_flash=request.args.get('error', ''),
    )


@app.route('/admin/orchestrator/action', methods=['POST'])
def admin_orchestrator_action():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))

    action = (request.form.get('action') or '').strip().lower()
    args: list[str]
    timeout = 180
    redirect_args: dict[str, str] = {}

    try:
        if action == 'doctor':
            args = ['doctor']
        elif action == 'explain':
            args = ['explain']
        elif action == 'validate':
            args = ['validate']
        elif action == 'reconcile':
            args = ['reconcile']
        elif action == 'once_dry_run':
            max_jobs = int(request.form.get('max_jobs', 7) or 7)
            args = ['once', '--dry-run', '--max-jobs', str(max_jobs)]
            timeout = max(60, min(600, 30 + max_jobs * 10))
        elif action == 'pause':
            target = (request.form.get('target') or '').strip()
            reason = (request.form.get('reason') or '').strip()
            if not target:
                return redirect(url_for('admin_orchestrator_page', error='Target pause kosong'))
            args = ['pause', '--target', target]
            if reason:
                args.extend(['--reason', reason])
        elif action == 'resume':
            target = (request.form.get('target') or '').strip()
            if not target:
                return redirect(url_for('admin_orchestrator_page', error='Target resume kosong'))
            args = ['resume', '--target', target]
        elif action == 'cancel':
            job_id = (request.form.get('job_id') or '').strip()
            if not job_id:
                return redirect(url_for('admin_orchestrator_page', error='Job ID kosong'))
            args = ['cancel', '--job-id', job_id]
            if request.form.get('force') in {'1', 'true', 'on', 'yes'}:
                args.append('--force')
            grace_seconds = int(request.form.get('grace_seconds', 10) or 10)
            args.extend(['--grace-seconds', str(max(1, grace_seconds))])
            timeout = max(30, min(300, 20 + grace_seconds * 3))
        elif action == 'cancel_stage':
            stage = (request.form.get('stage') or '').strip()
            if not stage:
                return redirect(url_for('admin_orchestrator_page', error='Stage kosong'))
            args = ['cancel-stage', stage]
            if request.form.get('force') in {'1', 'true', 'on', 'yes'}:
                args.append('--force')
            grace_seconds = int(request.form.get('grace_seconds', 10) or 10)
            args.extend(['--grace-seconds', str(max(1, grace_seconds))])
            timeout = max(30, min(300, 20 + grace_seconds * 3))
        elif action == 'cancel_group':
            group = (request.form.get('group') or '').strip()
            if not group:
                return redirect(url_for('admin_orchestrator_page', error='Group kosong'))
            args = ['cancel-group', group]
            if request.form.get('force') in {'1', 'true', 'on', 'yes'}:
                args.append('--force')
            grace_seconds = int(request.form.get('grace_seconds', 10) or 10)
            args.extend(['--grace-seconds', str(max(1, grace_seconds))])
            timeout = max(30, min(300, 20 + grace_seconds * 3))
        else:
            return redirect(url_for('admin_orchestrator_page', error=f'Aksi tidak dikenal: {action or "empty"}'))

        rc, stdout, stderr = _run_orchestrator_command(args, timeout=timeout)
        output = '\n'.join(part for part in [stdout.strip(), stderr.strip()] if part)
        if rc == 0:
            flash_text = f"Command {' '.join(args)} berhasil."
            if output:
                flash_text += f" { _shorten_control_output(output) }"
            redirect_args['flash'] = flash_text
        else:
            error_text = f"Command {' '.join(args)} gagal (rc={rc})."
            if output:
                error_text += f" { _shorten_control_output(output) }"
            redirect_args['error'] = error_text
    except Exception as exc:
        redirect_args['error'] = str(exc)

    return redirect(url_for('admin_orchestrator_page', **redirect_args))


@app.route('/admin/orchestrator/job/<job_id>/log', methods=['GET'], strict_slashes=False)
def admin_orchestrator_job_log(job_id):
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))

    state = OrchestratorState()
    try:
        job = state.get_job(job_id)
    finally:
        state.close()

    if not job:
        return redirect(url_for('admin_orchestrator_page', error='Job not found'))

    log_path = _resolve_orchestrator_job_log_path(str(job.get('log_path') or ''))
    if not log_path:
        return redirect(url_for('admin_orchestrator_page', error='Log file not found'))

    try:
        content = log_path.read_text(encoding='utf-8', errors='ignore')
        try:
            filename = str(log_path.relative_to(REPO_ROOT))
        except Exception:
            filename = log_path.name
    except Exception as exc:
        return redirect(url_for('admin_orchestrator_page', error=str(exc)))

    if request.args.get('partial') == '1':
        return f'<pre id="log-text">{html.escape(content)}</pre>'
    return render_template('admin_log_view.html', filename=filename, content=content)


def _channel_aliases_page(channel_id: str):
    db = get_db()
    channel = db.get_channel_by_id(channel_id)
    if not channel:
        return render_template(
            'error.html',
            error=f"Channel dengan ID '{channel_id}' tidak ditemukan",
        ), 404

    aliases = db.get_channel_aliases(channel_id)
    search_matches = db.search_channels(channel_id, limit=10)
    return render_template(
        'admin_channel_aliases.html',
        channel=channel,
        aliases=aliases,
        search_matches=search_matches,
        alias_count=len(aliases),
        flash=request.args.get('flash', ''),
        error_flash=request.args.get('error', ''),
    )


@app.route('/admin/data/fragment/channels')
def admin_data_fragment_channels():
    if not _admin_authenticated():
        return jsonify({'error': 'unauthorized'}), 403
    return _render_admin_channels_fragment()


@app.route('/admin/data/fragment/jobs')
def admin_data_fragment_jobs():
    if not _admin_authenticated():
        return jsonify({'error': 'unauthorized'}), 403
    return _render_admin_jobs_fragment()


@app.route('/admin/data/login', methods=['POST'])
def admin_data_login():
    token = (request.form.get('token') or '').strip()
    if token == ADMIN_DATA_TOKEN:
        resp = redirect(url_for('admin_data_page'))
        resp.set_cookie('admin_data_token', ADMIN_DATA_TOKEN, max_age=86400, httponly=True, samesite='Lax')
        return resp
    return render_template('admin_data.html', authed=False, error='Token tidak valid')


@app.route('/admin/data/logout', methods=['POST'])
def admin_data_logout():
    resp = redirect(url_for('admin_data_page'))
    resp.set_cookie('admin_data_token', '', max_age=0)
    return resp


@app.route('/admin/data/channel/<channel_id>/aliases', methods=['GET'], strict_slashes=False)
def admin_data_channel_aliases(channel_id):
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    return _channel_aliases_page(channel_id)


@app.route('/admin/data/action/rebuild-channel-aliases', methods=['POST'])
def admin_data_action_rebuild_channel_aliases():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    db = get_db()
    try:
        count = db.rebuild_channel_aliases(channel_id or None)
        if channel_id:
            return redirect(url_for('admin_data_channel_aliases', channel_id=channel_id, flash=f'Alias channel diperbarui ({count})'))
        return redirect(url_for('admin_data_page', flash=f'Alias seluruh channel dibangun ulang ({count})'))
    except Exception as exc:
        return redirect(url_for('admin_data_page', error=str(exc)))


@app.route('/admin/data/channel/add', methods=['POST'])
def admin_data_channel_add():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    raw = (request.form.get('channel_input') or '').strip()
    name = (request.form.get('channel_name') or '').strip()
    ch_id = _extract_channel_id(raw)
    if not ch_id:
        return redirect(url_for('admin_data_page', error='Channel ID kosong'))
    try:
        db = get_db()
        channel_name = name or ch_id
        channel_url = _build_channel_url(raw, ch_id)
        db.add_channel(ch_id, channel_name, channel_url)
        job_id, _ = _launch_new_channel_pipeline(ch_id, channel_name)
        return redirect(url_for('admin_data_page', flash=f'Channel {ch_id} ditambahkan dan pipeline dimulai (job: {job_id})'))
    except Exception as e:
        return redirect(url_for('admin_data_page', error=str(e)))


@app.route('/admin/data/channel/add-batch', methods=['POST'])
def admin_data_channel_add_batch():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    raw = (request.form.get('channels_input') or '').strip()
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    if not lines:
        return redirect(url_for('admin_data_page', error='Tidak ada channel yang dimasukkan'))
    db = get_db()
    added = 0
    pipelines_started = 0
    errors = []
    for line in lines:
        ch_id = _extract_channel_id(line)
        if not ch_id:
            continue
        try:
            channel_url = _build_channel_url(line, ch_id)
            db.add_channel(ch_id, ch_id, channel_url)
            added += 1
            try:
                _launch_new_channel_pipeline(ch_id, ch_id)
                pipelines_started += 1
            except Exception as pipeline_exc:
                errors.append(f'{ch_id}: pipeline start failed: {pipeline_exc}')
        except Exception as e:
            errors.append(f'{ch_id}: {e}')
    flash = f'{added} channel ditambahkan'
    if pipelines_started:
        flash += f' | {pipelines_started} pipeline dimulai'
    if errors:
        flash += f' | {len(errors)} error'
    return redirect(url_for('admin_data_page', flash=flash))


@app.route('/admin/data/action/discover', methods=['POST'])
def admin_data_action_discover():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    channel_name = (request.form.get('channel_name') or '').strip()
    scan_all = request.form.get('scan_all', 'false') == 'true'
    limit = int(request.form.get('limit', 0))
    
    # Use wrapper script for automatic .venv usage
    script = str(Path(__file__).parent.parent / 'scripts' / 'discover.sh')
    cmd = ['bash', script]
    
    if scan_all or limit <= 0:
        cmd.append('--scan-all-missing')
    else:
        cmd.append('--latest-only')
        cmd.extend(['--recent-per-channel', str(limit)])

    if not channel_id and not channel_name:
        cmd.extend(['--channel-limit', '0'])
    
    if channel_id:
        cmd.extend(['--channel-id', channel_id])
    elif channel_name:
        cmd.extend(['--channel-name', channel_name])
    
    # Always run discovery-only via wrapper
    # Full pipeline option can be enabled separately
    full_pipeline = request.form.get('full_pipeline', 'false') == 'true'
    if full_pipeline:
        cmd.append('--full-pipeline')

    job_id = f'discover_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{_secrets.token_hex(3)}'
    log_path = str(Path(__file__).parent.parent / 'logs' / f'{job_id}.log')
    _record_admin_job(
        job_id,
        'discover',
        'queued',
        command=cmd,
        log_path=log_path,
        source='admin_ui',
        target_channel_id=channel_id,
    )

    _start_admin_job_process(
        job_id=job_id,
        job_type='discover',
        command=cmd,
        log_path=log_path,
        source='admin_ui',
        target_channel_id=channel_id,
    )
    return redirect(url_for('admin_data_page', flash=f'Discovery dimulai (job: {job_id})'))


@app.route('/admin/data/action/transcript', methods=['POST'])
def admin_data_action_transcript():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    video_id = (request.form.get('video_id') or '').strip()
    limit = int(request.form.get('limit', 0))
    
    # Use wrapper script for automatic .venv usage
    script = str(Path(__file__).parent.parent / 'scripts' / 'transcript.sh')
    cmd = ['bash', script]
    
    if video_id:
        cmd.extend(['--video-id', video_id])
    else:
        # Batch mode: need CSV or run with limit
        if limit > 0:
            cmd.extend(['--limit', str(limit)])
        if channel_id:
            # For channel-specific runs, we still use the wrapper
            # but the actual filtering is done by the Python script
            cmd.extend(['--channel-id', channel_id])
    
    # Run dir for output
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = str(Path(__file__).parent.parent / 'runs' / f'flask_transcript_{ts}')
    cmd.extend(['--run-dir', run_dir])

    job_id = f'transcript_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{_secrets.token_hex(3)}'
    log_path = str(Path(__file__).parent.parent / 'logs' / f'{job_id}.log')
    _record_admin_job(
        job_id,
        'transcript',
        'queued',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=channel_id,
        target_video_id=video_id,
    )

    _start_admin_job_process(
        job_id=job_id,
        job_type='transcript',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=channel_id,
        target_video_id=video_id,
    )
    return redirect(url_for('admin_data_page', flash=f'Download transcript dimulai (job: {job_id})'))


@app.route('/admin/data/action/resume', methods=['POST'])
def admin_data_action_resume():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    video_id = (request.form.get('video_id') or '').strip()
    limit = int(request.form.get('limit', 0))
    
    script = str(Path(__file__).parent.parent / 'resume.sh')
    cmd = ['bash', script]
    
    if video_id:
        cmd.extend(['--video-id', video_id])
    else:
        if limit > 0:
            cmd.extend(['--limit', str(limit)])
        if channel_id:
            cmd.extend(['--channel-id', channel_id])
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = str(Path(__file__).parent.parent / 'runs' / f'flask_resume_{ts}')
    cmd.extend(['--run-dir', run_dir])

    job_id = f'resume_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{_secrets.token_hex(3)}'
    log_path = str(Path(__file__).parent.parent / 'logs' / f'{job_id}.log')
    
    _record_admin_job(job_id, 'resume', 'queued', command=cmd, log_path=log_path, run_dir=run_dir, source='admin_ui', target_channel_id=channel_id, target_video_id=video_id)
    _start_admin_job_process(job_id=job_id, job_type='resume', command=cmd, log_path=log_path, run_dir=run_dir, source='admin_ui', target_channel_id=channel_id, target_video_id=video_id)
    return redirect(url_for('admin_data_page', flash=f'Resume generation dimulai (job: {job_id})'))


@app.route('/admin/data/action/format', methods=['POST'])
def admin_data_action_format():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    limit = int(request.form.get('limit', 0))
    
    script = str(Path(__file__).parent.parent / 'format.sh')
    cmd = ['bash', script]
    
    if limit > 0:
        cmd.extend(['--limit', str(limit)])
    if channel_id:
        cmd.extend(['--channel-id', channel_id])
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = str(Path(__file__).parent.parent / 'runs' / f'flask_format_{ts}')
    cmd.extend(['--run-dir', run_dir])

    job_id = f'format_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{_secrets.token_hex(3)}'
    log_path = str(Path(__file__).parent.parent / 'logs' / f'{job_id}.log')
    
    _record_admin_job(job_id, 'format', 'queued', command=cmd, log_path=log_path, run_dir=run_dir, source='admin_ui', target_channel_id=channel_id)
    _start_admin_job_process(job_id=job_id, job_type='format', command=cmd, log_path=log_path, run_dir=run_dir, source='admin_ui', target_channel_id=channel_id)
    return redirect(url_for('admin_data_page', flash=f'Formatting dimulai (job: {job_id})'))


@app.route('/admin/data/action/update-meta', methods=['POST'])
def admin_data_action_update_meta():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    limit = int(request.form.get('limit', 0))
    
    # Use the same interpreter that started this app.
    python_bin = str(PYTHON_BIN)
    script = str(Path(__file__).parent.parent / 'fetch_youtube_metadata.py')
    cmd = [python_bin, script]
    
    if limit > 0:
        cmd.extend(['--limit', str(limit)])
    if channel_id:
        cmd.extend(['--channel-id', channel_id])

    job_id = f'meta_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{_secrets.token_hex(3)}'
    log_path = str(Path(__file__).parent.parent / 'logs' / f'{job_id}.log')
    _record_admin_job(
        job_id,
        'meta',
        'queued',
        command=cmd,
        log_path=log_path,
        source='admin_ui',
        target_channel_id=channel_id,
    )

    _start_admin_job_process(
        job_id=job_id,
        job_type='meta',
        command=cmd,
        log_path=log_path,
        source='admin_ui',
        target_channel_id=channel_id,
    )
    return redirect(url_for('admin_data_page', flash=f'Update metadata dimulai (job: {job_id})'))


@app.route('/admin/data/action/delete-channel', methods=['POST'])
def admin_data_action_delete_channel():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    if not channel_id:
        return redirect(url_for('admin_data_page', error='Channel ID kosong'))
    try:
        db = get_db()
        with db._get_cursor() as cur:
            ch_row = cur.execute('SELECT id FROM channels WHERE channel_id = ?', (channel_id,)).fetchone()
            if not ch_row:
                return redirect(url_for('admin_data_page', error=f'Channel {channel_id} tidak ditemukan'))
            cur.execute('DELETE FROM videos WHERE channel_id = ?', (ch_row['id'],))
            cur.execute('DELETE FROM channels WHERE id = ?', (ch_row['id'],))
            cur.execute('DELETE FROM channel_runtime_state WHERE channel_id = ?', (channel_id,))
        return redirect(url_for('admin_data_page', flash=f'Channel {channel_id} dan semua videonya dihapus'))
    except Exception as e:
        return redirect(url_for('admin_data_page', error=str(e)))


@app.route('/admin/data/action/run-all', methods=['POST'])
def admin_data_action_run_all():
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    channel_id = (request.form.get('channel_id') or '').strip()
    channel_name = (request.form.get('channel_name') or '').strip()
    log_base = str(Path(__file__).parent.parent / 'logs')
    os.makedirs(log_base, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    scripts_root = str(Path(__file__).parent.parent)
    # Jalankan pipeline sebagai 1 proses mandiri agar tidak tergantung umur
    # background thread Flask. Wrapper shell sudah menangani stage 1..4 secara
    # berurutan dan melaporkan status job ke tabel admin_jobs.
    if channel_id or channel_name:
        script = os.path.join(scripts_root, 'partial_ops', 'run_pipeline_one_channel.sh')
        cmd = ['bash', script]
        if channel_id:
            cmd.extend(['--channel-id', channel_id])
        else:
            cmd.extend(['--channel-name', channel_name])
        run_dir = os.path.join(scripts_root, 'runs', f'pipeline_one_channel_{ts}')
        cmd.extend(['--run-dir', run_dir])
        target_id = channel_id
        target_name = channel_name
        job_label = 'pipeline_one_channel'
    else:
        script = os.path.join(scripts_root, 'partial_ops', 'run_pipeline_all_channels.sh')
        cmd = ['bash', script]
        run_dir = os.path.join(scripts_root, 'runs', f'pipeline_all_channels_{ts}')
        target_id = ''
        target_name = ''
        job_label = 'pipeline_all_channels'

    job_id = f'{ts}_{job_label}_{_secrets.token_hex(3)}'
    log_path = os.path.join(log_base, f'{job_id}.log')
    _record_admin_job(
        job_id,
        'pipeline',
        'queued',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=target_id,
    )
    _start_admin_job_process(
        job_id=job_id,
        job_type='pipeline',
        command=cmd,
        log_path=log_path,
        run_dir=run_dir,
        source='admin_ui',
        target_channel_id=target_id,
    )
    flash_msg = 'Dimulai pipeline berurutan (stage 1→4)'
    if target_name and not target_id:
        flash_msg += f' untuk {target_name}'
    return redirect(url_for('admin_data_page', flash=flash_msg))


@app.route('/admin/data/jobs', strict_slashes=False)
def admin_data_jobs():
    """Return recent persistent background jobs as JSON."""
    if not _admin_authenticated():
        return jsonify({'error': 'unauthorized'}), 403
    _reconcile_stale_admin_jobs()
    limit = int(request.args.get('limit', ADMIN_JOB_LIST_LIMIT))
    status = (request.args.get('status') or '').strip() or None
    since_days = request.args.get('since_days')
    since_days_value = int(since_days) if since_days not in (None, '') else ADMIN_RECENT_JOB_DAYS
    jobs = get_db().list_admin_jobs(limit=limit, status=status, since_days=since_days_value)
    return jsonify({'jobs': jobs})


@app.route('/admin/data/logs', strict_slashes=False)
def admin_data_logs():
    """Show list of log files."""
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    
    log_dir = Path(__file__).parent.parent / 'logs'
    log_files = []
    
    if log_dir.exists():
        for f in sorted(log_dir.glob('*.log'), key=lambda x: x.stat().st_mtime, reverse=True):
            log_files.append({
                'name': f.name,
                'size': f.stat().st_size,
                'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'url': url_for('admin_data_view_log', filename=f.name)
            })
    return render_template(
        'admin_logs.html',
        log_files=log_files,
        job_log_entries=_job_log_entries(limit=ADMIN_JOB_LIST_LIMIT),
    )


@app.route('/admin/data/jobs/<job_id>/log')
def admin_data_job_log(job_id):
    """View log file associated with a tracked job."""
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))

    db = get_db()
    job = db.get_admin_job(job_id)
    if not job:
        return redirect(url_for('admin_data_logs', error='Job not found'))

    log_path = _resolve_admin_log_path(str(job.get('log_path') or ''))
    if not log_path:
        return redirect(url_for('admin_data_logs', error='Job log file not found'))

    try:
        content = log_path.read_text(encoding='utf-8')
        filename = str(log_path.relative_to(REPO_ROOT))
        if request.args.get('partial') == '1':
            return f'<pre id="log-text">{content}</pre>'
        return render_template('admin_log_view.html', filename=filename, content=content)
    except Exception as e:
        return redirect(url_for('admin_data_logs', error=str(e)))


@app.route('/admin/data/logs/<filename>')
def admin_data_view_log(filename):
    """View single log file content."""
    if not _admin_authenticated():
        return redirect(url_for('admin_data_page'))
    
    # Security: prevent directory traversal
    if '..' in filename or filename.startswith('/'):
        return redirect(url_for('admin_data_logs'))
    
    log_path = Path(__file__).parent.parent / 'logs' / filename
    
    if not log_path.exists():
        return redirect(url_for('admin_data_logs', error='Log file not found'))
    
    try:
        content = log_path.read_text(encoding='utf-8')
        
        # Support partial requests for AJAX polling
        if request.args.get('partial') == '1':
            return f'<pre id="log-text">{content}</pre>'
        
        return render_template('admin_log_view.html', filename=filename, content=content)
    except Exception as e:
        return redirect(url_for('admin_data_logs', error=str(e)))


def run_server(host='127.0.0.1', port=5000, debug=True):
    """Jalankan Flask development server"""
    print(f"🚀 Starting YouTube Transcript Manager")
    print(f"📊 Database statistics:")
    
    # Create application context for startup statistics
    with app.app_context():
        db = get_db()
        stats = db.get_statistics()
        file_stats = db.get_file_paths()
        
        print(f"   📺 Channels: {stats['total_channels']}")
        print(f"   📹 Videos: {stats['total_videos']}")
        print(f"   ✅ With Transcript: {stats['videos_with_transcript']}")
        print(f"   ❌ Without Transcript: {stats['videos_without_transcript']}")
        print(f"   📝 Total Words: {stats['total_word_count']:,}")
        print(f"   ⏱️  Total Duration: {stats['total_duration_hours']:.1f} hours")
        print(f"\n💾 File Statistics:")
        print(f"   📝 Transcript Files: {file_stats['transcripts']['total']} ({format_file_size(file_stats['transcripts']['total_size'])})")
        print(f"   📄 Summary Files: {file_stats['summaries']['total']} ({format_file_size(file_stats['summaries']['total_size'])})")
        print(f"\n🌐 Server running at http://{host}:{port}")
    start_public_page_prewarm()
    
    app.run(host=host, port=port, debug=debug)


# Prewarm on module load but in background — does not block Passenger startup.
start_public_page_prewarm()



if __name__ == '__main__':
    run_server()
