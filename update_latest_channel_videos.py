#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yt_dlp

from fill_missing_resumes_youtube_db import PROMPT_TEMPLATE
from local_services import (
    coordinator_acquire_accounts,
    coordinator_base_url,
    coordinator_heartbeat_lease,
    coordinator_release_lease,
    coordinator_report_provider_event,
    coordinator_status_accounts,
    fetch_youtube_upload_date,
    is_provider_blocking_enabled,
    is_transient_provider_limit_error,
    yt_dlp_auth_args,
)
from recover_transcripts import TranscriptRecoverer
from recover_transcripts_from_csv import safe_channel_slug

try:
    from groq import Groq
except Exception:
    Groq = None  # type: ignore[assignment]


DB_PATH = "youtube_transcripts.db"
RUNS_DIR = Path("runs")
RESUME_PROVIDERS = ("nvidia", "groq", "cerebras")
LEASE_TTL_SECONDS = 300
DEFAULT_PROMPT_MAX_CHARS = 50000
RESUME_CHUNK_SIZE = 10000
RESUME_CHUNK_MAX_TOKENS = 900
RESUME_CHUNK_RETRY_TOKENS = 1400
GROQ_SLEEP_CHUNK_SECONDS = 3.0
CHANNEL_RUNTIME_TABLE = "channel_runtime_state"
YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
YOUTUBE_VIDEO_URL_ID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{11})(?:[?&/]|$)")


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)


def _candidate_channel_urls(channel_url: str) -> list[str]:
    raw = str(channel_url or "").strip().rstrip("/")
    if not raw:
        return []
    if raw.endswith("/videos") or raw.endswith("/shorts"):
        return [raw]
    return [raw, f"{raw}/videos", f"{raw}/shorts"]


def _yt_dlp_base_cmd() -> list[str]:
    return [sys.executable, "-m", "yt_dlp"]


def _extract_video_id(entry: dict) -> str:
    for key in ("id", "url", "webpage_url", "original_url"):
        raw = str(entry.get(key) or "").strip()
        if not raw:
            continue
        if YOUTUBE_VIDEO_ID_RE.fullmatch(raw):
            return raw
        match = YOUTUBE_VIDEO_URL_ID_RE.search(raw)
        if match:
            video_id = match.group(1)
            if YOUTUBE_VIDEO_ID_RE.fullmatch(video_id):
                return video_id
    return ""


def _normalize_upload_date(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if re.fullmatch(r"\d{8}", value):
        return value
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value.replace("-", "")
    return value


def _current_utc_upload_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _classify_upload_date_error(message: str) -> str:
    text = str(message or "").lower()
    if "available to this channel's members" in text or "join this youtube channel" in text or "members-only" in text or "member-only" in text:
        return "member_only"
    if "age-restricted" in text or "age restricted" in text:
        return "age_restricted"
    if "video unavailable" in text:
        return "video_unavailable"
    if "429" in text or "too many requests" in text or "rate limit" in text:
        return "rate_limited"
    return "unknown"


def _merge_metadata(existing_raw: str, updates: dict[str, object]) -> str:
    payload: dict[str, object] = {}
    raw = str(existing_raw or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                payload.update(parsed)
        except Exception:
            payload = {}
    for key, value in updates.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        payload[key] = value
    return json.dumps(payload, ensure_ascii=False)


@dataclass(frozen=True)
class ProviderAccountLease:
    id: int
    provider: str
    account_name: str
    usage_method: str
    api_key: str
    endpoint_url: str
    model_name: str
    extra_headers: dict[str, str]
    lease_token: str
    model_limits: dict[str, object]

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.account_name}"


class LeaseHeartbeat:
    def __init__(self, lease_token: str, lease_ttl_seconds: int) -> None:
        self.lease_token = str(lease_token or "").strip()
        self.lease_ttl_seconds = max(60, int(lease_ttl_seconds or LEASE_TTL_SECONDS))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.lease_token:
            return
        self._thread = threading.Thread(target=self._run, name="provider-lease-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try:
                coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
            except Exception as exc:
                log(f"[LEASE] heartbeat gagal: {exc}")

    def stop(self, *, final_state: str = "idle", note: str = "") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if not self.lease_token:
            return
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception as exc:
            log(f"[LEASE] release gagal: {exc}")


def build_provider_rotation(provider_models: list[tuple[str, str]], start_index: int) -> list[tuple[int, str, str]]:
    if not provider_models:
        return []
    ordered: list[tuple[int, str, int]] = []
    total = len(provider_models)
    for offset in range(total):
        idx = (start_index + offset) % total
        provider, model_name = provider_models[idx]
        ordered.append((idx, provider, model_name))
    return ordered


def get_provider_model_pool(*, required_model: str = "") -> list[tuple[str, str]]:
    required_model = str(required_model or "").strip()
    available_counts: dict[tuple[str, str], int] = {}
    for provider in RESUME_PROVIDERS:
        try:
            items = coordinator_status_accounts(provider=provider, model_name=required_model)
        except Exception as exc:
            raise RuntimeError(f"Gagal preflight coordinator untuk {provider}/{required_model or '*'}: {exc}") from exc
        for item in items:
            item_provider = str(item.get("provider") or "").strip().lower()
            model_name = str(item.get("runtime_model_name") or item.get("default_model_name") or "").strip()
            state = str(item.get("state") or "idle").strip().lower()
            is_active = int(item.get("is_active") or 0) == 1
            leaseable = bool(item.get("leaseable")) if "leaseable" in item else (state == "idle")
            if not item_provider or not model_name or not is_active:
                continue
            if required_model and model_name != required_model:
                continue
            if not leaseable:
                continue
            key = (item_provider, model_name)
            available_counts[key] = int(available_counts.get(key, 0)) + 1

    pool: list[tuple[str, str]] = []
    for provider in RESUME_PROVIDERS:
        candidates = [(model_name, count) for (item_provider, model_name), count in available_counts.items() if item_provider == provider]
        if not candidates:
            continue
        candidates.sort(key=lambda item: (-int(item[1]), item[0]))
        pool.append((provider, candidates[0][0]))
    return pool


def acquire_provider_account(*, provider: str, model_name: str, holder: str, pid: int) -> ProviderAccountLease:
    leases = coordinator_acquire_accounts(
        provider=provider,
        model_name=model_name,
        count=1,
        holder=holder,
        host="",
        pid=pid,
        task_type="resume_generation",
        lease_ttl_seconds=LEASE_TTL_SECONDS,
    )
    if not leases:
        raise RuntimeError(f"No available coordinator account for {provider}/{model_name}")

    lease = leases[0]
    provider_account_id = int(lease["provider_account_id"])
    api_key = str(lease.get("api_key") or "").strip()
    if not api_key:
        raise RuntimeError(
            f"Coordinator acquire bundle tidak mengandung api_key plaintext untuk account {provider_account_id}"
        )

    headers: dict[str, str] = {}
    raw_headers = lease.get("extra_headers") or {}
    if isinstance(raw_headers, dict):
        headers = {str(k): str(v) for k, v in raw_headers.items()}

    return ProviderAccountLease(
        id=provider_account_id,
        provider=str(lease.get("provider") or provider).strip(),
        account_name=str(lease.get("account_name") or provider_account_id).strip(),
        usage_method=str(lease.get("usage_method") or "").strip(),
        api_key=api_key,
        endpoint_url=str(lease.get("endpoint_url") or "").strip(),
        model_name=model_name,
        extra_headers=headers,
        lease_token=str(lease["lease_token"] or "").strip(),
        model_limits=dict(lease.get("model_limits") or {}),
    )


def extract_message_content(message: object) -> str:
    if isinstance(message, dict):
        content = message.get("content", "")
    else:
        content = getattr(message, "content", "")
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(str(item["text"]))
            else:
                text = getattr(item, "text", None)
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts).strip()
    return str(content or "").strip()


def adaptive_resume_settings(model_limits: dict[str, object] | None) -> dict[str, object]:
    raw = dict(model_limits or {})
    chars_per_token = float(raw.get("chars_per_token") or 4.0)
    prompt_tokens = int(raw.get("recommended_prompt_tokens") or 0)
    completion_tokens = int(raw.get("recommended_completion_tokens") or 0)
    max_output_tokens = int(raw.get("max_output_tokens") or 0)

    chunk_prompt_tokens = prompt_tokens if prompt_tokens > 0 else 2500
    chunk_completion_tokens = completion_tokens if completion_tokens > 0 else RESUME_CHUNK_MAX_TOKENS
    retry_completion_tokens = max(
        chunk_completion_tokens + 400,
        int(chunk_completion_tokens * 1.5),
    )
    if max_output_tokens > 0:
        retry_completion_tokens = min(retry_completion_tokens, max_output_tokens)

    chunk_chars = int(chunk_prompt_tokens * chars_per_token * 0.78)
    chunk_chars = max(4000, min(chunk_chars, 32000))

    final_first = max(1800, int(chunk_completion_tokens * 2))
    final_second = max(2600, int(chunk_completion_tokens * 3))
    if max_output_tokens > 0:
        final_first = min(final_first, max_output_tokens)
        final_second = min(final_second, max_output_tokens)
    final_second = max(final_second, final_first)

    return {
        "chunk_chars": chunk_chars,
        "chunk_max_tokens": max(700, chunk_completion_tokens),
        "chunk_retry_tokens": max(900, retry_completion_tokens),
        "single_pass_max_tokens": (final_first, final_second),
    }


def provider_chat_once(account: ProviderAccountLease, prompt: str, *, max_tokens: int) -> tuple[str, str, int, int]:
    if account.provider == "groq" and account.usage_method == "groq_sdk":
        if Groq is None:
            raise RuntimeError("Paket groq tidak tersedia di venv aktif.")
        client = Groq(api_key=account.api_key, timeout=600)
        kwargs = {
            "model": account.model_name,
            "messages": [
                {"role": "system", "content": "Jawab dalam bahasa Indonesia baku."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
            "max_completion_tokens": int(max_tokens),
            "top_p": 1,
            "stream": False,
            "stop": None,
        }
        model_key = str(account.model_name or "").strip().lower()
        if "gpt-oss" in model_key:
            kwargs["reasoning_effort"] = "medium"
        elif "qwen3" in model_key:
            kwargs["reasoning_effort"] = "default"
        resp = client.chat.completions.create(**kwargs)
        if not getattr(resp, "choices", None):
            raise RuntimeError("Response tanpa choices.")
        choice = resp.choices[0]
        message = getattr(choice, "message", None)
        content = extract_message_content(message)
        finish_reason = str(getattr(choice, "finish_reason", "") or "").strip()
        usage = getattr(resp, "usage", None)
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        reasoning_len = len(str(getattr(message, "reasoning", None) or getattr(message, "reasoning_content", None) or ""))
        return content, finish_reason, completion_tokens, reasoning_len

    payload = {
        "model": account.model_name,
        "messages": [
            {"role": "system", "content": "Jawab dalam bahasa Indonesia baku."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": int(max_tokens),
    }
    if str(account.provider or "").strip().lower() == "z.ai":
        payload["thinking"] = {"type": "disabled"}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(account.endpoint_url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {account.api_key}")
    req.add_header("Content-Type", "application/json")
    for key, value in account.extra_headers.items():
        req.add_header(str(key), str(value))

    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        detail = raw.strip() or exc.reason
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error: {exc.reason}") from exc

    choice = (body.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content = extract_message_content(message)
    finish_reason = str(choice.get("finish_reason") or "").strip()
    completion_tokens = int((body.get("usage") or {}).get("completion_tokens") or 0)
    reasoning_len = len(str(message.get("reasoning") or message.get("reasoning_content") or ""))
    return content, finish_reason, completion_tokens, reasoning_len


def provider_chat_complete(
    account: ProviderAccountLease,
    prompt: str,
    *,
    max_tokens_values: tuple[int, ...] = (3600, 5200),
) -> str:
    last_error = "empty response"
    for max_tokens in max_tokens_values:
        content, finish_reason, completion_tokens, reasoning_len = provider_chat_once(
            account,
            prompt,
            max_tokens=max_tokens,
        )
        if content:
            return content
        if finish_reason == "length":
            last_error = (
                "response_truncated_in_reasoning"
                f" max_tokens={max_tokens}"
                f" completion_tokens={completion_tokens}"
                f" reasoning_len={reasoning_len}"
            )
            log(f"[RESUME-RETRY] {account.label} | {account.model_name} | {last_error}")
            continue
        last_error = (
            "empty response"
            f" finish_reason={finish_reason or '-'}"
            f" completion_tokens={completion_tokens}"
            f" reasoning_len={reasoning_len}"
        )
        break
    raise RuntimeError(last_error)


def fetch_channel_entries(
    channel_url: str,
    *,
    max_entries: Optional[int] = None,
    timeout_seconds: Optional[float] = None,
) -> list[dict]:
    rows: list[dict] = []
    seen_video_ids: set[str] = set()
    errors: list[str] = []
    success_seen = False
    candidates = _candidate_channel_urls(channel_url)
    if not candidates:
        return rows

    effective_timeout = float(timeout_seconds or (300.0 if max_entries is None else 90.0))

    for candidate_url in candidates:
        cmd = _yt_dlp_base_cmd()
        cmd.extend([
            "--dump-single-json",
            "--flat-playlist",
            "--skip-download",
            "--quiet",
            "--no-warnings",
        ])
        cmd.extend(yt_dlp_auth_args(rotate=True))
        if max_entries is not None and int(max_entries) > 0:
            cmd.extend(["--playlist-end", str(int(max_entries))])
        cmd.append(candidate_url)
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            errors.append(f"yt-dlp timeout while scanning channel: {candidate_url}")
            continue
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            errors.append(detail or f"yt-dlp failed for {candidate_url}")
            continue
        success_seen = True
        try:
            info = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            errors.append(f"Invalid yt-dlp JSON output for {candidate_url}")
            continue
        entries = info.get("entries") or []
        for entry in entries:
            if not entry:
                continue
            video_id = _extract_video_id(entry)
            if not video_id or video_id in seen_video_ids:
                continue
            seen_video_ids.add(video_id)
            rows.append(
                {
                    "video_id": video_id,
                    "title": str(entry.get("title") or "Unknown").strip(),
                    "video_url": entry.get("url")
                    or entry.get("webpage_url")
                    or entry.get("original_url")
                    or f"https://www.youtube.com/watch?v={video_id}",
                    "upload_date": str(entry.get("upload_date") or "").strip(),
                    "duration": int(entry.get("duration") or 0),
                    "view_count": int(entry.get("view_count") or 0),
                    "thumbnail_url": str(entry.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"),
                }
            )
    if not rows and not success_seen and errors:
        raise RuntimeError(errors[-1])
    return rows


def ensure_channel_runtime_table(con: sqlite3.Connection) -> None:
    with con:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CHANNEL_RUNTIME_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE NOT NULL,
                scan_enabled INTEGER NOT NULL DEFAULT 1,
                skip_reason TEXT NOT NULL DEFAULT '',
                source_status TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def get_channel_runtime_state(con: sqlite3.Connection, channel_id: str) -> sqlite3.Row | None:
    return con.execute(
        f"""
        SELECT channel_id, scan_enabled, skip_reason, source_status, updated_at
        FROM {CHANNEL_RUNTIME_TABLE}
        WHERE channel_id = ?
        LIMIT 1
        """,
        (str(channel_id).strip(),),
    ).fetchone()


def set_channel_scan_state(
    con: sqlite3.Connection,
    *,
    channel_id: str,
    scan_enabled: bool,
    skip_reason: str = "",
    source_status: str = "",
) -> None:
    with con:
        con.execute(
            f"""
            INSERT INTO {CHANNEL_RUNTIME_TABLE} (
                channel_id, scan_enabled, skip_reason, source_status, updated_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_id) DO UPDATE SET
                scan_enabled=excluded.scan_enabled,
                skip_reason=excluded.skip_reason,
                source_status=excluded.source_status,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                str(channel_id).strip(),
                1 if scan_enabled else 0,
                str(skip_reason or "")[:1000],
                str(source_status or "")[:200],
            ),
        )


def insert_video_if_missing(con: sqlite3.Connection, *, channel_db_id: int, row: dict) -> str:
    upload_date = _normalize_upload_date(str(row.get("upload_date") or ""))
    upload_date_status = "exact"
    upload_date_reason = ""
    upload_date_source = "yt_dlp"
    note = ""
    if not upload_date:
        skip_lookup = str(os.getenv("YT_DISCOVERY_SKIP_UPLOAD_DATE_LOOKUP", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not skip_lookup:
            try:
                upload_date = _normalize_upload_date(fetch_youtube_upload_date(row.get("video_url") or "", timeout_seconds=45))
            except Exception as exc:
                upload_date_status = "estimated"
                upload_date_reason = _classify_upload_date_error(str(exc))
                upload_date_source = "discovery_fallback"
                upload_date = _current_utc_upload_date()
                note = f"upload_date fallback ({upload_date_reason})"
                log(f"  upload_date fallback for {row.get('video_id')}: {upload_date_reason}")
        else:
            upload_date_status = "estimated"
            upload_date_reason = "lookup_skipped"
            upload_date_source = "discovery_direct"
            upload_date = _current_utc_upload_date()
            note = "upload_date lookup skipped"
    metadata_updates = {
        "upload_date_status": upload_date_status,
        "upload_date_reason": upload_date_reason,
        "upload_date_source": upload_date_source,
    }
    existing_metadata = con.execute(
        "SELECT COALESCE(metadata, '') AS metadata FROM videos WHERE video_id = ?",
        (row["video_id"],),
    ).fetchone()
    merged_metadata = _merge_metadata(
        str(existing_metadata["metadata"] or "") if existing_metadata else "",
        metadata_updates,
    )
    with con:
        con.execute(
            """
            INSERT INTO videos
            (video_id, channel_id, title, duration, upload_date, view_count, video_url, thumbnail_url, metadata, is_short)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id = excluded.channel_id,
            title = excluded.title,
            duration = excluded.duration,
            upload_date = CASE
                WHEN COALESCE(videos.upload_date, '') = '' THEN excluded.upload_date
                ELSE videos.upload_date
            END,
            view_count = excluded.view_count,
            video_url = excluded.video_url,
            thumbnail_url = excluded.thumbnail_url,
            metadata = excluded.metadata,
            is_short = excluded.is_short,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            row["video_id"],
            channel_db_id,
            row["title"],
            row["duration"],
            upload_date,
            row["view_count"],
            row["video_url"],
            row["thumbnail_url"],
            merged_metadata,
            1 if (int(row.get("duration") or 0) > 0 and int(row.get("duration") or 0) < 60) or "/shorts/" in (row.get("video_url") or "") else 0,
        ),
    )
    return note


def get_existing_video_state(con: sqlite3.Connection, *, channel_db_id: int) -> dict[str, sqlite3.Row]:
    rows = con.execute(
        """
        SELECT video_id,
               transcript_downloaded,
               COALESCE(transcript_file_path, '') AS transcript_file_path,
               COALESCE(summary_file_path, '') AS summary_file_path,
               COALESCE(transcript_language, '') AS transcript_language
        FROM videos
        WHERE channel_id = ?
        """,
        (channel_db_id,),
    ).fetchall()
    return {str(row["video_id"]): row for row in rows}


def mark_no_subtitle(con: sqlite3.Connection, video_id: str) -> None:
    with con:
        con.execute(
            """
            UPDATE videos
            SET transcript_language = 'no_subtitle',
                transcript_downloaded = 0,
                transcript_file_path = '',
                summary_file_path = '',
                word_count = 0,
                line_count = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (video_id,),
        )


def mark_blocked_member_only(con: sqlite3.Connection, video_id: str, reason: str) -> None:
    with con:
        con.execute(
            """
            UPDATE videos
            SET transcript_language = 'no_subtitle',
                transcript_downloaded = 0,
                transcript_file_path = '',
                summary_file_path = '',
                transcript_retry_reason = ?,
                transcript_retry_after = NULL,
                word_count = 0,
                line_count = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (str(reason or "blocked_member_only")[:500], video_id),
        )


def mark_retry_later(con: sqlite3.Connection, video_id: str, reason: str, retry_after_hours: int = 24) -> None:
    hours = max(1, int(retry_after_hours or 24))
    with con:
        con.execute(
            """
            UPDATE videos
            SET transcript_retry_after = datetime('now', ?),
                transcript_retry_reason = ?,
                transcript_retry_count = COALESCE(transcript_retry_count, 0) + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (f"+{hours} hours", str(reason or "")[:500], video_id),
        )


def save_transcript(recoverer: TranscriptRecoverer, *, channel_slug: str, video_id: str, result: dict) -> str:
    text_dir = Path("uploads") / channel_slug / "text"
    text_dir.mkdir(parents=True, exist_ok=True)
    file_path = text_dir / f"{video_id}_transcript_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    content = str(result["formatted"])
    file_path.write_text(content, encoding="utf-8")
    
    # Buffer update to JSON instead of direct DB write
    try:
        buffer_dir = Path("pending_updates")
        buffer_dir.mkdir(exist_ok=True)
        
        update_data = {
            "video_id": video_id,
            "type": "transcript",
            "status": "ok",
            "file_path": str(file_path),
            "content": content,
            "metadata": {
                "language": str(result["language"]),
                "word_count": int(result["word_count"]),
                "line_count": int(result["line_count"])
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        buffer_file = buffer_dir / f"update_transcript_{video_id}_{int(time.time())}.json"
        buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
        log(f"  [BUFFERED] Transcript result saved to {buffer_file.name}")
    except Exception as e_buffer:
        log(f"  [BUFFER-WARN] Gagal simpan buffer JSON transcript: {e_buffer}")

    return str(file_path)


def build_resume_prompt(title: str, transcript: str) -> str:
    return PROMPT_TEMPLATE.format(title=title, transcript=transcript[:DEFAULT_PROMPT_MAX_CHARS])


def chunk_text(text: str, max_chars: int) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    parts: list[str] = []
    paras = re.split(r"\n\s*\n", text)
    buf: list[str] = []
    size = 0
    for para in paras:
        para = para.strip()
        if not para:
            continue
        add_len = len(para) + (2 if buf else 0)
        if size + add_len <= max_chars:
            buf.append(para)
            size += add_len
            continue
        if buf:
            parts.append("\n\n".join(buf))
        if len(para) <= max_chars:
            buf = [para]
            size = len(para)
            continue
        for i in range(0, len(para), max_chars):
            parts.append(para[i : i + max_chars])
        buf = []
        size = 0
    if buf:
        parts.append("\n\n".join(buf))
    return parts


def split_chunk(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return [text]
    mid = len(text) // 2
    split_at = text.rfind("\n\n", 0, mid)
    if split_at < 0:
        split_at = text.rfind("\n", 0, mid)
    if split_at < 0:
        split_at = mid
    left = text[:split_at].strip()
    right = text[split_at:].strip()
    if not left or not right:
        left = text[:mid].strip()
        right = text[mid:].strip()
    if not left or not right:
        return [text]
    return [left, right]


def build_chunk_prompt(chunk: str, idx: int, total: int) -> str:
    return (
        "Anda adalah analis konten profesional.\n"
        "Tugas: ringkas bagian transkrip berikut secara faktual, rinci, dan menjaga detail penting.\n"
        "Aturan:\n"
        "- Gunakan HANYA informasi di potongan ini.\n"
        "- Jangan mengarang fakta/angka/nama yang tidak ada.\n"
        "- Gunakan Bahasa Indonesia.\n"
        "- Jangan membuat bullet yang terlalu pendek atau generik.\n"
        "- Cantumkan argumen utama, contoh, istilah penting, dan alur pembahasan yang muncul di potongan ini.\n"
        "- Output berupa bullet list yang cukup rinci. Jika perlu, kelompokkan dengan subjudul pendek.\n"
        "\n"
        f"Bagian {idx}/{total}:\n"
        f"{chunk}"
    )


def build_final_prompt(title: str, chunk_summaries: list[str]) -> str:
    joined = "\n\n".join(f"### Bagian {i}\n{txt.strip()}" for i, txt in enumerate(chunk_summaries, start=1))
    transcript_block = (
        "Catatan: Transkrip terlalu panjang, berikut ini ringkasan per bagian yang dibuat dari transkrip asli. "
        "Gunakan hanya informasi di bawah.\n\n"
        f"{joined}"
    )
    return PROMPT_TEMPLATE.format(title=title, transcript=transcript_block.strip())


def generate_resume_markdown(account: ProviderAccountLease, *, title: str, transcript: str) -> str:
    transcript = (transcript or "").strip()
    settings = adaptive_resume_settings(account.model_limits)
    chunk_chars = int(settings["chunk_chars"])
    chunk_max_tokens = int(settings["chunk_max_tokens"])
    chunk_retry_tokens = int(settings["chunk_retry_tokens"])
    single_pass_max_tokens = tuple(int(x) for x in settings["single_pass_max_tokens"])
    if len(transcript) <= chunk_chars:
        return provider_chat_complete(
            account,
            build_resume_prompt(title, transcript),
            max_tokens_values=single_pass_max_tokens,
        )

    chunks = chunk_text(transcript, max_chars=chunk_chars)
    summaries: list[str] = []
    min_chunk_chars = max(2000, int(chunk_chars * 0.3))
    idx = 0
    while idx < len(chunks):
        chunk = chunks[idx]
        try:
            part = provider_chat_complete(
                account,
                build_chunk_prompt(chunk, idx + 1, len(chunks)),
                max_tokens_values=(chunk_max_tokens, chunk_retry_tokens),
            ).strip()
            if not part:
                raise RuntimeError("Ringkasan chunk kosong setelah request.")
            summaries.append(part)
            if account.provider == "groq" and GROQ_SLEEP_CHUNK_SECONDS > 0:
                time.sleep(GROQ_SLEEP_CHUNK_SECONDS)
            idx += 1
        except Exception as exc:
            if len(chunk) > min_chunk_chars:
                new_chunks = split_chunk(chunk)
                if len(new_chunks) > 1:
                    log(
                        f"[RESUME-CHUNK-SPLIT] {account.label} chunk {idx + 1}/{len(chunks)} gagal ({exc}); "
                        f"split jadi {len(new_chunks)} bagian"
                    )
                    chunks[idx : idx + 1] = new_chunks
                    continue
            raise
    return provider_chat_complete(
        account,
        build_final_prompt(title, summaries),
        max_tokens_values=single_pass_max_tokens,
    )


def create_resume(
    con: sqlite3.Connection,
    *,
    video_id: str,
    title: str,
    transcript_path: str,
    provider_models: list[tuple[str, str]],
    start_index: int,
) -> tuple[str, int]:
    transcript_file = Path(transcript_path)
    transcript = transcript_file.read_text(encoding="utf-8")

    last_error = "no provider"
    holder = socket.gethostname()
    pid = os.getpid()
    rotation = build_provider_rotation(provider_models, start_index)
    for provider_index, provider, model_name in rotation:
        account: Optional[ProviderAccountLease] = None
        lease: Optional[LeaseHeartbeat] = None
        try:
            account = acquire_provider_account(
                provider=provider,
                model_name=model_name,
                holder=holder,
                pid=pid,
            )
            lease = LeaseHeartbeat(account.lease_token, LEASE_TTL_SECONDS)
            lease.start()

            log(f"[RESUME] {video_id} {account.label} | {account.model_name}")
            result = generate_resume_markdown(account, title=title, transcript=transcript)
            if not result:
                raise RuntimeError("empty response")
            resume_path = transcript_file.parent.parent / "resume" / f"{video_id}_summary.md"
            resume_path.parent.mkdir(parents=True, exist_ok=True)
            resume_path.write_text(result, encoding="utf-8")
            
            # Buffer update to JSON instead of direct DB write
            try:
                buffer_dir = Path("pending_updates")
                buffer_dir.mkdir(exist_ok=True)
                
                update_data = {
                    "video_id": video_id,
                    "type": "resume",
                    "status": "ok",
                    "file_path": str(resume_path),
                    "content": result,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                buffer_file = buffer_dir / f"update_resume_{video_id}_{int(time.time())}.json"
                buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                log(f"  [BUFFERED] Resume result saved to {buffer_file.name}")
            except Exception as e_buffer:
                log(f"  [BUFFER-WARN] Gagal simpan buffer JSON resume: {e_buffer}")
            lease.stop(final_state="idle")
            return str(resume_path), provider_index
        except Exception as exc:
            last_error = str(exc)
            label = account.label if account else f"{provider}:{model_name}"
            log(f"[RESUME-ERROR] {video_id} {label}: {last_error}")

            final_state = "error"
            if account is not None:
                try:
                    report_resp = coordinator_report_provider_event(
                        provider_account_id=account.id,
                        provider=account.provider,
                        model_name=account.model_name,
                        reason=last_error,
                        source="update_latest_channel_videos",
                        payload={"video_id": video_id, "title": title},
                    )
                    decision = report_resp.get("decision") or {}
                    action = str(decision.get("action") or "")
                    if action == "disabled":
                        final_state = "disabled"
                    elif action == "blocked":
                        final_state = "blocked"
                except Exception as report_exc:
                    log(f"[RESUME-REPORT-ERROR] {label}: {report_exc}")

            if lease is not None:
                lease.stop(final_state=final_state, note=last_error[:500])

            if account is not None and is_provider_blocking_enabled(account.provider):
                if is_transient_provider_limit_error(last_error, provider=account.provider):
                    continue
    raise RuntimeError(last_error)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--channel-limit", type=int, default=0)
    parser.add_argument("--recent-per-channel", type=int, default=50)
    parser.add_argument("--channel-id", default="", help="Jalankan hanya untuk satu channel_id tertentu.")
    parser.add_argument("--channel-name", default="", help="Jalankan hanya untuk satu channel_name tertentu.")
    parser.add_argument(
        "--resume-model",
        default="openai/gpt-oss-120b",
        help="Model resume yang diwajibkan. Provider yang tidak punya model ini tidak dipakai.",
    )
    parser.add_argument(
        "--scan-all-missing",
        action="store_true",
        help="Scan seluruh riwayat channel untuk menangkap semua video yang belum ada di DB atau masih incomplete.",
    )
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--discovery-only", action="store_true")
    parser.add_argument(
        "--rate-limit-safe",
        action="store_true",
        help="Kurangi tekanan ke YouTube dengan menunda antar channel dan melewati lookup upload_date tambahan.",
    )
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir) if args.run_dir else RUNS_DIR / f"update_latest_channels_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    provider_models: list[tuple[str, str]] = []
    if args.discovery_only:
        log("Mode: discovery-only")
    else:
        provider_models = get_provider_model_pool(required_model=str(args.resume_model or "").strip())
        if not provider_models:
            raise SystemExit(
                f"No active provider/model pool found in coordinator DB for {', '.join(RESUME_PROVIDERS)} with model {args.resume_model}"
            )
        log(f"Coordinator: {coordinator_base_url()}")
        log(
            "Resume pool: "
            + ", ".join(f"{provider}:{model_name}" for provider, model_name in provider_models)
        )

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    ensure_channel_runtime_table(con)
    recoverer = TranscriptRecoverer()

    channel_id_filter = str(args.channel_id or "").strip()
    channel_name_filter = str(args.channel_name or "").strip()
    if channel_id_filter and channel_name_filter:
        raise SystemExit("Gunakan hanya salah satu dari --channel-id atau --channel-name")

    channels_sql = """
        SELECT id, channel_name, channel_id, channel_url
        FROM channels
    """
    channels_params: list[object] = []
    if channel_id_filter:
        channels_sql += " WHERE channel_id = ? "
        channels_params.append(channel_id_filter)
    elif channel_name_filter:
        channels_sql += " WHERE channel_name = ? "
        channels_params.append(channel_name_filter)
    channels_sql += """
        ORDER BY id ASC
    """
    if not channel_id_filter and not channel_name_filter and args.channel_limit > 0:
        channels_sql += f" LIMIT {int(args.channel_limit)}"
    channels = con.execute(channels_sql, channels_params).fetchall()
    if (channel_id_filter or channel_name_filter) and not channels:
        label = f"channel_id={channel_id_filter}" if channel_id_filter else f"channel_name={channel_name_filter}"
        raise SystemExit(f"Channel tidak ditemukan untuk filter {label}")
    log(f"Scanning {len(channels)} channels")
    scope_label = "full_history" if (args.scan_all_missing or args.recent_per_channel == 0) else f"latest_{int(args.recent_per_channel)}"
    next_provider_index = 0
    channel_delay_seconds = 0.0
    if args.rate_limit_safe:
        channel_delay_seconds = max(0.0, float(os.getenv("YT_DISCOVERY_CHANNEL_DELAY_SECONDS", "2") or 2))
    max_consecutive_hard_blocks = max(1, int(str(os.getenv("YT_DISCOVERY_MAX_CONSECUTIVE_HARD_BLOCKS", "3")).strip() or "3"))
    consecutive_hard_blocks = 0
    stopped_early = False

    report_path = run_dir / "report.csv"
    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "channel_name",
                "channel_id",
                "video_id",
                "video_title",
                "video_url",
                "upload_date",
                "duration",
                "view_count",
                "thumbnail_url",
                "discovery_status",
                "transcript_status",
                "resume_status",
                "transcript_file_path",
                "summary_file_path",
                "scanned_entries",
                "scan_scope",
                "note",
            ],
        )
        writer.writeheader()

        for ch_index, channel in enumerate(channels, start=1):
            channel_name = str(channel["channel_name"])
            channel_id = str(channel["channel_id"])
            channel_slug = safe_channel_slug(channel_id)
            channel_url = str(channel["channel_url"])
            runtime_state = get_channel_runtime_state(con, channel_id)
            if runtime_state is not None and int(runtime_state["scan_enabled"] or 0) == 0:
                note = str(runtime_state["skip_reason"] or runtime_state["source_status"] or "scan disabled").strip()
                log(f"[CHANNEL {ch_index}/{len(channels)}] {channel_name} | skip permanen | {note}")
                writer.writerow(
                    {
                        "channel_name": channel_name,
                        "channel_id": channel_id,
                        "video_id": "",
                        "video_title": "",
                        "video_url": "",
                        "upload_date": "",
                        "duration": "",
                        "view_count": "",
                        "thumbnail_url": "",
                        "discovery_status": "channel_skipped",
                        "transcript_status": "",
                        "resume_status": "",
                        "transcript_file_path": "",
                        "summary_file_path": "",
                        "scanned_entries": 0,
                        "scan_scope": scope_label,
                        "note": note,
                    }
                )
                continue
            log(f"[CHANNEL {ch_index}/{len(channels)}] {channel_name} | {channel_url}")

            try:
                full_history_scan = args.scan_all_missing or args.recent_per_channel == 0
                fetched_entries = fetch_channel_entries(
                    channel_url,
                    max_entries=None if full_history_scan else args.recent_per_channel,
                    timeout_seconds=300.0 if full_history_scan else 90.0,
                )
            except Exception as exc:
                detail = str(exc).strip()
                if ("HTTP Error 404" in detail or "HTTP 404" in detail) and channel_id == "parkerprompts":
                    set_channel_scan_state(
                        con,
                        channel_id=channel_id,
                        scan_enabled=False,
                        skip_reason="YouTube channel/account banned or unavailable; skip permanent from normal scan",
                        source_status="source_banned_404",
                    )
                writer.writerow(
                    {
                        "channel_name": channel_name,
                        "channel_id": channel_id,
                        "video_id": "",
                        "video_title": "",
                        "video_url": "",
                        "upload_date": "",
                        "duration": "",
                        "view_count": "",
                        "thumbnail_url": "",
                        "discovery_status": "channel_error",
                        "transcript_status": "",
                        "resume_status": "",
                        "transcript_file_path": "",
                        "summary_file_path": "",
                        "scanned_entries": 0,
                        "scan_scope": scope_label,
                        "note": detail,
                    }
                )
                continue

            existing_state = get_existing_video_state(con, channel_db_id=int(channel["id"]))
            candidates: list[tuple[str, dict]] = []
            for entry in fetched_entries:
                duration = int(entry.get("duration") or 0)
                video_url = str(entry.get("video_url") or "")
                
                # Filter Shorts: Jangan simpan, jangan proses (Kebijakan Baru)
                is_short = (duration > 0 and duration < 60) or "/shorts/" in video_url
                if is_short:
                    continue

                state = existing_state.get(entry["video_id"])
                if state is None:
                    candidates.append(("new", entry))
                    continue

                transcript_path = str(state["transcript_file_path"] or "")
                summary_path = str(state["summary_file_path"] or "")
                transcript_language = str(state["transcript_language"] or "")
                transcript_downloaded = int(state["transcript_downloaded"] or 0)

                if transcript_language == "no_subtitle":
                    continue
                if transcript_downloaded == 1 and transcript_path and summary_path:
                    continue

                candidates.append(("retry_incomplete", entry))

            log(f"  found {len(candidates)} actionable videos within {len(fetched_entries)} scanned entries ({scope_label})")
            if args.discovery_only and not candidates:
                writer.writerow(
                    {
                        "channel_name": channel_name,
                        "channel_id": channel["channel_id"],
                        "video_id": "",
                        "video_title": "",
                        "video_url": "",
                        "upload_date": "",
                        "duration": "",
                        "view_count": "",
                        "thumbnail_url": "",
                        "discovery_status": "no_actionable",
                        "transcript_status": "",
                        "resume_status": "",
                        "transcript_file_path": "",
                        "summary_file_path": "",
                        "scanned_entries": len(fetched_entries),
                        "scan_scope": scope_label,
                        "note": "",
                    }
                )

            for discovery_status, entry in candidates:
                if args.discovery_only:
                    insert_note = ""
                    if discovery_status == "new":
                        insert_note = insert_video_if_missing(con, channel_db_id=int(channel["id"]), row=entry)
                    writer.writerow(
                        {
                            "channel_name": channel_name,
                            "channel_id": channel["channel_id"],
                            "video_id": entry["video_id"],
                            "video_title": entry["title"],
                            "video_url": entry["video_url"],
                            "upload_date": entry["upload_date"],
                            "duration": entry["duration"],
                            "view_count": entry["view_count"],
                            "thumbnail_url": entry["thumbnail_url"],
                            "discovery_status": discovery_status,
                            "transcript_status": "",
                            "resume_status": "",
                            "transcript_file_path": "",
                            "summary_file_path": "",
                            "scanned_entries": len(fetched_entries),
                            "scan_scope": scope_label,
                            "note": insert_note,
                        }
                    )
                    continue

                note = insert_video_if_missing(con, channel_db_id=int(channel["id"]), row=entry)

                transcript_status = ""
                resume_status = ""
                transcript_path = ""
                summary_path = ""
                current = con.execute(
                    """
                    SELECT transcript_downloaded,
                           COALESCE(transcript_file_path, '') AS transcript_file_path,
                           COALESCE(summary_file_path, '') AS summary_file_path
                    FROM videos
                    WHERE video_id = ?
                    """,
                    (entry["video_id"],),
                ).fetchone()

                existing_transcript_path = str(current["transcript_file_path"] or "") if current else ""
                existing_summary_path = str(current["summary_file_path"] or "") if current else ""
                existing_transcript_ok = bool(current and int(current["transcript_downloaded"] or 0) == 1 and existing_transcript_path)
                existing_summary_ok = bool(existing_summary_path)

                result = None
                outcome = "fatal"
                if existing_transcript_ok and Path(existing_transcript_path).exists():
                    consecutive_hard_blocks = 0
                    transcript_path = existing_transcript_path
                    transcript_status = "existing"
                else:
                    try:
                        result, outcome = recoverer.download_transcript(entry["video_id"])
                    except Exception as exc:
                        result, outcome = None, "fatal"
                        note = "; ".join(part for part in [note, str(exc)] if part)

                if result:
                    consecutive_hard_blocks = 0
                    transcript_path = save_transcript(
                        recoverer,
                        channel_slug=channel_slug,
                        video_id=entry["video_id"],
                        result=result,
                        )
                    transcript_status = "downloaded"
                elif outcome == "retry_later":
                    consecutive_hard_blocks = 0
                    retry_reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip()
                    mark_retry_later(con, entry["video_id"], retry_reason or "retry_later")
                    transcript_status = "retry_later"
                    note = note or "challenge/rate-limit; retry later"
                elif outcome == "proxy_block":
                    consecutive_hard_blocks += 1
                    retry_reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip()
                    mark_retry_later(con, entry["video_id"], retry_reason or "proxy_block")
                    transcript_status = "proxy_block"
                    note = note or "proxy block; retry later"
                elif outcome == "fatal":
                    consecutive_hard_blocks = 0
                    transcript_status = "fatal_error"
                    note = note or "fatal transcript error"
                elif outcome == "blocked":
                    consecutive_hard_blocks += 1
                    retry_reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip()
                    mark_blocked_member_only(con, entry["video_id"], retry_reason or "blocked_member_only")
                    transcript_status = "blocked"
                    resume_status = "skipped"
                    note = note or retry_reason or "blocked member-only"
                elif not transcript_status:
                    consecutive_hard_blocks = 0
                    mark_no_subtitle(con, entry["video_id"])
                    transcript_status = "no_subtitle"
                    resume_status = "skipped"

                if transcript_status in {"downloaded", "existing"}:
                    if existing_summary_ok and Path(existing_summary_path).exists():
                        summary_path = existing_summary_path
                        resume_status = "existing"
                    else:
                        try:
                            summary_path, used_key_index = create_resume(
                                con,
                                video_id=entry["video_id"],
                                title=entry["title"],
                                transcript_path=transcript_path,
                                provider_models=provider_models,
                                start_index=next_provider_index,
                            )
                            next_provider_index = (used_key_index + 1) % len(provider_models)
                            resume_status = "done"
                        except Exception as exc:
                            resume_status = "error"
                            note = "; ".join(part for part in [note, str(exc)] if part)

                writer.writerow(
                    {
                        "channel_name": channel_name,
                        "channel_id": channel["channel_id"],
                        "video_id": entry["video_id"],
                        "video_title": entry["title"],
                        "video_url": entry["video_url"],
                        "upload_date": entry["upload_date"],
                        "duration": entry["duration"],
                        "view_count": entry["view_count"],
                        "thumbnail_url": entry["thumbnail_url"],
                        "discovery_status": discovery_status,
                        "transcript_status": transcript_status,
                        "resume_status": resume_status,
                        "transcript_file_path": transcript_path,
                        "summary_file_path": summary_path,
                        "scanned_entries": len(fetched_entries),
                        "scan_scope": scope_label,
                        "note": note,
                    }
                )

                if consecutive_hard_blocks >= max_consecutive_hard_blocks:
                    log(
                        f"🛑 BERHENTI: {consecutive_hard_blocks} hard block berturut-turut "
                        f"(threshold={max_consecutive_hard_blocks})."
                    )
                    stopped_early = True
                    break

            if stopped_early:
                break
            if channel_delay_seconds > 0 and ch_index < len(channels):
                log(f"  sleeping {channel_delay_seconds:.1f}s before next channel")
                time.sleep(channel_delay_seconds)

        if stopped_early:
            log("🛑 Batch discovery/transcript dihentikan lebih awal karena hard block berulang.")
            return 2

    log(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
