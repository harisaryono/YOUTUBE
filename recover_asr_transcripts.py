#!/usr/bin/env python3
"""
Recover transcript audio YouTube via ASR provider (Groq / NVIDIA Whisper).

Pipeline:
1. Resolve target videos.
2. Download audio once with yt-dlp.
3. Split audio into fixed-size chunks.
4. Send each chunk to provider(s) in order until one succeeds.
5. Persist every chunk result to SQLite immediately.
6. Merge chunk text only when all chunks succeed.
7. Persist final transcript path + merged text back to videos table.

This script is intentionally resumable: chunk results are stored per video,
provider, and chunk index so a rerun can continue from the first missing chunk.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import time
import threading
import wave
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

try:
    import grpc
except Exception:  # pragma: no cover - optional dependency guard
    grpc = None

try:
    from google.protobuf.json_format import MessageToJson
except Exception:  # pragma: no cover - optional dependency guard
    MessageToJson = None

try:
    from riva.client import ASRService as RivaASRService
    from riva.client import AudioEncoding, Auth, RecognitionConfig, add_audio_file_specs_to_config
except Exception:  # pragma: no cover - optional dependency guard
    RivaASRService = None
    AudioEncoding = None
    Auth = None
    RecognitionConfig = None
    add_audio_file_specs_to_config = None

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency guard
    OpenAI = None

load_dotenv()
for _candidate in (Path(__file__).resolve().parent / ".env", Path(__file__).resolve().parent / ".env.local"):
    if _candidate.exists():
        load_dotenv(_candidate, override=False)

from database_optimized import OptimizedDatabase
from local_services import (
    coordinator_acquire_accounts,
    coordinator_heartbeat_lease,
    coordinator_release_lease,
    coordinator_report_provider_event,
    coordinator_status_accounts,
    yt_dlp_auth_args,
    yt_dlp_command,
)


DB_PATH = "youtube_transcripts.db"
BASE_DIR = Path("uploads")
LOG_FILE = str(os.getenv("ASR_LOG_FILE", "recover_asr_transcripts.log") or "recover_asr_transcripts.log")

DEFAULT_GROQ_BASE_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
DEFAULT_NVIDIA_BASE_URL = "http://127.0.0.1:9000/v1/audio/transcriptions"
DEFAULT_NVIDIA_RIVA_SERVER = "grpc.nvcf.nvidia.com:443"
DEFAULT_NVIDIA_RIVA_FUNCTION_ID = "b702f636-f60c-4a3d-a6f4-f3568c13bd7d"
DEFAULT_ASR_AUDIO_FORMAT_SELECTOR = "ba[abr<=96]/ba[abr<=128]/ba[abr<=160]/ba/b"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on", "y"}:
        return True
    if raw in {"0", "false", "no", "off", "n"}:
        return False
    return default


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "item"


def _clean_text(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lines = [re.sub(r"\s+", " ", line).strip() for line in raw.splitlines()]
    return "\n".join(line for line in lines if line)


def _format_ms(total_ms: int) -> str:
    total_ms = max(0, int(total_ms))
    hours, remainder = divmod(total_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, ms = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{ms:03d}"


def _format_youtube_timestamp(total_ms: int) -> str:
    total_ms = max(0, int(total_ms))
    total_seconds = total_ms // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _coerce_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return float(value)
    except Exception:
        return None


def _strip_code_fences(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _extract_message_content(message: object) -> str:
    if message is None:
        return ""
    if isinstance(message, str):
        return message.strip()
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts).strip()
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
                elif isinstance(item, str):
                    parts.append(item)
            return "".join(parts).strip()
    return str(content or "").strip()


def _looks_timestamped_transcript(text: str) -> bool:
    non_empty = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if not non_empty:
        return False
    timestamp_re = re.compile(r"^(?:\d{1,2}:\d{2}(?::\d{2})?|\d{1,2}:\d{2}\.\d{3}|\[\d{1,2}:\d{2}(?::\d{2})?(?:\.\d{3})?\])\s+")
    matches = sum(1 for line in non_empty if timestamp_re.match(line))
    return matches >= max(1, len(non_empty) // 2)


def _normalize_transcript_line(line: str) -> str:
    raw = str(line or "").strip()
    if not raw:
        return ""
    raw = _strip_code_fences(raw)
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip()


def _parse_line_timestamp(line: str) -> tuple[int, str] | None:
    raw = str(line or "").strip()
    if not raw:
        return None
    match = re.match(r"^(?P<ts>(?:\d{1,2}:\d{2}(?::\d{2})?|\d{1,2}:\d{2}\.\d{3}|\[\d{1,2}:\d{2}(?::\d{2})?(?:\.\d{3})?\]))\s+(?P<text>.+)$", raw)
    if not match:
        return None
    ts = match.group("ts").strip("[]")
    text = _normalize_transcript_line(match.group("text"))
    parts = ts.split(":")
    try:
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = float(parts[1])
            total_ms = int(round((minutes * 60 + seconds) * 1000))
        elif len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
            total_ms = int(round((hours * 3600 + minutes * 60 + seconds) * 1000))
        else:
            return None
    except Exception:
        return None
    return total_ms, text


def _count_transcript_words(text: str) -> int:
    total = 0
    for raw_line in str(text or "").splitlines():
        line = _normalize_transcript_line(raw_line)
        if not line:
            continue
        parsed = _parse_line_timestamp(line)
        if parsed is not None:
            _, content = parsed
        else:
            content = line
        total += len([word for word in content.split() if word])
    return total


def _extract_timestamp_sequence(text: str) -> list[int]:
    sequence: list[int] = []
    for raw_line in str(text or "").splitlines():
        parsed = _parse_line_timestamp(_normalize_transcript_line(raw_line))
        if parsed is None:
            continue
        sequence.append(int(parsed[0]))
    return sequence


def _read_json_payload(raw_text: str) -> dict:
    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {"text": raw_text}


def _parse_provider_error(status_code: int, payload: dict | None, raw_text: str) -> str:
    text = ""
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            text = str(err.get("message") or err.get("detail") or "").strip()
        elif err:
            text = str(err).strip()
        if not text:
            text = str(payload.get("message") or payload.get("detail") or payload.get("text") or "").strip()
    if not text:
        text = str(raw_text or "").strip()
    if not text:
        text = f"HTTP {status_code}"
    return text[:1000]


def _is_fatal_provider_error(status_code: int, error_text: str) -> bool:
    lowered = error_text.lower()
    if status_code in {401, 403, 404}:
        return True
    fatal_markers = [
        "invalid api key",
        "missing authentication",
        "unauthorized",
        "forbidden",
        "unauthenticated",
        "permission denied",
        "not found",
        "model not found",
        "access denied",
        "unimplemented",
        "rpc error",
    ]
    return any(marker in lowered for marker in fatal_markers)


@dataclass
class ProviderConfig:
    name: str
    base_url: str
    api_key: str = ""
    model: str = ""


@dataclass
class ProviderLease:
    provider: str
    model_name: str
    provider_account_id: int
    account_name: str
    api_key: str
    lease_token: str
    endpoint_url: str
    usage_method: str
    extra_headers: dict[str, str]
    model_limits: dict[str, object]
    lease_ttl_seconds: int
    audio_endpoint_url: str = ""
    _heartbeat_stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _heartbeat_thread: threading.Thread | None = None

    def start_heartbeat(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        def _loop() -> None:
            interval = max(30.0, float(self.lease_ttl_seconds) / 3.0)
            while not self._heartbeat_stop.wait(interval):
                try:
                    coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
                except Exception:
                    # Heartbeat failure should not crash the job immediately; the next
                    # provider request will reveal whether the lease is still valid.
                    continue

        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(target=_loop, name=f"lease-heartbeat-{self.provider}-{self.provider_account_id}", daemon=True)
        self._heartbeat_thread.start()

    def stop(self, *, final_state: str = "idle", note: str = "") -> None:
        try:
            self._heartbeat_stop.set()
        except Exception:
            pass
        try:
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception:
            pass


@dataclass
class TranscriptionAttempt:
    ok: bool
    text: str = ""
    language: str = ""
    raw_response_json: str = ""
    error_text: str = ""
    status_code: int = 0
    fatal: bool = False


@dataclass
class TranscriptEntry:
    start_ms: int
    end_ms: int
    text: str


class ASRPipeline:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.db = OptimizedDatabase(DB_PATH, str(BASE_DIR))
        self.repo_root = Path(__file__).resolve().parent
        self.run_dir = Path(args.run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.audio_dir = self._resolve_audio_dir()
        self.cache_root = self._resolve_cache_root()
        self.tasks_path = self.run_dir / "tasks.csv"
        self.report_path = self.run_dir / "recover_asr_report.csv"
        self.video_root = BASE_DIR / "asr"
        self.video_root.mkdir(parents=True, exist_ok=True)
        self.source_dir = self.audio_dir
        self.chunk_dir = self.run_dir / "chunks"
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.http = requests.Session()
        self.providers = self._build_provider_configs()
        self.provider_leases: dict[str, ProviderLease] = {}
        self.provider_order = self._provider_order()
        self.chunk_seconds = max(10, int(args.chunk_seconds))
        self.overlap_seconds = max(0, int(args.overlap_seconds))
        self.language = str(args.language or "multi").strip() or "multi"
        self.enable_postprocess = bool(getattr(args, "postprocess", False))
        self.download_only = bool(getattr(args, "download_only", False))
        self.local_audio_only = bool(getattr(args, "local_audio_only", False))
        self.require_cached_audio = bool(getattr(args, "require_cached_audio", False))
        self.delete_audio_after_success = bool(getattr(args, "delete_audio_after_success", False))
        self.video_workers = max(1, int(getattr(args, "video_workers", 1) or 1))
        self.postprocess_provider = str(os.getenv("ASR_POSTPROCESS_PROVIDER") or "nvidia").strip().lower() or "nvidia"
        self.postprocess_model = str(os.getenv("ASR_POSTPROCESS_MODEL") or "openai/gpt-oss-120b").strip()
        self.lease_ttl_seconds = max(300, _env_int("ASR_LEASE_TTL_SECONDS", 300))
        self.max_chunk_failures = max(1, _env_int("ASR_MAX_CHUNK_FAILURES", 1))
        self.request_timeout = max(30, _env_int("ASR_HTTP_TIMEOUT_SECONDS", 600))
        self.postprocess_timeout = max(60, _env_int("ASR_POSTPROCESS_TIMEOUT_SECONDS", 600))
        self.retry_delay = max(1.0, _env_float("ASR_RETRY_DELAY_SECONDS", 5.0))
        self.max_retries = max(1, _env_int("ASR_HTTP_RETRIES", 2))
        self.postprocess_max_chars = max(12000, _env_int("ASR_POSTPROCESS_MAX_CHARS", 30000))
        self.postprocess_max_lines = max(60, _env_int("ASR_POSTPROCESS_MAX_LINES", 240))
        self.postprocess_max_chunks = max(1, _env_int("ASR_POSTPROCESS_MAX_CHUNKS", 12))
        self.groq_asph_cooldown_seconds = max(300, _env_int("ASR_GROQ_ASPH_COOLDOWN_SECONDS", 86400))
        self.prefer_existing = True
        self.coordinator_holder = str(args.coordinator_holder or "").strip() or f"asr-{os.getpid()}"
        self.coordinator_host = os.uname().nodename
        self.coordinator_pid = os.getpid()
        self._report_status_path = self.run_dir / "coordinator_status.json"
        self._provider_disabled_until: dict[str, float] = {}
        self._ensure_coordinator_ready()

    def _resolve_audio_dir(self) -> Path:
        raw = str(
            getattr(self.args, "audio_dir", "")
            or os.getenv("ASR_AUDIO_DIR")
            or os.getenv("YT_ASR_AUDIO_DIR")
            or (BASE_DIR / "audio")
        ).strip()
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        return path

    def _resolve_cache_root(self) -> Path:
        if self.run_dir.parent.name == "workers" and self.run_dir.parent.parent.exists():
            return self.run_dir.parent.parent
        return self.run_dir

    def _audio_cache_dirs(self, video_id: str) -> list[Path]:
        slug = _slug(video_id)
        dirs = [self.source_dir]
        legacy_local = self.run_dir / "source"
        if legacy_local != self.source_dir:
            dirs.append(legacy_local)
        if self.cache_root != self.run_dir:
            legacy_workers_root = self.cache_root / "workers"
            if legacy_workers_root.exists():
                dirs.extend(sorted(path for path in legacy_workers_root.glob("*/source") if path.is_dir()))
        return [directory / slug for directory in dirs]

    def _provider_order(self) -> list[str]:
        raw = str(self.args.providers or "groq,nvidia").strip()
        items = [item.strip().lower() for item in raw.split(",") if item.strip()]
        if not items:
            items = ["groq", "nvidia"]
        return items

    def _build_provider_configs(self) -> dict[str, ProviderConfig]:
        configs: dict[str, ProviderConfig] = {}
        groq_model = str(os.getenv("ASR_MODEL_GROQ") or self.args.model or "whisper-large-v3").strip()
        nvidia_model = str(os.getenv("ASR_MODEL_NVIDIA") or self.args.model or "whisper-large-v3").strip()
        groq_base = str(os.getenv("GROQ_ASR_BASE_URL") or DEFAULT_GROQ_BASE_URL).strip()
        nvidia_base = str(os.getenv("NVIDIA_ASR_BASE_URL") or DEFAULT_NVIDIA_BASE_URL).strip()
        configs["groq"] = ProviderConfig(name="groq", base_url=groq_base, model=groq_model)
        configs["nvidia"] = ProviderConfig(name="nvidia", base_url=nvidia_base, model=nvidia_model)

        return configs

    def _ensure_coordinator_ready(self) -> None:
        if self.download_only:
            return
        if not str(os.getenv("YT_PROVIDER_COORDINATOR_URL") or "").strip():
            raise RuntimeError("YT_PROVIDER_COORDINATOR_URL harus diatur agar ASR memakai lease coordinator.")

    def _refresh_coordinator_status(self) -> list[dict]:
        rows: list[dict] = []
        try:
            for provider_name in self.provider_order:
                provider_cfg = self.providers.get(provider_name)
                if not provider_cfg:
                    continue
                rows.extend(
                    coordinator_status_accounts(
                        provider=provider_name,
                        model_name=provider_cfg.model,
                        include_inactive=True,
                    )
                )
        except Exception as exc:
            logger.warning("Coordinator status preflight gagal: %s", exc)
        try:
            self._report_status_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return rows

    def _derive_audio_endpoint(self, provider: str, endpoint_url: str) -> str:
        base = str(endpoint_url or "").strip()
        if base:
            if "/audio/transcriptions" in base:
                return base
            if base.endswith("/chat/completions"):
                return base[: -len("/chat/completions")] + "/audio/transcriptions"
            if base.endswith("/completions"):
                return base[: -len("/completions")] + "/audio/transcriptions"
            if base.endswith("/v1"):
                return base + "/audio/transcriptions"
            return base.rstrip("/") + "/audio/transcriptions"
        if provider == "groq":
            return DEFAULT_GROQ_BASE_URL
        return DEFAULT_NVIDIA_BASE_URL

    def _acquire_provider_lease(self, provider_name: str, provider_cfg: ProviderConfig) -> ProviderLease:
        rows = self._refresh_coordinator_status()
        matching_ids: list[int] = []
        fallback_ids: list[int] = []
        for row in rows:
            if str(row.get("provider") or "").strip().lower() != provider_name:
                continue
            model_name = str(row.get("runtime_model_name") or row.get("default_model_name") or row.get("model_name") or "").strip()
            if row.get("leaseable") is False:
                continue
            try:
                account_id = int(row.get("provider_account_id") or 0)
            except Exception:
                continue
            fallback_ids.append(account_id)
            if model_name and model_name == provider_cfg.model:
                matching_ids.append(account_id)
        eligible = matching_ids or fallback_ids or None
        leases = coordinator_acquire_accounts(
            provider=provider_name,
            model_name=provider_cfg.model,
            count=1,
            eligible_account_ids=eligible,
            holder=self.coordinator_holder,
            host=self.coordinator_host,
            pid=self.coordinator_pid,
            task_type="asr_transcription",
            lease_ttl_seconds=self.lease_ttl_seconds,
        )
        if not leases:
            raise RuntimeError(f"Tidak ada lease coordinator untuk {provider_name}/{provider_cfg.model}")
        lease = dict(leases[0])
        api_key = str(lease.get("api_key") or "").strip()
        if not api_key:
            raise RuntimeError(
                f"Coordinator acquire bundle tidak mengandung api_key plaintext untuk account {lease.get('provider_account_id')}"
            )
        endpoint_url = str(lease.get("endpoint_url") or provider_cfg.base_url or "").strip()
        lease_obj = ProviderLease(
            provider=provider_name,
            model_name=provider_cfg.model,
            provider_account_id=int(lease.get("provider_account_id") or 0),
            account_name=str(lease.get("account_name") or ""),
            api_key=api_key,
            lease_token=str(lease.get("lease_token") or ""),
            endpoint_url=endpoint_url,
            usage_method=str(lease.get("usage_method") or ""),
            extra_headers={str(k): str(v) for k, v in dict(lease.get("extra_headers") or {}).items()},
            model_limits=dict(lease.get("model_limits") or {}),
            lease_ttl_seconds=self.lease_ttl_seconds,
            audio_endpoint_url=self._derive_audio_endpoint(provider_name, endpoint_url),
        )
        lease_obj.start_heartbeat()
        return lease_obj

    def _acquire_provider_leases(self) -> None:
        acquired: dict[str, ProviderLease] = {}
        errors: list[str] = []
        try:
            for provider_name in self.provider_order:
                if self._provider_is_disabled(provider_name):
                    logger.info("Skip acquire provider %s karena masih cooldown/disabled.", provider_name)
                    continue
                cfg = self.providers.get(provider_name)
                if not cfg:
                    continue
                try:
                    acquired[provider_name] = self._acquire_provider_lease(provider_name, cfg)
                    logger.info(
                        "Coordinator lease acquired: %s/%s account=%s id=%s",
                        provider_name,
                        cfg.model,
                        acquired[provider_name].account_name,
                        acquired[provider_name].provider_account_id,
                    )
                except Exception as exc:
                    errors.append(f"{provider_name}/{cfg.model}: {exc}")
                    logger.warning("Lease coordinator gagal untuk %s/%s: %s", provider_name, cfg.model, exc)
                    continue
        except Exception:
            for lease in acquired.values():
                try:
                    lease.stop(final_state="error", note="partial acquire cleanup")
                except Exception:
                    pass
            raise
        self.provider_leases = acquired
        if not self.provider_leases:
            raise RuntimeError("Tidak ada lease coordinator yang tersedia untuk provider ASR yang diminta: " + "; ".join(errors))

    def _provider_is_disabled(self, provider_name: str) -> bool:
        disabled_until = float(self._provider_disabled_until.get(provider_name, 0.0) or 0.0)
        return disabled_until > time.time()

    def _disable_provider(
        self,
        provider_name: str,
        *,
        lease: ProviderLease | None = None,
        reason: str = "",
        cooldown_seconds: int | None = None,
        final_state: str = "disabled",
    ) -> None:
        provider_name = str(provider_name or "").strip().lower()
        if not provider_name:
            return
        now = time.time()
        if cooldown_seconds is None:
            until = float("inf")
        else:
            until = now + max(1, int(cooldown_seconds))
        self._provider_disabled_until[provider_name] = until
        note = str(reason or "").strip()[:500]
        if lease is None:
            lease = self.provider_leases.get(provider_name)
        if lease is not None:
            try:
                lease.stop(final_state=final_state, note=note or "provider disabled for this run")
            except Exception:
                pass
        self.provider_leases.pop(provider_name, None)
        if cooldown_seconds is None:
            logger.warning("ASR provider %s dimatikan untuk sisa run: %s", provider_name, note or "tanpa alasan")
        else:
            logger.warning(
                "ASR provider %s cooldown %ss: %s",
                provider_name,
                int(cooldown_seconds),
                note or "tanpa alasan",
            )

    def _prune_disabled_provider_leases(self) -> None:
        for provider_name in list(self.provider_leases.keys()):
            if self._provider_is_disabled(provider_name):
                self.provider_leases.pop(provider_name, None)

    def _has_active_provider_capacity(self) -> bool:
        for provider_name in self.provider_order:
            if self._provider_is_disabled(provider_name):
                continue
            return True
        return False

    def _should_disable_provider_after_failure(self, provider_name: str, attempt: TranscriptionAttempt) -> tuple[bool, int | None, str]:
        provider_name = str(provider_name or "").strip().lower()
        error_text = str(attempt.error_text or "").strip()
        lower = error_text.lower()
        if provider_name == "groq":
            if (
                attempt.status_code == 429
                and (
                    "asph" in lower
                    or "seconds of audio per hour" in lower
                    or "rate limit" in lower
                    or "too many requests" in lower
                )
            ):
                return True, self.groq_asph_cooldown_seconds, "Groq ASPH/rate limit"
        if provider_name == "nvidia":
            if (
                "ssleoferror" in lower
                or "ssl" in lower
                or "certificate verify failed" in lower
                or "unexpected eof" in lower
                or "tls" in lower
            ):
                return True, None, "NVIDIA SSL/TLS transport error"
        return False, None, ""

    def _acquire_postprocess_lease(self) -> ProviderLease:
        provider_name = self.postprocess_provider
        if provider_name != "nvidia":
            raise RuntimeError(f"Provider postprocess tidak didukung: {provider_name}")
        cfg = ProviderConfig(
            name=provider_name,
            base_url=str(os.getenv("NVIDIA_CHAT_BASE_URL") or "https://integrate.api.nvidia.com/v1/chat/completions").strip(),
            model=self.postprocess_model,
        )
        return self._acquire_provider_lease(provider_name, cfg)

    def close(self) -> None:
        for lease in list(self.provider_leases.values()):
            try:
                lease.stop(final_state="idle", note="recover_asr_transcripts cleanup")
            except Exception:
                pass
        self.provider_leases.clear()
        try:
            self.http.close()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass

    def _provider_headers(self, lease: ProviderLease) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if lease.api_key:
            headers["Authorization"] = f"Bearer {lease.api_key}"
        for key, value in (lease.extra_headers or {}).items():
            if key:
                headers[str(key)] = str(value)
        return headers

    def _chat_base_url(self, lease: ProviderLease) -> str:
        base = str(lease.endpoint_url or "").strip()
        if base.endswith("/chat/completions"):
            base = base[: -len("/chat/completions")]
        return base.rstrip("/")

    def _build_timestamp_entries_from_payload(
        self,
        payload: dict,
        *,
        chunk_start_ms: int,
        chunk_end_ms: int,
        fallback_text: str,
    ) -> list[TranscriptEntry]:
        entries: list[TranscriptEntry] = []
        raw_segments = payload.get("segments")
        if isinstance(raw_segments, list):
            for segment in raw_segments:
                if not isinstance(segment, dict):
                    continue
                text = _clean_text(segment.get("text") or "")
                if not text:
                    continue
                seg_start = _coerce_float(segment.get("start"))
                seg_end = _coerce_float(segment.get("end"))
                if seg_start is None:
                    continue
                abs_start_ms = chunk_start_ms + int(round(seg_start * 1000.0))
                abs_end_ms = chunk_start_ms + int(round((seg_end if seg_end is not None else seg_start) * 1000.0))
                if abs_end_ms <= abs_start_ms:
                    abs_end_ms = max(abs_start_ms + 1, chunk_end_ms)
                entries.append(
                    TranscriptEntry(
                        start_ms=max(0, abs_start_ms),
                        end_ms=max(abs_start_ms + 1, abs_end_ms),
                        text=text,
                    )
                )

        if entries:
            return self._dedupe_transcript_entries(entries)

        text = _clean_text(payload.get("text") or payload.get("transcript") or fallback_text or "")
        if not text:
            return []
        return [
            TranscriptEntry(
                start_ms=max(0, int(chunk_start_ms)),
                end_ms=max(int(chunk_start_ms) + 1, int(chunk_end_ms)),
                text=text,
            )
        ]

    def _dedupe_transcript_entries(self, entries: list[TranscriptEntry]) -> list[TranscriptEntry]:
        if not entries:
            return []
        ordered = sorted(entries, key=lambda entry: (int(entry.start_ms), int(entry.end_ms), entry.text))
        deduped: list[TranscriptEntry] = []
        recent_texts: list[tuple[int, int, str]] = []
        overlap_window_ms = max(2_000, self.overlap_seconds * 1000 + 1_500)
        for entry in ordered:
            text = _clean_text(entry.text)
            if not text:
                continue
            norm = re.sub(r"\s+", " ", text).strip().lower()
            if not norm:
                continue
            skip = False
            for prev_start, prev_end, prev_norm in recent_texts[-6:]:
                if norm == prev_norm and abs(int(entry.start_ms) - int(prev_start)) <= overlap_window_ms:
                    skip = True
                    break
                if (norm in prev_norm or prev_norm in norm) and abs(int(entry.start_ms) - int(prev_start)) <= overlap_window_ms:
                    skip = True
                    break
            if skip:
                continue
            deduped.append(
                TranscriptEntry(
                    start_ms=int(entry.start_ms),
                    end_ms=int(entry.end_ms),
                    text=text,
                )
            )
            recent_texts.append((int(entry.start_ms), int(entry.end_ms), norm))
        return deduped

    def _render_timestamp_entries(self, entries: list[TranscriptEntry]) -> str:
        lines: list[str] = []
        for entry in entries:
            ts = _format_youtube_timestamp(int(entry.start_ms))
            text = _clean_text(entry.text)
            if not text:
                continue
            lines.append(f"{ts}  {text}")
        return "\n".join(lines).strip()

    def _build_postprocess_prompt(self, transcript_text: str, *, video_id: str, title: str, language: str) -> str:
        return (
            "Anda adalah editor transcript YouTube.\n"
            "Tugas Anda hanya merapikan transcript berikut tanpa mengubah urutan timestamp.\n\n"
            "Aturan keras:\n"
            "- Jangan merangkum.\n"
            "- Jangan menambahkan fakta baru.\n"
            "- Jangan menerjemahkan bahasa.\n"
            "- Jangan menambah, menghapus, atau mengubah timestamp.\n"
            "- Pertahankan format satu baris per timestamp seperti sumber.\n"
            "- Rapikan tanda baca, kapitalisasi, spasi, dan pemenggalan kalimat seperlunya.\n"
            "- Jika ada kata yang jelas hasil ASR salah, perbaiki secukupnya tanpa mengubah makna.\n"
            "- Jangan menulis komentar asisten, disclaimer, atau penjelasan apa pun.\n"
            "- Output hanya transcript final.\n\n"
            f"Metadata:\n- video_id: {video_id}\n- title: {title}\n- language_hint: {language}\n\n"
            "Transcript sumber:\n"
            f"{transcript_text}\n"
        )

    def _build_postprocess_client(self, lease: ProviderLease) -> OpenAI:
        if OpenAI is None:
            raise RuntimeError("Paket openai tidak tersedia di environment aktif.")
        base_url = self._chat_base_url(lease)
        if not base_url:
            raise RuntimeError("Endpoint chat completions kosong untuk lease postprocess.")
        return OpenAI(
            api_key=lease.api_key,
            base_url=base_url,
            timeout=self.postprocess_timeout,
            default_headers=self._provider_headers(lease),
        )

    def _parallel_worker_fieldnames(self) -> list[str]:
        return [
            "video_id",
            "title",
            "channel_id",
            "channel_name",
            "status",
            "provider",
            "chunk_count",
            "processed_chunks",
            "language",
            "word_count",
            "line_count",
            "raw_transcript_path",
            "transcript_path",
            "postprocess_status",
            "postprocess_provider",
            "postprocess_model",
            "postprocess_error",
            "error_text",
        ]

    def _write_report_rows(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self._parallel_worker_fieldnames())
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "video_id": row.get("video_id", ""),
                        "title": row.get("title", ""),
                        "channel_id": row.get("channel_id", ""),
                        "channel_name": row.get("channel_name", ""),
                        "status": row.get("status", ""),
                        "provider": row.get("provider", ""),
                        "chunk_count": row.get("chunk_count", 0),
                        "processed_chunks": row.get("processed_chunks", 0),
                        "language": row.get("language", ""),
                        "word_count": row.get("word_count", 0),
                        "line_count": row.get("line_count", 0),
                        "raw_transcript_path": row.get("raw_transcript_path", ""),
                        "transcript_path": row.get("transcript_path", ""),
                        "postprocess_status": row.get("postprocess_status", ""),
                        "postprocess_provider": row.get("postprocess_provider", ""),
                        "postprocess_model": row.get("postprocess_model", ""),
                        "postprocess_error": str(row.get("postprocess_error", ""))[:500],
                        "error_text": str(row.get("error_text", ""))[:500],
                    }
                )

    def _combine_worker_reports(self, report_paths: list[Path]) -> None:
        combined_rows: list[dict] = []
        for report_path in report_paths:
            if not report_path.exists():
                continue
            with report_path.open("r", encoding="utf-8", newline="") as fp:
                combined_rows.extend(list(csv.DictReader(fp)))
        self._write_report_rows(self.report_path, combined_rows)

    def _parallel_worker_command(self, *, worker_run_dir: Path, worker_csv: Path) -> list[str]:
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--run-dir",
            str(worker_run_dir),
            "--csv",
            str(worker_csv),
            "--providers",
            str(self.args.providers or "groq,nvidia"),
            "--model",
            str(self.args.model or "whisper-large-v3"),
            "--language",
            str(self.args.language or "multi"),
            "--chunk-seconds",
            str(self.chunk_seconds),
            "--overlap-seconds",
            str(self.overlap_seconds),
            "--video-workers",
            "1",
        ]
        if self.enable_postprocess:
            cmd.append("--postprocess")
        return cmd

    def _run_parallel_workers(self, rows: list[dict]) -> int:
        worker_count = min(max(1, self.video_workers), len(rows))
        if worker_count <= 1:
            return self._run_serial(rows)

        self._write_tasks_csv(rows)
        workers_root = self.run_dir / "workers"
        workers_root.mkdir(parents=True, exist_ok=True)
        shard_rows: list[list[dict]] = [[] for _ in range(worker_count)]
        for idx, row in enumerate(rows):
            shard_rows[idx % worker_count].append(row)

        processes: list[tuple[int, subprocess.Popen, Path, Path]] = []
        for worker_index, worker_rows in enumerate(shard_rows, start=1):
            if not worker_rows:
                continue
            worker_run_dir = workers_root / f"worker_{worker_index:02d}"
            worker_run_dir.mkdir(parents=True, exist_ok=True)
            worker_csv = worker_run_dir / "tasks.csv"
            self._write_tasks_csv(worker_rows, path=worker_csv)
            worker_log = worker_run_dir / "recover_asr_transcripts.log"
            worker_env = os.environ.copy()
            worker_env["ASR_LOG_FILE"] = str(worker_log)
            cmd = self._parallel_worker_command(worker_run_dir=worker_run_dir, worker_csv=worker_csv)
            logger.info(
                "ASR worker %s/%s start: tasks=%s run_dir=%s",
                worker_index,
                worker_count,
                len(worker_rows),
                worker_run_dir,
            )
            proc = subprocess.Popen(
                cmd,
                cwd=str(self.repo_root),
                env=worker_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append((worker_index, proc, worker_run_dir, worker_csv))

        if not processes:
            logger.info("ASR worker parallel mode tidak mendapat shard yang bisa diproses.")
            self._write_report_rows(self.report_path, [])
            return 0

        exit_codes: list[int] = []
        report_paths: list[Path] = []
        for worker_index, proc, worker_run_dir, _worker_csv in processes:
            exit_code = int(proc.wait())
            exit_codes.append(exit_code)
            report_path = worker_run_dir / "recover_asr_report.csv"
            report_paths.append(report_path)
            logger.info(
                "ASR worker %s/%s selesai exit=%s report=%s",
                worker_index,
                worker_count,
                exit_code,
                report_path,
            )

        self._combine_worker_reports(report_paths)
        return 0 if all(code == 0 for code in exit_codes) else 1

    def _split_timestamped_transcript(self, transcript_text: str, *, max_chars: int | None = None, max_lines: int | None = None) -> list[str]:
        max_chars = max_chars or self.postprocess_max_chars
        max_lines = max_lines or self.postprocess_max_lines
        lines = [line.rstrip() for line in str(transcript_text or "").splitlines() if line.strip()]
        if not lines:
            return []
        chunks: list[str] = []
        buffer: list[str] = []
        size = 0
        for line in lines:
            line_size = len(line) + (1 if buffer else 0)
            if buffer and (len(buffer) >= max_lines or size + line_size > max_chars):
                chunks.append("\n".join(buffer).strip())
                buffer = [line]
                size = len(line)
                continue
            buffer.append(line)
            size += line_size
        if buffer:
            chunks.append("\n".join(buffer).strip())
        return chunks

    def _postprocess_timestamped_chunk(
        self,
        *,
        client: OpenAI,
        lease_label: str,
        model_name: str,
        chunk_text: str,
        video_id: str,
        title: str,
        language: str,
        chunk_index: int,
        chunk_total: int,
    ) -> str:
        source_timestamps = _extract_timestamp_sequence(chunk_text)
        prompt = self._build_postprocess_prompt(
            chunk_text,
            video_id=video_id,
            title=title,
            language=f"{language} (chunk {chunk_index}/{chunk_total})",
        )
        kwargs: dict[str, object] = {
            "model": model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "Anda adalah editor transcript profesional. Jangan merangkum atau mengubah timestamp.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 4096,
        }
        response = client.chat.completions.create(**kwargs)
        choice = response.choices[0] if getattr(response, "choices", None) else None
        content = _strip_code_fences(_extract_message_content(getattr(choice, "message", None)))
        if not content:
            raise RuntimeError("postprocess response kosong")
        cleaned_lines = [line.rstrip() for line in content.splitlines()]
        cleaned = "\n".join(line for line in cleaned_lines if line.strip()).strip()
        if not _looks_timestamped_transcript(cleaned):
            raise RuntimeError("postprocess output tidak mempertahankan timestamp")
        if _extract_timestamp_sequence(cleaned) != source_timestamps:
            raise RuntimeError("postprocess output mengubah urutan timestamp")
        return cleaned

    def _postprocess_timestamped_transcript(self, *, transcript_text: str, video_id: str, title: str, language: str) -> tuple[str, str]:
        cleaned_source = _clean_text(transcript_text)
        if not cleaned_source:
            raise RuntimeError("postprocess input kosong")
        source_line_count = len([line for line in cleaned_source.splitlines() if line.strip()])
        source_char_count = len(cleaned_source)
        if source_line_count > self.postprocess_max_lines or source_char_count > self.postprocess_max_chars:
            raise RuntimeError(
                f"postprocess skipped for long transcript lines={source_line_count} chars={source_char_count}"
            )

        lease = self._acquire_postprocess_lease()
        lease_label = f"{lease.provider}/{lease.model_name}"
        try:
            client = self._build_postprocess_client(lease)
            chunks = self._split_timestamped_transcript(transcript_text)
            if not chunks:
                raise RuntimeError("postprocess input kosong")
            if len(chunks) > self.postprocess_max_chunks:
                raise RuntimeError(f"postprocess skipped due to chunk count {len(chunks)}")
            outputs: list[str] = []
            for idx, chunk in enumerate(chunks, start=1):
                try:
                    outputs.append(
                        self._postprocess_timestamped_chunk(
                            client=client,
                            lease_label=lease_label,
                            model_name=lease.model_name,
                            chunk_text=chunk,
                            video_id=video_id,
                            title=title,
                            language=language,
                            chunk_index=idx,
                            chunk_total=len(chunks),
                        )
                    )
                except Exception as exc:
                    logger.warning(
                        "ASR postprocess chunk fallback ke raw transcript video=%s chunk=%s/%s err=%s",
                        video_id,
                        idx,
                        len(chunks),
                        str(exc)[:300],
                    )
                    outputs.append(chunk.strip())
            cleaned = "\n".join(part.strip() for part in outputs if part.strip()).strip()
            if not cleaned:
                raise RuntimeError("postprocess output kosong")
            if _extract_timestamp_sequence(cleaned) != _extract_timestamp_sequence(transcript_text):
                raise RuntimeError("postprocess output mengubah urutan timestamp")
            return cleaned + "\n", lease_label
        finally:
            try:
                lease.stop(final_state="idle", note=f"asr postprocess {lease_label}")
            except Exception:
                pass

    def _resolve_target_rows(self) -> list[dict]:
        if self.args.csv:
            return self._load_rows_from_csv(Path(self.args.csv))

        with self.db._get_cursor() as cursor:
            if self.args.video_id:
                if self.download_only:
                    cursor.execute(
                        """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name,
                               COALESCE(a.audio_file_path, '') AS audio_file_path,
                               COALESCE(a.status, 'pending') AS audio_status
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        LEFT JOIN video_audio_assets a ON a.video_id = v.video_id
                        WHERE v.video_id = ?
                        """,
                        (self.args.video_id,),
                    )
                elif self.local_audio_only:
                    cursor.execute(
                        """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name,
                               COALESCE(a.audio_file_path, '') AS audio_file_path,
                               COALESCE(a.audio_format, '') AS audio_format,
                               COALESCE(a.status, '') AS audio_status
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        JOIN video_audio_assets a ON a.video_id = v.video_id
                        WHERE v.video_id = ?
                          AND COALESCE(a.status, '') = 'downloaded'
                        """,
                        (self.args.video_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        WHERE v.video_id = ?
                        """,
                        (self.args.video_id,),
                    )
            else:
                if self.download_only:
                    query = """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name,
                               COALESCE(a.audio_file_path, '') AS audio_file_path,
                               COALESCE(a.status, 'pending') AS audio_status
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        LEFT JOIN video_audio_assets a ON a.video_id = v.video_id
                        WHERE COALESCE(v.is_short, 0) = 0
                          AND COALESCE(v.is_member_only, 0) = 0
                          AND COALESCE(v.transcript_downloaded, 0) = 0
                          AND v.transcript_language = 'no_subtitle'
                          AND (
                              a.video_id IS NULL
                              OR COALESCE(a.status, '') IN ('pending', 'failed')
                          )
                          AND (
                              a.retry_after IS NULL
                              OR a.retry_after <= datetime('now')
                          )
                    """
                elif self.local_audio_only:
                    query = """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name,
                               COALESCE(a.audio_file_path, '') AS audio_file_path,
                               COALESCE(a.audio_format, '') AS audio_format,
                               COALESCE(a.status, '') AS audio_status
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        JOIN video_audio_assets a ON a.video_id = v.video_id
                        WHERE COALESCE(v.is_short, 0) = 0
                          AND COALESCE(v.is_member_only, 0) = 0
                          AND COALESCE(v.transcript_downloaded, 0) = 0
                          AND v.transcript_language = 'no_subtitle'
                          AND COALESCE(a.status, '') = 'downloaded'
                          AND COALESCE(a.audio_file_path, '') != ''
                    """
                else:
                    query = """
                        SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                               COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                               COALESCE(v.transcript_language, '') AS transcript_language,
                               c.channel_id, c.channel_name
                        FROM videos v
                        JOIN channels c ON c.id = v.channel_id
                        WHERE COALESCE(v.is_short, 0) = 0
                          AND COALESCE(v.is_member_only, 0) = 0
                          AND COALESCE(v.transcript_downloaded, 0) = 0
                          AND (COALESCE(v.transcript_language, '') = '' OR v.transcript_language = 'no_subtitle')
                    """
                params: list[object] = []
                if self.args.channel_id:
                    query += " AND (c.channel_id = ? OR c.channel_id = ?)"
                    params.extend([self.args.channel_id, self.args.channel_id.lstrip("@")])
                query += " ORDER BY v.created_at DESC, v.id DESC"
                if self.args.limit > 0:
                    query += " LIMIT ?"
                    params.append(self.args.limit)
                cursor.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]
        return rows

    def _load_rows_from_csv(self, csv_path: Path) -> list[dict]:
        ids: list[str] = []
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                video_id = str(row.get("video_id") or "").strip()
                if video_id:
                    ids.append(video_id)
        if self.args.limit > 0:
            ids = ids[: self.args.limit]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        with self.db._get_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT v.video_id, v.title, v.video_url, COALESCE(v.duration, 0) AS duration,
                       COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                       COALESCE(v.transcript_language, '') AS transcript_language,
                       c.channel_id, c.channel_name
                FROM videos v
                JOIN channels c ON c.id = v.channel_id
                WHERE v.video_id IN ({placeholders})
                """,
                ids,
            )
            rows = [dict(row) for row in cursor.fetchall()]
        row_map = {str(row["video_id"]): row for row in rows}
        return [row_map[video_id] for video_id in ids if video_id in row_map]

    def _write_tasks_csv(self, rows: list[dict], path: Path | None = None) -> None:
        target_path = Path(path) if path is not None else self.tasks_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=["video_id", "title", "channel_id", "channel_name"])
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "video_id": row["video_id"],
                        "title": row.get("title", ""),
                        "channel_id": row.get("channel_id", ""),
                        "channel_name": row.get("channel_name", ""),
                    }
                )

    def _download_audio(self, video_id: str, video_url: str) -> Path:
        video_dir = self.source_dir / _slug(video_id)
        video_dir.mkdir(parents=True, exist_ok=True)
        cached = self._cached_audio_path(video_id)
        if cached is not None:
            return cached

        template = str(video_dir / f"{_slug(video_id)}.%(ext)s")
        format_selector = str(
            os.getenv("ASR_AUDIO_FORMAT_SELECTOR")
            or os.getenv("YT_ASR_AUDIO_FORMAT_SELECTOR")
            or DEFAULT_ASR_AUDIO_FORMAT_SELECTOR
        ).strip()
        yt_dlp_cmd = yt_dlp_command()
        cmd = [
            *yt_dlp_cmd,
            "--no-playlist",
            "--format",
            format_selector,
            "--retries",
            "3",
            "--fragment-retries",
            "3",
            "--output",
            template,
            *yt_dlp_auth_args(rotate=True),
            video_url,
        ]
        if _env_bool("YT_ASR_RATE_LIMIT_SAFE", False):
            cmd[len(yt_dlp_cmd):len(yt_dlp_cmd)] = [
                "--sleep-interval",
                "8",
                "--max-sleep-interval",
                "15",
            ]
        logger.info("Download audio %s via yt-dlp (format=%s)", video_id, format_selector)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "yt-dlp download failed").strip()[:1200])

        candidates = sorted(
            [
                path
                for path in video_dir.glob(f"{_slug(video_id)}.*")
                if path.is_file() and not path.name.endswith(".part")
            ]
        )
        if not candidates:
            raise RuntimeError("yt-dlp selesai tetapi file audio tidak ditemukan")
        return candidates[0]

    def _cached_audio_path(self, video_id: str) -> Path | None:
        shared_video_dir = self.source_dir / _slug(video_id)
        shared_video_dir.mkdir(parents=True, exist_ok=True)
        seen: set[Path] = set()
        for video_dir in self._audio_cache_dirs(video_id):
            if video_dir in seen:
                continue
            seen.add(video_dir)
            if not video_dir.exists():
                continue
            candidates = sorted(
                [
                    path
                    for path in video_dir.glob(f"{_slug(video_id)}.*")
                    if path.is_file() and not path.name.endswith(".part") and path.stat().st_size > 0
                ]
            )
            if candidates:
                cached = candidates[0]
                if cached.parent != shared_video_dir:
                    try:
                        mirror = shared_video_dir / cached.name
                        if not mirror.exists():
                            shutil.copy2(cached, mirror)
                            cached = mirror
                    except Exception:
                        pass
                return cached
        return None

    def _resolve_local_audio_source(self, video_id: str) -> Path | None:
        asset = self.db.get_video_audio_asset(video_id)
        if asset:
            asset_path = str(asset.get("audio_file_path") or "").strip()
            if asset_path:
                candidate = Path(asset_path)
                if candidate.exists() and candidate.stat().st_size > 0:
                    return candidate
        cached = self._cached_audio_path(video_id)
        if cached is not None:
            try:
                self.db.mark_video_audio_downloaded(
                    video_id=video_id,
                    audio_file_path=str(cached),
                    audio_format=cached.suffix.lstrip("."),
                    file_size_bytes=cached.stat().st_size if cached.exists() else 0,
                )
            except Exception:
                pass
        return cached

    def _probe_access_state(self, video_id: str, video_url: str) -> tuple[bool, str, dict]:
        yt_dlp_cmd = yt_dlp_command()
        cmd = [
            *yt_dlp_cmd,
            "--no-playlist",
            "--skip-download",
            "--dump-single-json",
            "--retries",
            "1",
            "--fragment-retries",
            "1",
            *yt_dlp_auth_args(rotate=True),
            video_url,
        ]
        logger.info("Probe access %s via yt-dlp", video_id)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        payload: dict = {}
        if stdout:
            try:
                parsed = json.loads(stdout)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                pass

        availability = str(payload.get("availability") or "").strip().lower()
        is_private = bool(payload.get("is_private"))
        if proc.returncode == 0:
            if availability in {"private", "members_only", "subscriber_only", "premium_only", "needs_auth", "paid"} or is_private:
                reason = f"availability={availability or 'private'}"
                return True, reason, payload
            return False, "", payload

        combined = "\n".join(part for part in [stderr, stdout] if part).strip()
        lowered = combined.lower()
        markers = [
            "private video",
            "members-only",
            "members only",
            "available to this channel's members",
            "join this channel to get access",
            "sign in if you've been granted access",
            "this video is available to this channel's members",
        ]
        if any(marker in lowered for marker in markers):
            return True, combined[:1000] or "private/members-only", payload
        return False, combined[:1000], payload

    def _probe_duration_ms(self, source_path: Path, fallback_seconds: int = 0) -> int:
        ffprobe = shutil.which("ffprobe")
        if ffprobe:
            proc = subprocess.run(
                [
                    ffprobe,
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(source_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                try:
                    seconds = float((proc.stdout or "").strip())
                    if seconds > 0:
                        return int(seconds * 1000)
                except Exception:
                    pass
        if fallback_seconds > 0:
            return int(fallback_seconds * 1000)
        raise RuntimeError(f"Gagal membaca durasi audio: {source_path}")

    def _extract_chunk(self, source_path: Path, chunk_path: Path, start_ms: int, end_ms: int) -> Path:
        if chunk_path.exists() and chunk_path.stat().st_size > 0:
            return chunk_path
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        duration_ms = max(1, end_ms - start_ms)
        duration_seconds = duration_ms / 1000.0
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise RuntimeError("ffmpeg tidak ditemukan di PATH")
        cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{start_ms / 1000.0:.3f}",
            "-t",
            f"{duration_seconds:.3f}",
            "-i",
            str(source_path),
            "-ac",
            "1",
            "-ar",
            "16000",
            "-vn",
            "-c:a",
            "pcm_s16le",
            str(chunk_path),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0 or not chunk_path.exists() or chunk_path.stat().st_size == 0:
            detail = (proc.stderr or proc.stdout or "ffmpeg chunk extraction failed").strip()
            raise RuntimeError(detail[:1200])
        return chunk_path

    def _build_request_data(self, lease: ProviderLease) -> dict[str, str]:
        data: dict[str, str] = {}
        language = self.language.lower().strip()
        if language in {"", "auto", "default", "multi"}:
            language = ""
        if lease.model_name:
            data["model"] = lease.model_name
        if lease.provider == "groq":
            data["response_format"] = "verbose_json"
            data["timestamp_granularities[]"] = "segment"
            data["temperature"] = "0"
            if language and language not in {"auto", "default"}:
                data["language"] = language
        elif lease.provider == "nvidia":
            if language:
                data["language"] = language
        return data

    def _nvidia_riva_server(self) -> str:
        return str(os.getenv("NVIDIA_RIVA_SERVER") or DEFAULT_NVIDIA_RIVA_SERVER).strip()

    def _nvidia_riva_function_id(self) -> str:
        return str(os.getenv("NVIDIA_RIVA_FUNCTION_ID") or DEFAULT_NVIDIA_RIVA_FUNCTION_ID).strip()

    def _nvidia_riva_use_ssl(self) -> bool:
        return _env_bool("NVIDIA_RIVA_USE_SSL", True)

    def _nvidia_riva_metadata(self, lease: ProviderLease) -> list[list[str]]:
        metadata: list[list[str]] = [
            ["function-id", self._nvidia_riva_function_id()],
            ["authorization", f"Bearer {lease.api_key}"],
        ]
        for key, value in (lease.extra_headers or {}).items():
            key_str = str(key or "").strip()
            if not key_str:
                continue
            if key_str.lower() in {"authorization", "function-id"}:
                continue
            metadata.append([key_str, str(value)])
        return metadata

    def _nvidia_riva_language_code(self) -> str:
        language = str(self.language or "multi").strip().lower()
        if language in {"", "auto", "default"}:
            return "multi"
        return language

    def _transcribe_chunk_nvidia_grpc(self, lease: ProviderLease, audio_path: Path) -> TranscriptionAttempt:
        if RivaASRService is None or Auth is None or RecognitionConfig is None or AudioEncoding is None:
            raise RuntimeError(
                "nvidia-riva-client/grpcio belum terpasang. Jalur NVIDIA gRPC tidak tersedia di environment ini."
            )

        raw_audio = b""
        sample_rate = 16000
        channels = 1
        with wave.open(str(audio_path), "rb") as wf:
            sample_rate = int(wf.getframerate() or 16000)
            channels = int(wf.getnchannels() or 1)
            raw_audio = wf.readframes(wf.getnframes())

        auth = Auth(
            uri=self._nvidia_riva_server(),
            use_ssl=self._nvidia_riva_use_ssl(),
            metadata_args=self._nvidia_riva_metadata(lease),
        )
        asr_service = RivaASRService(auth)

        config = RecognitionConfig()
        config.encoding = AudioEncoding.LINEAR_PCM
        config.sample_rate_hertz = sample_rate
        config.audio_channel_count = channels
        config.max_alternatives = 1
        config.enable_automatic_punctuation = True
        config.model = lease.model_name
        config.language_code = self._nvidia_riva_language_code()
        if add_audio_file_specs_to_config is not None:
            try:
                add_audio_file_specs_to_config(config, audio_path)
            except Exception:
                pass

        try:
            future = asr_service.offline_recognize(raw_audio, config, future=True)
            response = future.result(timeout=self.request_timeout)
        except Exception as exc:
            error_text = str(exc).strip()
            return TranscriptionAttempt(
                ok=False,
                error_text=error_text[:1000],
                fatal=_is_fatal_provider_error(0, error_text),
            )

        transcript_parts: list[str] = []
        for result in getattr(response, "results", []) or []:
            alternatives = getattr(result, "alternatives", []) or []
            if not alternatives:
                continue
            text = _clean_text(getattr(alternatives[0], "transcript", "") or "")
            if text:
                transcript_parts.append(text)
        text = _clean_text(" ".join(transcript_parts))
        raw_json = ""
        if MessageToJson is not None:
            try:
                raw_json = MessageToJson(response, preserving_proto_field_name=True)
            except Exception:
                try:
                    raw_json = MessageToJson(response)
                except Exception:
                    raw_json = ""
        return TranscriptionAttempt(
            ok=True,
            text=text,
            language=self._nvidia_riva_language_code(),
            raw_response_json=raw_json,
            status_code=200,
        )

    def _transcribe_chunk(self, lease: ProviderLease, audio_path: Path) -> TranscriptionAttempt:
        if lease.provider == "nvidia" and RivaASRService is not None and Auth is not None and RecognitionConfig is not None:
            return self._transcribe_chunk_nvidia_grpc(lease, audio_path)

        data = self._build_request_data(lease)
        headers = self._provider_headers(lease)

        last_attempt_error = ""
        for attempt in range(1, self.max_retries + 1):
            with audio_path.open("rb") as fp:
                files = {"file": (audio_path.name, fp, "audio/wav")}
                try:
                    response = self.http.post(
                        lease.audio_endpoint_url or lease.endpoint_url,
                        headers=headers,
                        data=data,
                        files=files,
                        timeout=self.request_timeout,
                    )
                except requests.RequestException as exc:
                    last_attempt_error = str(exc)
                    if attempt < self.max_retries:
                        time.sleep(self.retry_delay * attempt)
                        continue
                    return TranscriptionAttempt(
                        ok=False,
                        error_text=last_attempt_error[:1000],
                        fatal=False,
                    )

            raw_text = response.text or ""
            payload = _read_json_payload(raw_text)
            if 200 <= response.status_code < 300:
                text = ""
                language = ""
                if isinstance(payload, dict):
                    text = _clean_text(payload.get("text") or payload.get("transcript") or "")
                    language = _clean_text(payload.get("language") or payload.get("lang") or "")
                    segments = payload.get("segments")
                    if not text and isinstance(segments, list):
                        texts = []
                        for seg in segments:
                            if isinstance(seg, dict):
                                seg_text = _clean_text(seg.get("text") or "")
                                if seg_text:
                                    texts.append(seg_text)
                        text = "\n".join(texts)
                else:
                    text = _clean_text(str(payload))

                if not text:
                    text = _clean_text(raw_text)
                return TranscriptionAttempt(
                    ok=True,
                    text=text,
                    language=language,
                    raw_response_json=json.dumps(payload, ensure_ascii=False),
                    status_code=response.status_code,
                )

            last_attempt_error = _parse_provider_error(response.status_code, payload, raw_text)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                time.sleep(self.retry_delay * attempt)
                continue

            fatal = _is_fatal_provider_error(response.status_code, last_attempt_error)
            try:
                coordinator_report_provider_event(
                    provider_account_id=lease.provider_account_id,
                    provider=lease.provider,
                    model_name=lease.model_name,
                    reason=last_attempt_error,
                    source="recover_asr_transcripts",
                    http_status=response.status_code,
                    payload=payload,
                )
            except Exception:
                pass
            return TranscriptionAttempt(
                ok=False,
                error_text=last_attempt_error,
                raw_response_json=json.dumps(payload, ensure_ascii=False),
                status_code=response.status_code,
                fatal=fatal,
            )

        return TranscriptionAttempt(ok=False, error_text=last_attempt_error[:1000], fatal=False)

    def _load_existing_chunk_map(self, video_id: str) -> dict[int, dict]:
        with self.db._get_cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM video_asr_chunks
                WHERE video_id = ?
                ORDER BY chunk_index ASC, updated_at ASC, id ASC
                """,
                (video_id,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
        by_index: dict[int, list[dict]] = {}
        for row in rows:
            by_index.setdefault(int(row["chunk_index"]), []).append(row)
        best: dict[int, dict] = {}
        for chunk_index, chunk_rows in by_index.items():
            success = next((row for row in chunk_rows if str(row.get("status") or "") == "done" and str(row.get("transcript_text") or "").strip()), None)
            if success:
                best[chunk_index] = success
        return best

    def _report_chunk_failure(self, lease: ProviderLease, reason: str, status_code: int = 0) -> None:
        try:
            coordinator_report_provider_event(
                provider_account_id=lease.provider_account_id,
                provider=lease.provider,
                model_name=lease.model_name,
                reason=reason,
                source="recover_asr_transcripts",
                http_status=status_code,
            )
        except Exception:
            pass

    def _merge_chunk_texts(self, chunk_texts: list[str]) -> str:
        cleaned = [_clean_text(text) for text in chunk_texts if _clean_text(text)]
        return "\n\n".join(cleaned)

    def _majority_language(self, languages: list[str], fallback: str) -> str:
        values = [str(lang or "").strip() for lang in languages if str(lang or "").strip()]
        if not values:
            return fallback
        return Counter(values).most_common(1)[0][0]

    def process_video(self, row: dict) -> dict:
        video_id = str(row.get("video_id") or "").strip()
        title = str(row.get("title") or "").strip()
        video_url = str(row.get("video_url") or "").strip() or f"https://www.youtube.com/watch?v={video_id}"
        channel_id = str(row.get("channel_id") or "").strip()
        channel_name = str(row.get("channel_name") or "").strip()
        logger.info("ASR start: %s | %s", video_id, title)

        video_work_dir = self.run_dir / "videos" / _slug(video_id)
        video_work_dir.mkdir(parents=True, exist_ok=True)
        transcript_dir = self.video_root / _slug(video_id)
        transcript_dir.mkdir(parents=True, exist_ok=True)
        final_transcript_path = transcript_dir / "transcript.txt"
        existing_done = self._load_existing_chunk_map(video_id)
        cached_audio = self._resolve_local_audio_source(video_id)
        if self.download_only:
            try:
                source_path = cached_audio or self._download_audio(video_id, video_url)
                file_size_bytes = source_path.stat().st_size if source_path.exists() else 0
                self.db.mark_video_audio_downloaded(
                    video_id=video_id,
                    audio_file_path=str(source_path),
                    audio_format=source_path.suffix.lstrip("."),
                    duration=int(row.get("duration") or 0),
                    file_size_bytes=file_size_bytes,
                )
            except Exception as exc:
                reason = str(exc) or "audio_download_failed"
                logger.warning("Audio download failed for video=%s: %s", video_id, reason)
                self.db.mark_video_audio_download_retry_later(video_id, reason=reason, retry_after_hours=24)
                return {
                    "video_id": video_id,
                    "status": "retry_later",
                    "provider": "yt-dlp",
                    "chunk_count": 0,
                    "processed_chunks": 0,
                    "error_text": reason,
                    "transcript_path": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                    "source_path": "",
                }
            return {
                "video_id": video_id,
                "status": "audio_cached" if source_path.exists() else "audio_downloaded",
                "provider": "yt-dlp",
                "chunk_count": 0,
                "processed_chunks": 0,
                "error_text": "",
                "transcript_path": "",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "title": title,
                "source_path": str(source_path),
            }

        self._prune_disabled_provider_leases()
        if not self._has_active_provider_capacity():
            reason = "No active ASR provider capacity available (cooldown/disabled)"
            self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=24)
            return {
                "video_id": video_id,
                "status": "retry_later",
                "provider": ",".join(self.provider_order),
                "chunk_count": 0,
                "processed_chunks": 0,
                "error_text": reason,
                "transcript_path": "",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "title": title,
            }

        cached_audio = self._resolve_local_audio_source(video_id)
        if self.local_audio_only or self.require_cached_audio:
            if cached_audio is None:
                reason = "audio_file_missing"
                self.db.mark_video_audio_download_retry_later(video_id, reason=reason, retry_after_hours=24)
                return {
                    "video_id": video_id,
                    "status": "retry_later",
                    "provider": "local-audio",
                    "chunk_count": 0,
                    "processed_chunks": 0,
                    "error_text": reason,
                    "transcript_path": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                }
            source_path = cached_audio
        else:
            if cached_audio is None:
                access_blocked, access_reason, access_payload = self._probe_access_state(video_id, video_url)
                if access_blocked:
                    reason = f"skip_access_blocked: {access_reason or 'private_or_members_only'}"
                    self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=8760)
                    try:
                        self.db.upsert_video_asr_chunk(
                            video_id=video_id,
                            provider="yt-dlp",
                            model_name="access-probe",
                            chunk_index=0,
                            chunk_start_ms=0,
                            chunk_end_ms=0,
                            audio_path="",
                            status="skipped",
                            transcript_text="",
                            language="skip_access_blocked",
                            raw_response_json=json.dumps(access_payload, ensure_ascii=False),
                            error_text=reason,
                        )
                    except Exception:
                        pass
                    return {
                        "video_id": video_id,
                        "status": "skip_access_blocked",
                        "provider": "yt-dlp",
                        "chunk_count": 0,
                        "processed_chunks": 0,
                        "error_text": reason,
                        "transcript_path": "",
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "title": title,
                    }
            else:
                logger.info("ASR reuse cached audio %s", cached_audio.name)

            source_path = cached_audio or self._download_audio(video_id, video_url)

        if not existing_done and not self.provider_leases:
            try:
                self._acquire_provider_leases()
            except Exception as exc:
                reason = str(exc) or "No ASR lease available"
                logger.warning("ASR lease unavailable for video=%s: %s", video_id, reason)
                self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=24)
                return {
                    "video_id": video_id,
                    "status": "retry_later",
                    "provider": ",".join(self.provider_order),
                    "chunk_count": 0,
                    "processed_chunks": 0,
                    "error_text": reason,
                    "transcript_path": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                }

        duration_ms = self._probe_duration_ms(source_path, int(row.get("duration") or 0))
        chunk_ms = self.chunk_seconds * 1000
        overlap_ms = self.overlap_seconds * 1000
        step_ms = max(1, chunk_ms - overlap_ms)
        chunk_count = max(1, int(math.ceil(max(1, duration_ms) / step_ms)))

        detected_languages: list[str] = []
        chunk_records: dict[int, dict] = {}
        missing_chunk_count = sum(1 for chunk_index in range(chunk_count) if chunk_index not in existing_done)
        if missing_chunk_count > 0 and not self.provider_leases:
            try:
                self._acquire_provider_leases()
            except Exception as exc:
                reason = str(exc) or "No ASR lease available"
                logger.warning("ASR lease unavailable for video=%s: %s", video_id, reason)
                self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=24)
                return {
                    "video_id": video_id,
                    "status": "retry_later",
                    "provider": ",".join(self.provider_order),
                    "chunk_count": chunk_count,
                    "processed_chunks": 0,
                    "error_text": reason,
                    "transcript_path": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                }

        for chunk_index in range(chunk_count):
            start_ms = chunk_index * step_ms
            end_ms = min(duration_ms, start_ms + chunk_ms)
            if chunk_index in existing_done:
                chunk_row = existing_done[chunk_index]
                chunk_records[chunk_index] = dict(chunk_row)
                chunk_text = str(chunk_row.get("transcript_text") or "").strip()
                if chunk_text:
                    detected_languages.append(str(chunk_row.get("language") or "").strip())
                continue

            chunk_path = self.chunk_dir / _slug(video_id) / f"chunk_{chunk_index:05d}.wav"
            self._extract_chunk(source_path, chunk_path, start_ms, end_ms)

            chunk_failed = True
            last_error = ""
            last_lease: ProviderLease | None = None
            for provider_name in self.provider_order:
                if self._provider_is_disabled(provider_name):
                    logger.info("ASR skip provider=%s karena cooldown/disabled untuk video=%s chunk=%s", provider_name, video_id, chunk_index)
                    continue
                lease = self.provider_leases.get(provider_name)
                if not lease:
                    continue
                attempt = self._transcribe_chunk(lease, chunk_path)
                self.db.upsert_video_asr_chunk(
                    video_id=video_id,
                    provider=lease.provider,
                    model_name=lease.model_name or "whisper-large-v3",
                    chunk_index=chunk_index,
                    chunk_start_ms=start_ms,
                    chunk_end_ms=end_ms,
                    audio_path=str(chunk_path),
                    status="done" if attempt.ok else "failed",
                    transcript_text=attempt.text if attempt.ok else "",
                    language=attempt.language if attempt.ok else "",
                    raw_response_json=attempt.raw_response_json,
                    error_text=attempt.error_text,
                )
                chunk_records[chunk_index] = {
                    "video_id": video_id,
                    "provider": lease.provider,
                    "model_name": lease.model_name or "whisper-large-v3",
                    "chunk_index": chunk_index,
                    "chunk_start_ms": start_ms,
                    "chunk_end_ms": end_ms,
                    "audio_path": str(chunk_path),
                    "status": "done" if attempt.ok else "failed",
                    "transcript_text": attempt.text if attempt.ok else "",
                    "language": attempt.language if attempt.ok else "",
                    "raw_response_json": attempt.raw_response_json,
                    "error_text": attempt.error_text,
                }
                if attempt.ok:
                    detected_languages.append(attempt.language)
                    chunk_failed = False
                    break
                last_lease = lease
                last_error = attempt.error_text
                logger.warning(
                    "ASR chunk failed video=%s chunk=%s provider=%s status=%s fatal=%s err=%s",
                    video_id,
                    chunk_index,
                    lease.provider,
                    attempt.status_code,
                    attempt.fatal,
                    attempt.error_text[:300],
                )
                disable_provider, cooldown_seconds, disable_reason = self._should_disable_provider_after_failure(provider_name, attempt)
                if disable_provider:
                    self._disable_provider(
                        provider_name,
                        lease=lease,
                        reason=f"{disable_reason}: {attempt.error_text}",
                        cooldown_seconds=cooldown_seconds,
                        final_state="retry_later" if cooldown_seconds is not None else "error",
                    )
                if attempt.fatal:
                    # Fatal error on one provider does not automatically stop the batch;
                    # the next provider may still work. The row is already persisted.
                    continue

            if chunk_failed:
                reason = last_error or "asr_chunk_failed"
                self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=24)
                if last_lease:
                    self._report_chunk_failure(last_lease, reason, status_code=0)
                return {
                    "video_id": video_id,
                    "status": "chunk_failed",
                    "provider": ",".join(self.provider_order),
                    "chunk_count": chunk_count,
                    "processed_chunks": sum(1 for item in chunk_records.values() if str(item.get("status") or "") == "done"),
                    "error_text": reason,
                    "transcript_path": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                }

        timestamp_entries: list[TranscriptEntry] = []
        for chunk_index in range(chunk_count):
            chunk_row = chunk_records.get(chunk_index)
            if not chunk_row:
                continue
            payload = _read_json_payload(str(chunk_row.get("raw_response_json") or ""))
            timestamp_entries.extend(
                self._build_timestamp_entries_from_payload(
                    payload,
                    chunk_start_ms=int(chunk_row.get("chunk_start_ms") or (chunk_index * step_ms)),
                    chunk_end_ms=int(chunk_row.get("chunk_end_ms") or min(duration_ms, (chunk_index * step_ms) + chunk_ms)),
                    fallback_text=str(chunk_row.get("transcript_text") or ""),
                )
            )

        raw_timestamped_text = self._render_timestamp_entries(timestamp_entries)
        if not raw_timestamped_text:
            reason = "merged transcript empty"
            self.db.mark_video_transcript_retry_later(video_id, reason=reason, retry_after_hours=24)
            return {
                "video_id": video_id,
                "status": "empty",
                "provider": ",".join(self.provider_order),
                "chunk_count": chunk_count,
                "processed_chunks": sum(1 for item in chunk_records.values() if str(item.get("status") or "") == "done"),
                "error_text": reason,
                "transcript_path": "",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "title": title,
            }

        raw_transcript_path = transcript_dir / "transcript_raw.txt"
        raw_transcript_path.write_text(raw_timestamped_text + "\n", encoding="utf-8")

        merged_language = self._majority_language(detected_languages, self.language if self.language else "multi")
        postprocess_status = "disabled"
        postprocess_error = ""
        final_transcript_text = raw_timestamped_text
        final_transcript_path.parent.mkdir(parents=True, exist_ok=True)
        if self.enable_postprocess:
            try:
                final_transcript_text, postprocess_lease_label = self._postprocess_timestamped_transcript(
                    transcript_text=raw_timestamped_text,
                    video_id=video_id,
                    title=title,
                    language=merged_language,
                )
                postprocess_status = f"done:{postprocess_lease_label}"
            except Exception as exc:
                postprocess_error = str(exc)
                if postprocess_error.startswith("postprocess skipped"):
                    postprocess_status = "skipped_long"
                    logger.info(
                        "ASR postprocess dilewati untuk video=%s karena transcript terlalu panjang: %s",
                        video_id,
                        postprocess_error[:300],
                    )
                else:
                    logger.warning(
                        "ASR postprocess fallback ke raw timestamped transcript video=%s err=%s",
                        video_id,
                        postprocess_error[:300],
                    )

        final_transcript_path.parent.mkdir(parents=True, exist_ok=True)
        final_transcript_path.write_text(final_transcript_text.rstrip() + "\n", encoding="utf-8")
        word_count = _count_transcript_words(final_transcript_text)
        line_count = len([line for line in final_transcript_text.splitlines() if line.strip()])
        self.db.update_video_with_transcript(
            video_id=video_id,
            transcript_file_path=str(final_transcript_path),
            summary_file_path="",
            transcript_language=merged_language,
            word_count=word_count,
            line_count=line_count,
            transcript_text=final_transcript_text,
        )

        if self.delete_audio_after_success and source_path.exists():
            try:
                source_path.unlink()
            except Exception as exc:
                logger.warning("Gagal menghapus audio lokal %s: %s", source_path, str(exc)[:300])
        try:
            self.db.mark_video_audio_consumed(
                video_id=video_id,
                audio_file_path=str(source_path),
                audio_format=source_path.suffix.lstrip("."),
                duration=duration_ms // 1000,
                file_size_bytes=source_path.stat().st_size if source_path.exists() else 0,
            )
        except Exception:
            pass

        return {
            "video_id": video_id,
            "status": "done",
            "provider": ",".join(self.provider_order),
            "chunk_count": chunk_count,
            "processed_chunks": sum(1 for item in chunk_records.values() if str(item.get("status") or "") == "done"),
            "error_text": "",
            "transcript_path": str(final_transcript_path),
            "raw_transcript_path": str(raw_transcript_path),
            "channel_id": channel_id,
            "channel_name": channel_name,
            "title": title,
            "word_count": word_count,
            "line_count": line_count,
            "language": merged_language,
            "postprocess_status": postprocess_status,
            "postprocess_error": postprocess_error,
            "postprocess_provider": self.postprocess_provider,
            "postprocess_model": self.postprocess_model,
        }

    def run(self) -> int:
        rows = self._resolve_target_rows()
        if not rows:
            logger.info("Tidak ada target ASR yang diproses.")
            self._write_tasks_csv([])
            return 0

        if self.video_workers > 1 and len(rows) > 1:
            return self._run_parallel_workers(rows)
        return self._run_serial(rows)

    def _run_serial(self, rows: list[dict]) -> int:
        self._write_tasks_csv(rows)
        fieldnames = self._parallel_worker_fieldnames()
        failures = 0
        with self.report_path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            for idx, row in enumerate(rows, start=1):
                try:
                    result = self.process_video(row)
                    if result.get("status") != "done":
                        failures += 1
                    writer.writerow(
                        {
                            "video_id": result.get("video_id", ""),
                            "title": result.get("title", ""),
                            "channel_id": result.get("channel_id", ""),
                            "channel_name": result.get("channel_name", ""),
                            "status": result.get("status", ""),
                            "provider": result.get("provider", ""),
                            "chunk_count": result.get("chunk_count", 0),
                            "processed_chunks": result.get("processed_chunks", 0),
                            "language": result.get("language", ""),
                            "word_count": result.get("word_count", 0),
                            "line_count": result.get("line_count", 0),
                            "raw_transcript_path": result.get("raw_transcript_path", ""),
                            "transcript_path": result.get("transcript_path", ""),
                            "postprocess_status": result.get("postprocess_status", ""),
                            "postprocess_provider": result.get("postprocess_provider", ""),
                            "postprocess_model": result.get("postprocess_model", ""),
                            "postprocess_error": result.get("postprocess_error", "")[:500],
                            "error_text": result.get("error_text", "")[:500],
                        }
                    )
                    fp.flush()
                    logger.info("ASR progress %s/%s video=%s status=%s", idx, len(rows), row["video_id"], result.get("status"))
                except Exception as exc:
                    failures += 1
                    error_text = str(exc)
                    logger.exception("ASR gagal untuk video=%s", row.get("video_id"))
                    writer.writerow(
                        {
                            "video_id": row.get("video_id", ""),
                            "title": row.get("title", ""),
                            "channel_id": row.get("channel_id", ""),
                            "channel_name": row.get("channel_name", ""),
                            "status": "exception",
                            "provider": ",".join(self.provider_order),
                            "chunk_count": 0,
                            "processed_chunks": 0,
                            "language": "",
                            "word_count": 0,
                            "line_count": 0,
                            "raw_transcript_path": "",
                            "transcript_path": "",
                            "postprocess_status": "",
                            "postprocess_provider": "",
                            "postprocess_model": "",
                            "postprocess_error": "",
                            "error_text": error_text[:500],
                        }
                    )
                    fp.flush()

        logger.info("ASR selesai: targets=%s failures=%s report=%s", len(rows), failures, self.report_path)
        return 0 if failures == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recover YouTube transcripts via ASR (Groq / NVIDIA Whisper)")
    parser.add_argument("--csv", default="", help="CSV target yang berisi kolom video_id")
    parser.add_argument("--video-id", default="", help="Proses satu video_id tertentu")
    parser.add_argument("--channel-id", default="", help="Proses video pending dari satu channel")
    parser.add_argument("--limit", type=int, default=0, help="Batasi jumlah target")
    parser.add_argument("--run-dir", required=True, help="Directory run untuk report dan chunk cache")
    parser.add_argument("--audio-dir", default="", help="Directory local untuk audio cache")
    parser.add_argument("--providers", default="groq,nvidia", help="Urutan provider fallback, contoh: groq,nvidia")
    parser.add_argument("--model", default="whisper-large-v3", help="Model default untuk Groq")
    parser.add_argument("--language", default="multi", help="Bahasa target: multi, auto, id, en, ar, ...")
    parser.add_argument("--chunk-seconds", type=int, default=45, help="Durasi chunk audio dalam detik")
    parser.add_argument("--overlap-seconds", type=int, default=2, help="Overlap antar chunk dalam detik")
    parser.add_argument("--video-workers", type=int, default=1, help="Jumlah worker video paralel")
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Hanya download audio ke cache shared tanpa menjalankan ASR",
    )
    parser.add_argument(
        "--local-audio-only",
        action="store_true",
        help="Jalankan ASR hanya dari file audio lokal yang sudah ada",
    )
    parser.add_argument(
        "--require-cached-audio",
        action="store_true",
        help="Jangan download audio saat ASR; hanya pakai cache audio yang sudah ada",
    )
    parser.add_argument(
        "--delete-audio-after-success",
        action="store_true",
        help="Hapus audio lokal setelah ASR sukses",
    )
    parser.add_argument(
        "--postprocess",
        action="store_true",
        help="Aktifkan polishing transcript timestamped via GPT OSS post-process",
    )
    parser.add_argument("--coordinator-holder", default="", help="Label holder lease coordinator")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    if args.csv and (args.video_id or args.channel_id):
        parser.error("--csv tidak bisa digabung dengan --video-id atau --channel-id")

    pipeline = ASRPipeline(args)
    try:
        return pipeline.run()
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())
