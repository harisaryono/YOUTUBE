#!/usr/bin/env python3
"""
Format Transcripts - Multi Worker Pool (Updated)

Memformat transkrip YouTube menggunakan multiple worker paralel dengan coordinator lease system.
Pattern mengikuti fill_missing_resumes_youtube_db.py dan smoke_test_format_models.py

Setiap worker = 1 akun API key. Worker acquire lease, process, heartbeat, release.
"""

import argparse
import csv
import json
import os
import re
import socket
import sqlite3
import sys
import threading
import time
import traceback
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from queue import Queue, Empty
from urllib import request as urllib_request
from urllib import error as urllib_error
from zoneinfo import ZoneInfo

from database_optimized import OptimizedDatabase
from local_services import (
    coordinator_base_url,
    coordinator_acquire_accounts,
    coordinator_heartbeat_lease,
    coordinator_release_lease,
    coordinator_report_provider_event,
    coordinator_status_accounts,
    is_provider_blocking_enabled,
)

LOCAL_TZ = ZoneInfo("Asia/Jakarta")
DEFAULT_DB = "youtube_transcripts.db"
DEFAULT_UPLOADS = "uploads"
LEASE_TTL_SECONDS = 300
DEFAULT_ORDER = "asc"  # oldest first

DEFAULT_PROVIDER_PLAN = "nvidia_only"
NVIDIA_BASELINE_PROVIDER = "nvidia"
NVIDIA_BASELINE_MODEL = "openai/gpt-oss-120b"


def provider_plan_order(plan: str) -> list[dict[str, object]]:
    """Return provider order for a given plan.

    Notes:
    - Groq `llama-3.3-70b-versatile` is a proven formatting model in the older yt_channel scripts.
    - Groq `openai/gpt-oss-120b` commonly becomes unavailable when the daily token quota (TPD) is exhausted
      (often surfaced as HTTP 429 / "TPD exceeded"). For sustained throughput, prefer llama/qwen and re-enable
      `gpt-oss-120b` after the quota window resets.
    """
    key = str(plan or DEFAULT_PROVIDER_PLAN).strip().lower()
    nvidia = [{"provider": NVIDIA_BASELINE_PROVIDER, "model": NVIDIA_BASELINE_MODEL, "priority": 10}]
    groq = [
        {"provider": "groq", "model": "llama-3.3-70b-versatile", "priority": 15},
        {"provider": "groq", "model": "qwen/qwen3-32b", "priority": 20},
        {"provider": "groq", "model": "openai/gpt-oss-20b", "priority": 30},
        {"provider": "groq", "model": "openai/gpt-oss-120b", "priority": 40},
    ]
    kimi = [
        {"provider": "groq", "model": "moonshotai/kimi-k2-instruct", "priority": 10},
    ]
    # Formatting "equal pool" (models treated as peers; failures are handed off to NVIDIA baseline).
    format_equal = [
        {"provider": "groq", "model": "moonshotai/kimi-k2-instruct", "priority": 10},
        {"provider": "cerebras", "model": "qwen-3-235b-a22b-instruct-2507", "priority": 10},
        {"provider": "nvidia", "model": "mistralai/mistral-small-24b-instruct", "priority": 10},
        {"provider": "groq", "model": "openai/gpt-oss-20b", "priority": 10},
        {"provider": "cerebras", "model": "llama3.1-8b", "priority": 10},
        {"provider": NVIDIA_BASELINE_PROVIDER, "model": NVIDIA_BASELINE_MODEL, "priority": 10},
    ]
    cerebras: list[dict[str, object]] = [
        {"provider": "cerebras", "model": "qwen-3-235b-a22b-instruct-2507", "priority": 50},
        {"provider": "cerebras", "model": "llama3.1-8b", "priority": 60},
    ]

    if key in ("nvidia_only", "nvidia"):
        return nvidia
    if key in ("kimi_then_nvidia", "kimi_first", "groq_kimi_then_nvidia"):
        return kimi + nvidia
    if key in ("groq_only", "groq"):
        return groq
    if key in ("format_equal", "equal", "peer", "peers", "balanced_equal"):
        return format_equal
    if key in ("groq_then_nvidia", "groq_first"):
        return groq + nvidia
    if key in ("nvidia_then_groq", "nvidia_first"):
        return nvidia + groq
    if key in ("all", "mixed_all", "full", "everything"):
        return nvidia + groq + cerebras
    # Fallback: keep previous default behavior (NVIDIA only).
    return nvidia

PROMPT_FORMAT = """Tugas: Format transkrip berikut agar lebih mudah dibaca.

Instruksi:
1. Hapus semua timestamp (format [00:00:00.000] atau sejenisnya)
2. Pertahankan SEMUA isi konten asli - jangan ringkas, jangan terjemahkan
3. Perbaiki pemenggalan kata yang terpotong di akhir baris
4. Tambahkan paragraf yang logis berdasarkan alur bicara
5. Pertahankan nama orang, istilah teknis, angka, dan fakta persis seperti aslinya
6. Gunakan bahasa yang sama dengan transkrip asli (jangan ubah bahasa)
7. Jangan tambahkan komentar, interpretasi, atau kesimpulan yang tidak ada di transkrip

Output:
- Hanya kembalikan transkrip yang sudah diformat
- Jangan tambahkan penjelasan tentang apa yang Anda lakukan
- Jangan menyebut diri Anda sebagai AI

Transkrip asli:
{transcript}

Transkrip terformat:"""

STRICT_RETRY_PROMPT_FORMAT = """Tugas Anda hanya satu: keluarkan transkrip yang sudah diformat ulang agar lebih mudah dibaca.

Aturan keras:
- JANGAN membalas sebagai asisten
- JANGAN meminta transkrip lengkap, file lain, URL, atau konteks tambahan
- JANGAN menulis komentar seperti "silakan kirim", "please provide", "I notice", "maaf", atau penjelasan apa pun
- JANGAN merangkum
- JANGAN menambah fakta baru
- JANGAN mengubah makna pembicara
- Pertahankan isi sedekat mungkin dengan sumber
- Rapikan hanya tanda baca, kapitalisasi, paragraf, dan heading
- Output HARUS langsung berupa hasil akhir saja

Transkrip asli:
{transcript}

Transkrip terformat:"""


@dataclass
class VideoTask:
    id: int
    channel_id: int
    video_id: str
    title: str
    transcript_path: str
    channel_slug: str
    transcript_text: str = ""
    transcript_chars: int = 0
    # When all keys are busy/blocked, we should requeue instead of marking the task failed.
    defer_count: int = 0
    # Do not retry this task before this monotonic timestamp (seconds).
    not_before: float = 0.0
    # When true, retry this task only on the NVIDIA baseline model.
    force_nvidia: bool = False


@dataclass
class WorkerLease:
    provider: str
    model: str
    account_id: int
    account_name: str
    api_key: str
    lease_token: str
    lease_expires_at: str
    endpoint_url: str
    usage_method: str
    extra_headers: Dict[str, str]
    model_limits: Dict[str, Any] = field(default_factory=dict)


class LeaseHeartbeat:
    """Background heartbeat untuk lease"""
    
    def __init__(self, lease_token: str, lease_ttl_seconds: int = LEASE_TTL_SECONDS):
        self.lease_token = str(lease_token or "").strip()
        self.lease_ttl_seconds = max(60, int(lease_ttl_seconds or LEASE_TTL_SECONDS))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        
    def start(self) -> None:
        if not self.lease_token:
            return
        self._thread = threading.Thread(target=self._run, name="lease-heartbeat", daemon=True)
        self._thread.start()
        
    def _run(self) -> None:
        # Heartbeat setiap TTL/3 (sekitar 100 detik untuk TTL 300)
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try:
                coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
            except Exception:
                pass
                
    def stop(self, final_state: str = "idle", note: str = "") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if not self.lease_token:
            return
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception:
            pass


class TaskQueue:
    """Thread-safe task queue"""
    
    def __init__(self):
        self.queue = Queue()
        self.lock = threading.Lock()
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.deferred = 0
        
    def put(self, task: VideoTask):
        self.queue.put(task)
        
    def get(self, timeout: float = 1.0) -> Optional[VideoTask]:
        # Avoid hot-looping on tasks that are deferred until a future time (e.g. TPD reset).
        deadline = time.monotonic() + max(0.0, float(timeout or 0.0))
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                return None
            try:
                task = self.queue.get(timeout=min(0.25, remaining))
            except Empty:
                return None
            try:
                not_before = float(getattr(task, "not_before", 0.0) or 0.0)
            except Exception:
                not_before = 0.0
            if not_before and not_before > time.monotonic():
                # Put it back at the end; other tasks can proceed.
                self.queue.put(task)
                time.sleep(0.05)
                continue
            return task
            
    def mark_completed(self):
        with self.lock:
            self.completed += 1
            
    def mark_failed(self):
        with self.lock:
            self.failed += 1
            
    def mark_skipped(self):
        with self.lock:
            self.skipped += 1

    def mark_deferred(self):
        with self.lock:
            self.deferred += 1
            
    def stats(self) -> Dict[str, int]:
        with self.lock:
            return {
                'completed': self.completed,
                'failed': self.failed,
                'skipped': self.skipped,
                'deferred': self.deferred,
                'pending': self.queue.qsize()
            }


class CsvResultsSink:
    """Thread-safe CSV writer for per-task results (so we can sync back without shipping the full DB)."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.fp = open(self.path, "a", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.fp,
            fieldnames=[
                "ts",
                "id",
                "video_id",
                "channel_slug",
                "transcript_path",
                "ok",
                "formatted_rel_path",
                "provider",
                "model",
                "account_name",
                "error",
            ],
        )
        if self.path.stat().st_size == 0:
            self.writer.writeheader()
            self.fp.flush()

    def close(self) -> None:
        try:
            self.fp.close()
        except Exception:
            pass

    def write_task_result(self, task: "VideoTask", result: Dict[str, Any]) -> None:
        row = {
            "ts": datetime.now(tz=LOCAL_TZ).isoformat(timespec="seconds"),
            "id": getattr(task, "id", ""),
            "video_id": getattr(task, "video_id", ""),
            "channel_slug": getattr(task, "channel_slug", ""),
            "transcript_path": getattr(task, "transcript_path", ""),
            "ok": "1" if bool(result.get("ok")) else "0",
            "formatted_rel_path": str(result.get("rel_path") or ""),
            "provider": str(result.get("provider") or ""),
            "model": str(result.get("model") or ""),
            "account_name": str(result.get("account_name") or ""),
            "error": str(result.get("error") or ""),
        }
        with self.lock:
            self.writer.writerow(row)
            self.fp.flush()


class TranscriptFormatterWorker:
    """Single worker untuk format transcript dengan lease coordinator"""
    
    def __init__(
        self,
        worker_id: int,
        uploads_dir: Path,
        db_path: Optional[str],
        provider_order: list[dict[str, object]],
        provider_plan: str = "",
        results_sink: Optional[CsvResultsSink] = None,
    ):
        self.worker_id = worker_id
        self.uploads_dir = uploads_dir
        self.db_path = db_path
        self.provider_order = provider_order
        self.provider_plan = str(provider_plan or "").strip().lower()
        # In this mode, models are treated as peers; if a non-baseline model fails, requeue the task
        # and let NVIDIA baseline pick it up later.
        self.handoff_to_nvidia_on_fail = self.provider_plan in {
            "format_equal",
            "equal",
            "peer",
            "peers",
            "balanced_equal",
        }
        self.results_sink = results_sink
        self.db_obj: Optional[OptimizedDatabase] = OptimizedDatabase(db_path, str(uploads_dir)) if db_path else None
        self.lease: Optional[WorkerLease] = None
        self.heartbeat: Optional[LeaseHeartbeat] = None
        self.running = True
        self.current_task: Optional[VideoTask] = None
        self._last_result: Dict[str, Any] = {}
        # Stores the latest coordinator decision from /v1/provider-events/report so we can
        # release a lease with the correct final_state (blocked/disabled) instead of
        # always resetting to idle.
        self._last_event_decision: Dict[str, Any] = {}
        self._connect_db()

    def _provider_order_for_task(self, task: VideoTask) -> list[dict[str, object]]:
        order = list(self.provider_order or [])
        if not order:
            return []
        if bool(getattr(task, "force_nvidia", False) or False):
            baseline = [
                x
                for x in order
                if str(x.get("provider") or "").strip().lower() == NVIDIA_BASELINE_PROVIDER
                and str(x.get("model") or "").strip() == NVIDIA_BASELINE_MODEL
            ]
            return baseline or [
                {"provider": NVIDIA_BASELINE_PROVIDER, "model": NVIDIA_BASELINE_MODEL, "priority": 0}
            ]
        if not self.handoff_to_nvidia_on_fail:
            return order
        # Rotate the starting point per task so "peer" models are actually used, not always treated as fallback.
        try:
            seed = zlib.crc32(str(getattr(task, "video_id", "") or "").encode("utf-8")) & 0xFFFFFFFF
        except Exception:
            seed = int(getattr(task, "id", 0) or 0) & 0xFFFFFFFF
        start = int((seed + int(self.worker_id or 0)) % len(order))
        return order[start:] + order[:start]
        
    def _connect_db(self):
        """Koneksi ke database"""
        if not self.db_path:
            return
            
        # Initialize OptimizedDatabase for unified access (blobs/files)
        self.db = OptimizedDatabase(self.db_path, self.uploads_dir)
        
        # Keep internal connection for direct updates if needed
        self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db_conn.row_factory = sqlite3.Row
        try:
            self.db_conn.execute("PRAGMA busy_timeout=8000")
            # WAL improves concurrency when multiple processes write small updates.
            self.db_conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        
    def close(self):
        """Cleanup"""
        if self.db_conn:
            self.db_conn.close()
        self._release_lease_if_needed()
        
    def _acquire_lease(self, provider: str, model: str, holder: str) -> Optional[WorkerLease]:
        """Acquire lease dari coordinator"""
        try:
            leases = coordinator_acquire_accounts(
                provider=provider,
                model_name=model,
                count=1,
                holder=holder,
                host=os.uname().nodename,
                pid=os.getpid(),
                task_type="transcript_formatting",
                lease_ttl_seconds=LEASE_TTL_SECONDS
            )
            
            if not leases:
                return None
                
            lease_data = leases[0]
            api_key = str(lease_data.get("api_key") or "").strip()
            if not api_key:
                raise RuntimeError(f"Coordinator acquire bundle tidak mengandung api_key untuk account {lease_data['provider_account_id']}")
                
            return WorkerLease(
                provider=provider,
                model=model,
                account_id=lease_data['provider_account_id'],
                account_name=lease_data['account_name'],
                api_key=api_key,
                lease_token=lease_data['lease_token'],
                lease_expires_at=lease_data['lease_expires_at'],
                endpoint_url=lease_data.get('endpoint_url', ''),
                usage_method=lease_data.get('usage_method', ''),
                extra_headers=lease_data.get('extra_headers', {}),
                model_limits=lease_data.get('model_limits', {})
            )
        except Exception as e:
            print(f"   [Worker-{self.worker_id}] ❌ Acquire lease failed: {e}")
            return None
            
    def _release_lease_if_needed(self, final_state: str = "idle", note: str = ""):
        """Release lease jika ada"""
        if self.heartbeat:
            self.heartbeat.stop(final_state=final_state, note=note)
            self.heartbeat = None
            self.lease = None
        # Avoid carrying a previous decision into the next provider attempt.
        self._last_event_decision = {}

    def _is_retryable_provider_error(self, error: str) -> bool:
        low = str(error or "").lower()
        if not low:
            return False
        # Explicit quota / rate limit signals.
        if "error code: 429" in low or "http 429" in low:
            return True
        if "rate limit" in low or "rate_limit_exceeded" in low or "too many requests" in low:
            return True
        if "tokens per day" in low or " tpd" in low or "(tpd)" in low:
            return True
        # Network / transient failures.
        if "network/timeout error" in low or "timed out" in low or "timeout" in low:
            return True
        if "temporarily unavailable" in low or "service unavailable" in low or "bad gateway" in low:
            return True
        if "connection reset" in low or "connection aborted" in low:
            return True
        return False

    def _retry_after_seconds(self, error: str, decision: Dict[str, Any]) -> float:
        # 1) Coordinator may return a concrete retry hint.
        try:
            raw = (decision or {}).get("retry_after_seconds")
            if raw is not None:
                v = float(raw)
                if v > 0:
                    return min(60.0 * 30, v)  # cap 30 minutes
        except Exception:
            pass

        # 2) blocked_until -> delay
        try:
            blocked_until = str((decision or {}).get("blocked_until") or "").strip()
            if blocked_until:
                # Coordinator uses ISO timestamps; interpret as absolute time.
                dt = datetime.fromisoformat(blocked_until.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                delta = (dt.astimezone(timezone.utc) - now).total_seconds()
                if delta > 0:
                    return min(60.0 * 60 * 12, delta)  # cap 12 hours
        except Exception:
            pass

        # 3) Provider message: "Please try again in 4m52.89s"
        try:
            m = re.search(r"try again in\\s+(\\d+)m([0-9.]+)s", str(error or ""), flags=re.I)
            if m:
                return min(60.0 * 30, float(m.group(1)) * 60.0 + float(m.group(2)))
            m = re.search(r"try again in\\s+([0-9.]+)s", str(error or ""), flags=re.I)
            if m:
                return min(60.0 * 30, float(m.group(1)))
        except Exception:
            pass

        # Default backoff (short, but will be multiplied by defer_count elsewhere).
        return 8.0
        
    def _report_event(self, reason: str, http_status: int = 0, error_code: str = "") -> Dict[str, Any]:
        """Laporkan event ke coordinator"""
        if not self.lease:
            self._last_event_decision = {}
            return {}

        try:
            resp = coordinator_report_provider_event(
                provider_account_id=self.lease.account_id,
                provider=self.lease.provider,
                model_name=self.lease.model,
                reason=reason[:4000],
                source="format_transcripts_pool",
                http_status=http_status,
                error_code=error_code
            )
            decision = resp.get("decision") if isinstance(resp, dict) else None
            self._last_event_decision = decision if isinstance(decision, dict) else {}
            return self._last_event_decision
        except Exception as e:
            print(f"   [Worker-{self.worker_id}] ⚠️ Report event failed: {e}")
            self._last_event_decision = {}
            return {}

    def _release_state_from_last_decision(self) -> tuple[str, str]:
        decision = self._last_event_decision if isinstance(self._last_event_decision, dict) else {}
        action = str(decision.get("action") or "").strip().lower()
        if action == "blocked":
            blocked_until = str(decision.get("blocked_until") or "").strip()
            blocked_model_name = str(decision.get("blocked_model_name") or "").strip()
            note_bits = ["blocked by coordinator"]
            if blocked_model_name and blocked_model_name != str(self.lease.model if self.lease else "").strip():
                note_bits.append(f"model={blocked_model_name}")
            if blocked_until:
                note_bits.append(f"until={blocked_until}")
            return "blocked", " | ".join(note_bits)[:1000]
        if action == "disabled":
            return "disabled", "disabled by coordinator"[:1000]
        return "idle", "provider failed"[:1000]
            
    def _read_transcript(self, task: VideoTask) -> Optional[str]:
        """Ambil konten transkrip dari database (blob) atau fallback ke file."""
        cached_text = str(getattr(task, "transcript_text", "") or "").strip()
        if cached_text:
            return cached_text
        if hasattr(self, 'db'):
            return self.db.get_transcript_content(task.video_id)
            
        # Fallback manual jika db tidak terinisialisasi
        transcript_file = Path(task.transcript_path)
        if not transcript_file.exists():
            return None
        try:
            return transcript_file.read_text(encoding='utf-8')
        except Exception:
            return None

    def _transcript_signal_stats(self, text: str) -> Dict[str, int]:
        lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
        cue_lines = 0
        word_lines = 0
        words = 0
        alpha_words = 0
        short_lines = 0
        very_short_lines = 0
        for ln in lines:
            if re.match(r"^\[\d{2}:\d{2}:\d{2}(?:[.,]\d{3})?\]$", ln):
                cue_lines += 1
            toks = re.findall(r"[A-Za-zÀ-ÿ\u0600-\u06FF']+", ln)
            if toks:
                word_lines += 1
                words += len(toks)
                alpha_words += sum(1 for t in toks if re.search(r"[A-Za-zÀ-ÿ\u0600-\u06FF]", t))
                if len(toks) <= 6:
                    short_lines += 1
                if len(toks) <= 3:
                    very_short_lines += 1
        return {
            "lines": len(lines),
            "cue_lines": cue_lines,
            "word_lines": word_lines,
            "words": words,
            "alpha_words": alpha_words,
            "short_lines": short_lines,
            "very_short_lines": very_short_lines,
        }

    def _is_low_content_transcript(self, text: str) -> bool:
        compact = re.sub(r"\s+", " ", str(text or "")).strip()
        # For very short transcripts, deterministic cleanup is more reliable than an LLM call
        # (LLMs may refuse or return outputs that fail length-based validation).
        if len(compact) < 220:
            return True
        stats = self._transcript_signal_stats(text)
        if stats["alpha_words"] <= 8:
            return True
        if stats["lines"] > 0 and stats["cue_lines"] >= max(1, stats["lines"] - 2):
            return True
        if stats["word_lines"] <= 2 and stats["alpha_words"] <= 16:
            return True
        return False

    def _format_low_content_transcript(self, text: str) -> str:
        """Best-effort local formatter for very short/low-signal transcripts.

        For tiny transcripts, calling an LLM often produces refusals or extremely short outputs that
        fail validation. In those cases we still want deterministic output on disk and in the DB.
        """
        raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
        lines: list[str] = []
        for ln in raw.splitlines():
            # Strip common timestamp prefixes like:
            # [00:00:00.000] hello
            # 00:00:00 hello
            ln = re.sub(r"^\\s*(?:\\[\\s*)?\\d{1,2}:\\d{2}:\\d{2}(?:[\\.,]\\d{3})?(?:\\s*\\])?\\s*", "", ln)
            ln = ln.strip()
            if ln:
                lines.append(ln)
        cleaned = "\n".join(lines).strip()
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _strip_code_fences(self, text: str) -> str:
        cleaned = str(text or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```$", "", cleaned)
        # Some models include reasoning wrapped in tags; strip it so we keep only the transcript body.
        # Handle both well-formed and truncated/malformed tag blocks.
        cleaned = re.sub(r"(?is)<think>.*?(</think>|$)", "", cleaned)
        cleaned = re.sub(r"(?is)<analysis>.*?(</analysis>|$)", "", cleaned)
        cleaned = cleaned.replace("</think>", "").replace("</analysis>", "")
        return cleaned.strip()

    def _extract_message_content(self, message: object) -> str:
        if isinstance(message, dict):
            content = message.get("content", "")
        else:
            content = getattr(message, "content", "")
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(str(item["text"]))
            return "".join(parts).strip()
        return str(content or "").strip()

    def _adaptive_format_settings(self, model_limits: Dict[str, Any]) -> Dict[str, int]:
        raw = dict(model_limits or {})
        chars_per_token = float(raw.get("chars_per_token") or 4.0)
        prompt_tokens = int(raw.get("recommended_prompt_tokens") or 0)
        completion_tokens = int(raw.get("recommended_completion_tokens") or 0)
        max_output_tokens = int(raw.get("max_output_tokens") or 0)

        chunk_chars = int(prompt_tokens * chars_per_token * 0.78) if prompt_tokens > 0 else 16000
        chunk_chars = max(3000, min(chunk_chars, 32000))
        provider = str(getattr(self.lease, "provider", "") or "").strip().lower()
        model_name = str(getattr(self.lease, "model", "") or "").strip().lower()
        if provider == "groq":
            if "kimi-k2" in model_name:
                chunk_chars = min(chunk_chars, 9000)
            elif "gpt-oss-20b" in model_name:
                chunk_chars = min(chunk_chars, 7500)
        elif provider == "cerebras":
            if "llama3.1-8b" in model_name:
                chunk_chars = min(chunk_chars, 4500)
            else:
                chunk_chars = min(chunk_chars, 9000)
        max_tokens = int(completion_tokens or 6000)
        retry_max_tokens = max(9000, int(max_tokens * 1.5))
        if max_output_tokens > 0:
            max_tokens = min(max_tokens, max_output_tokens)
            retry_max_tokens = min(retry_max_tokens, max_output_tokens)
        return {
            "chunk_chars": int(chunk_chars),
            "max_tokens": max(700, int(max_tokens)),
            "retry_max_tokens": max(900, int(retry_max_tokens)),
        }
            
    def _is_request_too_large_error(self, error: str) -> bool:
        low = str(error or "").lower()
        return "413" in low or "request too large" in low

    def _call_provider_once(self, prompt: str, budget: int) -> Dict[str, Any]:
        if self.lease.usage_method == "groq_sdk":
            return self._call_groq_sdk(prompt, budget)
        if self.lease.usage_method == "cerebras_sdk":
            return self._call_cerebras_sdk(prompt, budget)
        return self._call_openai_compatible(prompt, budget)

    def _format_chunk_recursive(
        self,
        text: str,
        *,
        chunk_chars: int,
        max_tokens: int,
        retry_max_tokens: int,
        strict_retry: bool = False,
        depth: int = 0,
        total_parts_hint: int | None = None,
        part_index_hint: int | None = None,
    ) -> Dict[str, Any]:
        if depth > 8:
            return {"ok": False, "error": "max_split_depth_exceeded"}

        if chunk_chars > 0 and len(text) > chunk_chars:
            parts = self._chunk_transcript(text, chunk_chars)
            rendered: List[str] = []
            total = len(parts)
            for idx, part in enumerate(parts, start=1):
                result = self._format_chunk_recursive(
                    part,
                    chunk_chars=chunk_chars,
                    max_tokens=max_tokens,
                    retry_max_tokens=retry_max_tokens,
                    strict_retry=strict_retry,
                    depth=depth + 1,
                    total_parts_hint=total,
                    part_index_hint=idx,
                )
                if not result.get("ok"):
                    return result
                rendered.append(str(result.get("content") or "").strip())
                if idx < total:
                    time.sleep(1)
            return {"ok": True, "content": "\n\n".join(x for x in rendered if x).strip()}

        prompt_template = STRICT_RETRY_PROMPT_FORMAT if strict_retry else PROMPT_FORMAT
        prompt = prompt_template.format(transcript=text)
        if total_parts_hint and part_index_hint:
            prompt += (
                f"\n\nCatatan bagian: ini hanya bagian {part_index_hint}/{total_parts_hint} dari transcript penuh."
                "\n- Formatkan bagian ini apa adanya."
                "\n- Jangan tambahkan pembuka atau penutup global."
            )

        last_error = "empty response"
        for budget in (max_tokens, retry_max_tokens):
            result = self._call_provider_once(prompt, budget)
            if not result.get("ok"):
                error = str(result.get("error") or "")
                if self._is_request_too_large_error(error) and len(text) > 2000:
                    smaller = max(1800, min(len(text) // 2, max(2000, int(chunk_chars * 0.6))))
                    if smaller < len(text):
                        return self._format_chunk_recursive(
                            text,
                            chunk_chars=smaller,
                            max_tokens=max_tokens,
                            retry_max_tokens=retry_max_tokens,
                            strict_retry=strict_retry,
                            depth=depth + 1,
                            total_parts_hint=total_parts_hint,
                            part_index_hint=part_index_hint,
                        )
                return result

            content = self._strip_code_fences(result.get("content") or "")
            if content:
                return {"ok": True, "content": content}

            finish_reason = str(result.get("finish_reason") or "").strip()
            completion_tokens = int(result.get("completion_tokens") or 0)
            reasoning_len = int(result.get("reasoning_len") or 0)
            if finish_reason == "length":
                last_error = (
                    "response_truncated_in_reasoning"
                    f" max_tokens={budget}"
                    f" completion_tokens={completion_tokens}"
                    f" reasoning_len={reasoning_len}"
                )
                # If the model keeps truncating, reduce chunk size and try again so we can
                # preserve the full transcript output without exceeding max_tokens.
                if budget >= retry_max_tokens and len(text) > 2000:
                    smaller = max(1800, min(len(text) // 2, max(2000, int(chunk_chars * 0.6))))
                    if smaller < len(text):
                        return self._format_chunk_recursive(
                            text,
                            chunk_chars=smaller,
                            max_tokens=max_tokens,
                            retry_max_tokens=retry_max_tokens,
                            strict_retry=strict_retry,
                            depth=depth + 1,
                            total_parts_hint=total_parts_hint,
                            part_index_hint=part_index_hint,
                        )
                continue
            last_error = (
                "empty response"
                f" finish_reason={finish_reason or '-'}"
                f" completion_tokens={completion_tokens}"
                f" reasoning_len={reasoning_len}"
            )
            break
        return {"ok": False, "error": last_error}

    def _call_provider_api(
        self,
        transcript: str,
        *,
        strict_retry: bool = False,
        chunk_chars_override: int | None = None,
    ) -> Dict[str, Any]:
        """Call provider API langsung dengan urllib atau SDK"""
        if not self.lease:
            return {"ok": False, "error": "No lease"}

        settings = self._adaptive_format_settings(self.lease.model_limits)
        chunk_chars = int(settings["chunk_chars"])
        if chunk_chars_override and int(chunk_chars_override) > 0:
            chunk_chars = max(1800, min(chunk_chars, int(chunk_chars_override)))
        result = self._format_chunk_recursive(
            transcript,
            chunk_chars=chunk_chars,
            max_tokens=settings["max_tokens"],
            retry_max_tokens=settings["retry_max_tokens"],
            strict_retry=strict_retry,
        )
        if not result.get("ok"):
            return result
        return {"ok": True, "formatted": str(result.get("content") or "").strip()}
        
    def _call_groq_sdk(self, prompt: str, max_tokens: int) -> Dict[str, Any]:
        """Call Groq menggunakan Groq SDK"""
        try:
            from groq import Groq
        except ImportError:
            return {"ok": False, "error": "Paket groq tidak tersedia di venv aktif"}

        try:
            
            client = Groq(api_key=self.lease.api_key, timeout=180)
            kwargs: Dict[str, Any] = {
                "model": self.lease.model,
                "messages": [
                    {"role": "system", "content": "Anda adalah formatter transkrip profesional. Tugas Anda hanya memformat transkrip, bukan merangkum atau menerjemahkan."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_completion_tokens": int(max_tokens),
                "top_p": 1,
                "stream": False,
                "stop": None,
            }
            model_lower = str(self.lease.model or "").strip().lower()
            if "gpt-oss" in model_lower:
                kwargs["reasoning_effort"] = "medium"
            elif "qwen3" in model_lower:
                kwargs["reasoning_effort"] = "default"

            response = client.chat.completions.create(**kwargs)

            choice = response.choices[0]
            message = choice.message
            content = self._extract_message_content(message)
            finish_reason = str(getattr(choice, "finish_reason", "") or "").strip()
            completion_tokens = int(getattr(getattr(response, "usage", None), "completion_tokens", 0) or 0)
            reasoning_len = len(str(getattr(message, "reasoning", "") or getattr(message, "reasoning_content", "") or ""))
            return {
                "ok": True,
                "content": content,
                "finish_reason": finish_reason,
                "completion_tokens": completion_tokens,
                "reasoning_len": reasoning_len,
            }
            
        except Exception as e:
            self._report_event(f"Groq SDK error: {str(e)}")
            return {"ok": False, "error": str(e)}
            
    def _call_cerebras_sdk(self, prompt: str, max_tokens: int) -> Dict[str, Any]:
        """Call Cerebras menggunakan Cerebras SDK"""
        try:
            from cerebras.cloud.sdk import Cerebras
        except ImportError:
            return {"ok": False, "error": "Paket cerebras-cloud-sdk tidak tersedia di venv aktif"}

        try:
            
            client = Cerebras(api_key=self.lease.api_key, timeout=180)
            
            response = client.chat.completions.create(
                model=self.lease.model,
                messages=[
                    {"role": "system", "content": "Anda adalah formatter transkrip profesional. Tugas Anda hanya memformat transkrip, bukan merangkum atau menerjemahkan."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.3,
                stream=False,
            )

            choice = response.choices[0]
            message = choice.message
            content = self._extract_message_content(message)
            finish_reason = str(getattr(choice, "finish_reason", "") or "").strip()
            completion_tokens = int(getattr(getattr(response, "usage", None), "completion_tokens", 0) or 0)
            reasoning_len = len(str(getattr(message, "reasoning", "") or getattr(message, "reasoning_content", "") or ""))
            return {
                "ok": True,
                "content": content,
                "finish_reason": finish_reason,
                "completion_tokens": completion_tokens,
                "reasoning_len": reasoning_len,
            }
            
        except Exception as e:
            self._report_event(f"Cerebras SDK error: {str(e)}")
            return {"ok": False, "error": str(e)}
            
    def _call_openai_compatible(self, prompt: str, max_tokens: int) -> Dict[str, Any]:
        """Call provider OpenAI-compatible dengan urllib"""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.lease.api_key}",
        }
        if self.lease.extra_headers:
            headers.update(self.lease.extra_headers)
            
        # Payload standard
        payload = {
            "model": self.lease.model,
            "messages": [
                {"role": "system", "content": "Anda adalah formatter transkrip profesional. Tugas Anda hanya memformat transkrip, bukan merangkum atau menerjemahkan."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3,
            "stream": False,
        }
        
        # Special handling untuk z.ai glm-4.7 (disable thinking)
        if self.lease.provider == "z.ai" and self.lease.model == "glm-4.7":
            payload["thinking"] = {"type": "disabled"}
            
        # endpoint_url dari coordinator sudah lengkap
        endpoint = self.lease.endpoint_url
        if not endpoint:
            # Fallback URLs
            urls = {
                "nvidia": "https://integrate.api.nvidia.com/v1/chat/completions",
                "groq": "https://api.groq.com/openai/v1/chat/completions",
                "cerebras": "https://api.cerebras.ai/v1/chat/completions",
                "z.ai": "https://api.z.ai/api/coding/paas/v4/chat/completions"
            }
            endpoint = urls.get(self.lease.provider, "")
            
        if not endpoint:
            return {"ok": False, "error": f"No endpoint for {self.lease.provider}"}
            
        req = urllib_request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        
        timeout = 180  # 3 minutes for long formatting
        try:
            with urllib_request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    result = json.loads(raw)
                except Exception as exc:
                    self._report_event(f"JSON decode error: {exc} | raw={raw[:800]}")
                    return {"ok": False, "error": f"JSON decode error: {exc}"}
                
                # Extract content
                choices = result.get("choices") or []
                if not choices:
                    return {"ok": False, "error": "No choices in response"}
                    
                choice = choices[0]
                message = choice.get("message") or {}
                content = self._extract_message_content(message)
                finish_reason = str(choice.get("finish_reason") or "").strip()
                completion_tokens = int((result.get("usage") or {}).get("completion_tokens") or 0)
                reasoning_len = len(str(message.get("reasoning") or message.get("reasoning_content") or ""))

                return {
                    "ok": True,
                    "content": content,
                    "finish_reason": finish_reason,
                    "completion_tokens": completion_tokens,
                    "reasoning_len": reasoning_len,
                }
                
        except urllib_error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raw_lower = raw.lower()
            error_code = ""
            if exc.code == 429:
                # Distinguish daily quota (TPD) vs generic rate-limit when possible.
                if ("tpd" in raw_lower) or ("tokens per day" in raw_lower) or ("per day" in raw_lower) or ("daily" in raw_lower):
                    error_code = "tpd_exceeded"
                else:
                    error_code = "rate_limited"
            elif exc.code in (401, 403):
                error_code = "auth"
            elif exc.code == 413:
                error_code = "request_too_large"
            self._report_event(f"HTTP {exc.code}: {raw}", http_status=exc.code, error_code=error_code)
            return {"ok": False, "error": f"HTTP {exc.code}: {raw}"}
        except (urllib_error.URLError, TimeoutError, socket.timeout) as exc:
            reason = getattr(exc, "reason", exc)
            self._report_event(f"Network/timeout error: {reason}")
            return {"ok": False, "error": f"Network/timeout error: {reason}"}
        except Exception as exc:
            self._report_event(f"Unexpected provider error: {exc}")
            return {"ok": False, "error": str(exc)}
        
    def _chunk_transcript(self, transcript: str, max_chars: int) -> List[str]:
        """Pecah transkrip menjadi chunks"""
        if max_chars <= 0 or len(transcript) <= max_chars:
            return [transcript]

        paragraphs = [p.strip() for p in re.split(r"\n{2,}", transcript) if p.strip()]
        chunks: List[str] = []
        current: List[str] = []
        current_len = 0

        for para in paragraphs:
            para_len = len(para) + (2 if current else 0)
            if current and (current_len + para_len) > max_chars:
                chunks.append("\n\n".join(current).strip())
                current = [para]
                current_len = len(para)
                continue
            current.append(para)
            current_len += para_len

        if current:
            chunks.append("\n\n".join(current).strip())

        out: List[str] = []
        for chunk in chunks:
            if len(chunk) <= max_chars:
                out.append(chunk)
                continue
            start = 0
            while start < len(chunk):
                end = min(len(chunk), start + max_chars)
                split_at = chunk.rfind("\n", start, end)
                if split_at <= start:
                    split_at = end
                part = chunk[start:split_at].strip()
                if part:
                    out.append(part)
                start = split_at
        return out

    def _validate_formatted(self, formatted: str, original: str) -> tuple[bool, str]:
        """Validasi hasil formatting dengan guard yang sama seperti compare tool."""
        original_compact = re.sub(r"\s+", " ", original or "").strip()
        formatted = self._strip_code_fences(formatted)
        formatted_compact = re.sub(r"\s+", " ", formatted or "").strip()

        if not formatted_compact:
            return False, "empty_output"
        if "<think>" in formatted.lower() or "</think>" in formatted.lower():
            return False, "reasoning_leak"

        low_content = self._is_low_content_transcript(original)
        min_len = 40 if low_content else max(160, min(1200, len(original_compact) // 3))
        if len(formatted_compact) < min_len:
            return False, "too_short"

        low = formatted_compact.lower()
        banned = [
            "could you please provide",
            "please provide the full transcript",
            "please provide the complete transcript",
            "i notice the transcript you've provided",
            "i see you've shared",
            "share a youtube url",
            "once you provide the full transcript",
            "full transcript you'd like me to format",
            "could you share the complete transcript",
            "if you provide the full transcript",
            "please paste the transcript",
            "please share the raw youtube transcript",
            "silakan kirim",
            "tolong kirim",
            "harap kirim",
        ]
        for needle in banned:
            if needle in low[:600]:
                return False, "assistant_reply"

        if re.search(r"(?im)^(you can:|1\.\s+paste the complete transcript|2\.\s+provide the file path|3\.\s+share a youtube url)", formatted):
            return False, "assistant_reply"
        if re.search(r"(?im)^(maaf[, ]|silakan kirim|tolong kirim|harap kirim|please provide|could you provide)", formatted):
            return False, "assistant_reply"
        return True, "ok"
        
    def _save_formatted(self, task: VideoTask, formatted_text: str) -> str:
        """Simpan hasil format"""
        output_dir = self.uploads_dir / task.channel_slug / "text_formatted"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{task.video_id}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(formatted_text)
            
        rel_path = str(Path("uploads") / task.channel_slug / "text_formatted" / f"{task.video_id}.txt")
        
        # Buffer update to JSON instead of direct DB write
        if self.db_obj is not None:
            self.db_obj.update_video_with_formatted(task.video_id, rel_path)
            print(f"      [Worker-{self.worker_id}] [DB] formatted path persisted: {rel_path}")

        # Buffer update to JSON instead of direct DB write
        try:
            buffer_dir = Path("pending_updates")
            buffer_dir.mkdir(exist_ok=True)
            
            update_data = {
                "video_id": task.video_id,
                "type": "formatted",
                "status": "ok",
                "file_path": rel_path,
                "content": formatted_text,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            buffer_file = buffer_dir / f"update_format_{task.video_id}_{int(time.time())}.json"
            buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
        except Exception as e_buffer:
            print(f"      [Worker-{self.worker_id}] ⚠️ Gagal menyimpan buffer JSON ({task.video_id}): {e_buffer}")

        return rel_path
        
    def process_task(self, task: VideoTask) -> str:
        """Process satu task dengan retry logic"""
        self.current_task = task
        self._last_result = {
            "ok": False,
            "rel_path": "",
            "error": "",
            "provider": "",
            "model": "",
            "account_name": "",
        }
        
        # Baca transcript
        transcript = self._read_transcript(task)
        if not transcript:
            print(f"      ⚠️ Transcript file not found: {task.transcript_path}")
            self._last_result["error"] = "transcript_file_not_found"
            return "fail"
            
        task.transcript_chars = len(transcript)
        print(f"      📄 Read {len(transcript):,} characters")

        # For very small/low-signal transcripts, skip the model call and just do deterministic cleanup.
        # This avoids retry storms and validation failures like `too_short`/`assistant_reply`.
        if self._is_low_content_transcript(transcript):
            cleaned = self._format_low_content_transcript(transcript)
            if cleaned:
                rel_path = self._save_formatted(task, cleaned)
                print(f"      ✅ Low-content transcript: saved without provider call: {rel_path}")
                self._last_result.update(
                    {
                        "ok": True,
                        "rel_path": rel_path,
                        "provider": "local",
                        "model": "local_clean",
                        "account_name": "",
                    }
                )
                return "ok"
        
        # When workers ~= total keys, some workers can briefly fail to acquire due to timing.
        # Retry a few rounds before marking the task as failed. We intentionally release the lease
        # after each task so keys can rebalance across providers (e.g. Groq TPD -> NVIDIA).
        attempted_with_lease = False
        saw_retryable_error = False
        max_retry_after = 0.0
        for round_idx in range(6):
            attempted_with_lease = False
            saw_no_lease_any = False
            for provider_config in self._provider_order_for_task(task):
                provider = str(provider_config["provider"])
                model = str(provider_config["model"])
                
                # Acquire lease jika belum ada atau provider berbeda
                if not self.lease or self.lease.provider != provider or self.lease.model != model:
                    self._release_lease_if_needed(final_state="idle", note="Switching provider")
                    # Include pid so multiple formatter processes don't collide in coordinator runtime state.
                    holder = f"format-worker-{os.getpid()}-{self.worker_id}"
                    self.lease = self._acquire_lease(provider, model, holder)
                    
                    if not self.lease:
                        saw_no_lease_any = True
                        continue
                        
                    # Start heartbeat
                    self.heartbeat = LeaseHeartbeat(self.lease.lease_token, LEASE_TTL_SECONDS)
                    self.heartbeat.start()
                    print(f"      🔑 Acquired lease: {self.lease.account_name}")
                
                attempted_with_lease = True

                # Format dengan provider ini
                print(f"      🔄 Calling {provider}/{model}...")
                # Reset last decision at the start of an attempt so we don't inherit a previous error.
                self._last_event_decision = {}
                attempt_failed = False
                result = self._call_provider_api(transcript)

                # If coordinator says "retry", do a single backoff retry on the same lease before moving on.
                decision_action = str((self._last_event_decision or {}).get("action") or "").strip().lower()
                if (not result.get("ok")) and decision_action == "retry":
                    time.sleep(0.4 + (self.worker_id % 5) * 0.05)
                    self._last_event_decision = {}
                    result = self._call_provider_api(transcript)

                if result.get("ok") and result.get("formatted"):
                    formatted = result["formatted"]
                    ok, reason = self._validate_formatted(formatted, transcript)
                    if not ok:
                        # Retry dengan strict prompt untuk assistant_reply atau too_short
                        if reason in ("assistant_reply", "too_short"):
                            print(f"      🔁 Retrying same provider with strict prompt...")
                            retry_result = self._call_provider_api(transcript, strict_retry=True)
                            if retry_result.get("ok") and retry_result.get("formatted"):
                                formatted = retry_result["formatted"]
                                ok, reason = self._validate_formatted(formatted, transcript)

                            # If we still get assistant-style replies, try again with smaller chunks
                            # so the model reliably sees the transcript in-context.
                            if not ok and reason in ("assistant_reply", "too_short"):
                                settings = self._adaptive_format_settings(self.lease.model_limits)
                                smaller = max(2500, min(9000, int(settings["chunk_chars"]) // 2))
                                print(f"      🔁 Retrying with smaller chunks (chunk_chars={smaller})...")
                                retry2 = self._call_provider_api(
                                    transcript,
                                    strict_retry=True,
                                    chunk_chars_override=smaller,
                                )
                                if retry2.get("ok") and retry2.get("formatted"):
                                    formatted = retry2["formatted"]
                                    ok, reason = self._validate_formatted(formatted, transcript)
                                
                    if ok:
                        # Simpan hasil
                        rel_path = self._save_formatted(task, formatted)
                        print(f"      ✅ Formatted successfully: {rel_path}")
                        self._last_result.update(
                            {
                                "ok": True,
                                "rel_path": rel_path,
                                "provider": str(self.lease.provider if self.lease else provider),
                                "model": str(self.lease.model if self.lease else model),
                                "account_name": str(self.lease.account_name if self.lease else ""),
                            }
                        )
                        self._release_lease_if_needed(final_state="idle", note="Task completed")
                        return "ok"
                    else:
                        print(f"      ⚠️ Output validation failed: {reason}")
                        self._report_event(f"Output validation failed: {reason}")
                        attempt_failed = True
                        self._last_result.update(
                            {
                                "error": f"validation_failed:{reason}",
                                "provider": str(self.lease.provider if self.lease else provider),
                                "model": str(self.lease.model if self.lease else model),
                                "account_name": str(self.lease.account_name if self.lease else ""),
                            }
                        )
                else:
                    error_msg = result.get("error", "Unknown error")
                    print(f"      ❌ Failed: {error_msg[:100]}")
                    attempt_failed = True
                    if self._is_retryable_provider_error(str(error_msg)):
                        saw_retryable_error = True
                        try:
                            max_retry_after = max(
                                max_retry_after,
                                self._retry_after_seconds(str(error_msg), (self._last_event_decision or {})),
                            )
                        except Exception:
                            pass
                    self._last_result.update(
                        {
                            "error": str(error_msg),
                            "provider": str(self.lease.provider if self.lease else provider),
                            "model": str(self.lease.model if self.lease else model),
                            "account_name": str(self.lease.account_name if self.lease else ""),
                        }
                    )

                # Release lease dan coba provider berikutnya
                final_state, note = self._release_state_from_last_decision()
                self._release_lease_if_needed(final_state=final_state, note=note)

                # "Equal" mode: if a non-baseline model fails, don't keep cascading fallbacks.
                # Requeue and let NVIDIA baseline handle it.
                if (
                    self.handoff_to_nvidia_on_fail
                    and not bool(getattr(task, "force_nvidia", False) or False)
                    and not (
                        str(provider or "").strip().lower() == NVIDIA_BASELINE_PROVIDER
                        and str(model or "").strip() == NVIDIA_BASELINE_MODEL
                    )
                    and attempt_failed
                ):
                    task.force_nvidia = True
                    task.not_before = 0.0
                    return "defer"

            if attempted_with_lease and (saw_retryable_error or saw_no_lease_any):
                # Some failures are transient (TPD/rate limit/network). Give other providers time to free up.
                time.sleep(min(3.0, 0.35 + 0.25 * round_idx) + (self.worker_id % 7) * 0.02)
                continue
            if attempted_with_lease:
                break
            # No lease acquired at all in this round; back off and retry.
            time.sleep(0.25 + (round_idx * 0.15) + (self.worker_id % 5) * 0.02)

        # Semua provider gagal / tidak ada lease tersedia
        if not self._last_result.get("error"):
            self._last_result["error"] = "no_lease_available" if not attempted_with_lease else "all_providers_failed"
        self._release_lease_if_needed(final_state="idle", note="All providers failed")
        # If we couldn't acquire any lease at all, do not mark this task as permanently failed.
        # Requeue and try again later so throughput can recover when some keys free up.
        if self._last_result.get("error") == "no_lease_available":
            return "defer"
        # Transient provider failures (e.g. Groq TPD) should not permanently fail the task.
        if saw_retryable_error:
            # Longer cooldown for quota windows; TaskQueue will skip until not_before.
            dc = int(getattr(task, "defer_count", 0) or 0)
            base = float(max_retry_after or 8.0)
            delay = min(60.0 * 60 * 6, base + min(60.0 * 10, dc * 8.0))  # cap 6 hours
            task.not_before = time.monotonic() + delay
            return "defer"
        return "fail"
        
    def run(self, task_queue: TaskQueue, stop_event: threading.Event):
        """Worker main loop"""
        print(f"   [Worker-{self.worker_id}] 🚀 Started")
        
        while self.running and not stop_event.is_set():
            task = task_queue.get(timeout=1.0)
            if not task:
                continue
                
            self.current_task = task
            print(f"   [Worker-{self.worker_id}] 📹 Processing: {task.video_id} - {task.title[:40]}...")
            
            start_time = time.time()
            try:
                status = self.process_task(task)
            except Exception as exc:
                print(f"   [Worker-{self.worker_id}] 💥 Unhandled exception: {exc}")
                traceback.print_exc()
                self._report_event(f"Worker crashed: {exc}")
                self._release_lease_if_needed(final_state="error", note="worker exception")
                status = "fail"
            elapsed = time.time() - start_time
            
            if status == "defer":
                # Put it back and backoff a bit to avoid hot-looping when all keys are busy/blocked.
                task.defer_count = int(getattr(task, "defer_count", 0) or 0) + 1
                task_queue.put(task)
                task_queue.mark_deferred()
                # Short sleep; TaskQueue will honor task.not_before for longer cooldowns.
                time.sleep(0.10 + (self.worker_id % 7) * 0.02)
            elif status == "ok":
                print(f"   [Worker-{self.worker_id}] ✅ Completed in {elapsed:.1f}s")
                task_queue.mark_completed()
            else:
                print(f"   [Worker-{self.worker_id}] ❌ Failed")
                task_queue.mark_failed()

            if self.results_sink is not None and status != "defer":
                try:
                    self.results_sink.write_task_result(task, self._last_result)
                except Exception as exc:
                    print(f"   [Worker-{self.worker_id}] ⚠️ Failed writing results CSV: {exc}")
                
            self.current_task = None
            
        print(f"   [Worker-{self.worker_id}] 🛑 Stopped")


class TranscriptFormatterPool:
    """Pool manager untuk multiple workers"""
    
    def __init__(
        self,
        db_path: str,
        uploads_dir: str,
        num_workers: int = 3,
        order: str = DEFAULT_ORDER,
        provider_plan: str = DEFAULT_PROVIDER_PLAN,
        *,
        tasks_csv: Optional[str] = None,
        results_csv: Optional[str] = None,
    ):
        self.db_path = db_path
        self.uploads_dir = Path(uploads_dir)
        self.num_workers = num_workers
        self.order = str(order or DEFAULT_ORDER).strip().lower() or DEFAULT_ORDER
        self.provider_plan = str(provider_plan or DEFAULT_PROVIDER_PLAN).strip().lower() or DEFAULT_PROVIDER_PLAN
        self.provider_order = provider_plan_order(self.provider_plan)
        self.workers: List[TranscriptFormatterWorker] = []
        self.task_queue = TaskQueue()
        self.stop_event = threading.Event()
        self.db_conn = None
        self.tasks_csv = str(tasks_csv or "").strip() or None
        self.results_sink: Optional[CsvResultsSink] = CsvResultsSink(results_csv) if results_csv else None
        
        if not self.tasks_csv:
            self._connect_db()
            self._ensure_formatted_column()
        
    def _connect_db(self):
        """Koneksi ke database"""
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.row_factory = sqlite3.Row
        try:
            self.db_conn.execute("PRAGMA busy_timeout=8000")
            self.db_conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        
    def _ensure_formatted_column(self):
        """Pastikan kolom ada"""
        cursor = self.db_conn.cursor()
        cursor.execute("PRAGMA table_info(videos)")
        columns = [col['name'] for col in cursor.fetchall()]
        
        if 'transcript_formatted_path' not in columns:
            cursor.execute("ALTER TABLE videos ADD COLUMN transcript_formatted_path TEXT")
        if 'link_file_formatted' not in columns:
            try:
                cursor.execute("ALTER TABLE videos ADD COLUMN link_file_formatted TEXT")
            except Exception:
                pass
                
        self.db_conn.commit()
        
    def get_pending_videos(self, limit: int = None) -> List[VideoTask]:
        """Ambil video yang belum diformat"""
        cursor = self.db_conn.cursor()
        
        order_sql = "ASC" if self.order != "desc" else "DESC"
        base_query = """
            SELECT v.id, v.channel_id, v.video_id, v.title, 
                   v.transcript_file_path,
                   v.transcript_text,
                   REPLACE(REPLACE(c.channel_id, '@', ''), '/', '_') as channel_slug
            FROM videos v
            JOIN channels c ON v.channel_id = c.id
            WHERE v.transcript_downloaded = 1
              AND COALESCE(v.transcript_language, '') != 'no_subtitle'
              AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '')
              AND COALESCE(v.is_short, 0) = 0
            ORDER BY v.created_at {order_sql}
        """
        base_query = base_query.format(order_sql=order_sql)

        def add_rows(rows: list[sqlite3.Row], tasks: list[VideoTask]) -> int:
            stale = 0
            for row in rows:
                transcript_path = str(row["transcript_file_path"] or "").strip()
                tasks.append(
                    VideoTask(
                        id=row["id"],
                        channel_id=row["channel_id"],
                        video_id=row["video_id"],
                        title=row["title"],
                        transcript_path=transcript_path,
                        transcript_text=str(row["transcript_text"] or ""),
                        channel_slug=row["channel_slug"],
                    )
                )
                if limit and len(tasks) >= int(limit):
                    break
            return stale

        tasks: List[VideoTask] = []
        stale_paths = 0

        if not limit:
            cursor.execute(base_query)
            rows = cursor.fetchall()
            stale_paths += add_rows(rows, tasks)
        else:
            # We apply LIMIT at SQL-level in pages, but still need to filter out stale/missing files.
            # Keep scanning until we collect `limit` valid tasks or we run out of rows.
            wanted = int(limit)
            offset = 0
            page_size = max(500, min(5000, wanted * 2))
            while len(tasks) < wanted:
                cursor.execute(f"{base_query} LIMIT ? OFFSET ?", (page_size, offset))
                rows = cursor.fetchall()
                if not rows:
                    break
                offset += len(rows)
                stale_paths += add_rows(rows, tasks)

        if stale_paths:
            print(f"⚠️ Skipped {stale_paths} rows with stale/missing transcript files")

        return tasks[: int(limit)] if limit else tasks

    def get_pending_videos_from_csv(self, limit: int = None) -> List[VideoTask]:
        """Load tasks from a CSV file instead of querying sqlite."""
        path = Path(self.tasks_csv or "")
        if not path.exists():
            raise FileNotFoundError(f"tasks CSV not found: {path}")

        tasks: List[VideoTask] = []
        stale = 0
        with open(path, "r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                video_id = str((row.get("video_id") or "").strip())
                channel_slug = str((row.get("channel_slug") or "").strip())
                title = str((row.get("title") or "").strip())
                transcript_path = str((row.get("transcript_file_path") or row.get("transcript_path") or "").strip())
                transcript_text = str((row.get("transcript_text") or "").strip())
                raw_id = str((row.get("id") or "").strip())

                if not (video_id and channel_slug):
                    continue
                try:
                    vid_id = int(raw_id) if raw_id else 0
                except Exception:
                    vid_id = 0

                tasks.append(
                    VideoTask(
                        id=vid_id,
                        channel_id=0,
                        video_id=video_id,
                        title=title or video_id,
                        transcript_path=transcript_path,
                        transcript_text=transcript_text,
                        channel_slug=channel_slug,
                    )
                )
                if limit and len(tasks) >= int(limit):
                    break

        if stale:
            print(f"⚠️ Skipped {stale} CSV rows with missing transcript files on this machine")
        return tasks
        
    def run(self, limit: int = None):
        """Jalankan pool workers"""
        # Get pending tasks
        videos = self.get_pending_videos_from_csv(limit=limit) if self.tasks_csv else self.get_pending_videos(limit=limit)
        
        if not videos:
            print("\n✅ No videos pending formatting!")
            return 0
            
        print(f"📊 Found {len(videos)} videos pending formatting")
        print(f"👷 Starting {self.num_workers} workers...")
        
        # Fill queue
        for video in videos:
            self.task_queue.put(video)
            
        # Create workers
        for i in range(self.num_workers):
            worker = TranscriptFormatterWorker(
                i,
                self.uploads_dir,
                self.db_path,
                self.provider_order,
                provider_plan=self.provider_plan,
                results_sink=self.results_sink,
            )
            self.workers.append(worker)
            
        # Run workers in threads
        threads = []
        for worker in self.workers:
            t = threading.Thread(target=worker.run, args=(self.task_queue, self.stop_event))
            t.start()
            threads.append(t)
            
        # Monitor progress
        try:
            while True:
                stats = self.task_queue.stats()
                total_done = stats["completed"] + stats["failed"]

                if stats["pending"] == 0 and total_done >= len(videos):
                    break

                print(
                    f"\r📊 Progress: {total_done}/{len(videos)} "
                    f"(✅ {stats['completed']} ❌ {stats['failed']} ⏳ {stats['pending']})",
                    end="",
                    flush=True,
                )

                time.sleep(2)

        except KeyboardInterrupt:
            print("\n\n⚠️ Interrupted by user")
        finally:
            # Always signal workers to stop; otherwise threads can remain alive
            # and keep the process running even when the queue is empty.
            self.stop_event.set()
            for worker in self.workers:
                worker.running = False

        # Wait for threads to finish
        for t in threads:
            t.join(timeout=10)
            
        # Cleanup workers
        for worker in self.workers:
            worker.close()

        if self.results_sink is not None:
            self.results_sink.close()
            
        # Print report
        self._print_report(videos)
        
        return 0 if self.task_queue.failed == 0 else 1
        
    def _print_report(self, videos: List[VideoTask]):
        """Print laporan"""
        stats = self.task_queue.stats()
        
        print("\n\n" + "=" * 60)
        print("📊 FORMATTING REPORT")
        print("=" * 60)
        print(f"""
Total pending:     {len(videos):,}
✅ Completed:      {stats['completed']:,}
❌ Failed:         {stats['failed']:,}
⏳ Still pending:  {stats['pending']:,}
""")


def main():
    parser = argparse.ArgumentParser(
        description="Format YouTube transcripts with multi-worker pool"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help=f"Path to database (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--uploads",
        default=DEFAULT_UPLOADS,
        help=f"Uploads directory (default: {DEFAULT_UPLOADS})"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of videos to process (default: all)"
    )
    parser.add_argument(
        "--tasks-csv",
        default=None,
        help="Run from a CSV task list instead of querying sqlite. Columns: id,video_id,channel_slug,title,transcript_file_path,transcript_text",
    )
    parser.add_argument(
        "--results-csv",
        default=None,
        help="Write per-task results to CSV (recommended when using --tasks-csv).",
    )
    parser.add_argument(
        "--order",
        choices=("asc", "desc"),
        default=DEFAULT_ORDER,
        help="Order by created_at: asc=oldest first, desc=newest first (default: asc)",
    )
    parser.add_argument(
        "--provider-plan",
        default=DEFAULT_PROVIDER_PLAN,
        help=(
            "Provider plan. Examples: nvidia_only, groq_only, kimi_then_nvidia, groq_then_nvidia, nvidia_then_groq, format_equal, all (default: nvidia_only). Note: z.ai and gemini are intentionally excluded."
        ),
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📝 YouTube Transcript Formatter - Multi Worker Pool")
    print("=" * 60)
    print(f"📁 Database: {args.db}")
    print(f"📂 Uploads: {args.uploads}")
    print(f"👷 Workers: {args.workers}")
    print(f"📊 Limit: {args.limit or 'all'}")
    if args.tasks_csv:
        print(f"🧾 Tasks CSV: {args.tasks_csv}")
    if args.results_csv:
        print(f"🧾 Results CSV: {args.results_csv}")
    print(f"↕️ Order: {args.order}")
    print(f"🧭 Provider plan: {args.provider_plan}")
    print(f"🌐 Coordinator: {coordinator_base_url()}")
    print()
    
    # Check coordinator connectivity
    try:
        status = coordinator_status_accounts(include_inactive=False)
        print(f"✅ Coordinator connected ({len(status)} accounts)")
    except Exception as e:
        print(f"⚠️ Coordinator warning: {e}")
        
    try:
        pool = TranscriptFormatterPool(
            args.db,
            args.uploads,
            num_workers=args.workers,
            order=args.order,
            provider_plan=args.provider_plan,
            tasks_csv=args.tasks_csv,
            results_csv=args.results_csv,
        )
        return pool.run(limit=args.limit)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
