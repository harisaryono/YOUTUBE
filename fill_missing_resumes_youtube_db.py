#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import json
import os
import random
import re
import socket
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Iterable
from zoneinfo import ZoneInfo

from local_services import (
    coordinator_base_url,
    DEFAULT_PROVIDERS_DB,
    coordinator_enabled,
    coordinator_heartbeat_lease,
    coordinator_acquire_accounts,
    coordinator_release_lease,
    coordinator_report_provider_event,
    is_provider_blocking_enabled as provider_blocking_enabled,
    is_transient_provider_limit_error,
)
from database_optimized import OptimizedDatabase

LOCAL_TZ = ZoneInfo("Asia/Jakarta")
LEASE_TTL_SECONDS = 300
ACQUIRE_WAIT_TIMEOUT_SECONDS = max(3, int(os.getenv("YT_RESUME_ACQUIRE_TIMEOUT_SECONDS", "6") or 6))
ACQUIRE_WAIT_INITIAL_SECONDS = max(1, int(os.getenv("YT_RESUME_ACQUIRE_INITIAL_WAIT_SECONDS", "2") or 2))
ACQUIRE_WAIT_MAX_SECONDS = max(3, int(os.getenv("YT_RESUME_ACQUIRE_MAX_WAIT_SECONDS", "5") or 5))
GENERATION_TIMEOUT_SECONDS = max(60, int(os.getenv("YT_RESUME_GENERATION_TIMEOUT_SECONDS", "240") or 240))
DEFAULT_PROVIDER_FALLBACKS = tuple(
    item.strip().lower()
    for item in os.getenv("YT_RESUME_PROVIDER_FALLBACKS", "nvidia").split(",")
    if item.strip()
)
AUTH_FATAL_PATTERNS = (
    "error code: 401",
    "error code: 403",
    "user not found",
    "invalid api key",
    "invalid_api_key",
    "missing authentication header",
    "authentication failed",
    "unauthorized",
    "forbidden",
)
def is_fatal_auth_error(reason: str) -> bool:
    """Check if error is fatal authentication error."""
    low = (reason or "").strip().lower()
    if not low:
        return False
    return any(pattern in low for pattern in AUTH_FATAL_PATTERNS)
def next_local_midnight() -> datetime:
    """Get next midnight in local timezone."""
    now = datetime.now(LOCAL_TZ)
    tomorrow = now.date().toordinal() + 1
    next_date = datetime.fromordinal(tomorrow).date()
    return datetime(next_date.year, next_date.month, next_date.day, 0, 5, 0, tzinfo=LOCAL_TZ)

try:
    from groq import Groq
except Exception:
    Groq = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# Constants for the new schema
DEFAULT_YOUTUBE_DB = "youtube_transcripts.db"

@dataclass
class VideoRow:
    id: int
    channel_id: int
    video_id: str
    title: str
    link_file: str
    link_resume: Optional[str]
    slug: str

PROMPT_TEMPLATE = """Anda adalah penyusun catatan belajar yang teliti, faktual, dan sangat detail.

Tugas:
Buat resume isi video yang padat materi berdasarkan transkrip yang diberikan.

Tujuan:
- hasil harus terasa seperti catatan materi atau handout belajar,
- bukan sekadar penjelasan global tentang topik video,
- bukan komentar meta tentang apa yang dilakukan pembicara.

Aturan:
- Gunakan bahasa Indonesia baku yang rapi dan jelas.
- Jangan menyebut bahwa Anda AI.
- Jangan menulis komentar meta seperti meminta transkrip lain atau menjelaskan keterbatasan Anda.
- Jangan mengarang fakta, angka, nama, atau kesimpulan yang tidak tampak di transkrip.
- Jika ada bagian kurang jelas, tulis secara hati-hati tanpa menambah detail baru.
- Fokus pada isi materi, langkah penjelasan, rumus, istilah, contoh, kategori, syarat, daftar, dan kesimpulan praktis.
- Hindari gaya abstrak seperti:
  - "video ini membahas..."
  - "pembicara menjelaskan bahwa..."
  - "materi ini menyoroti..."
  - "contoh kasus diberikan untuk..."
- Tulis isi secara langsung. Bukan "pembicara menjelaskan pengolahan air limbah", tetapi tulis apa isi penjelasannya.
- Jika ada urutan langkah, jenis, klasifikasi, rumus, parameter, syarat, atau poin teknis, tampilkan secara eksplisit.
- Gunakan Markdown yang rapi.

Format output wajib:

# {title}

## Gambaran Umum
Tulis 1 sampai 2 paragraf ringkas yang menjelaskan konteks dan sasaran materi, tanpa terlalu banyak kalimat meta.

## Resume Lengkap
Tulis isi materi secara rinci dan runtut.
Pecah dengan subjudul `###` bila perlu.
Bagian ini adalah bagian terpenting.
Isinya harus menjelaskan muatan materi, bukan hanya mengatakan tema yang dibahas.

## Poin-Poin Kunci
Tulis 8 sampai 15 bullet yang benar-benar berisi informasi penting, bukan bullet generik.

## Struktur Argumen
Jelaskan susunan isi atau alur pembahasan secara konkret: dari mana materi dimulai, poin apa saja yang dibangun, dan bagaimana kesimpulan ditarik.

## Konsep, Istilah, dan Nama Penting
Buat daftar konsep, istilah, nama tokoh, kitab, teori, istilah teknis, rumus, parameter, atau regulasi yang muncul, lalu beri penjelasan singkat.

## Ringkasan Akhir
Tulis 1 sampai 2 paragraf penutup yang merangkum isi dan pelajaran praktisnya.

Judul video:
{title}

Transkrip:
{transcript}
"""

@dataclass(frozen=True)
class ProviderAccount:
    id: int
    provider: str
    account_name: str
    api_key: str
    endpoint_url: str
    model_name: str
    usage_method: str
    extra_headers: dict[str, str]
    lease_token: str
    model_limits: dict[str, object]

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.account_name}"


class LeaseUnavailable(RuntimeError):
    pass

def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)

def iter_rows_youtube(
    con: sqlite3.Connection, *, channels: Optional[list[str]], missing_only: bool, limit: int
) -> Iterable[VideoRow]:
    # Adaptation for youtube_transcripts.db schema
    where = [
        "v.transcript_downloaded = 1",
        "COALESCE(v.is_short, 0) = 0",
    ]
    params: list[object] = []

    if missing_only:
        where.append("(v.summary_file_path IS NULL OR v.summary_file_path = '')")

    if channels:
        placeholders = ",".join("?" for _ in channels)
        where.append(f"c.channel_name IN ({placeholders})")
        params.extend(channels)

    sql = f"""
        SELECT v.id, v.channel_id, v.video_id, v.title, v.transcript_file_path, v.summary_file_path, c.channel_name
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE {' AND '.join(where)}
        ORDER BY c.channel_name ASC, v.id DESC
    """
    if limit > 0:
        sql += f" LIMIT {int(limit)}"

    cur = con.execute(sql, params)
    for row in cur.fetchall():
        yield VideoRow(
            id=int(row[0]),
            channel_id=int(row[1]),
            video_id=str(row[2]),
            title=str(row[3] or ""),
            link_file=str(row[4]),
            link_resume=(str(row[5]) if row[5] else None),
            slug=str(row[6]),
        )

def rows_from_tasks_csv(con: sqlite3.Connection, tasks_csv: str, *, limit: int = 0) -> list[VideoRow]:
    rows: list[VideoRow] = []
    with Path(tasks_csv).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for item in reader:
            video_id = str(item.get("video_id") or "").strip()
            if not video_id:
                continue
            row = con.execute(
                """
                SELECT v.id, v.channel_id, v.video_id, v.title, v.transcript_file_path, v.summary_file_path, c.channel_name
                FROM videos v
                JOIN channels c ON c.id = v.channel_id
                WHERE v.video_id = ?
                LIMIT 1
                """,
                (video_id,),
            ).fetchone()
            if row is None:
                continue
            rows.append(
                VideoRow(
                    id=int(row[0]),
                    channel_id=int(row[1]),
                    video_id=str(row[2]),
                    title=str(row[3] or ""),
                    link_file=str(row[4]),
                    link_resume=(str(row[5]) if row[5] else None),
                    slug=str(row[6]),
                )
            )
            if limit > 0 and len(rows) >= int(limit):
                break
    return rows

def append_report(report_csv: str, row: list[object]) -> None:
    if not report_csv:
        return
    path = Path(report_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if is_new:
            writer.writerow(["video_id", "channel_name", "status", "note", "provider_account", "model_name", "transcript_file_path", "summary_file_path"])
        writer.writerow(row)


def resolve_transcript_path(raw_path: str) -> Path:
    path = Path(str(raw_path or "").strip())
    if path.exists():
        return path
    if not path.is_absolute():
        candidate = Path(__file__).parent / "uploads" / path
        if candidate.exists():
            return candidate
    return path

def acquire_account_from_coordinator(
    provider: str,
    model_name: str,
    holder: str = "",
    pid: int = 0,
    eligible_account_ids: Optional[list[int]] = None,
    *,
    timeout_seconds: int = ACQUIRE_WAIT_TIMEOUT_SECONDS,
    immediate: bool = False,
) -> ProviderAccount:
    started_at = time.monotonic()
    attempt = 0
    last_error = ""
    timeout_seconds = max(1, int(timeout_seconds or ACQUIRE_WAIT_TIMEOUT_SECONDS))
    if immediate:
        try:
            leases = coordinator_acquire_accounts(
                provider=provider,
                model_name=model_name,
                count=1,
                holder=holder,
                pid=pid,
                eligible_account_ids=eligible_account_ids or [],
                task_type="resume_generation",
                lease_ttl_seconds=LEASE_TTL_SECONDS,
            )
            if leases:
                lease = leases[0]
                acct_id = int(lease["provider_account_id"])
                raw_headers = lease.get("extra_headers") or {}
                headers = raw_headers if isinstance(raw_headers, dict) else {}
                api_key = str(lease.get("api_key") or "").strip()
                if not api_key:
                    raise RuntimeError(
                        f"Coordinator acquire bundle tidak mengandung api_key untuk account {acct_id}"
                    )
                return ProviderAccount(
                    id=acct_id,
                    provider=str(lease.get("provider") or provider),
                    account_name=str(lease.get("account_name") or acct_id),
                    api_key=api_key,
                    endpoint_url=str(lease.get("endpoint_url") or ""),
                    model_name=model_name,
                    usage_method=str(lease.get("usage_method") or ""),
                    extra_headers={str(k): str(v) for k, v in headers.items()},
                    lease_token=str(lease["lease_token"]),
                    model_limits=dict(lease.get("model_limits") or {}),
                )
            last_error = f"No accounts for {provider}/{model_name}"
        except Exception as exc:
            last_error = str(exc).strip() or repr(exc)
        raise LeaseUnavailable(last_error or f"No lease available for {provider}/{model_name}")

    while True:
        try:
            leases = coordinator_acquire_accounts(
                provider=provider,
                model_name=model_name,
                count=1,
                holder=holder,
                pid=pid,
                eligible_account_ids=eligible_account_ids or [],
                task_type="resume_generation",
                lease_ttl_seconds=LEASE_TTL_SECONDS,
            )
            if leases:
                lease = leases[0]
                acct_id = int(lease["provider_account_id"])
                raw_headers = lease.get("extra_headers") or {}
                headers = raw_headers if isinstance(raw_headers, dict) else {}
                api_key = str(lease.get("api_key") or "").strip()
                if not api_key:
                    raise RuntimeError(
                        f"Coordinator acquire bundle tidak mengandung api_key untuk account {acct_id}"
                    )
                return ProviderAccount(
                    id=acct_id,
                    provider=str(lease.get("provider") or provider),
                    account_name=str(lease.get("account_name") or acct_id),
                    api_key=api_key,
                    endpoint_url=str(lease.get("endpoint_url") or ""),
                    model_name=model_name,
                    usage_method=str(lease.get("usage_method") or ""),
                    extra_headers={str(k): str(v) for k, v in headers.items()},
                    lease_token=str(lease["lease_token"]),
                    model_limits=dict(lease.get("model_limits") or {}),
                )
            last_error = f"No accounts for {provider}/{model_name}"
        except Exception as exc:
            last_error = str(exc).strip() or repr(exc)

        elapsed = time.monotonic() - started_at
        if elapsed >= timeout_seconds:
            raise LeaseUnavailable(last_error or f"No lease available for {provider}/{model_name}")

        sleep_seconds = min(
            ACQUIRE_WAIT_MAX_SECONDS,
            ACQUIRE_WAIT_INITIAL_SECONDS * (2 ** min(attempt, 4)),
        )
        sleep_seconds += random.uniform(0.0, min(3.0, sleep_seconds * 0.2))
        log(
            f"[WAIT] lease belum tersedia untuk {provider}/{model_name}; "
            f"retry dalam {sleep_seconds:.1f}s (elapsed={elapsed:.0f}s)"
        )
        time.sleep(sleep_seconds)
        attempt += 1

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
    chunk_completion_tokens = completion_tokens if completion_tokens > 0 else 900
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


def chat_once(account: ProviderAccount, prompt: str, timeout: int, *, max_tokens: int) -> tuple[str, str]:
    if account.provider == "groq" and account.usage_method == "groq_sdk" and Groq is not None:
        client = Groq(api_key=account.api_key, timeout=timeout)
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
        choice = resp.choices[0]
        return extract_message_content(choice.message), str(choice.finish_reason or "").strip()
    base_url = account.endpoint_url.strip()
    if base_url.endswith("/chat/completions"): base_url = base_url[:-len("/chat/completions")]
    client = OpenAI(api_key=account.api_key, base_url=base_url, timeout=timeout, default_headers=account.extra_headers)
    kwargs = {
        "model": account.model_name,
        "messages": [{"role": "system", "content": "Jawab dalam bahasa Indonesia baku."}, {"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": int(max_tokens),
    }
    if str(account.provider or "").strip().lower() == "z.ai":
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
    resp = client.chat.completions.create(**kwargs)
    choice = resp.choices[0]
    return extract_message_content(choice.message), str(choice.finish_reason or "").strip()


def chat_complete(
    account: ProviderAccount,
    prompt: str,
    timeout: int,
    *,
    max_tokens_values: tuple[int, ...],
) -> str:
    last_error = "empty response"
    for budget in max_tokens_values:
        content, finish_reason = chat_once(account, prompt, timeout, max_tokens=int(budget))
        if content:
            return content
        if finish_reason == "length":
            last_error = f"response_truncated_in_reasoning max_tokens={int(budget)}"
            continue
        last_error = f"empty response finish_reason={finish_reason or '-'}"
        break
    raise RuntimeError(last_error)


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
            parts.append(para[i:i + max_chars])
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


def generate_resume_markdown(account: ProviderAccount, *, title: str, transcript: str, timeout: int) -> str:
    transcript = (transcript or "").strip()
    settings = adaptive_resume_settings(account.model_limits)
    chunk_chars = int(settings["chunk_chars"])
    chunk_max_tokens = int(settings["chunk_max_tokens"])
    chunk_retry_tokens = int(settings["chunk_retry_tokens"])
    single_pass_max_tokens = tuple(int(x) for x in settings["single_pass_max_tokens"])
    if len(transcript) <= chunk_chars:
        prompt = PROMPT_TEMPLATE.format(title=title, transcript=transcript)
        return chat_complete(account, prompt, timeout, max_tokens_values=single_pass_max_tokens)

    chunks = chunk_text(transcript, max_chars=chunk_chars)
    summaries: list[str] = []
    min_chunk_chars = max(2000, int(chunk_chars * 0.3))
    idx = 0
    while idx < len(chunks):
        chunk = chunks[idx]
        try:
            part = chat_complete(
                account,
                build_chunk_prompt(chunk, idx + 1, len(chunks)),
                timeout,
                max_tokens_values=(chunk_max_tokens, chunk_retry_tokens),
            ).strip()
            if not part:
                raise RuntimeError("Ringkasan chunk kosong setelah request.")
            summaries.append(part)
            idx += 1
        except Exception:
            if len(chunk) > min_chunk_chars:
                new_chunks = split_chunk(chunk)
                if len(new_chunks) > 1:
                    chunks[idx:idx + 1] = new_chunks
                    continue
            raise
    return chat_complete(account, build_final_prompt(title, summaries), timeout, max_tokens_values=single_pass_max_tokens)

class LeaseHeartbeat:
    def __init__(self, token: str, lease_ttl_seconds: int = LEASE_TTL_SECONDS):
        self.token = token
        self.lease_ttl_seconds = max(60, int(lease_ttl_seconds or LEASE_TTL_SECONDS))
        self._stop = threading.Event()
    def start(self): threading.Thread(target=self._run, daemon=True).start()
    def _run(self):
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try: coordinator_heartbeat_lease(self.token, lease_ttl_seconds=self.lease_ttl_seconds)
            except: pass
    def stop(self, state="idle"):
        self._stop.set()
        try: coordinator_release_lease(self.token, final_state=state)
        except: pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_YOUTUBE_DB)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--channel", action="append")
    parser.add_argument("--tasks-csv", default="")
    parser.add_argument("--report-csv", default="")
    parser.add_argument("--provider-account-id", action="append", type=int, dest="provider_account_ids")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-attempts", type=int, default=6)
    parser.add_argument(
        "--fallback-provider",
        action="append",
        default=[],
        help="Provider fallback bila provider utama tidak mendapat lease. Default: nvidia",
    )
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    log(f"Using coordinator URL: {coordinator_base_url()}")
    log(f"Provider: {args.provider} | Model: {args.model}")
    log(f"Blocking enabled: {provider_blocking_enabled(args.provider)}")
    provider_fallbacks = [
        item.strip().lower()
        for item in (args.fallback_provider or []) + list(DEFAULT_PROVIDER_FALLBACKS)
        if item.strip()
    ]
    if args.provider.strip().lower() in provider_fallbacks:
        provider_fallbacks = [p for p in provider_fallbacks if p != args.provider.strip().lower()]

    db_obj = OptimizedDatabase(args.db)
    if args.tasks_csv:
        rows = rows_from_tasks_csv(con, args.tasks_csv, limit=args.limit)
    else:
        rows = list(iter_rows_youtube(con, channels=args.channel, missing_only=(not args.force), limit=args.limit))
    log(f"Found {len(rows)} targets")

    for v in rows:
        success = False
        last_reason = ""
        last_account_label = ""
        requeue_to_nvidia = False
        
        for attempt in range(args.max_attempts):
            try:
                # Resolution relative to script/DB dir
                transcript = db_obj.get_transcript_content(v.video_id)

                if not transcript:
                    log(f"Transcript missing for video: {v.video_id}")
                    break

                provider_chain = [args.provider.strip().lower()]
                provider_chain.extend(
                    p for p in provider_fallbacks if p and p not in provider_chain
                )
                account = None
                provider_used = ""
                acquire_errors: list[str] = []
                for candidate_idx, candidate_provider in enumerate(provider_chain):
                    candidate_ids = list(args.provider_account_ids or [])
                    if candidate_idx > 0:
                        candidate_ids = []
                    try:
                        account = acquire_account_from_coordinator(
                            candidate_provider,
                            args.model,
                            holder=socket.gethostname(),
                            pid=os.getpid(),
                            eligible_account_ids=candidate_ids,
                            immediate=True,
                        )
                        provider_used = candidate_provider
                        break
                    except LeaseUnavailable as lease_exc:
                        acquire_errors.append(f"{candidate_provider}:{lease_exc}")
                        log(f"[FALLBACK] lease tidak tersedia untuk {candidate_provider}/{args.model}; lanjut provider berikutnya")
                        continue

                if account is None:
                    last_reason = acquire_errors[-1] if acquire_errors else "lease_unavailable"
                    log(f"[LEASE-UNAVAILABLE] {v.video_id}: {last_reason}")
                    break

                last_account_label = account.label
                lease = LeaseHeartbeat(account.lease_token)
                lease.start()

                log(f"[ATTEMPT {attempt+1}] {v.slug} | {v.video_id} | using {account.label}")
                result = generate_resume_markdown(
                    account,
                    title=v.title,
                    transcript=transcript,
                    timeout=GENERATION_TIMEOUT_SECONDS,
                )

                # Save resume relative to transcript or default uploads location
                transcript_file = db_obj.get_transcript_file(v.video_id)
                if transcript_file:
                    resume_path = transcript_file.parent.parent / "summary" / transcript_file.name.replace(".txt", ".md").replace("_transcript", "_summary")
                else:
                    # Fallback to standard location: uploads/[channel_slug]/summary/[video_id]_summary.md
                    resume_path = Path(__file__).parent / "uploads" / v.slug / "summary" / f"{v.video_id}_summary.md"
                
                resume_path.parent.mkdir(parents=True, exist_ok=True)
                resume_path.write_text(result, encoding="utf-8")

                # Record relative path from ROOT
                resume_rel = os.path.relpath(resume_path, Path(__file__).parent)

                db_obj.update_video_with_summary(v.video_id, resume_rel, result)
                log(f"[DB] {v.video_id} summary persisted to SQLite")

                # DECENTRALIZED UPDATE: Save to JSON buffer instead of direct DB update
                buffer_dir = Path(__file__).parent / "pending_updates"
                buffer_dir.mkdir(exist_ok=True)
                
                update_data = {
                    "video_id": v.video_id,
                    "type": "resume",
                    "status": "ok",
                    "file_path": resume_rel,
                    "content": result,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                buffer_file = buffer_dir / f"update_resume_{v.video_id}_{int(time.time())}.json"
                buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                
                log(f"[BUFFERED] {v.video_id} result saved to {buffer_file.name}")

                lease.stop(state="idle")
                append_report(
                    args.report_csv,
                    [v.video_id, v.slug, "ok", "", account.label, account.model_name, v.link_file, resume_rel],
                )
                log(f"[DONE] {v.slug} | {v.video_id}")
                success = True
                break
                
            except Exception as e:
                last_reason = str(e).strip() or e.__class__.__name__
                last_account_label = f"{provider_used or args.provider}:{args.model}"
                log(f"[ERROR] {v.video_id}: {last_reason}")
                
                decision: dict[str, object] = {}
                try:
                    report_resp = coordinator_report_provider_event(
                        provider_account_id=account.id if 'account' in locals() else 0,
                        provider=account.provider if 'account' in locals() else (provider_used or args.provider),
                        model_name=args.model,
                        reason=last_reason,
                        source="fill_missing_resumes_youtube_db",
                        payload={
                            "video_id": v.video_id,
                            "channel": v.slug,
                            "attempt": attempt + 1,
                        },
                    )
                    if isinstance(report_resp.get("decision"), dict):
                        decision = dict(report_resp.get("decision") or {})
                except Exception as report_exc:
                    log(f"[WARN] failed to report provider event: {report_exc}")

                action = str(decision.get("action") or "")
                if action == "disabled":
                    append_report(
                        args.report_csv,
                        [v.video_id, v.slug, "disabled_auth", last_reason[:500], last_account_label or account.label, args.model, v.link_file, ""],
                    )
                    log(f"[DISABLED] {provider_used or args.provider}/{args.model} by coordinator")
                    break
                if action == "blocked":
                    append_report(
                        args.report_csv,
                        [v.video_id, v.slug, "blocked_provider_quota", last_reason[:500], last_account_label or account.label, args.model, v.link_file, ""],
                    )
                    log(
                        f"[BLOCKED] {provider_used or args.provider}/{args.model} until "
                        f"{str(decision.get('blocked_until') or '')}"
                    )
                    break  # Stop retry on provider quota block

                current_provider = str(provider_used or args.provider).strip().lower()
                groq_retry_to_nvidia = current_provider == "groq" and (
                    action == "retry" or is_transient_provider_limit_error(last_reason, provider=current_provider)
                )
                if groq_retry_to_nvidia:
                    append_report(
                        args.report_csv,
                        [v.video_id, v.slug, "retry", last_reason[:500], last_account_label or account.label, args.model, v.link_file, ""],
                    )
                    log("[REQUEUE] groq transient limit detected; exit now so launcher can fall back to Nvidia")
                    requeue_to_nvidia = True
                    if 'lease' in locals():
                        try:
                            lease.stop(state="error")
                        except:
                            pass
                    break

                # NVIDIA: No blocking, just retry with different account
                if not provider_blocking_enabled(provider_used or args.provider):
                    log(f"[RETRY] {provider_used or args.provider} has no blocking - will retry")
                elif action == "retry" or is_transient_provider_limit_error(last_reason, provider=provider_used or args.provider):
                    append_report(
                        args.report_csv,
                        [v.video_id, v.slug, "retry", last_reason[:500], last_account_label or account.label, args.model, v.link_file, ""],
                    )
                    log(f"[RETRY] transient provider limit detected for {provider_used or args.provider}")
                
                # Release lease on error
                if 'lease' in locals():
                    try:
                        lease.stop(state="error")
                    except:
                        pass
                
                time.sleep(2 ** attempt)  # Exponential backoff

            if requeue_to_nvidia and not success:
                return 0

        if not success:
            append_report(
                args.report_csv,
                [v.video_id, v.slug, "failed", last_reason[:500], last_account_label, args.model, v.link_file, ""],
            )
            log(f"[FAILED] {v.video_id} after {args.max_attempts} attempts: {last_reason}")

if __name__ == "__main__":
    main()
