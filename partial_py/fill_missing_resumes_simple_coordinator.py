#!/usr/bin/env python3
from __future__ import annotations

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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from local_services import (
    coordinator_base_url,
    DEFAULT_PROVIDERS_DB,
    coordinator_enabled,
    coordinator_heartbeat_lease,
    coordinator_acquire_accounts,
    coordinator_release_lease,
    coordinator_report_provider_event,
    is_transient_provider_limit_error,
)

try:
    from groq import Groq
except Exception:  # pragma: no cover
    Groq = None  # type: ignore[assignment]

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]

from partial_py.fill_missing_resumes import (  # noqa: E402
    default_resume_link,
    iter_rows,
    read_link_text_under,
    resolve_under,
)


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

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.account_name}"


LOCAL_TZ = ZoneInfo("Asia/Jakarta")
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


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)


def next_local_midnight() -> datetime:
    now = datetime.now(LOCAL_TZ)
    tomorrow = now.date().toordinal() + 1
    next_date = datetime.fromordinal(tomorrow).date()
    return datetime(next_date.year, next_date.month, next_date.day, 0, 5, 0, tzinfo=LOCAL_TZ)


def is_fatal_auth_error(reason: str) -> bool:
    low = (reason or "").strip().lower()
    if not low:
        return False
    return any(pattern in low for pattern in AUTH_FATAL_PATTERNS)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate resume sederhana ala PRED-HIS dari transcript YouTube (MENGGUNAKAN COORDINATOR SERVER)."
    )
    p.add_argument("--db", default="channels.db")
    p.add_argument("-o", "--out-root", default="out")
    p.add_argument("--provider", action="append", help="Filter provider, mis. nvidia/groq")
    p.add_argument("--provider-account-id", action="append", type=int, dest="provider_account_ids")
    p.add_argument("--model", default="", help="Override model")
    p.add_argument("--channel", action="append", help="Slug channel. Boleh diulang.")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--force", action="store_true")
    p.add_argument("--tasks-csv", default="", help="CSV task spesifik berisi slug,video_id.")
    p.add_argument("--report-csv", default="")
    p.add_argument("--max-attempts", type=int, default=6)
    p.add_argument("--max-chars", type=int, default=50000)
    p.add_argument("--temperature", type=float, default=0.2)
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--max-tokens", type=int, default=3600)
    p.add_argument("--lease-ttl-seconds", type=int, default=300, help="Durasi lease dalam detik (default: 300 = 5 menit)")
    return p.parse_args()


def normalize_base_url(url: str) -> str:
    value = (url or "").strip()
    for suffix in ("/chat/completions", "/responses", "/completions"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def acquire_account_from_coordinator(
    provider: str,
    model_name: str,
    *,
    eligible_account_ids: list[int] | None = None,
    holder: str = "",
    host: str = "",
    pid: int = 0,
    task_type: str = "",
    lease_ttl_seconds: int = 300,
) -> ProviderAccount:
    if not coordinator_enabled():
        raise RuntimeError("YT_PROVIDER_COORDINATOR_URL belum diatur di environment variable.")
    
    # Get lease from coordinator - returns list directly
    leases = coordinator_acquire_accounts(
        provider=provider,
        model_name=model_name,
        count=1,
        eligible_account_ids=eligible_account_ids,
        holder=holder,
        host=host,
        pid=pid,
        task_type=task_type,
        lease_ttl_seconds=lease_ttl_seconds,
    )
    
    if not leases:
        raise RuntimeError(f"Tidak ada akun tersedia dari coordinator untuk provider={provider}, model={model_name}")
    
    lease = leases[0]
    provider_account_id = int(lease["provider_account_id"])
    
    raw_headers = lease.get("extra_headers") or {}
    headers = raw_headers if isinstance(raw_headers, dict) else {}
    api_key = str(lease.get("api_key") or "").strip()
    if not api_key:
        raise RuntimeError(f"Coordinator acquire bundle tidak mengandung api_key untuk account {provider_account_id}")

    return ProviderAccount(
        id=provider_account_id,
        provider=str(lease.get("provider") or provider),
        account_name=str(lease.get("account_name") or provider_account_id),
        api_key=api_key,
        endpoint_url=str(lease.get("endpoint_url") or ""),
        model_name=model_name,
        usage_method=str(lease.get("usage_method") or ""),
        extra_headers={str(k): str(v) for k, v in headers.items()},
        lease_token=str(lease["lease_token"]),
    )


def load_tasks_csv(path: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            slug = str(row.get("slug") or "").strip()
            video_id = str(row.get("video_id") or "").strip()
            if slug and video_id:
                rows.append((slug, video_id))
    return rows


def trim_text(body: str, max_chars: int = 50000) -> str:
    text = (body or "").strip()
    if len(text) <= max_chars:
        return text
    part = max_chars // 3
    first = text[:part].strip()
    mid_start = max(0, len(text) // 2 - part // 2)
    middle = text[mid_start : mid_start + part].strip()
    last = text[-part:].strip()
    return (
        "[Bagian awal]\n"
        + first
        + "\n\n[Bagian tengah]\n"
        + middle
        + "\n\n[Bagian akhir]\n"
        + last
    )


def clean_transcript(text: str) -> str:
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""
    lines = raw.splitlines()
    while lines and re.match(r"^(Kind|Language)\s*:", lines[0], flags=re.I):
        lines.pop(0)
    raw = "\n".join(lines).strip()
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


def transcript_is_missing(text: str) -> tuple[bool, str]:
    compact = re.sub(r"\s+", " ", (text or "").strip()).lower()
    if not compact:
        return True, "empty"
    if len(compact) < 80:
        return True, "too_short"
    markers = (
        "no subtitle",
        "no subtitles",
        "subtitles not available",
        "captions not available",
        "tidak ada subtitle",
        "subtitle tidak tersedia",
        "transkrip tidak tersedia",
    )
    if any(marker in compact for marker in markers):
        return True, "subtitle_unavailable_marker"
    return False, ""


def looks_like_assistant_reply(text: str) -> bool:
    low = (text or "").strip().lower()
    if not low:
        return True
    bad_prefixes = (
        "i notice the transcript",
        "could you please provide",
        "please provide the full transcript",
        "the transcript you've provided",
        "transcript yang anda berikan terlihat",
        "mohon kirimkan transkrip lengkap",
        "silakan kirimkan transkrip lengkap",
        "saya membutuhkan transkrip lengkap",
    )
    if any(low.startswith(prefix) for prefix in bad_prefixes):
        return True
    if "could you please provide the full transcript" in low:
        return True
    if "please provide the full transcript" in low:
        return True
    return False


def looks_too_generic_resume(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return True
    low = raw.lower()
    generic_markers = (
        "video ini membahas",
        "video ini menjelaskan",
        "pembicara menjelaskan tentang",
        "pembicara membahas tentang",
        "materi ini membahas",
        "materi ini menjelaskan",
        "contoh kasus tentang",
        "dapat membantu memahami",
        "secara umum membahas",
    )
    hits = sum(low.count(marker) for marker in generic_markers)
    if hits >= 3:
        return True
    body_lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(raw) < 1200:
        return True
    if len(body_lines) < 12:
        return True
    return False


def build_rewrite_prompt(title: str, transcript: str, previous_output: str, *, max_chars: int) -> str:
    return f"""Perbaiki resume berikut agar menjadi catatan isi yang lebih padat materi.

Masalah resume lama:
- terlalu global,
- terlalu sering memakai kalimat meta seperti "video ini membahas" atau "pembicara menjelaskan",
- belum cukup menjelaskan isi materi.

Aturan revisi:
- Tulis ulang menjadi resume isi, bukan komentar tentang isi.
- Pertahankan fakta yang ada.
- Tambahkan rincian materi yang memang muncul pada transkrip.
- Gunakan frasa langsung dan informatif.
- Jika ada langkah, klasifikasi, rumus, syarat, daftar, atau parameter, tampilkan secara eksplisit.
- Jangan menulis komentar meta.

Judul:
{title}

Transkrip:
{trim_text(transcript, max_chars=max_chars)}

Resume lama:
{previous_output.strip()}
"""


def build_prompt(title: str, transcript: str, *, max_chars: int) -> str:
    clean_title = (title or "").strip() or "Resume Video"
    return PROMPT_TEMPLATE.format(title=clean_title, transcript=trim_text(transcript, max_chars=max_chars))


def make_client(account: ProviderAccount, timeout_s: int):
    base_url = normalize_base_url(account.endpoint_url)
    headers = account.extra_headers or None
    if account.provider == "groq" and account.usage_method == "groq_sdk" and Groq is not None:
        return Groq(api_key=account.api_key, timeout=timeout_s)
    if OpenAI is None:
        raise RuntimeError("Paket openai tidak tersedia.")
    return OpenAI(
        api_key=account.api_key,
        base_url=base_url,
        timeout=timeout_s,
        default_headers=headers,
    )


def chat_complete(
    account: ProviderAccount,
    prompt: str,
    *,
    max_tokens: int,
    temperature: float,
    timeout: int,
) -> str:
    client = make_client(account, timeout)
    if account.provider == "groq" and account.usage_method == "groq_sdk" and Groq is not None:
        resp = client.chat.completions.create(  # type: ignore[union-attr]
            model=account.model_name,
            messages=[
                {"role": "system", "content": "Seluruh jawaban wajib dalam bahasa Indonesia baku."},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
        )
        return (resp.choices[0].message.content or "").strip()

    resp = client.chat.completions.create(  # type: ignore[union-attr]
        model=account.model_name,
        messages=[
            {"role": "system", "content": "Seluruh jawaban wajib dalam bahasa Indonesia baku."},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    content = resp.choices[0].message.content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            text = getattr(item, "text", None)
            if isinstance(text, str):
                parts.append(text)
        return "".join(parts).strip()
    return (content or "").strip()


def append_report(path: str, row: list[str]) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_header = not p.exists() or p.stat().st_size == 0
    with p.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(
                [
                    "slug",
                    "video_id",
                    "status",
                    "reason",
                    "provider_account",
                    "model",
                    "link_file",
                    "link_resume",
                ]
            )
        writer.writerow(row)


class LeaseHeartbeat:
    def __init__(self, lease_token: str, lease_ttl_seconds: int) -> None:
        self.lease_token = str(lease_token or "").strip()
        self.lease_ttl_seconds = max(60, int(lease_ttl_seconds or 300))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.lease_token or not coordinator_enabled():
            return
        self._thread = threading.Thread(target=self._run, name="provider-lease-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try:
                coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
            except Exception as exc:  # noqa: BLE001
                log(f"[LEASE] heartbeat gagal: {exc}")

    def stop(self, *, final_state: str = "idle", note: str = "") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if not self.lease_token or not coordinator_enabled():
            return
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception as exc:  # noqa: BLE001
            log(f"[LEASE] release gagal: {exc}")


def rows_from_tasks_csv(con: sqlite3.Connection, tasks_csv: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for slug, video_id in load_tasks_csv(tasks_csv):
        row = con.execute(
            """
            SELECT v.id, v.channel_id, v.video_id, v.title, v.seq_num, v.link_file, v.link_resume, c.slug
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE c.slug = ? AND v.video_id = ?
            LIMIT 1
            """,
            (slug, video_id),
        ).fetchone()
        if row is None:
            continue
        rows.append(
            {
                "id": int(row[0]),
                "channel_id": int(row[1]),
                "video_id": str(row[2]),
                "title": str(row[3] or ""),
                "seq_num": int(row[4]) if row[4] is not None else None,
                "link_file": str(row[5] or ""),
                "link_resume": str(row[6] or "") if row[6] is not None else None,
                "slug": str(row[7]),
            }
        )
    return rows


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).resolve()
    out_root = Path(args.out_root).resolve()
    
    if not coordinator_enabled():
        log("ERROR: YT_PROVIDER_COORDINATOR_URL belum diatur. Script ini membutuhkan coordinator server.")
        return 1
    
    model_override = str(args.model or "").strip()
    providers = [str(x).strip() for x in (args.provider or []) if str(x).strip()]
    provider_account_ids = list(args.provider_account_ids or [])
    
    if not providers:
        log("ERROR: --provider wajib diisi (mis: --provider nvidia --provider groq)")
        return 1
    
    if not model_override:
        log("ERROR: --model wajib diisi (mis: --model openai/gpt-oss-120b)")
        return 1
    
    # Untuk sementara, kita gunakan provider pertama untuk semua requests
    # Dalam implementasi lanjut, bisa dibuat round-robin antar providers
    primary_provider = providers[0]
    
    holder = socket.gethostname()
    pid = os.getpid()
    lease_ttl_seconds = int(args.lease_ttl_seconds)
    
    log(f"Using coordinator: {coordinator_enabled()}")
    log(f"Primary provider: {primary_provider}")
    log(f"Model: {model_override}")
    
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if args.tasks_csv:
            raw_rows = rows_from_tasks_csv(con, args.tasks_csv)
        else:
            raw_rows = list(
                iter_rows(
                    con,
                    channels=args.channel,
                    missing_only=(not args.force),
                    limit=int(args.limit or 0),
                )
            )
        if args.limit > 0:
            raw_rows = raw_rows[: int(args.limit)]

        log(f"target rows: {len(raw_rows)}")
        done = 0
        current_lease: Optional[LeaseHeartbeat] = None

        for raw in raw_rows:
            if isinstance(raw, dict):
                row_id = int(raw["id"])
                slug = str(raw["slug"])
                video_id = str(raw["video_id"])
                title = str(raw["title"] or "")
                seq_num = raw["seq_num"]
                link_file = str(raw["link_file"] or "")
                link_resume = str(raw["link_resume"] or "") if raw.get("link_resume") else None
            else:
                row_id = int(raw.id)
                slug = str(raw.slug)
                video_id = str(raw.video_id)
                title = str(raw.title or "")
                seq_num = raw.seq_num
                link_file = str(raw.link_file or "")
                link_resume = str(raw.link_resume or "") if raw.link_resume else None

            base = out_root / slug
            if not link_file:
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", "missing_link_file", "", "", "", ""],
                )
                continue

            transcript = read_link_text_under(base, link_file)
            if transcript is None:
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", "transcript_not_found", "", "", link_file, ""],
                )
                continue

            transcript = clean_transcript(transcript)
            missing, reason = transcript_is_missing(transcript)
            if missing:
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", reason, "", "", link_file, ""],
                )
                continue

            resume_rel = default_resume_link(video_id, seq_num, link_file)
            if not resume_rel:
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", "invalid_resume_link", "", "", link_file, ""],
                )
                continue

            resume_path = resolve_under(base, resume_rel)
            if resume_path is None:
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", "invalid_resume_path", "", "", link_file, resume_rel],
                )
                continue

            if (not args.force) and link_resume and resume_path.exists():
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", "resume_exists", "", "", link_file, resume_rel],
                )
                continue

            prompt = build_prompt(title or video_id, transcript, max_chars=int(args.max_chars))
            success = False
            terminal_reported = False
            last_reason = ""
            last_account_label = ""
            last_model = ""

            # Acquire account from coordinator for this task
            try:
                log(f"[ACQUIRE] Requesting account from coordinator for {slug} | {video_id}")
                account = acquire_account_from_coordinator(
                    provider=primary_provider,
                    model_name=model_override,
                    eligible_account_ids=provider_account_ids if provider_account_ids else None,
                    holder=holder,
                    host="",
                    pid=pid,
                    task_type="resume_generation",
                    lease_ttl_seconds=lease_ttl_seconds,
                )
                log(f"[ACQUIRED] {account.label} | {account.model_name} | token={account.lease_token[:8]}...")
            except Exception as exc:
                log(f"[ACQUIRE FAILED] {exc}")
                append_report(
                    args.report_csv,
                    [slug, video_id, "skip", f"acquire_failed: {exc}", "", "", link_file, resume_rel],
                )
                continue

            # Start heartbeat
            current_lease = LeaseHeartbeat(account.lease_token, lease_ttl_seconds)
            current_lease.start()

            for attempt in range(1, int(args.max_attempts) + 1):
                last_account_label = account.label
                last_model = account.model_name
                try:
                    log(f"[RESUME] {slug} | {video_id} | attempt {attempt}/{args.max_attempts} | {account.label} | {account.model_name}")
                    result = chat_complete(
                        account,
                        prompt,
                        max_tokens=int(args.max_tokens),
                        temperature=float(args.temperature),
                        timeout=int(args.timeout),
                    )
                    if looks_like_assistant_reply(result):
                        raise RuntimeError("assistant_reply")
                    if len(result.strip()) < 400:
                        raise RuntimeError("weak_short_output")
                    if looks_too_generic_resume(result):
                        rewrite_prompt = build_rewrite_prompt(
                            title or video_id,
                            transcript,
                            result,
                            max_chars=int(args.max_chars),
                        )
                        result = chat_complete(
                            account,
                            rewrite_prompt,
                            max_tokens=int(args.max_tokens),
                            temperature=float(args.temperature),
                            timeout=int(args.timeout),
                        )
                        if looks_like_assistant_reply(result):
                            raise RuntimeError("assistant_reply_after_rewrite")
                        if looks_too_generic_resume(result):
                            raise RuntimeError("generic_resume_after_rewrite")

                    md = result.strip() + "\n"
                    resume_path.parent.mkdir(parents=True, exist_ok=True)
                    resume_path.write_text(md, encoding="utf-8")
                    with con:
                        con.execute("UPDATE videos SET link_resume=? WHERE id=?", (resume_rel, row_id))
                    append_report(
                        args.report_csv,
                        [slug, video_id, "ok", "", account.label, account.model_name, link_file, resume_rel],
                    )
                    done += 1
                    success = True
                    break
                except Exception as exc:  # noqa: BLE001
                    last_reason = str(exc).strip() or exc.__class__.__name__
                decision: dict[str, object] = {}
                try:
                    report_resp = coordinator_report_provider_event(
                        provider_account_id=account.id,
                        provider=account.provider,
                        model_name=account.model_name,
                        reason=last_reason,
                        source="fill_missing_resumes_simple_coordinator",
                        payload={
                            "slug": slug,
                            "video_id": video_id,
                            "attempt": attempt,
                            "task_type": "resume_generation",
                        },
                    )
                    if isinstance(report_resp.get("decision"), dict):
                        decision = dict(report_resp.get("decision") or {})
                except Exception as report_exc:  # noqa: BLE001
                    log(f"[WARN] gagal report provider event ke coordinator: {report_exc}")

                action = str(decision.get("action") or "")
                if action == "disabled":
                    current_lease.stop(final_state="disabled", note=last_reason[:500])
                    current_lease = None
                    append_report(
                        args.report_csv,
                        [
                            slug,
                            video_id,
                            "disabled_auth",
                            last_reason[:500],
                            account.label,
                            account.model_name,
                            link_file,
                            resume_rel,
                        ],
                    )
                    terminal_reported = True
                    break

                if action == "blocked":
                    current_lease.stop(final_state="blocked", note=last_reason[:500])
                    current_lease = None
                    append_report(
                        args.report_csv,
                        [
                            slug,
                            video_id,
                            "blocked_provider_quota",
                            last_reason[:500],
                            account.label,
                            account.model_name,
                            link_file,
                            resume_rel,
                        ],
                    )
                    terminal_reported = True
                    break

                if action == "retry" or is_transient_provider_limit_error(last_reason, provider=account.provider):
                    append_report(
                        args.report_csv,
                        [slug, video_id, "retry", last_reason[:500], account.label, account.model_name, link_file, resume_rel],
                    )
                    sleep_s = min(8 * attempt, 30)
                    time.sleep(sleep_s)

            # Release lease
            if current_lease:
                current_lease.stop(final_state="idle" if success else "error", note=last_reason[:500] if not success else "")
                current_lease = None

            if not success:
                if terminal_reported:
                    continue
                append_report(
                    args.report_csv,
                    [
                        slug,
                        video_id,
                        "failed",
                        last_reason[:500],
                        last_account_label,
                        last_model,
                        link_file,
                        resume_rel,
                    ],
                )

        log(f"done={done}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
