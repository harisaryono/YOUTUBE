#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from openai import OpenAI

try:
    from groq import Groq
except Exception:
    Groq = None

from local_services import (
    coordinator_acquire_accounts,
    coordinator_heartbeat_lease,
    coordinator_release_lease,
    coordinator_report_provider_event,
    coordinator_status_accounts,
    is_provider_blocking_enabled,
    is_transient_provider_limit_error,
)


LEASE_TTL_SECONDS = 300
RUNS_DIR = Path("runs")

PROMPT_TEMPLATE = """Anda bertugas memformat transcript YouTube mentah agar jauh lebih mudah dibaca dan dipahami, TANPA merangkum isi.

Tujuan:
- pertahankan isi pembahasan selengkap mungkin
- rapikan tanda baca, kapitalisasi, pemenggalan kalimat, dan paragraf
- kelompokkan pembahasan dengan heading seperlunya agar alur lebih jelas
- jika ada kalimat yang jelas patah karena hasil transcript, sambungkan seperlunya

Aturan ketat:
- jangan merangkum
- jangan menghilangkan detail penting
- jangan menambahkan fakta baru
- jangan mengubah makna pembicara
- jika ada istilah yang tidak jelas, pertahankan semirip mungkin dengan sumber
- abaikan header teknis seperti "Kind:" atau "Language:" jika ada
- anggap transcript yang diberikan SUDAH final; jangan pernah meminta "full transcript", file lain, URL lain, atau konteks tambahan
- jangan menulis komentar asisten, disclaimer, permintaan maaf, atau instruksi kepada pengguna
- jika transcript sangat pendek, terpotong, atau didominasi penanda seperti "[Music]" / "[Applause]", tetap kembalikan versi markdown terbaik dari isi yang ada
- jika isinya hanya fragmen, fokus pada pembersihan, pengelompokan seperlunya, dan keterbacaan; jangan mengarang isi yang tidak ada
- output hanya hasil akhir dalam Markdown, tanpa komentar tambahan
"""

STRICT_RETRY_PROMPT_TEMPLATE = """Tugas Anda hanya satu: keluarkan transcript yang sudah diformat ulang agar lebih mudah dibaca.

Aturan keras:
- JANGAN membalas sebagai asisten.
- JANGAN meminta transcript lengkap, file lain, URL, atau konteks tambahan.
- JANGAN menulis komentar seperti "silakan kirim", "I notice", "please provide", "maaf", atau penjelasan apa pun.
- JANGAN merangkum.
- JANGAN menambah fakta baru.
- JANGAN mengubah makna pembicara.
- Pertahankan isi sedekat mungkin dengan sumber.
- Rapikan hanya tanda baca, kapitalisasi, paragraf, dan heading.
- Output HARUS langsung berupa Markdown hasil akhir saja.
"""


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
        self._thread = threading.Thread(target=self._run, name="format-lease-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try:
                coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
            except Exception:
                pass

    def stop(self, *, final_state: str = "idle", note: str = "") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if not self.lease_token:
            return
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception:
            pass


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Bandingkan kualitas transcript formatting antar provider/model via coordinator.")
    ap.add_argument("--db", default="youtube_transcripts.db")
    ap.add_argument("--video-id", action="append", default=[])
    ap.add_argument("--provider-model", action="append", default=[], help="Format: provider:model_name")
    ap.add_argument("--limit", type=int, default=2)
    ap.add_argument("--min-words", type=int, default=1500)
    ap.add_argument("--max-words", type=int, default=3500)
    ap.add_argument("--chunk-chars", type=int, default=12000)
    ap.add_argument("--max-tokens", type=int, default=3200)
    ap.add_argument("--retry-max-tokens", type=int, default=4200)
    ap.add_argument("--run-dir", default="")
    return ap.parse_args()


def run_dir_from_args(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(args.run_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return RUNS_DIR / f"format_compare_{stamp}"


def parse_provider_models(raw_items: list[str]) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for raw in raw_items:
        item = str(raw or "").strip()
        if not item or ":" not in item:
            continue
        provider, model_name = item.split(":", 1)
        provider = str(provider or "").strip()
        model_name = str(model_name or "").strip()
        if not provider or not model_name:
            continue
        items.append((provider, model_name))
    return items


def pick_rows(con: sqlite3.Connection, *, video_ids: list[str], limit: int, min_words: int, max_words: int) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    if video_ids:
        placeholders = ",".join("?" for _ in video_ids)
        sql = f"""
            SELECT v.video_id, v.title, v.word_count, v.transcript_file_path, c.channel_name
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE v.video_id IN ({placeholders})
              AND v.transcript_downloaded = 1
              AND COALESCE(v.transcript_file_path, '') <> ''
            ORDER BY v.video_id
        """
        return list(con.execute(sql, video_ids).fetchall())
    sql = """
        SELECT v.video_id, v.title, v.word_count, v.transcript_file_path, c.channel_name
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE v.transcript_downloaded = 1
          AND COALESCE(v.transcript_file_path, '') <> ''
          AND v.word_count BETWEEN ? AND ?
        ORDER BY v.updated_at DESC
        LIMIT ?
    """
    return list(con.execute(sql, (int(min_words), int(max_words), int(limit))).fetchall())


def sanitize_raw_transcript(text: str) -> str:
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.splitlines()
    out: list[str] = []
    for i, line in enumerate(lines):
        s = line.strip()
        if i < 4 and (s.startswith("Kind:") or s.startswith("Language:")):
            continue
        out.append(line.rstrip())
    cleaned = "\n".join(out).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned


def transcript_signal_stats(text: str) -> dict[str, int]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    cue_lines = 0
    word_lines = 0
    words = 0
    alpha_words = 0
    short_lines = 0
    very_short_lines = 0
    for ln in lines:
        if re.fullmatch(r"\[[^\]]+\]", ln):
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


def is_low_content_transcript(text: str) -> bool:
    s = transcript_signal_stats(text)
    if s["alpha_words"] <= 8:
        return True
    if s["lines"] > 0 and s["cue_lines"] >= max(1, s["lines"] - 2):
        return True
    if s["word_lines"] <= 2 and s["alpha_words"] <= 16:
        return True
    return False


def strip_code_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t)
    return t.strip()


def is_formatted_acceptable(raw_text: str, formatted_text: str) -> tuple[bool, str]:
    raw_compact = re.sub(r"\s+", " ", raw_text or "").strip()
    fmt_compact = re.sub(r"\s+", " ", formatted_text or "").strip()
    if not fmt_compact:
        return False, "empty_output"
    if "<think>" in (formatted_text or "").lower() or "</think>" in (formatted_text or "").lower():
        return False, "reasoning_leak"
    low_content = is_low_content_transcript(raw_text)
    min_len = 40 if low_content else max(160, min(1200, len(raw_compact) // 3))
    if len(fmt_compact) < min_len:
        return False, "too_short"
    low = fmt_compact.lower()
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
    ]
    for needle in banned:
        if needle in low[:600]:
            return False, "assistant_reply"
    if re.search(r"(?im)^(you can:|1\.\s+paste the complete transcript|2\.\s+provide the file path|3\.\s+share a youtube url)", formatted_text or ""):
        return False, "assistant_reply"
    if re.search(r"(?im)^(maaf[, ]|silakan kirim|tolong kirim|harap kirim|please provide|could you provide)", formatted_text or ""):
        return False, "assistant_reply"
    return True, "ok"


def split_text_chunks(text: str, max_chars: int) -> list[str]:
    if max_chars <= 0 or len(text) <= max_chars:
        return [text]
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for para in paras:
        para_len = len(para) + (2 if cur else 0)
        if cur and (cur_len + para_len) > max_chars:
            chunks.append("\n\n".join(cur).strip())
            cur = [para]
            cur_len = len(para)
            continue
        cur.append(para)
        cur_len += para_len
    if cur:
        chunks.append("\n\n".join(cur).strip())
    out: list[str] = []
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


def adaptive_format_settings(model_limits: dict[str, object] | None, *, default_chunk_chars: int, default_max_tokens: int, default_retry_max_tokens: int) -> dict[str, int]:
    raw = dict(model_limits or {})
    chars_per_token = float(raw.get("chars_per_token") or 4.0)
    prompt_tokens = int(raw.get("recommended_prompt_tokens") or 0)
    completion_tokens = int(raw.get("recommended_completion_tokens") or 0)
    max_output_tokens = int(raw.get("max_output_tokens") or 0)

    chunk_chars = int(prompt_tokens * chars_per_token * 0.78) if prompt_tokens > 0 else int(default_chunk_chars)
    chunk_chars = max(3000, min(chunk_chars, 32000))
    max_tokens = int(completion_tokens or default_max_tokens)
    retry_max_tokens = max(int(default_retry_max_tokens), int(max_tokens * 1.5))
    if max_output_tokens > 0:
        max_tokens = min(max_tokens, max_output_tokens)
        retry_max_tokens = min(retry_max_tokens, max_output_tokens)
    return {
        "chunk_chars": int(chunk_chars),
        "max_tokens": max(700, int(max_tokens)),
        "retry_max_tokens": max(900, int(retry_max_tokens)),
    }


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
        return "".join(parts).strip()
    return str(content or "").strip()


def acquire_provider_account(*, provider: str, model_name: str, holder: str, pid: int) -> ProviderAccountLease:
    leases = coordinator_acquire_accounts(
        provider=provider,
        model_name=model_name,
        count=1,
        holder=holder,
        pid=pid,
        task_type="transcript_format_compare",
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
        lease_token=str(lease.get("lease_token") or "").strip(),
        model_limits=dict(lease.get("model_limits") or {}),
    )


def provider_chat_once(account: ProviderAccountLease, prompt: str, *, max_tokens: int) -> tuple[str, str, int, int]:
    if account.provider == "groq" and account.usage_method == "groq_sdk" and Groq is not None:
        client = Groq(api_key=account.api_key, timeout=600)
        kwargs = {
            "model": account.model_name,
            "messages": [
                {"role": "system", "content": "Keluarkan hanya hasil akhir transcript yang sudah diformat."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_completion_tokens": int(max_tokens),
            "top_p": 1,
            "stream": False,
            "stop": None,
        }
        model_lower = str(account.model_name or "").strip().lower()
        if "gpt-oss" in model_lower:
            kwargs["reasoning_effort"] = "medium"
        elif "qwen3" in model_lower:
            kwargs["reasoning_effort"] = "default"
        resp = client.chat.completions.create(**kwargs)
        choice = resp.choices[0]
        message = choice.message
        content = extract_message_content(message)
        finish_reason = str(getattr(choice, "finish_reason", "") or "").strip()
        completion_tokens = int(getattr(getattr(resp, "usage", None), "completion_tokens", 0) or 0)
        reasoning_len = len(str(getattr(message, "reasoning", "") or getattr(message, "reasoning_content", "") or ""))
        return content, finish_reason, completion_tokens, reasoning_len
    if account.provider != "groq" and account.endpoint_url.strip():
        base_url = account.endpoint_url.strip()
        if base_url.endswith("/chat/completions"):
            base_url = base_url[:-len("/chat/completions")]
        client = OpenAI(
            api_key=account.api_key,
            base_url=base_url,
            timeout=600,
            default_headers=account.extra_headers,
        )
        kwargs = {
            "model": account.model_name,
            "messages": [
                {"role": "system", "content": "Keluarkan hanya hasil akhir transcript yang sudah diformat."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": int(max_tokens),
        }
        if str(account.provider or "").strip().lower() == "z.ai":
            kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
        resp = client.chat.completions.create(**kwargs)
        choice = resp.choices[0]
        message = choice.message
        content = extract_message_content(message)
        finish_reason = str(getattr(choice, "finish_reason", "") or "").strip()
        completion_tokens = int(getattr(getattr(resp, "usage", None), "completion_tokens", 0) or 0)
        reasoning_len = len(str(getattr(message, "reasoning", "") or getattr(message, "reasoning_content", "") or ""))
        return content, finish_reason, completion_tokens, reasoning_len
    payload = {
        "model": account.model_name,
        "messages": [
            {"role": "system", "content": "Keluarkan hanya hasil akhir transcript yang sudah diformat."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
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

    choice = (body.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content = extract_message_content(message)
    finish_reason = str(choice.get("finish_reason") or "").strip()
    completion_tokens = int((body.get("usage") or {}).get("completion_tokens") or 0)
    reasoning_len = len(str(message.get("reasoning") or message.get("reasoning_content") or ""))
    return content, finish_reason, completion_tokens, reasoning_len


def provider_chat_complete(account: ProviderAccountLease, prompt: str, *, max_tokens: int, retry_max_tokens: int) -> str:
    last_error = "empty response"
    for budget in (int(max_tokens), int(retry_max_tokens)):
        content, finish_reason, completion_tokens, reasoning_len = provider_chat_once(
            account,
            prompt,
            max_tokens=budget,
        )
        content = strip_code_fences(content)
        if content:
            return content.rstrip() + "\n"
        if finish_reason == "length":
            last_error = (
                "response_truncated_in_reasoning"
                f" max_tokens={budget}"
                f" completion_tokens={completion_tokens}"
                f" reasoning_len={reasoning_len}"
            )
            continue
        last_error = (
            "empty response"
            f" finish_reason={finish_reason or '-'}"
            f" completion_tokens={completion_tokens}"
            f" reasoning_len={reasoning_len}"
        )
        break
    raise RuntimeError(last_error)


def format_text(account: ProviderAccountLease, text: str, *, chunk_chars: int, max_tokens: int, retry_max_tokens: int, strict_retry: bool = False) -> str:
    prompt_template = STRICT_RETRY_PROMPT_TEMPLATE if strict_retry else PROMPT_TEMPLATE
    if chunk_chars > 0 and len(text) > chunk_chars:
        parts = split_text_chunks(text, chunk_chars)
        rendered: list[str] = []
        total = len(parts)
        for idx, part in enumerate(parts, start=1):
            chunk_prompt = (
                prompt_template
                + f"\n\nIni hanya bagian {idx}/{total} dari transcript penuh.\n"
                  "- Formatkan bagian ini agar lebih mudah dibaca.\n"
                  "- Jangan menambahkan pembuka atau penutup global.\n"
                  "- Fokus pada pemenggalan kalimat, paragraf, dan heading lokal seperlunya.\n\n"
                  f"Transkrip:\n{part}"
            )
            rendered.append(
                provider_chat_complete(
                    account,
                    chunk_prompt,
                    max_tokens=max_tokens,
                    retry_max_tokens=retry_max_tokens,
                ).strip()
            )
        return "\n\n".join(x for x in rendered if x).strip() + "\n"
    return provider_chat_complete(
        account,
        prompt_template + f"\n\nTranskrip:\n{text}",
        max_tokens=max_tokens,
        retry_max_tokens=retry_max_tokens,
    )


def preflight_provider_model(provider: str, model_name: str) -> int:
    items = coordinator_status_accounts(provider=provider, model_name=model_name)
    available = 0
    for item in items:
        leaseable = item.get("leaseable")
        if leaseable is None:
            state = str(item.get("state") or "idle").strip().lower()
            is_active = int(item.get("is_active") or 0) == 1
            if is_active and state == "idle":
                available += 1
            continue
        if bool(leaseable):
            available += 1
    return available


def write_report_header(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "video_id",
            "channel_name",
            "provider",
            "model_name",
            "status",
            "acceptable",
            "reason",
            "output_path",
            "output_chars",
            "heading_count",
            "timestamp_markers",
            "lease_account",
            "elapsed_sec",
        ])


def append_report(path: Path, row: list[object]) -> None:
    with path.open("a", encoding="utf-8", newline="") as handle:
        csv.writer(handle).writerow(row)


def main() -> int:
    args = parse_args()
    run_dir = run_dir_from_args(args)
    run_dir.mkdir(parents=True, exist_ok=True)
    report_csv = run_dir / "report.csv"
    write_report_header(report_csv)

    providers = parse_provider_models(args.provider_model) or [
        ("nvidia", "openai/gpt-oss-120b"),
        ("z.ai", "glm-4.7"),
    ]
    preflight: dict[str, int] = {}
    for provider, model_name in providers:
        available = preflight_provider_model(provider, model_name)
        preflight[f"{provider}:{model_name}"] = available
        log(f"[PREFLIGHT] {provider}/{model_name} idle={available}")

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db": str(Path(args.db).resolve()),
        "providers": providers,
        "preflight": preflight,
        "limit": int(args.limit),
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    con = sqlite3.connect(args.db)
    rows = pick_rows(
        con,
        video_ids=[str(v).strip() for v in args.video_id if str(v).strip()],
        limit=args.limit,
        min_words=args.min_words,
        max_words=args.max_words,
    )
    if not rows:
        raise SystemExit("Tidak ada transcript sample yang cocok.")

    summary_lines = ["# Format Compare", ""]
    for row in rows:
        video_id = str(row["video_id"])
        channel_name = str(row["channel_name"])
        title = str(row["title"] or "")
        transcript_path = Path(str(row["transcript_file_path"]))
        if not transcript_path.is_absolute():
            transcript_path = Path(__file__).resolve().parents[1] / transcript_path
        raw_text = transcript_path.read_text(encoding="utf-8")
        source_text = sanitize_raw_transcript(raw_text)
        summary_lines.append(f"## {video_id} | {channel_name}")
        summary_lines.append(f"- title: {title}")
        summary_lines.append(f"- transcript_path: {transcript_path}")
        summary_lines.append(f"- source_chars: {len(source_text)}")
        for provider, model_name in providers:
            available = preflight.get(f"{provider}:{model_name}", 0)
            if available <= 0:
                append_report(
                    report_csv,
                    [video_id, channel_name, provider, model_name, "skip_no_idle_account", 0, "preflight_no_idle_account", "", 0, 0, 0, "", 0],
                )
                summary_lines.append(f"- {provider}/{model_name}: skipped (no idle account)")
                continue

            holder = f"format-compare:{video_id}:{provider}"
            lease: Optional[ProviderAccountLease] = None
            heartbeat: Optional[LeaseHeartbeat] = None
            start = time.time()
            try:
                lease = acquire_provider_account(provider=provider, model_name=model_name, holder=holder, pid=0)
                heartbeat = LeaseHeartbeat(lease.lease_token, LEASE_TTL_SECONDS)
                heartbeat.start()
                log(f"[FORMAT] {video_id} {lease.label} | {lease.model_name}")
                settings = adaptive_format_settings(
                    lease.model_limits,
                    default_chunk_chars=args.chunk_chars,
                    default_max_tokens=args.max_tokens,
                    default_retry_max_tokens=args.retry_max_tokens,
                )
                out = format_text(
                    lease,
                    source_text,
                    chunk_chars=settings["chunk_chars"],
                    max_tokens=settings["max_tokens"],
                    retry_max_tokens=settings["retry_max_tokens"],
                )
                ok, reason = is_formatted_acceptable(source_text, out)
                if (not ok) and reason == "assistant_reply":
                    out = format_text(
                        lease,
                        source_text,
                        chunk_chars=settings["chunk_chars"],
                        max_tokens=settings["max_tokens"],
                        retry_max_tokens=settings["retry_max_tokens"],
                        strict_retry=True,
                    )
                    ok, reason = is_formatted_acceptable(source_text, out)
                output_name = f"{video_id}__{provider.replace('.', '_')}__{model_name.replace('/', '__')}.md"
                output_path = run_dir / output_name
                output_path.write_text(out, encoding="utf-8")
                elapsed = round(time.time() - start, 2)
                append_report(
                    report_csv,
                    [
                        video_id,
                        channel_name,
                        provider,
                        model_name,
                        "ok" if ok else "invalid_output",
                        1 if ok else 0,
                        reason,
                        str(output_path.resolve()),
                        len(out),
                        len(re.findall(r"(?m)^#{1,6}\s", out)),
                        len(re.findall(r"\[\d{2}:\d{2}", out)),
                        lease.account_name,
                        elapsed,
                    ],
                )
                summary_lines.append(
                    f"- {provider}/{model_name}: {'OK' if ok else 'INVALID'} | reason={reason} | chars={len(out)} | account={lease.account_name}"
                )
                if heartbeat is not None:
                    heartbeat.stop(final_state="idle", note=f"formatted={video_id}")
                    heartbeat = None
            except Exception as exc:
                reason = str(exc)
                elapsed = round(time.time() - start, 2)
                append_report(
                    report_csv,
                    [
                        video_id,
                        channel_name,
                        provider,
                        model_name,
                        "error",
                        0,
                        reason,
                        "",
                        0,
                        0,
                        0,
                        lease.account_name if lease else "",
                        elapsed,
                    ],
                )
                if lease is not None:
                    try:
                        coordinator_report_provider_event(
                            provider_account_id=lease.id,
                            provider=lease.provider,
                            model_name=lease.model_name,
                            reason=reason,
                            source="compare_format_models",
                            http_status=429 if "429" in reason else 0,
                            error_code="rate_limit_exceeded" if (
                                is_provider_blocking_enabled(lease.provider)
                                and not is_transient_provider_limit_error(reason, provider=lease.provider)
                            ) else "",
                        )
                    except Exception:
                        pass
                summary_lines.append(f"- {provider}/{model_name}: ERROR | {reason}")
                if heartbeat is not None:
                    heartbeat.stop(final_state="idle", note=reason[:200])
                    heartbeat = None
            finally:
                if heartbeat is not None:
                    heartbeat.stop(final_state="idle")

        summary_lines.append("")

    (run_dir / "summary.md").write_text("\n".join(summary_lines).strip() + "\n", encoding="utf-8")
    log(f"Run dir: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
