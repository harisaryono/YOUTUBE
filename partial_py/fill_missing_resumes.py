#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import socket
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from dotenv import load_dotenv
import shard_storage

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency guard
    OpenAI = None  # type: ignore[assignment]


PROMPT_TEMPLATE = """Bertindaklah sebagai editor profesional dan ahli dalam membuat rangkuman konten digital. Tugas Anda adalah membuat resume yang sangat komprehensif, terstruktur, dan mudah dipahami dalam Bahasa Indonesia berdasarkan transkrip YouTube yang saya berikan di bawah ini.

Ikuti panduan berikut:
1.  **Judul yang Menarik**: Buatlah judul ringkasan yang relevan dengan konten video.
2.  **Inti Sari (Executive Summary)**: Tulislah paragraf pembuka (2-3 kalimat) yang menjelaskan topik utama video secara garis besar.
3.  **Poin-Poin Kunci (Key Takeaways)**: Buatlah daftar bullet point untuk poin-poin terpenting yang dibicarakan.
4.  **Rincian Materi (Detailed Breakdown)**: Uraikan isi video secara runut (kronologis) atau berdasarkan sub-topik. Gunakan sub-judul (Heading 2 atau **Bold**) untuk memisahkan setiap segmen pembahasan agar mudah dibaca. Pastikan penjelasannya padat namun tetap lengkap (tidak menghilangkan detail penting).
5.  **Kesimpulan & Pesan Penutup**: Ringkaslah kesimpulan akhir atau ajakan (call to action) yang disampaikan di akhir video.
6.  **Format Output**: Gunakan formatting Markdown yang rapi (seperti bold, italic, dan bullet list) agar nyaman dibaca. Gunakan Bahasa Indonesia yang baku, natural, dan profesional.

Berikut adalah transkripnya:
{transcript}
"""

AGENT_NAME = ""


@dataclass(frozen=True)
class VideoRow:
    id: int
    channel_id: int
    slug: str
    video_id: str
    title: str
    seq_num: Optional[int]
    link_file: str
    link_resume: Optional[str]


def load_env() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    if os.getenv("NVIDIA_API_KEY") or os.getenv("OPENAI_API_KEY"):
        return
    keys_csv = (os.getenv("NVIDIA_API_KEYS") or "").strip()
    if keys_csv:
        for part in keys_csv.split(","):
            key = part.strip()
            if key:
                os.environ["NVIDIA_API_KEY"] = key
                return
    idx = 1
    while True:
        val = os.getenv(f"NVIDIA_API_KEY_{idx}")
        if val is None:
            break
        key = val.strip()
        if key:
            os.environ["NVIDIA_API_KEY"] = key
            return
        idx += 1


def require_openai_client(base_url: str, api_key: str, timeout_s: int) -> "OpenAI":
    if OpenAI is None:
        raise RuntimeError("Paket 'openai' belum terpasang. Jalankan: pip install openai")
    return OpenAI(base_url=base_url, api_key=api_key, max_retries=0, timeout=timeout_s)


def resolve_under(base: Path, rel: str) -> Optional[Path]:
    if not rel:
        return None
    p = (base / Path(rel)).resolve()
    try:
        p.relative_to(base.resolve())
    except Exception:
        return None
    return p


def link_exists_under(base: Path, rel: Optional[str]) -> bool:
    return shard_storage.link_exists(base, rel)


def link_size_under(base: Path, rel: Optional[str]) -> Optional[int]:
    return shard_storage.link_size(base, rel)


def read_link_text_under(base: Path, rel: Optional[str], *, retries: int = 1, sleep_s: float = 0.15) -> Optional[str]:
    tries = max(0, int(retries)) + 1
    for i in range(tries):
        txt = shard_storage.read_link_text(base, rel)
        if txt is not None:
            return txt
        if not shard_storage.link_exists(base, rel):
            return None
        if i + 1 < tries:
            time.sleep(max(0.0, float(sleep_s)))
    return None


def shard_read_runtime_mode() -> str:
    has_module = getattr(shard_storage, "zstd", None) is not None
    has_bridge = False
    checker = getattr(shard_storage, "_check_system_zstd_bridge", None)
    if callable(checker):
        try:
            has_bridge = bool(checker())
        except Exception:
            has_bridge = False
    if has_module:
        return "module"
    if has_bridge:
        return "bridge"
    return "unavailable"


def parse_seq_from_link_file(link_file: Optional[str]) -> Optional[int]:
    if not link_file:
        return None
    name = Path(str(link_file)).name
    m = re.match(r"^(\d+)_", name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def default_resume_link(video_id: str, seq_num: Optional[int], link_file: Optional[str]) -> Optional[str]:
    seq = seq_num if seq_num else parse_seq_from_link_file(link_file)
    if not seq:
        return None
    return str(Path("resume") / f"{int(seq):04d}_{video_id}.md")


def iter_rows(
    con: sqlite3.Connection, *, channels: Optional[list[str]], missing_only: bool, limit: int
) -> Iterable[VideoRow]:
    where = ["v.link_file IS NOT NULL AND v.link_file != ''"]
    params: list[object] = []

    if missing_only:
        where.append("(v.link_resume IS NULL OR v.link_resume = '')")

    if channels:
        placeholders = ",".join("?" for _ in channels)
        where.append(f"c.slug IN ({placeholders})")
        params.extend(channels)

    sql = f"""
        SELECT v.id, v.channel_id, v.video_id, v.title, v.seq_num, v.link_file, v.link_resume, c.slug
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE {' AND '.join(where)}
        ORDER BY c.slug ASC, v.seq_num DESC, v.id DESC
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
            seq_num=(int(row[4]) if row[4] is not None else None),
            link_file=str(row[5]),
            link_resume=(str(row[6]) if row[6] is not None else None),
            slug=str(row[7]),
        )


def build_prompt(transcript: str) -> str:
    return PROMPT_TEMPLATE.format(transcript=transcript.strip())


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    prefix = f"[{stamp}]"
    if AGENT_NAME:
        prefix = f"{prefix} [{AGENT_NAME}]"
    print(f"{prefix} {msg}")


def lock_path_for(resume_path: Path) -> Path:
    return resume_path.with_suffix(resume_path.suffix + ".lock")


def read_lock_info(lock_path: Path) -> str:
    try:
        return lock_path.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        return ""


def parse_lock_info(lock_path: Path) -> dict[str, str]:
    raw = read_lock_info(lock_path)
    info: dict[str, str] = {}
    if not raw:
        return info
    for line in raw.splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        if not key:
            continue
        info[key] = v.strip()
    return info


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True


def lock_should_be_removed(lock_path: Path, ttl_s: int) -> bool:
    try:
        st = lock_path.stat()
    except FileNotFoundError:
        return False
    except Exception:
        return False

    now = time.time()
    age = now - st.st_mtime
    if ttl_s > 0 and age > ttl_s:
        return True

    info = parse_lock_info(lock_path)
    host = (info.get("host") or "").strip()
    pid_raw = (info.get("pid") or "").strip()
    local_hosts = {socket.gethostname(), socket.getfqdn(), "localhost", "127.0.0.1"}
    if host and host in local_hosts and pid_raw.isdigit():
        if not pid_is_alive(int(pid_raw)):
            return True
    return False


def try_acquire_lock(lock_path: Path, *, agent: str, ttl_s: int) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            if lock_should_be_removed(lock_path, ttl_s):
                lock_path.unlink()
        except Exception:
            pass
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(lock_path), flags)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(f"agent={agent}\n")
        f.write(f"pid={os.getpid()}\n")
        f.write(f"host={socket.gethostname()}\n")
        f.write(f"started={datetime.now(timezone.utc).isoformat()}\n")
    return True


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return
    except Exception:
        return


def clean_stale_locks(root: Path, ttl_s: int) -> list[Path]:
    removed: list[Path] = []
    for lock_path in root.rglob("*.lock"):
        try:
            if lock_should_be_removed(lock_path, ttl_s):
                lock_path.unlink()
                removed.append(lock_path)
        except Exception:
            continue
    return removed


def get_busy_channel_ids(con: sqlite3.Connection) -> set[int]:
    raw = (os.getenv("RESUME_BLOCK_QUEUED_JOBS") or "").strip().lower()
    include_queued = raw in {"1", "true", "yes", "y", "on"}
    statuses = ["running", "stopping"]
    if include_queued:
        statuses.insert(0, "queued")
    placeholders = ",".join("?" for _ in statuses)
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT channel_id
            FROM jobs
            WHERE channel_id IS NOT NULL
              AND status IN ({placeholders})
            """,
            tuple(statuses),
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    out: set[int] = set()
    for r in rows:
        try:
            out.add(int(r[0]))
        except Exception:
            continue
    return out


def file_is_stable(path: Path, *, min_age_s: int = 15) -> bool:
    try:
        st = path.stat()
    except Exception:
        return False
    if st.st_size <= 0:
        return False
    age_s = time.time() - st.st_mtime
    return age_s >= max(0, int(min_age_s))


def chunk_text(text: str, max_chars: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    parts: list[str] = []
    paras = re.split(r"\n\s*\n", text)
    buf: list[str] = []
    size = 0
    for p in paras:
        p = p.strip()
        if not p:
            continue
        add_len = len(p) + (2 if buf else 0)
        if size + add_len <= max_chars:
            buf.append(p)
            size += add_len
            continue
        if buf:
            parts.append("\n\n".join(buf))
        if len(p) <= max_chars:
            buf = [p]
            size = len(p)
        else:
            for i in range(0, len(p), max_chars):
                parts.append(p[i : i + max_chars])
            buf = []
            size = 0
    if buf:
        parts.append("\n\n".join(buf))
    return parts


def build_chunk_prompt(chunk: str, idx: int, total: int) -> str:
    return (
        "Anda adalah analis konten profesional.\n"
        "Tugas: ringkas bagian transkrip berikut secara faktual, padat, dan menjaga detail penting.\n"
        "Aturan:\n"
        "- Gunakan HANYA informasi di potongan ini.\n"
        "- Jangan mengarang fakta/angka/nama yang tidak ada.\n"
        "- Gunakan Bahasa Indonesia.\n"
        "- Output berupa poin-poin ringkas (bullet list) dan/atau subjudul pendek bila perlu.\n"
        "\n"
        f"Bagian {idx}/{total}:\n"
        f"{chunk}"
    )


def build_final_prompt(chunk_summaries: list[str]) -> str:
    joined = "\n\n".join(f"### Bagian {i}\n{txt.strip()}" for i, txt in enumerate(chunk_summaries, start=1))
    transcript_block = (
        "Catatan: Transkrip terlalu panjang, berikut ini ringkasan per bagian yang dibuat dari transkrip asli. "
        "Gunakan hanya informasi di bawah.\n\n"
        f"{joined}"
    )
    return PROMPT_TEMPLATE.format(transcript=transcript_block.strip())


def build_conclusion_repair_prompt(summary_md: str) -> str:
    return (
        "Anda adalah editor Markdown Bahasa Indonesia.\n"
        "Tugas: revisi ringkasan berikut agar tetap faktual, rapi, dan WAJIB punya bagian penutup.\n"
        "Aturan ketat:\n"
        "- Jangan tambahkan fakta baru.\n"
        "- Pertahankan struktur, poin penting, dan gaya bahasa semaksimal mungkin.\n"
        "- Jika belum ada, tambahkan heading persis: '## Kesimpulan & Pesan Penutup'.\n"
        "- Di bawah heading tersebut, tulis 2-4 kalimat kesimpulan/ajakan yang merangkum isi.\n"
        "- Output hanya Markdown hasil final.\n"
        "\n"
        "Ringkasan saat ini:\n"
        f"{summary_md.strip()}"
    )


def sanitize_transcript(text: str) -> str:
    if not text:
        return text
    # Normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Remove NUL bytes
    text = text.replace("\x00", "")
    # Drop other ASCII control chars except \n and \t
    text = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    # Trim trailing spaces per line
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    return text.strip()


def split_chunk(text: str) -> list[str]:
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
        return [text.strip()]
    return [left, right]


def min_summary_chars(source_chars: int, *, final: bool) -> int:
    n = max(0, int(source_chars))
    if final:
        if n < 800:
            return 80
        if n < 2000:
            return 120
        return min(900, max(180, n // 35))
    if n < 2500:
        return 70
    return min(220, max(100, n // 70))


def is_substantive_summary(text: str, *, min_chars: int) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    compact = re.sub(r"\s+", " ", raw).strip()
    if len(compact) < max(1, int(min_chars)):
        return False
    low = compact.lower()
    if "mohon maaf" in low and ("tidak dapat" in low or "tidak bisa" in low):
        return False
    last_line = raw.splitlines()[-1].strip()
    if (
        0 < len(last_line) <= 24
        and not re.search(r"[.!?:;)\]]$", last_line)
        and not last_line.startswith(("-", "*", "#"))
    ):
        return False
    return True


def resume_has_expected_ending(text: str) -> bool:
    low = (text or "").lower()
    if not low:
        return False
    tail = low[-3500:]
    tokens = (
        "kesimpulan",
        "pesan penutup",
        "penutup",
        "call to action",
        "ajakan",
        "ringkasan akhir",
        "akhir kata",
        "pesan akhir",
    )
    if any(tok in tail for tok in tokens):
        return True
    if re.search(r"(?mi)^#{1,6}\s*(kesimpulan|penutup|akhir kata|ringkasan akhir)\b", low):
        return True
    if re.search(r"(?mi)^\*\*(kesimpulan|penutup|akhir kata|ringkasan akhir)\b.*\*\*$", low):
        return True
    return False


def is_resume_acceptable(text: str, *, transcript_chars: int) -> tuple[bool, str]:
    min_chars = min_summary_chars(transcript_chars, final=True)
    if not is_substantive_summary(text, min_chars=min_chars):
        return False, f"too_short_or_truncated(min={min_chars})"
    # For short transcripts, forcing an explicit conclusion heading often creates false negatives.
    # Keep quality gate on content length/substance, but relax closing-section requirement.
    conclusion_min_src_chars_raw = (os.getenv("RESUME_CONCLUSION_MIN_TRANSCRIPT_CHARS") or "").strip()
    try:
        conclusion_min_src_chars = int(conclusion_min_src_chars_raw) if conclusion_min_src_chars_raw else 2500
    except Exception:
        conclusion_min_src_chars = 2500
    conclusion_required = int(transcript_chars) >= max(0, conclusion_min_src_chars)
    if conclusion_required and not resume_has_expected_ending(text):
        return False, "missing_conclusion_section"
    return True, "ok"


def run_startup_preflight(
    con: sqlite3.Connection,
    *,
    out_root: Path,
    channels: Optional[list[str]],
    limit: int,
    relink_existing: bool,
    lock_ttl_s: int,
    agent: str,
    busy_channels: set[int],
    dry_run: bool,
) -> None:
    preflight_lock = out_root / ".resume_preflight.lock"
    if not try_acquire_lock(preflight_lock, agent=agent, ttl_s=lock_ttl_s):
        info = read_lock_info(preflight_lock)
        extra = f" ({info})" if info else ""
        log(f"[PRECHECK] skip: preflight lock active{extra}")
        return

    checked = 0
    missing_transcript = 0
    cleared_missing_file = 0
    cleared_invalid = 0
    relinked = 0
    skipped_busy = 0
    try:
        to_clear: list[tuple[int]] = []
        to_relink: list[tuple[str, int]] = []

        def _flush_updates() -> None:
            if dry_run:
                to_clear.clear()
                to_relink.clear()
                return
            if not to_clear and not to_relink:
                return
            with con:
                if to_clear:
                    con.executemany("UPDATE videos SET link_resume=NULL WHERE id=?", to_clear)
                if to_relink:
                    con.executemany("UPDATE videos SET link_resume=? WHERE id=?", to_relink)
            to_clear.clear()
            to_relink.clear()

        for r in iter_rows(con, channels=channels, missing_only=False, limit=limit):
            checked += 1
            if r.channel_id in busy_channels:
                skipped_busy += 1
                continue
            base = out_root / r.slug
            if not link_exists_under(base, r.link_file):
                missing_transcript += 1
                continue
            transcript_chars = int(link_size_under(base, r.link_file) or 0)

            if r.link_resume:
                if not link_exists_under(base, r.link_resume):
                    to_clear.append((r.id,))
                    cleared_missing_file += 1
                    if len(to_clear) >= 500:
                        _flush_updates()
                    continue
                resume_text = read_link_text_under(base, r.link_resume)
                if resume_text is None:
                    # Storage transient/unreadable; don't clear link on uncertain read.
                    continue
                ok, _ = is_resume_acceptable(resume_text, transcript_chars=transcript_chars)
                if not ok:
                    to_clear.append((r.id,))
                    cleared_invalid += 1
                    if len(to_clear) >= 500:
                        _flush_updates()
                continue

            if not relink_existing:
                continue
            default_rel = default_resume_link(r.video_id, r.seq_num, r.link_file)
            if not default_rel:
                continue
            if not link_exists_under(base, default_rel):
                continue
            resume_text = read_link_text_under(base, default_rel)
            if resume_text is None:
                continue
            ok, _ = is_resume_acceptable(resume_text, transcript_chars=transcript_chars)
            if ok:
                to_relink.append((default_rel, r.id))
                relinked += 1
                if len(to_relink) >= 500:
                    _flush_updates()

        _flush_updates()

        log(
            "[PRECHECK] "
            f"checked={checked} "
            f"missing_transcript={missing_transcript} "
            f"requeue_missing_resume_file={cleared_missing_file} "
            f"requeue_invalid_resume={cleared_invalid} "
            f"relinked={relinked} "
            f"busy_channel_rows={skipped_busy} "
            f"dry_run={int(bool(dry_run))}"
        )
    finally:
        release_lock(preflight_lock)


def call_chat(
    client: "OpenAI",
    *,
    model: str,
    prompt: str,
    max_tokens: int,
    temperature: float,
    top_p: float,
    thinking: bool,
    clear_thinking: bool,
    allow_reasoning_fallback: bool,
    min_chars: int,
    retries: int,
    backoff_s: float,
    stream: bool,
) -> str:
    def _is_rate_limited_error(exc: Exception) -> bool:
        s = str(exc).lower()
        return ("429" in s) or ("too many requests" in s) or ("rate limit" in s)

    def _is_connection_error(exc: Exception) -> bool:
        s = str(exc).lower()
        tokens = (
            "connection error",
            "connection reset",
            "connection aborted",
            "connection timed out",
            "name or service not known",
            "temporary failure in name resolution",
            "failed to establish a new connection",
            "max retries exceeded",
            "network is unreachable",
            "remote end closed connection",
        )
        return any(tok in s for tok in tokens)

    def _coerce_text(obj: object) -> str:
        if obj is None:
            return ""
        if isinstance(obj, str):
            return obj
        if isinstance(obj, list):
            parts: list[str] = []
            for item in obj:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict):
                    txt = item.get("text")
                    if txt is None:
                        txt = item.get("content")
                    if isinstance(txt, str):
                        parts.append(txt)
                    continue
                txt = getattr(item, "text", None)
                if isinstance(txt, str):
                    parts.append(txt)
            return "".join(parts)
        return str(obj)

    def _extra_body(enable_thinking: bool, clear: bool) -> Optional[dict]:
        if not enable_thinking:
            return None
        return {"chat_template_kwargs": {"enable_thinking": True, "clear_thinking": clear}}

    def _request_nonstream(extra: Optional[dict]) -> tuple[str, str, Optional[str]]:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            stream=False,
            extra_body=extra,
        )
        if not getattr(resp, "choices", None):
            raise RuntimeError("Response tanpa choices.")
        choice0 = resp.choices[0]
        content = _coerce_text(getattr(choice0.message, "content", None)).strip()
        reasoning = _coerce_text(getattr(choice0.message, "reasoning_content", None)).strip()
        finish_reason = getattr(choice0, "finish_reason", None)
        finish_s = str(finish_reason) if finish_reason is not None else None
        return content, reasoning, finish_s

    def _request_stream(extra: Optional[dict]) -> tuple[str, str, Optional[str]]:
        completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            stream=True,
            extra_body=extra,
        )
        parts: list[str] = []
        reasoning_parts: list[str] = []
        finish_reason: Optional[str] = None
        for chunk in completion:
            if not getattr(chunk, "choices", None):
                continue
            if len(chunk.choices) == 0:
                continue
            choice0 = chunk.choices[0]
            delta = getattr(choice0, "delta", None)
            if delta is None:
                continue
            content = _coerce_text(getattr(delta, "content", None))
            if content:
                parts.append(content)
            reasoning = _coerce_text(getattr(delta, "reasoning_content", None))
            if reasoning:
                reasoning_parts.append(reasoning)
            fr = getattr(choice0, "finish_reason", None)
            if fr is not None:
                finish_reason = str(fr)
        return "".join(parts).strip(), "".join(reasoning_parts).strip(), finish_reason

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            variants: list[tuple[bool, bool, bool, str]] = [(stream, thinking, clear_thinking, "primary")]
            if stream:
                variants.append((False, thinking, clear_thinking, "fallback non-stream"))
            if thinking:
                variants.append((False, False, False, "fallback no-thinking"))

            seen: set[tuple[bool, bool, bool]] = set()
            best_reasoning = ""
            for use_stream, use_thinking, use_clear, label in variants:
                key = (use_stream, use_thinking, use_clear)
                if key in seen:
                    continue
                seen.add(key)

                extra = _extra_body(use_thinking, use_clear)
                if use_stream:
                    content, reasoning, finish_reason = _request_stream(extra)
                else:
                    content, reasoning, finish_reason = _request_nonstream(extra)

                if content:
                    if is_substantive_summary(content, min_chars=min_chars):
                        return content.strip() + "\n"
                    log(
                        f"[WARN] {label} returned weak/short content "
                        f"(chars={len(content.strip())}, finish_reason={finish_reason}); trying fallback"
                    )

                if reasoning:
                    if is_substantive_summary(reasoning, min_chars=min_chars):
                        if len(reasoning) > len(best_reasoning):
                            best_reasoning = reasoning
                        if allow_reasoning_fallback:
                            log(
                                f"[WARN] {label} returned reasoning without content; "
                                "keep reasoning as candidate and trying fallback variants first."
                            )
                        else:
                            log(f"[WARN] {label} returned reasoning without content; trying fallback")
                    else:
                        log(
                            f"[WARN] {label} returned non-substantive reasoning "
                            f"(chars={len(reasoning.strip())}); trying fallback"
                        )
                else:
                    log(f"[WARN] {label} returned empty content (finish_reason={finish_reason}); trying fallback")

            if allow_reasoning_fallback and best_reasoning:
                log("[WARN] using reasoning fallback as final output to avoid empty response.")
                return best_reasoning.strip() + "\n"

            raise RuntimeError("Response kosong dari model setelah fallback content.")
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = backoff_s * (2**attempt)
            if _is_rate_limited_error(exc):
                # 429 usually needs longer cooldown than generic retry.
                sleep_s = max(sleep_s, min(45.0, 6.0 * (2**attempt)))
                log(f"[WARN] rate limited (429), retry in {sleep_s:.1f}s")
            elif _is_connection_error(exc):
                # Network/provider transient error: use longer cooldown + jitter-ish progression.
                sleep_s = max(sleep_s, min(75.0, 8.0 * (2**attempt)))
                log(f"[WARN] connection error, retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
    raise RuntimeError(f"Gagal request setelah {retries + 1} percobaan: {last_exc}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Generate resume untuk video yang belum punya resume, lalu isi link_resume di DB."
        )
    )
    p.add_argument("--db", default="channels.db", help="Path SQLite DB (default: channels.db)")
    p.add_argument("--out-root", default="out", help="Folder output per channel (default: out)")
    p.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Filter hanya slug channel tertentu (bisa diulang).",
    )
    p.add_argument("--limit", type=int, default=0, help="Batasi jumlah video (0 = semua)")
    p.add_argument("--force", action="store_true", help="Regenerate meski resume/link sudah ada")
    p.add_argument(
        "--no-relink-existing",
        dest="relink_existing",
        action="store_false",
        help="Jangan isi link jika file resume sudah ada (default: relink).",
    )
    p.set_defaults(relink_existing=True)
    p.add_argument("--dry-run", action="store_true", help="Hanya tampilkan rencana, tanpa API/DB write")
    p.add_argument("--sleep", type=float, default=None, help="Jeda antar video (detik)")
    p.add_argument("--sleep-chunk", type=float, default=None, help="Jeda antar chunk (detik)")
    p.add_argument("--retries", type=int, default=3, help="Jumlah retry ketika error/timeout")
    p.add_argument("--backoff", type=float, default=2.0, help="Backoff base seconds (exponential)")
    p.add_argument("--timeout", type=int, default=300, help="Timeout request (detik)")
    p.add_argument("--model", default=os.getenv("RESUME_MODEL") or "z-ai/glm4.7", help="Model name")
    p.add_argument(
        "--base-url",
        default=os.getenv("NVIDIA_BASE_URL") or "https://integrate.api.nvidia.com/v1",
        help="Base URL OpenAI-compatible endpoint",
    )
    p.add_argument("--max-tokens", type=int, default=16384, help="Max output tokens")
    p.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Max chars per chunk sebelum diringkas per bagian",
    )
    p.add_argument(
        "--chunk-max-tokens",
        type=int,
        default=None,
        help="Max output tokens untuk ringkasan per chunk",
    )
    p.add_argument(
        "--chunking",
        action="store_true",
        help="Aktifkan chunking (default: off, single-pass streaming)",
    )
    p.add_argument("--temperature", type=float, default=0.4, help="Sampling temperature")
    p.add_argument("--top-p", type=float, default=1.0, help="Top-p")
    p.add_argument("--thinking", action="store_true", help="Aktifkan thinking (jika backend mendukung)")
    p.add_argument(
        "--clear-thinking",
        action="store_true",
        help="Jika thinking aktif, minta backend menghapus reasoning dari output.",
    )
    p.add_argument(
        "--allow-reasoning-fallback",
        action="store_true",
        help="Jika content kosong, izinkan pakai reasoning sebagai fallback terakhir agar tidak gagal.",
    )
    p.add_argument("--no-stream", dest="stream", action="store_false", help="Nonaktifkan streaming")
    p.add_argument(
        "--agent",
        default=os.getenv("RESUME_AGENT") or "",
        help="Nama agen untuk logging/lock (default: RESUME_AGENT atau host:pid)",
    )
    p.add_argument(
        "--lock-ttl",
        type=int,
        default=None,
        help="TTL lock file (detik). Jika lock lebih lama dari ini, dianggap stale dan dibersihkan.",
    )
    p.add_argument(
        "--no-clean-locks-on-start",
        dest="clean_locks_on_start",
        action="store_false",
        help="Nonaktifkan auto-clean stale lock saat start.",
    )
    p.add_argument(
        "--no-lock",
        dest="use_lock",
        action="store_false",
        help="Nonaktifkan lock file (tidak disarankan untuk multi-agent).",
    )
    p.add_argument(
        "--no-startup-preflight",
        dest="startup_preflight",
        action="store_false",
        help="Lewati scan precheck startup untuk hemat I/O dan memory.",
    )
    p.set_defaults(stream=True, use_lock=True, clean_locks_on_start=True, startup_preflight=True)
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    load_env()
    args = parse_args(argv)

    def _env_bool(name: str) -> Optional[bool]:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return None
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return None

    def _env_float(name: str, default: float) -> float:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except Exception:
            return default

    def _env_int(name: str, default: int) -> int:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except Exception:
            return default

    def _env_str(name: str) -> Optional[str]:
        raw = (os.getenv(name) or "").strip()
        return raw or None

    def _env_int_opt(name: str) -> Optional[int]:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except Exception:
            return None

    def _env_float_opt(name: str) -> Optional[float]:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    def _is_rate_limited_error_text(exc: Exception) -> bool:
        s = str(exc).lower()
        return ("429" in s) or ("too many requests" in s) or ("rate limit" in s)

    def _is_connection_error_text(exc: Exception) -> bool:
        s = str(exc).lower()
        tokens = (
            "connection error",
            "connection reset",
            "connection aborted",
            "connection timed out",
            "name or service not known",
            "temporary failure in name resolution",
            "failed to establish a new connection",
            "max retries exceeded",
            "network is unreachable",
            "remote end closed connection",
        )
        return any(tok in s for tok in tokens)

    # Env overrides (take precedence if set)
    if _env_str("RESUME_DB"):
        args.db = _env_str("RESUME_DB") or args.db
    if _env_str("RESUME_OUT_ROOT"):
        args.out_root = _env_str("RESUME_OUT_ROOT") or args.out_root
    channels_raw = _env_str("RESUME_CHANNELS")
    if channels_raw:
        args.channel = [c.strip() for c in channels_raw.split(",") if c.strip()]
    limit_env = _env_int_opt("RESUME_LIMIT")
    if limit_env is not None:
        args.limit = limit_env
    force_env = _env_bool("RESUME_FORCE")
    if force_env is not None:
        args.force = force_env
    relink_env = _env_bool("RESUME_RELINK_EXISTING")
    if relink_env is not None:
        args.relink_existing = relink_env
    dry_env = _env_bool("RESUME_DRY_RUN")
    if dry_env is not None:
        args.dry_run = dry_env
    retries_env = _env_int_opt("RESUME_RETRIES")
    if retries_env is not None:
        args.retries = retries_env
    backoff_env = _env_float_opt("RESUME_BACKOFF")
    if backoff_env is not None:
        args.backoff = backoff_env
    timeout_env = _env_int_opt("RESUME_TIMEOUT")
    if timeout_env is not None:
        args.timeout = timeout_env
    model_env = _env_str("RESUME_MODEL")
    if model_env:
        args.model = model_env
    base_env = _env_str("RESUME_BASE_URL") or _env_str("NVIDIA_BASE_URL")
    if base_env:
        args.base_url = base_env
    max_tokens_env = _env_int_opt("RESUME_MAX_TOKENS")
    if max_tokens_env is not None:
        args.max_tokens = max_tokens_env
    chunking_env = _env_bool("RESUME_CHUNKING")
    if chunking_env is not None:
        args.chunking = chunking_env
    temp_env = _env_float_opt("RESUME_TEMPERATURE")
    if temp_env is not None:
        args.temperature = temp_env
    top_p_env = _env_float_opt("RESUME_TOP_P")
    if top_p_env is not None:
        args.top_p = top_p_env
    thinking_env = _env_bool("RESUME_THINKING")
    if thinking_env is not None:
        args.thinking = thinking_env
    clear_thinking_env = _env_bool("RESUME_CLEAR_THINKING")
    if clear_thinking_env is not None:
        args.clear_thinking = clear_thinking_env
    allow_reasoning_fallback_env = _env_bool("RESUME_ALLOW_REASONING_FALLBACK")
    if allow_reasoning_fallback_env is not None:
        args.allow_reasoning_fallback = allow_reasoning_fallback_env
    stream_env = _env_bool("RESUME_STREAM")
    if stream_env is not None:
        args.stream = stream_env
    use_lock_env = _env_bool("RESUME_USE_LOCK")
    if use_lock_env is not None:
        args.use_lock = use_lock_env
    clean_locks_env = _env_bool("RESUME_CLEAN_LOCKS_ON_START")
    if clean_locks_env is not None:
        args.clean_locks_on_start = clean_locks_env
    startup_preflight_env = _env_bool("RESUME_STARTUP_PREFLIGHT")
    if startup_preflight_env is not None:
        args.startup_preflight = startup_preflight_env
    agent_env = _env_str("RESUME_AGENT")
    if agent_env:
        args.agent = agent_env

    db_path = Path(args.db)
    out_root = Path(args.out_root)
    args.sleep = args.sleep if args.sleep is not None else _env_float("RESUME_SLEEP", 1.5)
    args.sleep_chunk = args.sleep_chunk if args.sleep_chunk is not None else _env_float("RESUME_SLEEP_CHUNK", 0.5)
    args.chunk_size = args.chunk_size if args.chunk_size is not None else _env_int("RESUME_CHUNK_SIZE", 12000)
    args.chunk_max_tokens = (
        args.chunk_max_tokens
        if args.chunk_max_tokens is not None
        else _env_int("RESUME_CHUNK_MAX_TOKENS", 900)
    )
    timeout_cap = _env_int("RESUME_TIMEOUT_CAP", 300)
    timeout_cap = max(30, min(timeout_cap, 300))
    if args.timeout > timeout_cap:
        log(f"[BOOT] timeout capped: {args.timeout}s -> {timeout_cap}s (API limit 300s)")
        args.timeout = timeout_cap
    args.lock_ttl = args.lock_ttl if args.lock_ttl is not None else _env_int("RESUME_LOCK_TTL", 21600)
    args.agent = args.agent or f"{socket.gethostname()}:{os.getpid()}"
    global AGENT_NAME
    AGENT_NAME = args.agent
    log(
        "[BOOT] "
        f"pid={os.getpid()} "
        f"chunking={int(bool(args.chunking))} "
        f"stream={int(bool(args.stream))} "
        f"thinking={int(bool(args.thinking))} "
        f"allow_reasoning_fallback={int(bool(args.allow_reasoning_fallback))} "
        f"timeout={args.timeout}s "
        f"retries={args.retries} "
        f"max_tokens={args.max_tokens} "
        f"chunk_max_tokens={args.chunk_max_tokens} "
        f"chunk_size={args.chunk_size}"
    )
    if not db_path.exists():
        print(f"DB tidak ditemukan: {db_path.resolve()}", file=sys.stderr)
        return 2

    api_key = (os.getenv("NVIDIA_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key and not args.dry_run:
        print(
            "API key tidak ditemukan. Set NVIDIA_API_KEY/OPENAI_API_KEY "
            "(atau NVIDIA_API_KEYS / NVIDIA_API_KEY_1..N).",
            file=sys.stderr,
        )
        return 2

    client = None
    if not args.dry_run:
        client = require_openai_client(args.base_url, api_key, args.timeout)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA busy_timeout=15000;")
    temp_store = (_env_str("RESUME_SQLITE_TEMP_STORE") or "FILE").strip().upper()
    if temp_store == "MEMORY":
        con.execute("PRAGMA temp_store=MEMORY;")
    else:
        con.execute("PRAGMA temp_store=FILE;")
    cache_kb = max(512, _env_int("RESUME_SQLITE_CACHE_KB", 4096))
    con.execute(f"PRAGMA cache_size=-{int(cache_kb)};")

    if args.dry_run:
        log("DRY-RUN enabled: no API calls, no file writes, no DB updates.")

    total = 0
    generated = 0
    relinked = 0
    skipped = 0
    errors = 0

    try:
        if args.use_lock and args.clean_locks_on_start:
            removed = clean_stale_locks(out_root, args.lock_ttl)
            if removed:
                log(f"[CLEAN] removed {len(removed)} stale/dead lock(s)")
                for p in removed:
                    try:
                        rel = p.relative_to(out_root)
                        log(f"[CLEAN] {rel}")
                    except Exception:
                        log(f"[CLEAN] {p}")

        busy_channels = get_busy_channel_ids(con)
        if busy_channels:
            log(f"[PRECHECK] busy channels from active jobs: {len(busy_channels)}")

        if args.startup_preflight:
            run_startup_preflight(
                con,
                out_root=out_root,
                channels=(args.channel or None),
                limit=args.limit,
                relink_existing=args.relink_existing,
                lock_ttl_s=args.lock_ttl,
                agent=args.agent,
                busy_channels=busy_channels,
                dry_run=bool(args.dry_run),
            )
        else:
            log("[PRECHECK] startup preflight disabled")

        busy_notified: set[int] = set()
        for r in iter_rows(
            con,
            channels=(args.channel or None),
            missing_only=not args.force,
            limit=args.limit,
        ):
            total += 1
            if total % 200 == 0:
                busy_channels = get_busy_channel_ids(con)
            if r.channel_id in busy_channels:
                if r.channel_id not in busy_notified:
                    print(f"[SKIP] channel sedang busy job update: channel_id={r.channel_id}")
                    busy_notified.add(r.channel_id)
                skipped += 1
                continue
            base = out_root / r.slug
            transcript_path = resolve_under(base, r.link_file)
            has_transcript = link_exists_under(base, r.link_file)
            if not has_transcript:
                print(f"[SKIP] transcript tidak ditemukan: {r.slug} | {r.video_id} | {r.link_file}")
                if not args.dry_run:
                    con.execute(
                        "UPDATE videos SET status_download='pending' WHERE id=? AND status_download!='pending'",
                        (r.id,),
                    )
                    con.commit()
                skipped += 1
                continue
            if transcript_path and transcript_path.exists() and transcript_path.is_file() and not file_is_stable(
                transcript_path, min_age_s=15
            ):
                print(f"[SKIP] transcript masih ditulis/terlalu baru: {r.slug} | {r.video_id}")
                skipped += 1
                continue

            resume_rel = default_resume_link(r.video_id, r.seq_num, r.link_file)
            if not resume_rel:
                print(f"[SKIP] tidak bisa menentukan nama resume: {r.slug} | {r.video_id}")
                skipped += 1
                continue

            resume_path = resolve_under(base, resume_rel)
            if not resume_path:
                print(f"[SKIP] path resume tidak valid: {r.slug} | {r.video_id} | {resume_rel}")
                skipped += 1
                continue

            if not args.force:
                transcript_chars = int(link_size_under(base, r.link_file) or 0)
                resume_candidates: list[str] = []
                if r.link_resume:
                    resume_candidates.append(str(r.link_resume))
                if resume_rel not in resume_candidates:
                    resume_candidates.append(resume_rel)

                checked_existing = False
                for existing_rel in resume_candidates:
                    if not link_exists_under(base, existing_rel):
                        continue
                    checked_existing = True
                    existing_text = read_link_text_under(base, existing_rel)
                    if existing_text is None:
                        print(
                            f"[SKIP] resume tidak bisa dibaca saat precheck: "
                            f"{r.slug} | {r.video_id} | {existing_rel}"
                        )
                        skipped += 1
                        break
                    ok_resume, reason_resume = is_resume_acceptable(existing_text, transcript_chars=transcript_chars)
                    if ok_resume:
                        # Keep canonical naming in DB when requested.
                        if args.relink_existing and existing_rel != resume_rel:
                            if not args.dry_run:
                                con.execute("UPDATE videos SET link_resume=? WHERE id=?", (resume_rel, r.id))
                                con.commit()
                            print(f"[RELINK] {r.slug} | {r.video_id} -> {resume_rel}")
                            relinked += 1
                        else:
                            print(f"[SKIP] resume sudah ada: {r.slug} | {r.video_id} -> {existing_rel}")
                            skipped += 1
                        break
                    print(f"[REQUEUE] resume invalid ({reason_resume}): {r.slug} | {r.video_id} | {existing_rel}")
                else:
                    checked_existing = False

                if checked_existing:
                    continue

            lock_path = lock_path_for(resume_path)
            lock_acquired = False
            if args.use_lock:
                if args.dry_run:
                    if lock_path.exists():
                        info = read_lock_info(lock_path)
                        info_msg = f" | {info}" if info else ""
                        print(f"[SKIP] locked: {r.slug} | {r.video_id}{info_msg}")
                        skipped += 1
                        continue
                else:
                    lock_acquired = try_acquire_lock(lock_path, agent=args.agent, ttl_s=args.lock_ttl)
                    if not lock_acquired:
                        info = read_lock_info(lock_path)
                        info_msg = f" | {info}" if info else ""
                        print(f"[SKIP] locked: {r.slug} | {r.video_id}{info_msg}")
                        skipped += 1
                        continue

            if args.dry_run:
                print(f"[DRY-RUN] generate: {r.slug} | {r.video_id} -> {resume_rel}")
                continue

            try:
                log(f"[VIDEO] start {r.slug} | {r.video_id}")
                log(f"[STEP] read transcript {r.link_file}")
                transcript_raw = read_link_text_under(base, r.link_file, retries=2, sleep_s=0.2)
                if transcript_raw is None:
                    source = shard_storage.link_source_label(base, r.link_file)
                    runtime = shard_read_runtime_mode()
                    read_err = (getattr(shard_storage, "last_read_error", lambda: None)() or "").strip()
                    extra_err = f" | err={read_err}" if read_err else ""
                    print(
                        f"[SKIP] transcript tidak bisa dibaca: {r.slug} | {r.video_id} | {r.link_file}"
                        f" | source={source} | zstd_runtime={runtime}{extra_err}"
                    )
                    if "/.shards/" in source and runtime == "unavailable":
                        log(
                            "[WARN] zstandard tidak tersedia (module/bridge). "
                            "Install zstandard di python agent atau set SHARD_ZSTD_PYTHON."
                        )
                    if not args.dry_run:
                        mark_pending = (
                            read_err.startswith("short_read:")
                            or read_err in {
                                "entry_not_found",
                                "invalid_shard_path",
                                "invalid_entry_offset_or_length",
                                "invalid_entry_bounds",
                                "shard_io_failed",
                            }
                        )
                        if mark_pending:
                            con.execute(
                                "UPDATE videos SET status_download='pending' "
                                "WHERE id=? AND status_download!='pending'",
                                (r.id,),
                            )
                            con.commit()
                    skipped += 1
                    continue
                transcript_text = transcript_raw
                transcript_text = sanitize_transcript(transcript_text)
                if not transcript_text:
                    print(f"[SKIP] transcript kosong: {r.slug} | {r.video_id} | {r.link_file}")
                    skipped += 1
                    continue

                try:
                    assert client is not None
                    final_min_chars = min_summary_chars(len(transcript_text), final=True)
                    if not args.chunking:
                        log("[STEP] single-pass summary (streaming)")
                        prompt = build_prompt(transcript_text)
                        md = call_chat(
                            client,
                            model=args.model,
                            prompt=prompt,
                            max_tokens=args.max_tokens,
                            temperature=args.temperature,
                            top_p=args.top_p,
                            thinking=args.thinking,
                            clear_thinking=args.clear_thinking,
                            allow_reasoning_fallback=args.allow_reasoning_fallback,
                            min_chars=final_min_chars,
                            retries=args.retries,
                            backoff_s=args.backoff,
                            stream=args.stream,
                        )
                    else:
                        chunks = chunk_text(transcript_text, max_chars=args.chunk_size)
                        log(f"[STEP] chunking: {len(chunks)} bagian (max_chars={args.chunk_size})")
                        summaries: list[str] = []
                        min_chunk_chars = max(2000, int(args.chunk_size * 0.3))
                        idx = 0
                        total_chunks = len(chunks)
                        while idx < len(chunks):
                            chunk = chunks[idx]
                            total_chunks = len(chunks)
                            try:
                                log(f"[STEP] chunk {idx+1}/{total_chunks} request start (chars={len(chunk)})")
                                chunk_prompt = build_chunk_prompt(chunk, idx + 1, total_chunks)
                                chunk_min_chars = min_summary_chars(len(chunk), final=False)
                                part = call_chat(
                                    client,
                                    model=args.model,
                                    prompt=chunk_prompt,
                                    max_tokens=args.chunk_max_tokens,
                                    temperature=args.temperature,
                                    top_p=args.top_p,
                                    thinking=args.thinking,
                                    clear_thinking=args.clear_thinking,
                                    allow_reasoning_fallback=args.allow_reasoning_fallback,
                                    min_chars=chunk_min_chars,
                                    retries=args.retries,
                                    backoff_s=args.backoff,
                                    stream=args.stream,
                                )
                                part_clean = part.strip()
                                if not part_clean:
                                    raise RuntimeError("Ringkasan chunk kosong setelah request.")
                                summaries.append(part_clean)
                                log(f"[STEP] chunk {idx+1}/{total_chunks} done")
                                if args.sleep_chunk > 0:
                                    log(f"[STEP] sleep-chunk {args.sleep_chunk}s")
                                    time.sleep(args.sleep_chunk)
                                idx += 1
                            except Exception as exc:
                                if _is_rate_limited_error_text(exc):
                                    # Split chunk won't help for provider-side rate limit.
                                    raise
                                if _is_connection_error_text(exc):
                                    # Network/provider connectivity issue is unrelated to chunk size.
                                    # Avoid recursive split that only amplifies failing requests.
                                    raise
                                if len(chunk) > min_chunk_chars:
                                    new_chunks = split_chunk(chunk)
                                    if len(new_chunks) > 1:
                                        log(
                                            f"[WARN] chunk {idx+1}/{total_chunks} gagal ({exc}); "
                                            f"split jadi {len(new_chunks)} bagian (chars~{len(new_chunks[0])}/{len(new_chunks[1])})"
                                        )
                                        chunks[idx:idx + 1] = new_chunks
                                        continue
                                raise
                        log("[STEP] final summary from chunk summaries")
                        final_prompt = build_final_prompt(summaries)
                        md = call_chat(
                            client,
                            model=args.model,
                            prompt=final_prompt,
                            max_tokens=args.max_tokens,
                            temperature=args.temperature,
                            top_p=args.top_p,
                            thinking=args.thinking,
                            clear_thinking=args.clear_thinking,
                            allow_reasoning_fallback=args.allow_reasoning_fallback,
                            min_chars=final_min_chars,
                            retries=args.retries,
                            backoff_s=args.backoff,
                            stream=args.stream,
                        )
                    ok_final, reason_final = is_resume_acceptable(md, transcript_chars=len(transcript_text))
                    if not ok_final and reason_final == "missing_conclusion_section":
                        log("[STEP] repair missing conclusion section")
                        repair_prompt = build_conclusion_repair_prompt(md)
                        repaired_md = call_chat(
                            client,
                            model=args.model,
                            prompt=repair_prompt,
                            max_tokens=args.max_tokens,
                            temperature=min(0.3, args.temperature),
                            top_p=args.top_p,
                            thinking=False,
                            clear_thinking=False,
                            allow_reasoning_fallback=False,
                            min_chars=final_min_chars,
                            retries=max(1, args.retries),
                            backoff_s=args.backoff,
                            stream=False,
                        )
                        ok_repaired, reason_repaired = is_resume_acceptable(
                            repaired_md, transcript_chars=len(transcript_text)
                        )
                        if ok_repaired:
                            md = repaired_md
                            ok_final, reason_final = True, "ok"
                        else:
                            log(
                                "[WARN] repair conclusion failed "
                                f"(reason={reason_repaired}, chars={len(repaired_md.strip())})"
                            )
                    if not ok_final:
                        raise RuntimeError(
                            f"Resume final tidak valid ({reason_final}, chars={len(md.strip())}, min={final_min_chars})."
                        )
                except Exception as exc:
                    print(f"[ERROR] {r.slug} | {r.video_id}: {exc}")
                    errors += 1
                    if _is_connection_error_text(exc):
                        cooloff = max(5.0, min(60.0, float(args.backoff) * 4.0))
                        log(f"[WARN] connection error global cooloff {cooloff:.1f}s")
                        time.sleep(cooloff)
                    continue

                log(f"[STEP] write resume {resume_path}")
                resume_path.parent.mkdir(parents=True, exist_ok=True)
                resume_path.write_text(md, encoding="utf-8")
                log("[STEP] update DB link_resume")
                con.execute("UPDATE videos SET link_resume=? WHERE id=?", (resume_rel, r.id))
                con.commit()
                generated += 1
                print(f"[OK] {r.slug} | {r.video_id} -> {resume_rel}")

                if args.sleep > 0:
                    log(f"[STEP] sleep-video {args.sleep}s")
                    time.sleep(args.sleep)
            finally:
                if lock_acquired:
                    release_lock(lock_path)

        if total == 0:
            print("Tidak ada video yang cocok.")
            log("[DONE] tidak ada video yang cocok.")
            return 0

        print("\nSelesai.")
        print(f"- Total diproses: {total}")
        print(f"- Generated:      {generated}")
        print(f"- Relinked:       {relinked}")
        print(f"- Skipped:        {skipped}")
        print(f"- Errors:         {errors}")
        log(
            f"[DONE] total={total} generated={generated} "
            f"relinked={relinked} skipped={skipped} errors={errors}"
        )
        return 0 if errors == 0 else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
