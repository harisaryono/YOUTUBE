#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import os
import re
import sqlite3
import subprocess
import sys
import time
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


STOPWORDS = {
    "able",
    "about",
    "above",
    "across",
    "after",
    "again",
    "against",
    "all",
    "almost",
    "alone",
    "along",
    "already",
    "also",
    "although",
    "always",
    "am",
    "among",
    "an",
    "and",
    "any",
    "are",
    "around",
    "as",
    "at",
    "away",
    "back",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "cannot",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "down",
    "during",
    "each",
    "either",
    "else",
    "ever",
    "every",
    "few",
    "for",
    "from",
    "further",
    "get",
    "go",
    "going",
    "got",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "however",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "keep",
    "like",
    "made",
    "make",
    "many",
    "may",
    "me",
    "might",
    "more",
    "most",
    "much",
    "must",
    "my",
    "myself",
    "near",
    "need",
    "never",
    "no",
    "not",
    "now",
    "of",
    "off",
    "on",
    "once",
    "one",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "people",
    "really",
    "right",
    "said",
    "same",
    "say",
    "says",
    "see",
    "she",
    "should",
    "since",
    "so",
    "some",
    "such",
    "take",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "us",
    "very",
    "was",
    "we",
    "well",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "within",
    "without",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
}


@dataclass(frozen=True)
class VideoRow:
    id: int
    seq_num: int
    video_id: str
    title: str
    upload_date: Optional[str]
    link_file: str
    link_resume: Optional[str]


def _clean_caption_line(line: str) -> str:
    s = html.unescape(line.strip())
    s = s.replace("\u200b", "")
    s = re.sub(r"^>+\s*", "", s)  # ">>"
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    if s.startswith("[") and s.endswith("]"):
        return ""
    if s.lower().startswith("kind:") or s.lower().startswith("language:"):
        return ""
    return s


def _sentences_from_caption(path: Path) -> tuple[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    language = ""
    cleaned: list[str] = []
    for line in raw_lines[:10]:
        if line.lower().startswith("language:"):
            language = line.split(":", 1)[1].strip()
            break

    for line in raw_lines:
        s = _clean_caption_line(line)
        if not s:
            cleaned.append("")
            continue
        cleaned.append(s)

    # Join caption lines into sentence-like chunks.
    sentences: list[str] = []
    buf: list[str] = []
    for s in cleaned:
        if not s:
            if buf:
                sentences.append(" ".join(buf).strip())
                buf = []
            continue
        low = s.lower()
        if any(k in low for k in ("subscribe", "sponsor", "download", "comment section")):
            continue
        buf.append(s)
        joined = " ".join(buf).strip()
        if len(joined) >= 220 or re.search(r"[.!?]\s*$", joined):
            sentences.append(joined)
            buf = []
    if buf:
        sentences.append(" ".join(buf).strip())
    sentences = [s for s in sentences if len(s) >= 40]
    return language, sentences


def _chunk_text(lines: list[str], chunk_chars: int) -> list[str]:
    chunks: list[str] = []
    buf: list[str] = []
    size = 0
    for line in lines:
        if not line:
            continue
        if size + len(line) + 1 > chunk_chars and buf:
            chunks.append("\n".join(buf).strip())
            buf = []
            size = 0
        buf.append(line)
        size += len(line) + 1
    if buf:
        chunks.append("\n".join(buf).strip())
    return chunks


def _top_keywords(sentences: Iterable[str], n: int = 14) -> list[str]:
    words: list[str] = []
    for s in sentences:
        for w in re.findall(r"[A-Za-z]{4,}", s):
            wl = w.lower()
            if wl in STOPWORDS:
                continue
            words.append(wl)
    c = Counter(words)
    return [w for (w, _) in c.most_common(n)]


def _score_sentence(sentence: str, freq: Counter[str]) -> int:
    score = 0
    for w in re.findall(r"[A-Za-z]{4,}", sentence):
        wl = w.lower()
        if wl in STOPWORDS:
            continue
        score += freq.get(wl, 0)
    return score


def _select_key_sentences(sentences: list[str], keywords: list[str], k: int = 12) -> list[str]:
    freq = Counter()
    for kw in keywords:
        freq[kw] += 5
    for s in sentences:
        for w in re.findall(r"[A-Za-z]{4,}", s):
            wl = w.lower()
            if wl in STOPWORDS:
                continue
            freq[wl] += 1

    scored = []
    for s in sentences:
        if len(s) < 60 or len(s) > 260:
            continue
        low = s.lower()
        if any(k in low for k in ("subscribe", "sponsor", "comment section")):
            continue
        scored.append((_score_sentence(s, freq), s))
    scored.sort(key=lambda t: t[0], reverse=True)
    picked: list[str] = []
    seen = set()
    for _, s in scored:
        key = re.sub(r"\W+", "", s.lower())[:120]
        if key in seen:
            continue
        seen.add(key)
        picked.append(s)
        if len(picked) >= k:
            break
    if not picked:
        picked = sentences[: min(k, len(sentences))]
    return picked


def _format_language(lang: str) -> str:
    if not lang:
        return "Unknown (captions)"
    if lang.lower().startswith("en"):
        return "English (captions)"
    return f"{lang} (captions)"


def _format_upload_date(upload_date: Optional[str]) -> str:
    if not upload_date:
        return ""
    s = str(upload_date).strip()
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _render_resume_md(
    channel_slug: str,
    video_id: str,
    title: str,
    upload_date: Optional[str],
    language: str,
    keywords: list[str],
    intro: list[str],
    key_sents: list[str],
) -> str:
    upload = _format_upload_date(upload_date)
    kw = ", ".join(keywords[:10]) if keywords else "-"
    intro_txt = " ".join(intro).strip()
    if len(intro_txt) > 800:
        intro_txt = intro_txt[:800].rsplit(" ", 1)[0] + "…"

    lines: list[str] = []
    lines.append(f"# Resume — {title}".rstrip())
    lines.append("")
    lines.append(f"- Channel: {channel_slug}")
    lines.append(f"- Video ID: {video_id}")
    lines.append(f"- Upload date (DB): {upload}")
    lines.append(f"- Bahasa transcript: {_format_language(language)}")
    lines.append("")
    lines.append("## Ringkasan singkat")
    lines.append(
        "Dokumen ini dibuat **otomatis** dari transkrip (captions). "
        "Ringkasan di bawah menekankan tema dominan dan cuplikan penting; "
        "silakan edit/rapikan jika Anda ingin versi yang lebih naratif."
    )
    if intro_txt:
        lines.append("")
        lines.append(f"**Pembuka (cuplikan):** {intro_txt}")
    lines.append("")
    lines.append("## Topik dominan (otomatis)")
    lines.append(f"- Kata kunci: {kw}")
    lines.append("")
    lines.append("## Poin penting (cuplikan transkrip)")
    for s in key_sents:
        lines.append(f"- {s}")
    lines.append("")
    lines.append("## Catatan")
    lines.append(
        "Ringkasan ini merangkum transkrip otomatis (captions) sehingga bisa ada kesalahan kata/kalimat. "
        "Untuk keputusan penting, rujuk ke video asli."
    )
    lines.append("")
    return "\n".join(lines)


def _codex_text(*, model: str, prompt: str, timeout_s: int) -> str:
    """
    Run Codex CLI non-interactively and return the last assistant message.
    Uses your existing `codex login` credentials; no OPENAI_API_KEY needed.
    """
    # Don't write the last message to stdout, otherwise output may get duplicated.
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as f:
        out_file = f.name
    try:
        with subprocess.Popen(
            [
                "codex",
                "exec",
                "--skip-git-repo-check",
                "-s",
                "read-only",
                "--color",
                "never",
                "-m",
                model,
                "-",
                "-o",
                out_file,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as p:
            try:
                out, err = p.communicate(prompt, timeout=timeout_s)
            except subprocess.TimeoutExpired:
                p.kill()
                out, err = p.communicate()
                raise RuntimeError(f"codex exec timeout after {timeout_s}s")
            if p.returncode != 0:
                raise RuntimeError(f"codex exec failed (rc={p.returncode}): {err.strip() or out.strip()}")

        txt = Path(out_file).read_text(encoding="utf-8", errors="ignore").strip()
        if not txt:
            raise RuntimeError("codex exec returned empty output")
        return txt + "\n"
    finally:
        try:
            Path(out_file).unlink(missing_ok=True)
        except Exception:
            pass


def _render_resume_md_codex(
    *,
    model: str,
    channel_slug: str,
    video_id: str,
    title: str,
    upload_date: Optional[str],
    language: str,
    sentences: list[str],
    timeout_s: int,
) -> str:
    # Feed curated excerpts (not full transcript) to keep prompts small but informative.
    keywords = _top_keywords(sentences, n=18)
    key_sents = _select_key_sentences(sentences, keywords, k=40)
    intro = sentences[:12]
    excerpts = "\n".join(f"- {s}" for s in (intro + key_sents))

    upload = _format_upload_date(upload_date)
    kw = ", ".join(keywords[:12]) if keywords else "-"

    prefix = (
        f"# Resume — {title}\n\n"
        f"- Channel: {channel_slug}\n"
        f"- Video ID: {video_id}\n"
        f"- Upload date (DB): {upload}\n"
        f"- Bahasa transcript: {_format_language(language)}\n\n"
    )

    prompt = (
        "Tulis resume yang komprehensif dan enak dibaca dalam Bahasa Indonesia, mirip contoh web yang panjang dan terstruktur.\n"
        "Aturan keras:\n"
        "- Gunakan HANYA informasi yang ada di EXCERPTS (potongan transkrip).\n"
        "- Jangan mengarang fakta, angka, nama, atau kesimpulan yang tidak disebut.\n"
        "- Jika sesuatu tidak pasti, tulis sebagai 'dibahas'/'disebut'/'kemungkinan'.\n"
        "- Output HARUS Markdown (tanpa code fence).\n"
        "- JANGAN menulis ulang judul/metadata (itu sudah disediakan). Mulai output tepat dari heading pertama.\n"
        "\n"
        "Struktur wajib (selalu tampilkan semua heading):\n"
        "## Ringkasan singkat\n"
        "(4–10 paragraf, naratif, jelaskan konteks + benang merah.)\n"
        "\n"
        "## Gagasan besar (terstruktur)\n"
        "(Gunakan subheading bernomor 1), 2), 3) …; tiap poin boleh 1–3 paragraf atau bullet.)\n"
        "\n"
        "## Poin penting\n"
        "(15–28 bullet, spesifik, hindari generik.)\n"
        "\n"
        "## Apa yang bisa saya ambil dari transkrip ini\n"
        "(5–12 bullet berisi pelajaran/penerapan yang realistis, tetap berbasis kutipan/ide di transkrip.)\n"
        "\n"
        "## Penerapan praktis (jika relevan)\n"
        "(Boleh berupa langkah-langkah ringkas. Jika tidak relevan, tulis 1 bullet: '- (Tidak ada penerapan praktis yang jelas di potongan ini.)')\n"
        "\n"
        "## Catatan\n"
        "(1 paragraf disclaimer ringkasan transkrip.)\n"
        "\n"
        f"Konteks metadata (untuk akurasi, jangan dicetak ulang):\n"
        f"- title: {title}\n"
        f"- upload_date_db: {upload}\n"
        f"- language: {_format_language(language)}\n"
        f"- keywords: {kw}\n"
        "\n"
        "EXCERPTS (potongan transkrip):\n"
        f"{excerpts}\n"
    )
    body = _codex_text(model=model, prompt=prompt, timeout_s=timeout_s)
    return prefix + body


def _require_openai() -> "tuple[object, object]":
    try:
        from openai import OpenAI  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Paket `openai` belum terpasang. Install dulu: `pip install openai` "
            "atau `pip install -r requirements-web.txt`."
        ) from e
    return OpenAI, OpenAI()


def _openai_text(client: object, *, model: str, system: str, user: str, max_output_tokens: int) -> str:
    # OpenAI Responses API via official SDK. Keep it simple and robust.
    # We intentionally don't stream; we want deterministic file writes.
    try:
        resp = client.responses.create(  # type: ignore[attr-defined]
            model=model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_output_tokens=max_output_tokens,
            store=False,
        )
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"OpenAI request failed: {e}") from e

    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt.strip() + "\n"

    # Fallback: walk structured output (SDK versions differ).
    out = getattr(resp, "output", None)
    if isinstance(out, list):
        parts: list[str] = []
        for item in out:
            content = getattr(item, "content", None) if not isinstance(item, dict) else item.get("content")
            if not content:
                continue
            for c in content:
                c_type = getattr(c, "type", None) if not isinstance(c, dict) else c.get("type")
                if c_type in ("output_text", "text"):
                    t = getattr(c, "text", None) if not isinstance(c, dict) else c.get("text")
                    if isinstance(t, str):
                        parts.append(t)
        joined = "\n".join(p.strip() for p in parts if p.strip()).strip()
        if joined:
            return joined + "\n"

    raise RuntimeError("OpenAI response did not contain text output.")


def _render_resume_md_openai(
    *,
    model: str,
    client: object,
    channel_slug: str,
    video_id: str,
    title: str,
    upload_date: Optional[str],
    language: str,
    transcript_lines: list[str],
    chunk_chars: int,
    max_output_tokens: int,
    sleep_s: float,
) -> str:
    chunks = _chunk_text(transcript_lines, chunk_chars=chunk_chars)
    if not chunks:
        raise RuntimeError("Transcript kosong setelah dibersihkan.")

    system_chunk = (
        "Anda adalah asisten yang merangkum transkrip podcast/YouTube secara akurat.\n"
        "Aturan:\n"
        "- Gunakan HANYA informasi yang ada di potongan transkrip.\n"
        "- Jangan menambahkan fakta, angka, nama, atau klaim yang tidak muncul.\n"
        "- Tulis dalam Bahasa Indonesia.\n"
        "- Jika bagian tidak jelas, tulis sebagai 'dibahas' atau 'disebut' tanpa memastikan.\n"
        "- Output harus ringkas, berupa bullet points."
    )

    chunk_summaries: list[str] = []
    for i, chunk in enumerate(chunks, start=1):
        user_chunk = (
            f"Judul: {title}\n"
            f"Channel: {channel_slug}\n"
            f"Video ID: {video_id}\n"
            f"Bahasa captions: {language or 'unknown'}\n"
            f"\n"
            f"Potongan transkrip ({i}/{len(chunks)}):\n"
            f"{chunk}\n"
            f"\n"
            "Buat ringkasan poin penting dari potongan ini (8–14 bullet), fokus pada ide, argumen, "
            "contoh, saran, dan definisi yang disebut."
        )
        chunk_summaries.append(
            _openai_text(client, model=model, system=system_chunk, user=user_chunk, max_output_tokens=900)
        )
        if sleep_s > 0:
            time.sleep(sleep_s)

    system_final = (
        "Anda adalah penulis resume yang rapi dan komprehensif.\n"
        "Tugas: gabungkan ringkasan per-potongan menjadi 1 resume yang enak dibaca.\n"
        "Aturan keras:\n"
        "- Gunakan HANYA informasi yang terdapat pada ringkasan potongan (yang berasal dari transkrip).\n"
        "- Jangan halusinasi: jangan mengarang guest, studi, angka, atau kesimpulan.\n"
        "- Tulis Bahasa Indonesia yang natural.\n"
        "- Output HARUS berupa Markdown (tanpa code fence), mengikuti struktur di bawah."
    )

    upload = upload_date or ""
    user_final = (
        "Buat resume Markdown dengan format persis berikut:\n"
        "# Resume — <judul>\n"
        "\n"
        "- Channel: <channel>\n"
        "- Video ID: <video_id>\n"
        "- Upload date (DB): <YYYY-MM-DD atau kosong>\n"
        "- Bahasa transcript: <bahasa>\n"
        "\n"
        "## Ringkasan singkat\n"
        "(3–6 paragraf ringkas, jelaskan konteks, siapa yang berbicara jika jelas, dan benang merah.)\n"
        "\n"
        "## Poin penting\n"
        "(10–18 bullet; masing-masing spesifik dan tidak generik.)\n"
        "\n"
        "## Checklist praktis (jika relevan)\n"
        "(0–8 bullet; hanya jika ada saran/praktik yang bisa dilakukan.)\n"
        "\n"
        "## Catatan\n"
        "(1 paragraf disclaimer bahwa ini ringkasan transkrip.)\n"
        "\n"
        "Data:\n"
        f"- judul: {title}\n"
        f"- channel: {channel_slug}\n"
        f"- video_id: {video_id}\n"
        f"- upload_date_db: {upload}\n"
        f"- bahasa: {_format_language(language)}\n"
        "\n"
        "Ringkasan potongan (gabungkan semuanya):\n"
        + "\n\n".join(f"=== Chunk {i} ===\n{txt.strip()}" for i, txt in enumerate(chunk_summaries, start=1))
        + "\n"
    )
    md = _openai_text(client, model=model, system=system_final, user=user_final, max_output_tokens=max_output_tokens)
    return md


def _fetch_rows(
    con: sqlite3.Connection,
    channel_slug: str,
    seq_from: int,
    seq_to: int,
) -> list[VideoRow]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    lo, hi = (seq_to, seq_from) if seq_from >= seq_to else (seq_from, seq_to)
    cur.execute(
        """
        SELECT v.id, v.seq_num, v.video_id, v.title, v.upload_date, v.link_file, v.link_resume
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE c.slug = ?
          AND v.seq_num BETWEEN ? AND ?
          AND v.link_file IS NOT NULL AND v.link_file != ''
        ORDER BY v.seq_num DESC
        """,
        (channel_slug, lo, hi),
    )
    rows = []
    for r in cur.fetchall():
        rows.append(
            VideoRow(
                id=int(r["id"]),
                seq_num=int(r["seq_num"]),
                video_id=str(r["video_id"]),
                title=str(r["title"] or ""),
                upload_date=(str(r["upload_date"]) if r["upload_date"] else None),
                link_file=str(r["link_file"]),
                link_resume=(str(r["link_resume"]) if r["link_resume"] else None),
            )
        )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate draft resume markdown files from caption transcripts.")
    ap.add_argument("--db", default="channels.db")
    ap.add_argument("--out", default="out", help="Base folder that contains per-channel outputs (default: out)")
    ap.add_argument("--channel", required=True, help="Channel slug, e.g. TheDiaryOfACEO")
    ap.add_argument("--seq-from", type=int, required=True)
    ap.add_argument("--seq-to", type=int, required=True)
    ap.add_argument("--force", action="store_true", help="Overwrite existing resume files and DB link_resume")
    ap.add_argument(
        "--engine",
        choices=["heuristic", "openai", "codex"],
        default="heuristic",
        help="Resume generator engine. `heuristic` is fast/offline; `openai` uses OpenAI API; `codex` uses Codex CLI login.",
    )
    ap.add_argument("--model", default="gpt-5", help="OpenAI model name (engine=openai)")
    ap.add_argument("--chunk-chars", type=int, default=12000, help="Chunk size for transcript (engine=openai)")
    ap.add_argument("--max-output-tokens", type=int, default=1600, help="Max output tokens for final resume (openai)")
    ap.add_argument("--sleep-s", type=float, default=0.0, help="Sleep between OpenAI calls (seconds)")
    ap.add_argument("--codex-timeout-s", type=int, default=240, help="Timeout per Codex call (engine=codex)")
    ap.add_argument("--dry-run", action="store_true", help="Don't write files or update DB")
    ap.add_argument(
        "--backup-dir",
        default="",
        help="If set, copy existing resume files here before overwriting (relative to channel folder).",
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    out_base = Path(args.out)
    channel_slug = args.channel

    if args.engine == "openai":
        if args.dry_run:
            pass
        elif not os.environ.get("OPENAI_API_KEY"):
            print("ERROR: OPENAI_API_KEY environment variable belum di-set.", file=sys.stderr)
            print("Contoh: export OPENAI_API_KEY='sk-...'", file=sys.stderr)
            return 2

    con = sqlite3.connect(str(db_path))
    try:
        rows = _fetch_rows(con, channel_slug, args.seq_from, args.seq_to)
        if not rows:
            print("No rows found.")
            return 0

        channel_dir = out_base / channel_slug
        resume_dir = channel_dir / "resume"
        resume_dir.mkdir(parents=True, exist_ok=True)

        cur = con.cursor()
        changed = 0
        openai_client = None
        if args.engine == "openai":
            if not args.dry_run:
                _, openai_client = _require_openai()

        for r in rows:
            transcript_path = channel_dir / Path(r.link_file)
            if r.seq_num is None:
                print(f"[SKIP {channel_slug}] seq_num NULL for video_id={r.video_id}; cannot name resume.")
                continue
            resume_rel = Path("resume") / f"{int(r.seq_num):04d}_{r.video_id}.md"
            resume_path = channel_dir / resume_rel

            if not args.force:
                if r.link_resume and str(r.link_resume).strip():
                    continue
                if resume_path.exists():
                    continue

            if args.dry_run:
                print(f"[DRY-RUN {channel_slug}] seq={r.seq_num} video_id={r.video_id} -> {resume_rel}")
                continue

            language, sentences = _sentences_from_caption(transcript_path)
            if args.engine == "heuristic":
                keywords = _top_keywords(sentences, n=16)
                intro = sentences[:5]
                key_sents = _select_key_sentences(sentences, keywords, k=12)
                md = _render_resume_md(
                    channel_slug=channel_slug,
                    video_id=r.video_id,
                    title=r.title,
                    upload_date=r.upload_date,
                    language=language,
                    keywords=keywords,
                    intro=intro,
                    key_sents=key_sents,
                )
            elif args.engine == "openai":
                assert openai_client is not None
                md = _render_resume_md_openai(
                    model=args.model,
                    client=openai_client,
                    channel_slug=channel_slug,
                    video_id=r.video_id,
                    title=r.title,
                    upload_date=r.upload_date,
                    language=language,
                    transcript_lines=sentences,
                    chunk_chars=args.chunk_chars,
                    max_output_tokens=args.max_output_tokens,
                    sleep_s=args.sleep_s,
                )
            else:
                md = _render_resume_md_codex(
                    model=args.model,
                    channel_slug=channel_slug,
                    video_id=r.video_id,
                    title=r.title,
                    upload_date=r.upload_date,
                    language=language,
                    sentences=sentences,
                    timeout_s=args.codex_timeout_s,
                )

            if args.backup_dir and resume_path.exists():
                backup_base = channel_dir / Path(args.backup_dir)
                backup_base.mkdir(parents=True, exist_ok=True)
                backup_path = backup_base / resume_path.name
                backup_path.write_text(resume_path.read_text(encoding="utf-8", errors="ignore"), encoding="utf-8")

            resume_path.write_text(md, encoding="utf-8")
            cur.execute("UPDATE videos SET link_resume=? WHERE id=?", (str(resume_rel), r.id))
            con.commit()
            changed += 1
            print(f"[{channel_slug}] seq={r.seq_num} video_id={r.video_id} -> {resume_rel}")

        print(f"Done. Updated {changed} videos.")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
