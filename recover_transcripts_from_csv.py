#!/usr/bin/env python3
"""
Recover transcript untuk daftar video spesifik dari CSV.
Jika transcript berhasil didownload, DB dan file transcript diperbarui.
Jika subtitle memang tidak ada, video ditandai no_subtitle dan state stale dibersihkan.
Jika terjadi error fatal/rate-limit, state tidak diubah.
"""

from __future__ import annotations

import argparse
import csv
import gc
import os
import subprocess
import sys
import time
from pathlib import Path

from recover_transcripts import BASE_DIR, TranscriptRecoverer, logger

MAX_CONSECUTIVE_FATAL = max(5, int(str(os.getenv("YT_TRANSCRIPT_MAX_CONSECUTIVE_FATAL", "20")).strip() or "20"))
MAX_CONSECUTIVE_HARD_BLOCKS = max(
    1,
    int(str(os.getenv("YT_TRANSCRIPT_MAX_CONSECUTIVE_HARD_BLOCKS", "3")).strip() or "3"),
)
RETRY_LATER_HOURS = max(1, int(str(os.getenv("YT_TRANSCRIPT_RETRY_LATER_HOURS", "24")).strip() or "24"))
LONG_VIDEO_ASR_CHUNK_SECONDS = max(
    600,
    int(str(os.getenv("YT_TRANSCRIPT_LONG_VIDEO_ASR_CHUNK_SECONDS", "3600")).strip() or "3600"),
)
LONG_VIDEO_ASR_OVERLAP_SECONDS = max(
    0,
    int(str(os.getenv("YT_TRANSCRIPT_LONG_VIDEO_ASR_OVERLAP_SECONDS", "2")).strip() or "2"),
)
LONG_VIDEO_ASR_PROVIDERS = str(os.getenv("YT_TRANSCRIPT_LONG_VIDEO_ASR_PROVIDERS", "groq,nvidia")).strip() or "groq,nvidia"


def safe_channel_slug(channel_id: str) -> str:
    return (
        str(channel_id or "")
        .replace("@", "")
        .replace(" ", "_")
        .replace("?", "_")
        .replace(":", "_")
    )


def load_targets(csv_path: Path, limit: int) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    ids = [str(row.get("video_id") or "").strip() for row in rows]
    ids = [video_id for video_id in ids if video_id]
    if limit > 0:
        ids = ids[:limit]
    return ids


def fetch_video_rows(recoverer: TranscriptRecoverer, video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    placeholders = ",".join("?" for _ in video_ids)
    with recoverer.db._get_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT v.video_id, v.title, v.transcript_file_path, v.transcript_language,
                   c.channel_name, c.channel_id
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE v.video_id IN ({placeholders})
            """,
            video_ids,
        )
        rows = [dict(row) for row in cursor.fetchall()]
    row_map = {str(row["video_id"]): row for row in rows}
    return [row_map[video_id] for video_id in video_ids if video_id in row_map]


def mark_no_subtitle(recoverer: TranscriptRecoverer, video_id: str) -> None:
    with recoverer.db._get_cursor() as cursor:
        cursor.execute(
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


def mark_blocked_member_only(recoverer: TranscriptRecoverer, video_id: str, reason: str) -> None:
    with recoverer.db._get_cursor() as cursor:
        cursor.execute(
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
            (reason or "blocked_member_only", video_id),
        )


def mark_retry_later(recoverer: TranscriptRecoverer, video_id: str, reason: str) -> None:
    recoverer.db.mark_video_transcript_retry_later(
        video_id=video_id,
        reason=reason or "retry_later",
        retry_after_hours=RETRY_LATER_HOURS,
    )


def looks_like_too_long_failure(recoverer: TranscriptRecoverer, reason: str) -> bool:
    text = str(reason or "").strip()
    if not text:
        return False
    try:
        return bool(recoverer._looks_like_video_too_long_error(text))
    except Exception:
        lowered = text.lower()
        return any(
            marker in lowered
            for marker in [
                "too long",
                "request too large",
                "payload too large",
                "entity too large",
                "content too large",
                "413",
                "maximum length",
                "length exceeded",
                "video too long",
            ]
        )


def run_long_video_asr_split(
    recoverer: TranscriptRecoverer,
    row: dict,
    run_dir: Path,
) -> tuple[dict | None, str]:
    video_id = str(row.get("video_id") or "").strip()
    channel_id = str(row.get("channel_id") or "").strip()
    channel_name = str(row.get("channel_name") or "").strip()
    if not video_id:
        return None, "missing_video_id"

    asr_script = Path(__file__).resolve().parent / "recover_asr_transcripts.py"
    asr_run_dir = run_dir / "asr_long_video" / safe_channel_slug(channel_id) / video_id
    asr_run_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(asr_script),
        "--video-id",
        video_id,
        "--run-dir",
        str(asr_run_dir),
        "--providers",
        LONG_VIDEO_ASR_PROVIDERS,
        "--language",
        "multi",
        "--chunk-seconds",
        str(LONG_VIDEO_ASR_CHUNK_SECONDS),
        "--overlap-seconds",
        str(LONG_VIDEO_ASR_OVERLAP_SECONDS),
    ]

    logger.info(
        "   🎧 Fallback ASR split 1 jam untuk %s (%s) via %s",
        video_id,
        channel_name,
        LONG_VIDEO_ASR_PROVIDERS,
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()[:1200]
        logger.warning("   ⚠️  ASR split 1 jam gagal untuk %s: %s", video_id, detail[:300])
        return None, detail or "asr_long_video_failed"

    file_path = recoverer.db.get_transcript_file(video_id)
    video_row = recoverer.db.get_video_by_id(video_id) or {}
    transcript_text = ""
    if file_path and file_path.exists():
        try:
            transcript_text = file_path.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            transcript_text = ""

    if not transcript_text:
        try:
            transcript_text = str(recoverer.db.get_transcript_content(video_id) or "").strip()
        except Exception:
            transcript_text = ""

    if not transcript_text and file_path and file_path.exists():
        try:
            transcript_text = file_path.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            transcript_text = ""

    if not transcript_text:
        return None, "asr_long_video_empty_output"

    if not file_path or not file_path.exists():
        safe_ch = safe_channel_slug(channel_id)
        text_dir = Path(BASE_DIR) / safe_ch / "text"
        text_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        file_path = text_dir / f"{video_id}_transcript_{timestamp}.txt"
        file_path.write_text(transcript_text + "\n", encoding="utf-8")
        recoverer.db.update_video_with_transcript(
            video_id=video_id,
            transcript_file_path=str(file_path),
            summary_file_path="",
            transcript_language=str(video_row.get("transcript_language") or "multi"),
            word_count=int(video_row.get("word_count") or len(transcript_text.split())),
            line_count=int(video_row.get("line_count") or len([line for line in transcript_text.splitlines() if line.strip()])),
            transcript_text=transcript_text,
        )

    return (
        {
            "formatted": transcript_text,
            "language": str(video_row.get("transcript_language") or "multi"),
            "word_count": int(video_row.get("word_count") or len(transcript_text.split())),
            "line_count": int(video_row.get("line_count") or len([line for line in transcript_text.splitlines() if line.strip()])),
            "transcript_file_path": str(file_path),
        },
        "ok",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="CSV target yang berisi kolom video_id")
    parser.add_argument("--run-dir", required=True, help="Directory log/report run")
    parser.add_argument("--limit", type=int, default=0, help="Batasi jumlah target")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    report_path = run_dir / "recover_report.csv"
    savesubs_status_path = run_dir / "savesubs_status_summary.csv"
    retry_later_path = run_dir / "retry_later.csv"
    target_ids = load_targets(csv_path, args.limit)
    if not target_ids:
        logger.info("Tidak ada target video_id pada CSV.")
        return 0

    recoverer = TranscriptRecoverer()
    rows = fetch_video_rows(recoverer, target_ids)
    if not rows:
        logger.info("Target video tidak ditemukan di database.")
        return 1

    success_count = 0
    no_subtitle_count = 0
    blocked_count = 0
    proxy_block_count = 0
    retry_later_count = 0
    asr_split_count = 0
    skipped_long_video_count = 0
    error_count = 0
    consecutive_fatal = 0
    consecutive_hard_blocks = 0
    stopped_early = False
    savesubs_status_counts: dict[str, int] = {
        "available": 0,
        "no_subtitle": 0,
        "blocked": 0,
        "unknown": 0,
        "error": 0,
    }

    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "video_id",
                "channel_name",
                "status",
                "language",
                "transcript_file_path",
                "note",
            ],
        )
        writer.writeheader()
        with retry_later_path.open("w", encoding="utf-8", newline="") as retry_f:
            retry_writer = csv.DictWriter(
                retry_f,
                fieldnames=[
                    "video_id",
                    "channel_name",
                    "reason",
                    "retry_after_hours",
                ],
            )
            retry_writer.writeheader()

            for idx, row in enumerate(rows, 1):
                if idx > 1 and idx % 50 == 0:
                    logger.info("🔄 Recreating session & running GC to prevent memory leaks...")
                    recoverer.session = recoverer._create_session()
                    gc.collect()

                video_id = str(row["video_id"])
                logger.info(
                    f"[{idx}/{len(rows)}] Recover transcript {video_id} - {row['channel_name']}"
                )

                existing_rel = str(row.get("transcript_file_path") or "").strip()
                existing_path = Path(existing_rel) if existing_rel else None
                existing_ok = (
                    existing_path is not None
                    and existing_rel not in {"", "uploads/"}
                    and existing_path.exists()
                    and existing_path.is_file()
                )
                if existing_ok:
                    consecutive_fatal = 0
                    consecutive_hard_blocks = 0
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "skip_exists",
                            "language": row.get("transcript_language") or "",
                            "transcript_file_path": str(existing_path),
                            "note": "file transcript already exists",
                        }
                    )
                    continue

                try:
                    result, outcome = recoverer.download_transcript(video_id)
                except Exception as exc:
                    result, outcome = None, "fatal"
                    recoverer.last_transcript_failure_reason = str(exc)
                    logger.error(f"   💥 Error tak terduga: {exc}")

                savesubs_status = str(getattr(recoverer, "last_savesubs_status", "") or "").strip().lower()
                if savesubs_status in savesubs_status_counts:
                    savesubs_status_counts[savesubs_status] += 1
                elif savesubs_status:
                    savesubs_status_counts["error"] += 1
                else:
                    savesubs_status_counts["error"] += 1

                asr_split_used = False
                if not result and looks_like_too_long_failure(
                    recoverer,
                    str(getattr(recoverer, "last_transcript_failure_reason", "") or ""),
                ):
                    asr_result, asr_outcome = run_long_video_asr_split(recoverer, row, run_dir)
                    if asr_result:
                        result = asr_result
                        outcome = asr_outcome
                        asr_split_used = True
                        asr_split_count += 1
                    else:
                        outcome = "skipped_long_video"
                        recoverer.last_transcript_failure_reason = str(asr_outcome or "asr_long_video_failed")

                if result:
                    consecutive_fatal = 0
                    consecutive_hard_blocks = 0
                    file_path_str = str(result.get("transcript_file_path") or "").strip()
                    file_path = Path(file_path_str).expanduser() if file_path_str else None
                    if file_path is not None and not file_path.is_absolute():
                        candidate = Path(__file__).resolve().parent / file_path
                        if candidate.exists():
                            file_path = candidate
                    if file_path is None or not file_path.exists():
                        safe_ch = safe_channel_slug(str(row["channel_id"]))
                        text_dir = Path(BASE_DIR) / safe_ch / "text"
                        text_dir.mkdir(parents=True, exist_ok=True)

                        timestamp = time.strftime("%Y%m%d_%H%M%S")
                        file_name = f"{video_id}_transcript_{timestamp}.txt"
                        file_path = text_dir / file_name
                        file_path.write_text(str(result["formatted"]), encoding="utf-8")
                    recoverer.db.update_video_with_transcript(
                        video_id=video_id,
                        transcript_file_path=str(file_path),
                        summary_file_path="",
                        transcript_language=str(result["language"]),
                        word_count=int(result["word_count"]),
                        line_count=int(result["line_count"]),
                        transcript_text=str(result["formatted"]),
                    )
                    success_count += 1
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "downloaded_asr_split" if asr_split_used else "downloaded",
                            "language": result["language"],
                            "transcript_file_path": str(file_path),
                            "note": "fallback=asr_split_1h" if asr_split_used else "",
                        }
                    )
                    if asr_split_used:
                        logger.info(f"   ✅ ASR split 1 jam tersimpan ke {file_path}")
                    else:
                        logger.info(f"   ✅ Transcript tersimpan ke {file_path}")
                elif outcome == "retry_later":
                    consecutive_fatal = 0
                    consecutive_hard_blocks = 0
                    retry_later_count += 1
                    reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip() or "retry_later"
                    mark_retry_later(recoverer, video_id, reason)
                    retry_writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "reason": reason[:500],
                            "retry_after_hours": RETRY_LATER_HOURS,
                        }
                    )
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "retry_later",
                            "language": "",
                            "transcript_file_path": str(row.get("transcript_file_path") or ""),
                            "note": f"challenge/rate-limit; retry_after={RETRY_LATER_HOURS}h",
                        }
                    )
                    logger.info(
                        f"   ⏭️  Tantangan/rate-limit terdeteksi. Dijadwalkan ulang {RETRY_LATER_HOURS} jam lagi."
                    )
                elif outcome == "proxy_block":
                    consecutive_fatal = 0
                    consecutive_hard_blocks += 1
                    proxy_block_count += 1
                    reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip() or "proxy_block"
                    mark_retry_later(recoverer, video_id, reason)
                    retry_writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "reason": reason[:500],
                            "retry_after_hours": RETRY_LATER_HOURS,
                        }
                    )
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "proxy_block",
                            "language": "",
                            "transcript_file_path": str(row.get("transcript_file_path") or ""),
                            "note": f"proxy block; retry_after={RETRY_LATER_HOURS}h",
                        }
                    )
                    logger.info(
                        f"   🧱 Proxy block terdeteksi. Dijadwalkan ulang {RETRY_LATER_HOURS} jam lagi."
                    )
                    if consecutive_hard_blocks >= MAX_CONSECUTIVE_HARD_BLOCKS:
                        logger.error(
                            f"🛑 BERHENTI: {consecutive_hard_blocks} hard block berturut-turut "
                            f"(threshold={MAX_CONSECUTIVE_HARD_BLOCKS})."
                        )
                        stopped_early = True
                        break
                elif outcome == "blocked":
                    consecutive_fatal = 0
                    consecutive_hard_blocks += 1
                    blocked_count += 1
                    reason = str(getattr(recoverer, "last_transcript_failure_reason", "") or "").strip() or "blocked_member_only"
                    mark_blocked_member_only(recoverer, video_id, reason)
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "blocked",
                            "language": "no_subtitle",
                            "transcript_file_path": str(row.get("transcript_file_path") or ""),
                            "note": reason[:500],
                        }
                    )
                    logger.info("   ⛔ SaveSubs blocked/member-only. Ditandai terminal tanpa fallback.")
                    if consecutive_hard_blocks >= MAX_CONSECUTIVE_HARD_BLOCKS:
                        logger.error(
                            f"🛑 BERHENTI: {consecutive_hard_blocks} hard block berturut-turut "
                            f"(threshold={MAX_CONSECUTIVE_HARD_BLOCKS})."
                        )
                        stopped_early = True
                        break
                elif outcome == "fatal":
                    consecutive_fatal += 1
                    consecutive_hard_blocks = 0
                    error_count += 1
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "fatal_error",
                            "language": "",
                            "transcript_file_path": str(row.get("transcript_file_path") or ""),
                            "note": f"fatal technical error (consecutive_fatal={consecutive_fatal})",
                        }
                    )
                    logger.info(
                        f"   ❌ Gagal fatal/teknis [{consecutive_fatal}/{MAX_CONSECUTIVE_FATAL}]. State tidak diubah."
                    )
                    if consecutive_fatal >= MAX_CONSECUTIVE_FATAL:
                        logger.error(
                            f"🛑 BERHENTI: {MAX_CONSECUTIVE_FATAL} kegagalan fatal berturut-turut. "
                            f"Jalankan ulang nanti."
                        )
                        break
                elif outcome == "skipped_long_video":
                    consecutive_fatal = 0
                    consecutive_hard_blocks = 0
                    skipped_long_video_count += 1
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "skipped_long_video",
                            "language": "",
                            "transcript_file_path": str(row.get("transcript_file_path") or ""),
                            "note": str(getattr(recoverer, "last_transcript_failure_reason", "") or "asr_long_video_failed")[:500],
                        }
                    )
                    logger.info("   ⏭️  Video terlalu panjang dan fallback ASR split gagal. Dilewati.")
                else:
                    consecutive_fatal = 0
                    consecutive_hard_blocks = 0
                    mark_no_subtitle(recoverer, video_id)
                    no_subtitle_count += 1
                    writer.writerow(
                        {
                            "video_id": video_id,
                            "channel_name": row["channel_name"],
                            "status": "no_subtitle",
                            "language": "no_subtitle",
                            "transcript_file_path": "",
                            "note": "subtitle not available",
                        }
                    )
                    logger.info("   ❌ Subtitle tidak tersedia. Ditandai no_subtitle.")
                # Inter-video pacing is handled inside recoverer.download_transcript()

    logger.info(
        f"RINGKASAN: downloaded={success_count}, "
        f"asr_split={asr_split_count}, skipped_long_video={skipped_long_video_count}, "
        f"no_subtitle={no_subtitle_count}, blocked={blocked_count}, proxy_block={proxy_block_count}, retry_later={retry_later_count}, fatal_error={error_count}"
    )
    with savesubs_status_path.open("w", encoding="utf-8", newline="") as status_f:
        status_writer = csv.DictWriter(status_f, fieldnames=["status", "count"])
        status_writer.writeheader()
        for status in ["available", "no_subtitle", "blocked", "unknown", "error"]:
            status_writer.writerow({"status": status, "count": savesubs_status_counts.get(status, 0)})
    logger.info(f"Report: {report_path}")
    logger.info(f"SaveSubs status summary: {savesubs_status_path}")
    logger.info(f"Retry-later queue: {retry_later_path}")
    return 2 if stopped_early else 0


if __name__ == "__main__":
    raise SystemExit(main())
