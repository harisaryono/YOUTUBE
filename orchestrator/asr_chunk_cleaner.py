"""
Safe ASR chunk cleaner.

Prinsip keselamatan:
- Jangan hapus chunk untuk video yang belum selesai ditranskrip.
- Hapus hanya folder chunk per-video jika DB utama sudah menandai transcript_downloaded=1
  dan transcript_file_path/transcript_text tersedia.
- Skip run_dir yang masih punya active job.
- Default dry-run tersedia untuk audit sebelum eksekusi.

Contoh:
  python -m orchestrator.asr_chunk_cleaner status --top 20
  python -m orchestrator.asr_chunk_cleaner clean --dry-run --older-than-hours 1
  python -m orchestrator.asr_chunk_cleaner clean --older-than-hours 1
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs" / "orchestrator"
YOUTUBE_DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts.db"


def _size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for filename in files:
            try:
                total += int((Path(root) / filename).stat().st_size)
            except OSError:
                continue
    return total


def _human_size(value: int) -> str:
    size = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _age_hours(path: Path) -> float:
    try:
        return (time.time() - path.stat().st_mtime) / 3600
    except OSError:
        return 0.0


def _running_run_dirs(state: OrchestratorState) -> set[str]:
    try:
        rows = state.list_running_jobs()
    except Exception:
        return set()
    result: set[str] = set()
    for row in rows:
        raw = str(row.get("run_dir") or "").strip()
        if raw:
            try:
                result.add(str(Path(raw).resolve()))
            except Exception:
                result.add(raw)
    return result


def _connect_youtube_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(YOUTUBE_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _video_transcribed_map(video_ids: list[str]) -> dict[str, bool]:
    if not video_ids or not YOUTUBE_DB_PATH.exists():
        return {}
    result: dict[str, bool] = {}
    conn = _connect_youtube_db()
    try:
        for start in range(0, len(video_ids), 500):
            batch = video_ids[start:start + 500]
            placeholders = ",".join("?" for _ in batch)
            rows = conn.execute(
                f"""
                SELECT video_id,
                       COALESCE(transcript_downloaded, 0) AS transcript_downloaded,
                       COALESCE(transcript_file_path, '') AS transcript_file_path,
                       COALESCE(transcript_text, '') AS transcript_text
                FROM videos
                WHERE video_id IN ({placeholders})
                """,
                batch,
            ).fetchall()
            for row in rows:
                video_id = str(row["video_id"] or "").strip()
                downloaded = int(row["transcript_downloaded"] or 0) == 1
                has_payload = bool(str(row["transcript_file_path"] or "").strip() or str(row["transcript_text"] or "").strip())
                result[video_id] = bool(downloaded and has_payload)
    finally:
        conn.close()
    return result


def _iter_chunk_video_dirs() -> list[Path]:
    if not RUNS_DIR.exists():
        return []
    dirs: list[Path] = []
    for chunks_dir in RUNS_DIR.glob("**/chunks"):
        if not chunks_dir.is_dir():
            continue
        for video_dir in chunks_dir.iterdir():
            if video_dir.is_dir():
                dirs.append(video_dir)
    return dirs


def build_status(top: int = 20) -> dict[str, Any]:
    video_dirs = _iter_chunk_video_dirs()
    total = 0
    items: list[tuple[int, Path]] = []
    for path in video_dirs:
        size = _size_bytes(path)
        total += size
        items.append((size, path))
    items.sort(reverse=True, key=lambda item: item[0])
    return {
        "chunk_video_dirs": len(video_dirs),
        "total_bytes": total,
        "total_size": _human_size(total),
        "top": [
            {"size_bytes": size, "size": _human_size(size), "path": str(path.relative_to(PROJECT_ROOT))}
            for size, path in items[: max(1, int(top or 20))]
        ],
    }


def clean_transcribed_chunks(*, older_than_hours: float = 1.0, dry_run: bool = False, limit: int = 0) -> dict[str, Any]:
    state = OrchestratorState()
    try:
        running_dirs = _running_run_dirs(state)
    finally:
        state.close()

    chunk_dirs = _iter_chunk_video_dirs()
    video_ids = sorted({path.name for path in chunk_dirs})
    transcribed = _video_transcribed_map(video_ids)

    result: dict[str, Any] = {
        "success": True,
        "dry_run": dry_run,
        "scanned": 0,
        "deleted": 0,
        "skipped_running": 0,
        "skipped_young": 0,
        "skipped_not_transcribed": 0,
        "bytes_deleted": 0,
        "deleted_size": "0 B",
        "samples_deleted": [],
    }
    max_delete = max(0, int(limit or 0))

    for path in chunk_dirs:
        if max_delete and result["deleted"] >= max_delete:
            break
        result["scanned"] += 1
        try:
            run_dir = next(parent for parent in path.parents if parent.parent == RUNS_DIR)
            run_dir_resolved = str(run_dir.resolve())
        except Exception:
            run_dir_resolved = ""
        if run_dir_resolved and run_dir_resolved in running_dirs:
            result["skipped_running"] += 1
            continue
        if _age_hours(path) < float(older_than_hours or 0):
            result["skipped_young"] += 1
            continue
        video_id = path.name
        if not transcribed.get(video_id, False):
            result["skipped_not_transcribed"] += 1
            continue
        size = _size_bytes(path)
        if dry_run:
            result["deleted"] += 1
            result["bytes_deleted"] += size
        else:
            shutil.rmtree(path, ignore_errors=True)
            result["deleted"] += 1
            result["bytes_deleted"] += size
        if len(result["samples_deleted"]) < 20:
            result["samples_deleted"].append({"video_id": video_id, "size": _human_size(size), "path": str(path.relative_to(PROJECT_ROOT))})

    result["deleted_size"] = _human_size(int(result["bytes_deleted"] or 0))
    return result


def _cmd_status(args: argparse.Namespace) -> int:
    print(json.dumps(build_status(top=int(args.top or 20)), indent=2, ensure_ascii=False))
    return 0


def _cmd_clean(args: argparse.Namespace) -> int:
    payload = clean_transcribed_chunks(
        older_than_hours=float(args.older_than_hours),
        dry_run=bool(args.dry_run),
        limit=int(args.limit or 0),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if payload.get("success") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safely clean ASR chunks only for already-transcribed videos")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("status", help="Show ASR chunk storage usage")
    p.add_argument("--top", type=int, default=20)
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser("clean", help="Delete chunk dirs only when the video is already transcribed")
    p.add_argument("--older-than-hours", type=float, default=1.0)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_clean)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
