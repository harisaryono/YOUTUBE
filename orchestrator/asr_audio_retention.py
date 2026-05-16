from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "youtube_transcripts.db"
DEFAULT_AUDIO_DIR = PROJECT_ROOT / "uploads" / "audio"


def _resolve(value: str | Path) -> Path:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def _size_text(value: int) -> str:
    size = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def trim_completed_audio(db_path: Path, audio_dir: Path, *, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
    db_path = _resolve(db_path)
    audio_dir = _resolve(audio_dir)
    out: dict[str, Any] = {
        "db_path": str(db_path),
        "audio_dir": str(audio_dir),
        "dry_run": bool(dry_run),
        "scanned": 0,
        "eligible": 0,
        "trimmed": 0,
        "already_gone": 0,
        "skipped_outside_audio_dir": 0,
        "skipped_part_file": 0,
        "bytes_trimmed": 0,
        "trimmed_size": "0 B",
        "samples": [],
    }
    if not db_path.exists():
        out["error"] = "database_not_found"
        return out

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=8000")
    try:
        sql = """
            SELECT a.video_id,
                   COALESCE(a.audio_file_path, '') AS audio_file_path,
                   COALESCE(a.status, '') AS audio_status,
                   COALESCE(v.transcript_downloaded, 0) AS transcript_downloaded,
                   COALESCE(v.transcript_file_path, '') AS transcript_file_path,
                   COALESCE(v.transcript_text, '') AS transcript_text
            FROM video_audio_assets a
            JOIN videos v ON v.video_id = a.video_id
            WHERE COALESCE(a.audio_file_path, '') != ''
              AND COALESCE(v.transcript_downloaded, 0) = 1
              AND (COALESCE(v.transcript_file_path, '') != '' OR COALESCE(v.transcript_text, '') != '')
              AND COALESCE(a.status, '') IN ('downloaded', 'consumed')
            ORDER BY a.updated_at ASC, a.video_id ASC
        """
        params: list[Any] = []
        if int(limit or 0) > 0:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = list(conn.execute(sql, params).fetchall())
        out["scanned"] = len(rows)
        for row in rows:
            video_id = str(row["video_id"] or "")
            path = _resolve(str(row["audio_file_path"] or ""))
            if path.name.endswith(".part"):
                out["skipped_part_file"] += 1
                continue
            if not _inside(path, audio_dir):
                out["skipped_outside_audio_dir"] += 1
                continue
            out["eligible"] += 1
            exists = path.exists() and path.is_file()
            size = int(path.stat().st_size) if exists else 0
            if exists:
                if not dry_run:
                    path.unlink()
                out["trimmed"] += 1
                out["bytes_trimmed"] += size
            else:
                out["already_gone"] += 1
            if not dry_run:
                conn.execute(
                    """
                    UPDATE video_audio_assets
                       SET status = 'consumed',
                           file_size_bytes = CASE WHEN ? THEN 0 ELSE file_size_bytes END,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE video_id = ?
                    """,
                    (1 if exists else 0, video_id),
                )
            if len(out["samples"]) < 20:
                out["samples"].append({"video_id": video_id, "path": str(path), "bytes": size})
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    out["trimmed_size"] = _size_text(int(out["bytes_trimmed"] or 0))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Trim completed ASR audio files")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--audio-dir", default=str(DEFAULT_AUDIO_DIR))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    payload = trim_completed_audio(
        _resolve(args.db),
        _resolve(args.audio_dir),
        limit=int(args.limit or 0),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
