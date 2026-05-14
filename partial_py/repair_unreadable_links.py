#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import shard_storage

try:
    import zstandard as zstd
except Exception:  # pragma: no cover - optional dependency guard
    zstd = None  # type: ignore[assignment]


@dataclass(frozen=True)
class VideoRow:
    id: int
    slug: str
    video_id: str
    status_download: str
    link_file: Optional[str]
    link_resume: Optional[str]


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def iter_video_rows(con: sqlite3.Connection, *, channels: list[str]) -> Iterable[VideoRow]:
    where = []
    params: list[object] = []
    if channels:
        placeholders = ",".join("?" for _ in channels)
        where.append(f"c.slug IN ({placeholders})")
        params.extend(channels)
    sql = """
        SELECT v.id, c.slug, v.video_id, v.status_download, v.link_file, v.link_resume
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    cur = con.execute(sql, params)
    for row in cur.fetchall():
        yield VideoRow(
            id=int(row[0]),
            slug=str(row[1]),
            video_id=str(row[2]),
            status_download=str(row[3] or ""),
            link_file=(str(row[4]) if row[4] is not None else None),
            link_resume=(str(row[5]) if row[5] is not None else None),
        )


def _read_plain(base: Path, rel_path: Optional[str]) -> Optional[bytes]:
    p = shard_storage.safe_resolve(base, rel_path)
    if p and p.exists() and p.is_file():
        try:
            return p.read_bytes()
        except Exception:
            return None
    return None


def _read_shard_blob(base: Path, rel_path: Optional[str]) -> tuple[Optional[bytes], Optional[dict], str]:
    entry = shard_storage.get_entry(base, rel_path)
    if not entry:
        return None, None, "entry_not_found"
    shard_name = entry.get("shard")
    if not isinstance(shard_name, str) or not shard_name.strip():
        return None, None, "invalid_shard_name"
    shard_path = shard_storage.safe_resolve((base / shard_storage.SHARD_DIR), shard_name)
    if not shard_path or (not shard_path.exists()) or (not shard_path.is_file()):
        return None, None, "invalid_shard_path"
    try:
        offset = int(entry.get("offset"))
        length = int(entry.get("length"))
    except Exception:
        return None, None, "invalid_entry_bounds"
    if offset < 0 or length <= 0:
        return None, None, "invalid_entry_bounds"
    try:
        with shard_path.open("rb") as f:
            f.seek(offset)
            blob = f.read(length)
    except Exception:
        return None, None, "shard_io_failed"
    if len(blob) != length:
        return None, None, f"short_read:{len(blob)}/{length}"
    return blob, entry, "ok"


def _decode_blob(blob: bytes, entry: dict) -> tuple[bool, str]:
    codec = str(entry.get("codec") or "zstd").strip().lower()
    if codec in {"raw", "plain", "none"}:
        return True, "raw"
    if codec not in {"", "zstd"}:
        return False, f"unsupported_codec:{codec}"
    if zstd is None:
        return False, "zstd_unavailable"

    dctx = zstd.ZstdDecompressor()
    out_size = 0
    try:
        out_size = max(0, int(entry.get("orig_size") or 0))
    except Exception:
        out_size = 0

    try:
        if out_size > 0:
            dctx.decompress(blob, max_output_size=out_size)
        else:
            dctx.decompress(blob)
        return True, "zstd_ok"
    except Exception:
        # Fallback stream mode: handles some frames without content size.
        try:
            with dctx.stream_reader(io.BytesIO(blob)) as r:
                while r.read(65536):
                    pass
            return True, "zstd_stream_ok"
        except Exception as ex2:
            return False, f"zstd_decode_failed:{type(ex2).__name__}:{ex2}"


def link_is_readable(out_root: Path, slug: str, rel_path: Optional[str]) -> tuple[bool, str]:
    base = out_root / slug
    if not rel_path or not str(rel_path).strip():
        return False, "empty_link"

    plain = _read_plain(base, rel_path)
    if plain is not None:
        return True, "plain_ok"

    blob, entry, status = _read_shard_blob(base, rel_path)
    if blob is None or entry is None:
        return False, status
    ok, reason = _decode_blob(blob, entry)
    return ok, reason


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Perbaiki kasus link 'ada' tetapi gagal dibaca. "
            "Aksi: text unreadable pada status_download='downloaded' -> pending; "
            "resume unreadable pada link_resume terisi -> link_resume=NULL."
        )
    )
    parser.add_argument("--db", default="channels.db", help="Path SQLite DB (default: channels.db)")
    parser.add_argument("--out-root", default="out", help="Folder output per channel (default: out)")
    parser.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Batasi channel slug (repeatable). Contoh: --channel FirandaAndirjaOfficial",
    )
    parser.add_argument(
        "--no-fix-text",
        dest="fix_text",
        action="store_false",
        help="Jangan ubah status text unreadable menjadi pending.",
    )
    parser.add_argument(
        "--no-fix-resume",
        dest="fix_resume",
        action="store_false",
        help="Jangan clear link_resume unreadable.",
    )
    parser.set_defaults(fix_text=True, fix_resume=True)
    parser.add_argument("--dry-run", action="store_true", help="Hanya tampilkan perubahan, tanpa update DB")
    parser.add_argument("--no-backup", action="store_true", help="Jangan buat backup DB sebelum update")
    parser.add_argument("--limit-print", type=int, default=25, help="Jumlah contoh perubahan yang ditampilkan")
    args = parser.parse_args()

    if zstd is None:
        raise RuntimeError("Module 'zstandard' tidak tersedia. Install dulu: pip install zstandard")

    db_path = Path(args.db)
    out_root = Path(args.out_root)
    if not db_path.exists():
        raise FileNotFoundError(f"DB tidak ditemukan: {db_path.resolve()}")

    backup_path: Optional[Path] = None
    if not args.dry_run and not args.no_backup:
        out_dir = Path("tmp")
        out_dir.mkdir(parents=True, exist_ok=True)
        backup_path = out_dir / f"{db_path.name}.bak-unreadable-fix-{now_stamp()}"
        shutil.copy2(db_path, backup_path)

    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys=ON;")
        rows = list(iter_video_rows(con, channels=list(args.channel or [])))

        text_checked = 0
        resume_checked = 0
        text_unreadable = 0
        resume_unreadable = 0

        to_pending: list[int] = []
        to_clear_resume: list[int] = []
        sample: list[str] = []

        for r in rows:
            lf = (r.link_file or "").strip()
            lr = (r.link_resume or "").strip()

            if args.fix_text and r.status_download == "downloaded" and lf:
                text_checked += 1
                ok_txt, reason_txt = link_is_readable(out_root, r.slug, lf)
                if not ok_txt:
                    text_unreadable += 1
                    to_pending.append(r.id)
                    if len(sample) < args.limit_print:
                        sample.append(f"TEXT->PENDING {r.slug} | {r.video_id} | {lf} | {reason_txt}")

            if args.fix_resume and lr:
                resume_checked += 1
                ok_resume, reason_resume = link_is_readable(out_root, r.slug, lr)
                if not ok_resume:
                    resume_unreadable += 1
                    to_clear_resume.append(r.id)
                    if len(sample) < args.limit_print:
                        sample.append(f"RESUME->NULL  {r.slug} | {r.video_id} | {lr} | {reason_resume}")

        # dedup ids to avoid redundant updates
        to_pending = sorted(set(to_pending))
        to_clear_resume = sorted(set(to_clear_resume))

        if args.dry_run:
            print(f"DB: {db_path.resolve()}")
            print(f"Out root: {out_root.resolve()}")
            print(f"Text checked:              {text_checked}")
            print(f"Resume checked:            {resume_checked}")
            print(f"Text unreadable found:     {text_unreadable}")
            print(f"Resume unreadable found:   {resume_unreadable}")
            print(f"Will set pending:          {len(to_pending)}")
            print(f"Will clear link_resume:    {len(to_clear_resume)}")
            if sample:
                print("\nContoh perubahan:")
                for s in sample:
                    print(f"- {s}")
            return 0

        if to_pending or to_clear_resume:
            con.execute("BEGIN;")
            if to_pending:
                con.executemany(
                    "UPDATE videos SET status_download='pending' WHERE id=?",
                    [(x,) for x in to_pending],
                )
            if to_clear_resume:
                con.executemany(
                    "UPDATE videos SET link_resume=NULL WHERE id=?",
                    [(x,) for x in to_clear_resume],
                )
            con.commit()

        print(f"DB: {db_path.resolve()}")
        if backup_path:
            print(f"Backup: {backup_path.resolve()}")
        print(f"Text checked:               {text_checked}")
        print(f"Resume checked:             {resume_checked}")
        print(f"Text unreadable fixed:      {len(to_pending)}")
        print(f"Resume unreadable fixed:    {len(to_clear_resume)}")
        if sample:
            print("\nContoh perubahan:")
            for s in sample:
                print(f"- {s}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

