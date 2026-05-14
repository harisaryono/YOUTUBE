#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import shard_storage

try:
    import zstandard as zstd
except Exception:  # pragma: no cover - optional dependency guard
    zstd = None  # type: ignore[assignment]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Compact file plain di out/<channel>/(text|resume) ke shard .zst per channel. "
            "DB link_file/link_resume tetap sama (path relatif lama), baca via index shard."
        )
    )
    p.add_argument("--db", default="channels.db", help="Path SQLite DB (default: channels.db)")
    p.add_argument("--out-root", default="out", help="Root output folder (default: out)")
    p.add_argument(
        "--kind",
        choices=["text", "resume", "both"],
        default="both",
        help="Jenis file yang akan di-compact",
    )
    p.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Batasi channel slug (repeatable).",
    )
    p.add_argument(
        "--max-shard-mb",
        type=int,
        default=128,
        help="Maks ukuran tiap shard .zst (MB), default 128",
    )
    p.add_argument("--zstd-level", type=int, default=10, help="Level kompresi zstd (default: 10)")
    p.add_argument(
        "--min-age-minutes",
        type=int,
        default=60,
        help="Skip file yang mtime lebih baru dari ini (hot/in-progress safety)",
    )
    p.add_argument("--dry-run", action="store_true", help="Hanya simulasi, tanpa write/delete")
    return p.parse_args()


def _query_rows(
    con: sqlite3.Connection,
    *,
    kind: str,
    channels: list[str],
) -> Iterable[Tuple[str, str]]:
    if kind == "text":
        field = "v.link_file"
        extra = "AND v.status_download='downloaded'"
    else:
        field = "v.link_resume"
        extra = ""

    where = [f"IFNULL({field}, '') != ''"]
    params: List[Any] = []
    if channels:
        placeholders = ",".join("?" for _ in channels)
        where.append(f"c.slug IN ({placeholders})")
        params.extend(channels)

    sql = f"""
        SELECT c.slug AS slug, {field} AS rel_path
        FROM videos v
        JOIN channels c ON c.id=v.channel_id
        WHERE {' AND '.join(where)} {extra}
    """
    for row in con.execute(sql, tuple(params)).fetchall():
        slug = str(row[0])
        rel = str(row[1])
        yield slug, rel


def _collect_candidates(
    con: sqlite3.Connection,
    *,
    kind: str,
    channels: list[str],
) -> Dict[str, List[str]]:
    out: Dict[str, set[str]] = {}
    for slug, rel in _query_rows(con, kind=kind, channels=channels):
        rel_norm = shard_storage.normalize_rel_path(rel)
        if not rel_norm:
            continue
        if not rel_norm.startswith(f"{kind}/"):
            continue
        out.setdefault(slug, set()).add(rel_norm)
    return {slug: sorted(vals) for slug, vals in out.items()}


def _compact_one_channel_kind(
    *,
    channel_root: Path,
    kind: str,
    rel_paths: List[str],
    max_shard_bytes: int,
    min_age_s: int,
    dry_run: bool,
    compressor: Any,
) -> Dict[str, int]:
    stats = {
        "seen": 0,
        "packed": 0,
        "skipped_missing": 0,
        "skipped_hot": 0,
        "skipped_sharded_only": 0,
        "errors": 0,
        "bytes_in": 0,
        "bytes_out": 0,
    }
    now = time.time()
    index = shard_storage.load_index(channel_root)
    entries = index.get("entries")
    if not isinstance(entries, dict):
        index = {"version": 1, "entries": {}}
        entries = index["entries"]

    for rel in rel_paths:
        stats["seen"] += 1
        plain_path = shard_storage.safe_resolve(channel_root, rel)
        if not plain_path or not plain_path.exists() or not plain_path.is_file():
            if shard_storage.link_exists(channel_root, rel):
                stats["skipped_sharded_only"] += 1
            else:
                stats["skipped_missing"] += 1
            continue

        try:
            st = plain_path.stat()
        except Exception:
            stats["errors"] += 1
            continue
        if min_age_s > 0 and (now - float(st.st_mtime) < min_age_s):
            stats["skipped_hot"] += 1
            continue

        if dry_run:
            stats["packed"] += 1
            stats["bytes_in"] += int(st.st_size)
            continue

        try:
            raw = plain_path.read_bytes()
        except Exception:
            stats["errors"] += 1
            continue
        compressed = compressor.compress(raw)
        shard_path = shard_storage.choose_append_shard(
            channel_root,
            kind=kind,
            incoming_bytes=len(compressed),
            max_shard_bytes=max_shard_bytes,
        )
        try:
            offset, length = shard_storage.append_blob(shard_path, compressed)
        except Exception:
            stats["errors"] += 1
            continue

        entry = {
            "codec": "zstd",
            "kind": kind,
            "shard": shard_path.name,
            "offset": int(offset),
            "length": int(length),
            "orig_size": int(len(raw)),
            "mtime": float(st.st_mtime),
            "sha1": hashlib.sha1(raw).hexdigest(),
        }
        entries[rel] = entry
        try:
            shard_storage.save_index(channel_root, index)
        except Exception:
            stats["errors"] += 1
            continue
        try:
            plain_path.unlink()
        except Exception:
            stats["errors"] += 1
            continue

        stats["packed"] += 1
        stats["bytes_in"] += len(raw)
        stats["bytes_out"] += len(compressed)

    return stats


def main() -> int:
    args = parse_args()
    if zstd is None:
        raise RuntimeError("Module 'zstandard' tidak tersedia. Install dulu: pip install zstandard")

    db_path = Path(args.db)
    out_root = Path(args.out_root)
    if not db_path.exists():
        raise FileNotFoundError(f"DB tidak ditemukan: {db_path.resolve()}")
    if not out_root.exists() or not out_root.is_dir():
        raise FileNotFoundError(f"Out root tidak ditemukan: {out_root.resolve()}")

    kinds = ["text", "resume"] if args.kind == "both" else [args.kind]
    max_shard_bytes = max(1, int(args.max_shard_mb)) * 1024 * 1024
    min_age_s = max(0, int(args.min_age_minutes)) * 60

    con = sqlite3.connect(str(db_path))
    try:
        global_stats = {
            "seen": 0,
            "packed": 0,
            "skipped_missing": 0,
            "skipped_hot": 0,
            "skipped_sharded_only": 0,
            "errors": 0,
            "bytes_in": 0,
            "bytes_out": 0,
        }
        compressor = zstd.ZstdCompressor(level=int(args.zstd_level))

        for kind in kinds:
            by_channel = _collect_candidates(con, kind=kind, channels=args.channel)
            for slug, rels in sorted(by_channel.items()):
                channel_root = out_root / slug
                if not channel_root.exists() or not channel_root.is_dir():
                    continue
                st = _compact_one_channel_kind(
                    channel_root=channel_root,
                    kind=kind,
                    rel_paths=rels,
                    max_shard_bytes=max_shard_bytes,
                    min_age_s=min_age_s,
                    dry_run=bool(args.dry_run),
                    compressor=compressor,
                )
                for k, v in st.items():
                    global_stats[k] += v
                if st["packed"] <= 0 and st["errors"] <= 0:
                    continue
                print(
                    f"[{kind}] {slug}: packed={st['packed']} seen={st['seen']} "
                    f"hot={st['skipped_hot']} missing={st['skipped_missing']} "
                    f"already_sharded={st['skipped_sharded_only']} errors={st['errors']}"
                )

        print("Done.")
        print(f"- Seen:            {global_stats['seen']}")
        print(f"- Packed:          {global_stats['packed']}")
        print(f"- Skipped hot:     {global_stats['skipped_hot']}")
        print(f"- Skipped missing: {global_stats['skipped_missing']}")
        print(f"- Already sharded: {global_stats['skipped_sharded_only']}")
        print(f"- Errors:          {global_stats['errors']}")
        print(f"- Input bytes:     {global_stats['bytes_in']}")
        print(f"- Shard bytes:     {global_stats['bytes_out']}")
        if global_stats["bytes_in"] > 0 and not args.dry_run:
            ratio = (global_stats["bytes_out"] * 100.0) / global_stats["bytes_in"]
            print(f"- Compression:     {ratio:.2f}% of original")
        if args.dry_run:
            print("(dry-run: tidak ada perubahan file/index)")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
