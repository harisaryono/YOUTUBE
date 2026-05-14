#!/usr/bin/env python3
"""Backfill explicit per-channel ranks for legacy video navigation.

Rank 1 means the newest visible video in a channel.
"""

from __future__ import annotations

import argparse
from typing import Optional

from database_optimized import OptimizedDatabase


def _resolve_channel_db_id(db: OptimizedDatabase, token: str) -> Optional[int]:
    token = str(token or "").strip()
    if not token:
        return None
    if token.isdigit():
        return int(token)
    with db._get_cursor() as cursor:  # noqa: SLF001 - local maintenance script
        row = cursor.execute(
            """
            SELECT id
            FROM channels
            WHERE channel_id = ? OR channel_id = ? OR channel_name = ?
            LIMIT 1
            """,
            (token, token.lstrip("@"), token),
        ).fetchone()
    return int(row["id"]) if row else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute explicit channel ranks.")
    parser.add_argument(
        "--channel-id",
        help="Optional channel db id, channel token, or channel name. If omitted, all channels are rebuilt.",
    )
    args = parser.parse_args()

    db = OptimizedDatabase()
    channel_db_id = _resolve_channel_db_id(db, args.channel_id) if args.channel_id else None

    if args.channel_id and channel_db_id is None:
        raise SystemExit(f"Channel '{args.channel_id}' tidak ditemukan.")

    if channel_db_id is None:
        total = db.recompute_channel_ranks()
        print(f"Recomputed ranks for all channels. Updated rows: {total}")
    else:
        total = db.recompute_channel_ranks(channel_db_id)
        print(f"Recomputed ranks for channel {channel_db_id}. Updated rows: {total}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
