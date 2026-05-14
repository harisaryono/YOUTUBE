#!/usr/bin/env python3
"""Repair SQLite stats triggers to avoid cached_stats key conflicts."""

from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).resolve().with_name("youtube_transcripts.db")


def main() -> int:
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("PRAGMA foreign_keys=ON")
        con.executescript(
            """
            DROP TRIGGER IF EXISTS trg_videos_bump_stats_insert;
            DROP TRIGGER IF EXISTS trg_videos_bump_stats_update;
            DROP TRIGGER IF EXISTS trg_videos_bump_stats_delete;
            DROP TRIGGER IF EXISTS trg_channels_bump_stats_insert;
            DROP TRIGGER IF EXISTS trg_channels_bump_stats_update;
            DROP TRIGGER IF EXISTS trg_channels_bump_stats_delete;

            CREATE TRIGGER trg_videos_bump_stats_insert
            AFTER INSERT ON videos
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;

            CREATE TRIGGER trg_videos_bump_stats_update
            AFTER UPDATE ON videos
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;

            CREATE TRIGGER trg_videos_bump_stats_delete
            AFTER DELETE ON videos
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;

            CREATE TRIGGER trg_channels_bump_stats_insert
            AFTER INSERT ON channels
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;

            CREATE TRIGGER trg_channels_bump_stats_update
            AFTER UPDATE ON channels
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;

            CREATE TRIGGER trg_channels_bump_stats_delete
            AFTER DELETE ON channels
            BEGIN
                UPDATE cached_stats
                   SET value = CAST(
                           COALESCE(
                               (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                               0
                           ) + 1 AS TEXT
                       ),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE key = 'stats_version';
                INSERT INTO cached_stats (key, value, updated_at)
                SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                WHERE changes() = 0;
                DELETE FROM cached_stats WHERE key = 'global_stats';
            END;
            """
        )
        con.commit()
        print(f"Repaired stats triggers in {DB_PATH}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
