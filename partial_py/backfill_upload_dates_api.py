#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local_services import youtube_api_key_pool  # noqa: E402


DEFAULT_DB_PATH = PROJECT_ROOT / "youtube_transcripts.db"
RUNS_DIR = PROJECT_ROOT / "runs"
YOUTUBE_VIDEOS_API = "https://www.googleapis.com/youtube/v3/videos"


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


@dataclass(frozen=True)
class VideoTask:
    video_id: str
    channel_id: str
    channel_name: str


class ApiKeyPool:
    def __init__(self, keys: List[Dict[str, str]]) -> None:
        self.keys = [dict(item) for item in keys if str(item.get("key") or "").strip()]
        self.index = 0
        self.usage = Counter()
        if not self.keys:
            raise RuntimeError("No YouTube API keys configured")

    @property
    def current(self) -> Dict[str, str]:
        return self.keys[self.index]

    def rotate(self) -> bool:
        if len(self.keys) <= 1:
            return False
        old_index = self.index
        self.index = (self.index + 1) % len(self.keys)
        return self.index != old_index


def load_tasks(db_path: Path, channel_id_filter: str, limit: int) -> List[VideoTask]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT v.video_id, c.channel_id, c.channel_name
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE COALESCE(v.upload_date, '') = ''
        """
        params: List[object] = []
        if channel_id_filter:
            sql += " AND c.channel_id = ? "
            params.append(channel_id_filter)
        sql += " ORDER BY c.channel_name ASC, v.id DESC "
        if limit > 0:
            sql += " LIMIT ? "
            params.append(limit)
        rows = con.execute(sql, params).fetchall()
        return [
            VideoTask(
                video_id=str(row["video_id"] or "").strip(),
                channel_id=str(row["channel_id"] or "").strip(),
                channel_name=str(row["channel_name"] or "").strip(),
            )
            for row in rows
            if str(row["video_id"] or "").strip()
        ]
    finally:
        con.close()


def chunked(items: List[VideoTask], size: int) -> List[List[VideoTask]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_batch(video_ids: List[str], key_name: str, api_key: str, timeout: int = 60) -> Tuple[Dict[str, str], str]:
    query = urllib_parse.urlencode(
        {
            "part": "snippet",
            "id": ",".join(video_ids),
            "maxResults": str(min(50, len(video_ids))),
            "key": api_key,
        }
    )
    url = f"{YOUTUBE_VIDEOS_API}?{query}"
    req = urllib_request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            body = str(exc)
        raise RuntimeError(f"http_{exc.code}:{body[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"url_error:{exc}") from exc

    found: Dict[str, str] = {}
    for item in payload.get("items", []):
        video_id = str(item.get("id") or "").strip()
        published_at = str(((item.get("snippet") or {}).get("publishedAt")) or "").strip()
        if not video_id or not published_at:
            continue
        found[video_id] = published_at[:10].replace("-", "")
    return found, key_name


def is_quota_error(message: str) -> bool:
    text = str(message or "").lower()
    return "quota" in text or "daily limit" in text or "rate limit" in text or "http_403" in text


def write_meta(run_dir: Path, db_path: Path, keys: List[Dict[str, str]], task_count: int) -> None:
    meta = run_dir / "meta.txt"
    meta.write_text(
        "\n".join(
            [
                f"db_path={db_path}",
                f"task_count={task_count}",
                "api_keys=" + ",".join(item["name"] for item in keys),
            ]
        ) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill upload_date via YouTube Data API.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--run-dir", default="", help="Direktori run untuk log/report.")
    parser.add_argument("--channel-id", default="", help="Batasi ke satu channel_id.")
    parser.add_argument("--limit", type=int, default=0, help="Batasi jumlah target. 0 = semua.")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size API. Max 50.")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir).resolve() if args.run_dir else (RUNS_DIR / f"backfill_upload_dates_api_{timestamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.csv"

    tasks = load_tasks(db_path, str(args.channel_id or "").strip(), int(args.limit or 0))
    keys = youtube_api_key_pool()
    write_meta(run_dir, db_path, keys, len(tasks))

    if not tasks:
        log("no tasks loaded")
        with report_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["video_id", "channel_id", "channel_name", "status", "upload_date", "api_key_name", "note"],
            )
            writer.writeheader()
        return 0

    key_pool = ApiKeyPool(keys)
    con = sqlite3.connect(str(db_path))
    counters = Counter()
    counters["total"] = len(tasks)

    with con, report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["video_id", "channel_id", "channel_name", "status", "upload_date", "api_key_name", "note"],
        )
        writer.writeheader()

        for batch_index, batch in enumerate(chunked(tasks, max(1, min(50, int(args.batch_size or 50)))), 1):
            attempts = 0
            found_map: Dict[str, str] = {}
            used_key_name = key_pool.current["name"]
            batch_failed = False
            while attempts < max(1, len(key_pool.keys)):
                current = key_pool.current
                used_key_name = current["name"]
                try:
                    found_map, used_key_name = fetch_batch(
                        [task.video_id for task in batch],
                        current["name"],
                        current["key"],
                    )
                    key_pool.usage[used_key_name] += 1
                    break
                except Exception as exc:
                    detail = str(exc)
                    if is_quota_error(detail) and key_pool.rotate():
                        log(f"quota/block on {current['name']}; rotate to {key_pool.current['name']}")
                        attempts += 1
                        time.sleep(1.0)
                        continue
                    for task in batch:
                        writer.writerow(
                            {
                                "video_id": task.video_id,
                                "channel_id": task.channel_id,
                                "channel_name": task.channel_name,
                                "status": "api_error",
                                "upload_date": "",
                                "api_key_name": current["name"],
                                "note": detail[:1000],
                            }
                        )
                        counters["api_error"] += 1
                    found_map = {}
                    used_key_name = current["name"]
                    batch_failed = True
                    break
            if batch_failed:
                f.flush()
                processed = counters["updated"] + counters["missing_from_api"] + counters["api_error"]
                if processed % 250 == 0 or processed >= counters["total"]:
                    log(
                        f"progress {processed}/{counters['total']} | "
                        f"updated={counters['updated']} "
                        f"missing_from_api={counters['missing_from_api']} "
                        f"api_error={counters['api_error']}"
                    )
                continue
            for task in batch:
                upload_date = str(found_map.get(task.video_id) or "").strip()
                if upload_date:
                    con.execute(
                        "UPDATE videos SET upload_date = ? WHERE video_id = ? AND COALESCE(upload_date, '') = ''",
                        (upload_date, task.video_id),
                    )
                    writer.writerow(
                        {
                            "video_id": task.video_id,
                            "channel_id": task.channel_id,
                            "channel_name": task.channel_name,
                            "status": "updated",
                            "upload_date": upload_date,
                            "api_key_name": used_key_name,
                            "note": "",
                        }
                    )
                    counters["updated"] += 1
                else:
                    writer.writerow(
                        {
                            "video_id": task.video_id,
                            "channel_id": task.channel_id,
                            "channel_name": task.channel_name,
                            "status": "missing_from_api",
                            "upload_date": "",
                            "api_key_name": used_key_name,
                            "note": "",
                        }
                    )
                    counters["missing_from_api"] += 1
            f.flush()
            processed = counters["updated"] + counters["missing_from_api"] + counters["api_error"]
            if processed % 250 == 0 or processed >= counters["total"]:
                log(
                    f"progress {processed}/{counters['total']} | "
                    f"updated={counters['updated']} "
                    f"missing_from_api={counters['missing_from_api']} "
                    f"api_error={counters['api_error']}"
                )

    usage_path = run_dir / "api_key_usage.json"
    usage_path.write_text(json.dumps(dict(key_pool.usage), indent=2), encoding="utf-8")
    con.close()
    log(
        f"done | updated={counters['updated']} "
        f"missing_from_api={counters['missing_from_api']} "
        f"api_error={counters['api_error']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
