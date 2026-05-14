#!/usr/bin/env python3
"""
scrap_channel_sqlite.py

- Input CLI: hanya URL channel (contoh: https://www.youtube.com/@azhealthid)
- Output: out/<channel_slug>/text/*.txt + out/<channel_slug>/videos_text.csv
- DB: SQLite (default channels.db) menyimpan channel & video:
    channel, video_id, title, status_download, link_file, link_resume,
    upload_date, seq_num (oldest->newest; nomor terbesar = video terbaru)

Fitur penting:
1) Penomoran file (seq_num) berdasarkan upload_date ASC (paling lama = 1).
2) Nama file TXT disederhanakan:
      ####_<video_id>.txt
3) Jika file lama untuk video_id sudah ada tapi namanya berbeda -> AUTO RENAME
4) Jika TXT untuk video_id sudah ada (setelah rename/sync) -> SKIP download ulang
5) Tidak menyimpan VTT permanen (download ke _tmp lalu dihapus)

Dependencies:
    pip install -U yt-dlp
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import subprocess
import sys
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple


# =========================
# Helpers umum
# =========================
def run(cmd: List[str], capture: bool = False) -> subprocess.CompletedProcess:
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    return subprocess.run(cmd)

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def log(msg: str) -> None:
    print(msg, flush=True)

def ensure_videos_url(channel_url: str) -> str:
    u = channel_url.strip().rstrip("/")
    if u.endswith("/videos"):
        return u
    return u + "/videos"

def parse_channel_slug(channel_url: str) -> str:
    m = re.search(r"youtube\.com/@([^/]+)", channel_url)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/(channel|c|user)/([^/]+)", channel_url)
    if m:
        return m.group(2)
    return "channel"

def safe_filename(s: str, max_len: int = 140) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:*?\"<>|]", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len].rstrip() if len(s) > max_len else s

def txt_exists_ok(path: Path, min_bytes: int = 80) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size >= min_bytes


def ytdlp_base_cmd() -> List[str]:
    """
    Return base command to invoke yt-dlp robustly across environments (local/WSGI/Passenger).

    Priority:
    1) YTDLP_BIN env var (absolute path or executable name)
    2) python -m yt_dlp (same interpreter running this script)
    3) yt-dlp from PATH
    """
    override = (os.getenv("YTDLP_BIN") or "").strip()
    if override:
        return [override]

    # Prefer module invocation so it works even when PATH does not include venv/bin.
    try:
        import yt_dlp  # noqa: F401

        return [sys.executable, "-m", "yt_dlp"]
    except Exception:
        return [shutil.which("yt-dlp") or "yt-dlp"]


# =========================
# VTT -> Text
# =========================
_vtt_time = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}\.\d{3}")
_vtt_header = re.compile(r"^(WEBVTT|NOTE|STYLE|REGION)\b")
_html_tag = re.compile(r"<[^>]+>")

def vtt_to_text(vtt_text: str) -> str:
    lines_out = []
    for raw in vtt_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if _vtt_header.match(line):
            continue
        if _vtt_time.match(line):
            continue
        if line.isdigit():
            continue

        line = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d{3}>", "", line)
        line = _html_tag.sub("", line)
        line = line.replace("&nbsp;", " ").replace("&amp;", "&")
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines_out.append(line)

    # dedup berurutan
    dedup = []
    prev = None
    for l in lines_out:
        if l != prev:
            dedup.append(l)
        prev = l

    return "\n".join(dedup).strip()


# =========================
# yt-dlp: list video + upload_date
# =========================
def ytdlp_flat_list(
    videos_url: str,
    cookies_from_browser: Optional[str] = None,
    extra_args: Optional[List[str]] = None,
) -> Dict[str, Any]:
    cmd = ytdlp_base_cmd() + (extra_args or []) + ["--flat-playlist", "-J", videos_url]
    if cookies_from_browser:
        cmd += ["--cookies-from-browser", cookies_from_browser]
    p = run(cmd, capture=True)
    if p.returncode != 0:
        raise RuntimeError(f"yt-dlp gagal ambil list video:\n{p.stderr.strip()}")
    return json.loads(p.stdout)

def ytdlp_print_upload_date(
    video_url: str,
    cookies_from_browser: Optional[str],
    extra_args: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Return upload_date format YYYYMMDD atau None.
    """
    cmd = ytdlp_base_cmd() + (extra_args or []) + ["--print", "%(upload_date)s", "--skip-download", video_url]
    if cookies_from_browser:
        cmd += ["--cookies-from-browser", cookies_from_browser]
    p = run(cmd, capture=True)
    out = (p.stdout or "").strip()
    if p.returncode != 0:
        return None
    if re.fullmatch(r"\d{8}", out):
        return out
    return None


# =========================
# yt-dlp: pilih bahasa & download subs (sementara)
# =========================
def ytdlp_list_subs(
    video_url: str,
    cookies_from_browser: Optional[str],
    extra_args: Optional[List[str]] = None,
) -> str:
    cmd = ytdlp_base_cmd() + (extra_args or []) + ["--list-subs", "--skip-download", video_url]
    if cookies_from_browser:
        cmd += ["--cookies-from-browser", cookies_from_browser]
    p = run(cmd, capture=True)
    # list-subs kadang menulis sebagian ke stderr
    return (p.stdout or "") + "\n" + (p.stderr or "")

def extract_lang_codes(list_subs_output: str) -> List[str]:
    """
    Parse output `yt-dlp --list-subs`.

    Penting: output juga berisi baris seperti:
      <video_id> has no subtitles
    Jangan sampai video_id ikut terbaca sebagai "kode bahasa".
    """
    langs: List[str] = []

    in_table = False
    for raw in list_subs_output.splitlines():
        line = raw.strip()
        if not line:
            in_table = False
            continue

        if ("Available subtitles for" in line) or ("Available automatic captions for" in line):
            in_table = False
            continue

        # Header table: "Language  Formats" atau "Language Name Formats"
        if line.lower().startswith("language") and ("format" in line.lower()):
            in_table = True
            continue

        if not in_table:
            continue

        # Table row: first token is language code (e.g., en, en-orig, zh-Hans, es-419)
        token = line.split()[0]
        if token.lower() == "language":
            continue
        if not re.fullmatch(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,15})*", token):
            continue
        if token not in langs:
            langs.append(token)

    return langs

def choose_best_lang(langs: List[str]) -> Optional[str]:
    if not langs:
        return None
    # prioritas "paling mungkin original"
    # NOTE: yt-dlp bisa menampilkan banyak "automatic captions" yang sebenarnya auto-translate.
    #       Biasanya track original ditandai "-orig", jadi utamakan itu dulu.
    # Jangan "memaksakan" bahasa tertentu: prefer English jika tersedia,
    # fallback ke Indonesian jika memang tidak ada English.
    priority = ["en-orig", "id-orig", "en", "id"]
    lower_map = {l.lower(): l for l in langs}
    for p in priority:
        if p in lower_map:
            return lower_map[p]
    for base in ("id", "en"):
        for l in langs:
            if l.lower().startswith(base + "-"):
                return l
    return langs[0]

def download_subs_to_tmp(video_url: str, tmp_dir: Path, sub_langs: str,
                         cookies_from_browser: Optional[str],
                         extra_args: Optional[List[str]] = None,
                         video_id: Optional[str] = None) -> Optional[str]:
    outtmpl = str(tmp_dir / "%(id)s_%(title)s.%(ext)s")
    cmd = [
        *ytdlp_base_cmd(),
        *(extra_args or []),
        "--skip-download",
        "--write-subs",
        "--write-auto-subs",
        "--sub-langs", sub_langs,
        "--sub-format", "vtt",
        "--no-warnings",
        "-o", outtmpl,
        video_url,
    ]
    if cookies_from_browser:
        cmd += ["--cookies-from-browser", cookies_from_browser]
    p = run(cmd, capture=True)
    # Jangan raise di sini: kadang yt-dlp return non-zero (mis. 429)
    # walaupun ada subtitle yang sudah berhasil ditulis.
    #
    # Caller akan memutuskan:
    # - kalau ada VTT yang valid -> lanjut convert
    # - kalau tidak ada -> tandai error/no_subtitle sesuai kondisi
    if p.returncode != 0:
        return ((p.stdout or "") + "\n" + (p.stderr or "")).strip()
    return None

def choose_lang_candidates(langs: List[str]) -> List[str]:
    """
    Susun kandidat bahasa yang akan dicoba untuk download subtitle.

    Tujuan:
    - utamakan "-orig" (biasanya track original, bukan auto-translate)
    - tetap prefer id/en jika tersedia
    - tetap punya fallback bila parsing terbatas
    """
    if not langs:
        return []
    lower_map = {l.lower(): l for l in langs}

    ordered: List[str] = []
    has_en = ("en" in lower_map) or ("en-orig" in lower_map)

    # Prefer "-orig" dan English jika ada.
    for key in ("en-orig", "id-orig", "en"):
        if key in lower_map:
            ordered.append(lower_map[key])

    # Jangan memaksakan 'id' kalau sudah ada English; tapi kalau tidak ada English, 'id' jadi fallback yang bagus.
    if (not has_en) and ("id" in lower_map):
        ordered.append(lower_map["id"])

    # tambahkan semua varian "-orig" lain (jika ada), agar tetap dapat track original
    for l in langs:
        if l.lower().endswith("-orig") and l not in ordered:
            ordered.append(l)

    # unique preserve order
    out: List[str] = []
    seen = set()
    for l in ordered:
        k = l.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(l)
    return out

def pick_vtts_sorted(
    tmp_dir: Path,
    video_id: str,
    preferred_langs: Optional[List[str]],
) -> List[Path]:
    candidates = list(tmp_dir.glob(f"{video_id}_*.vtt"))
    if not candidates:
        return []

    preferred_rank = {}
    if preferred_langs:
        for idx, l in enumerate(preferred_langs):
            preferred_rank[l.lower()] = idx

    def score(p: Path) -> tuple:
        name = p.name.lower()
        is_auto = ("auto" in name) or (".asr" in name)
        is_orig = ("-orig" in name) or (".orig." in name)

        lang_rank = 999
        if preferred_rank:
            for lang_l, r in preferred_rank.items():
                if (f".{lang_l}." in name) or name.endswith(f".{lang_l}.vtt") or (f"_{lang_l}." in name) or (lang_l in name):
                    lang_rank = min(lang_rank, r)
        return (
            0 if is_orig else 1,
            0 if not is_auto else 1,
            lang_rank,
            len(name),
        )

    return sorted(candidates, key=score)


# =========================
# SQLite layer
# =========================
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL,
  last_scanned TEXT
);

CREATE TABLE IF NOT EXISTS videos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id INTEGER NOT NULL,
  video_id TEXT NOT NULL,
  title TEXT,
  upload_date TEXT,       -- YYYYMMDD
  seq_num INTEGER,        -- 1..N oldest->newest
  status_download TEXT NOT NULL DEFAULT 'pending',
  link_file TEXT,
  link_resume TEXT,
  read_at TEXT,           -- ISO UTC when marked read (optional)
  last_attempt TEXT,
  error_msg TEXT,
  UNIQUE(channel_id, video_id),
  FOREIGN KEY(channel_id) REFERENCES channels(id)
);

CREATE INDEX IF NOT EXISTS idx_videos_channel_seq ON videos(channel_id, seq_num);
CREATE INDEX IF NOT EXISTS idx_videos_channel_status ON videos(channel_id, status_download);
"""

def _migrate_drop_video_url(con: sqlite3.Connection) -> None:
    cols = [r[1] for r in con.execute("PRAGMA table_info(videos)").fetchall()]
    if "video_url" not in cols:
        return
    has_read_at = "read_at" in cols

    # PRAGMA foreign_keys only takes effect outside transactions.
    con.execute("PRAGMA foreign_keys=OFF;")
    con.execute("BEGIN;")
    try:
        con.execute("ALTER TABLE videos RENAME TO videos_old;")
        con.execute(
            """
            CREATE TABLE videos (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              channel_id INTEGER NOT NULL,
              video_id TEXT NOT NULL,
              title TEXT,
              upload_date TEXT,
              seq_num INTEGER,
              status_download TEXT NOT NULL DEFAULT 'pending',
              link_file TEXT,
              link_resume TEXT,
              read_at TEXT,
              last_attempt TEXT,
              error_msg TEXT,
              UNIQUE(channel_id, video_id),
              FOREIGN KEY(channel_id) REFERENCES channels(id)
            );
            """
        )
        if has_read_at:
            con.execute(
                """
                INSERT INTO videos(
                  id, channel_id, video_id, title, upload_date, seq_num,
                  status_download, link_file, link_resume, read_at, last_attempt, error_msg
                )
                SELECT
                  id, channel_id, video_id, title, upload_date, seq_num,
                  status_download, link_file, link_resume, read_at, last_attempt, error_msg
                FROM videos_old;
                """
            )
        else:
            con.execute(
                """
                INSERT INTO videos(
                  id, channel_id, video_id, title, upload_date, seq_num,
                  status_download, link_file, link_resume, last_attempt, error_msg
                )
                SELECT
                  id, channel_id, video_id, title, upload_date, seq_num,
                  status_download, link_file, link_resume, last_attempt, error_msg
                FROM videos_old;
                """
            )
        con.execute("DROP TABLE videos_old;")

        # Recreate indexes (dropped with old table)
        con.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_seq ON videos(channel_id, seq_num);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_status ON videos(channel_id, status_download);")

        # Keep AUTOINCREMENT sequence sane (sqlite_sequence may not exist on some DBs)
        max_id_row = con.execute("SELECT MAX(id) FROM videos;").fetchone()
        max_id = int(max_id_row[0] or 0)
        try:
            con.execute("DELETE FROM sqlite_sequence WHERE name='videos';")
            con.execute("INSERT INTO sqlite_sequence(name, seq) VALUES('videos', ?);", (max_id,))
        except sqlite3.OperationalError:
            pass

        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.execute("PRAGMA foreign_keys=ON;")

def _migrate_add_read_at(con: sqlite3.Connection) -> None:
    cols = [r[1] for r in con.execute("PRAGMA table_info(videos)").fetchall()]
    if "read_at" in cols:
        return
    try:
        con.execute("ALTER TABLE videos ADD COLUMN read_at TEXT;")
    except sqlite3.OperationalError:
        pass

def _db_checkpoint_truncate(con: sqlite3.Connection) -> None:
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchall()
    except sqlite3.OperationalError:
        # Non-WAL DB or older SQLite build.
        pass

def _cleanup_wal_shm_files(db_path: Path) -> None:
    wal = db_path.with_name(db_path.name + "-wal")
    shm = db_path.with_name(db_path.name + "-shm")
    for p in (wal, shm):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

def _busy_timeout_ms() -> int:
    raw = (os.getenv("DB_BUSY_TIMEOUT_MS") or "20000").strip()
    if raw.isdigit():
        v = int(raw)
        if v > 0:
            return v
    return 20000

def _db_maintenance_blocker_reason(db_path: Path) -> Optional[str]:
    con: Optional[sqlite3.Connection] = None
    try:
        con = sqlite3.connect(str(db_path), timeout=0.2)
        con.execute("PRAGMA busy_timeout=200;")
        con.execute("BEGIN IMMEDIATE;")
        con.execute("ROLLBACK;")
        return None
    except sqlite3.OperationalError as ex:
        msg = str(ex).lower()
        if "locked" in msg or "busy" in msg:
            return "database sedang sibuk (writer lain masih aktif)"
        return f"maintenance tidak tersedia: {ex}"
    except Exception as ex:
        return f"maintenance tidak tersedia: {ex}"
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

def db_connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute(f"PRAGMA busy_timeout={_busy_timeout_ms()};")
    con.executescript(SCHEMA_SQL)
    _migrate_drop_video_url(con)
    _migrate_add_read_at(con)
    return con

def upsert_channel(con: sqlite3.Connection, url: str, slug: str) -> int:
    con.execute(
        "INSERT INTO channels(url, slug, last_scanned) VALUES (?, ?, ?) "
        "ON CONFLICT(url) DO UPDATE SET slug=excluded.slug;",
        (url, slug, now_iso())
    )
    row = con.execute("SELECT id FROM channels WHERE url=?", (url,)).fetchone()
    return int(row[0])

def upsert_video(con: sqlite3.Connection, channel_id: int, video_id: str,
                 title: str, upload_date: Optional[str]) -> None:
    # Judul harus selalu mengikuti judul terbaru dari YouTube -> UPDATE title selalu
    con.execute(
        """
        INSERT INTO videos(channel_id, video_id, title, upload_date)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(channel_id, video_id) DO UPDATE SET
          title=excluded.title,
          upload_date=COALESCE(excluded.upload_date, videos.upload_date)
        """,
        (channel_id, video_id, title, upload_date)
    )

def recompute_seq_nums(con: sqlite3.Connection, channel_id: int) -> None:
    """
    Set seq_num berdasarkan upload_date ASC (oldest->newest).
    Kalau upload_date NULL, jatuh ke bawah.
    """
    rows = con.execute(
        """
        SELECT id, upload_date FROM videos
        WHERE channel_id=?
        ORDER BY
          CASE WHEN upload_date IS NULL THEN 1 ELSE 0 END,
          upload_date ASC,
          id ASC
        """,
        (channel_id,)
    ).fetchall()

    for idx, (vid_pk, _) in enumerate(rows, start=1):
        con.execute("UPDATE videos SET seq_num=? WHERE id=?", (idx, vid_pk))

def get_all_videos(
    con: sqlite3.Connection,
    channel_id: int,
    *,
    statuses: Optional[List[str]] = None,
    video_id: Optional[str] = None,
) -> List[Tuple]:
    where = ["channel_id=?"]
    params: List[Any] = [channel_id]
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        where.append(f"status_download IN ({placeholders})")
        params.extend(list(statuses))
    if video_id:
        where.append("video_id=?")
        params.append(video_id)
    sql = f"""
        SELECT video_id, title, upload_date, seq_num, status_download, link_file, link_resume
        FROM videos
        WHERE {' AND '.join(where)}
        ORDER BY seq_num ASC
    """
    return con.execute(sql, tuple(params)).fetchall()

def update_video(con: sqlite3.Connection, channel_id: int, video_id: str,
                 status: Optional[str] = None,
                 link_file: Optional[str] = None,
                 link_resume: Optional[str] = None,
                 error_msg: Optional[str] = None) -> None:
    # Update sebagian field yang diberikan
    sets = []
    params: List[Any] = []

    if status is not None:
        sets.append("status_download=?")
        params.append(status)
    if link_file is not None:
        sets.append("link_file=?")
        params.append(link_file)
    if link_resume is not None:
        sets.append("link_resume=?")
        params.append(link_resume)
    sets.append("last_attempt=?")
    params.append(now_iso())
    sets.append("error_msg=?")
    params.append(error_msg)

    params.extend([channel_id, video_id])

    con.execute(
        f"UPDATE videos SET {', '.join(sets)} WHERE channel_id=? AND video_id=?",
        tuple(params)
    )

def channel_progress(con: sqlite3.Connection, channel_id: int) -> Tuple[int, int, int, int]:
    total = con.execute("SELECT COUNT(*) FROM videos WHERE channel_id=?", (channel_id,)).fetchone()[0]
    downloaded = con.execute(
        "SELECT COUNT(*) FROM videos WHERE channel_id=? AND status_download='downloaded'",
        (channel_id,)
    ).fetchone()[0]
    no_sub = con.execute(
        "SELECT COUNT(*) FROM videos WHERE channel_id=? AND status_download='no_subtitle'",
        (channel_id,)
    ).fetchone()[0]
    err = con.execute(
        "SELECT COUNT(*) FROM videos WHERE channel_id=? AND status_download='error'",
        (channel_id,)
    ).fetchone()[0]
    return int(total), int(downloaded), int(no_sub), int(err)


# =========================
# File sync/rename based on video_id & seq
# =========================
def find_any_txt_by_video_id(txt_dir: Path, video_id: str) -> Optional[Path]:
    """
    Cari file TXT lama milik video_id.
    Pola utama: *_{video_id}.txt (format baru) atau *_{video_id}_*.txt (format lama)
    Fallback: *{video_id}*.txt
    Pilih yang paling besar (biasanya paling lengkap).
    """
    hits = list(txt_dir.glob(f"*_{video_id}.txt"))
    hits = [p for p in hits if p.is_file()]
    if hits:
        hits.sort(key=lambda p: p.stat().st_size, reverse=True)
        return hits[0]

    hits = list(txt_dir.glob(f"*_{video_id}_*.txt"))
    hits = [p for p in hits if p.is_file()]
    if hits:
        hits.sort(key=lambda p: p.stat().st_size, reverse=True)
        return hits[0]

    hits = [p for p in txt_dir.glob("*.txt") if video_id in p.name]
    if hits:
        hits.sort(key=lambda p: p.stat().st_size, reverse=True)
        return hits[0]

    return None

def desired_txt_name(seq_num: int, video_id: str, title: str) -> str:
    return f"{seq_num:04d}_{video_id}.txt"

def smart_rename_to_latest(
    txt_dir: Path,
    seq_num: int,
    video_id: str,
    latest_title: str,
    current_db_link_file: Optional[str]
) -> Optional[str]:
    """
    Pastikan file TXT untuk video_id bernama sesuai format terbaru:
        ####_<video_id>.txt
    Return link_file relatif ("text/<filename>") jika ada/berhasil, else None.
    """
    target_name = desired_txt_name(seq_num, video_id, latest_title)
    target_path = txt_dir / target_name
    rel_target = str(Path("text") / target_name)

    # 0) jika target sudah ada dan valid -> selesai
    if txt_exists_ok(target_path):
        return rel_target

    # 1) jika DB menunjuk ke file, dan file itu ada -> rename ke target
    if current_db_link_file:
        p = Path(current_db_link_file)
        # current_db_link_file disimpan relatif "text/xxx.txt"
        candidate = txt_dir / p.name if p.name else None
        if candidate and candidate.exists() and candidate.is_file():
            if candidate.name == target_name:
                return rel_target
            # rename candidate -> target
            if not target_path.exists():
                candidate.rename(target_path)
                return rel_target

    # 2) cari file lama berdasarkan video_id
    old = find_any_txt_by_video_id(txt_dir, video_id)
    if not old or not old.exists():
        return None

    if old.name == target_name:
        return rel_target

    # 3) rename old -> target (dengan resolusi konflik)
    if not target_path.exists():
        old.rename(target_path)
        return rel_target

    # target sudah ada tapi mungkin kecil/korup
    old_size = old.stat().st_size
    tgt_size = target_path.stat().st_size

    if tgt_size < 80 and old_size >= 80:
        try:
            target_path.unlink()
        except Exception:
            pass
        old.rename(target_path)
        return rel_target

    # kalau old jauh lebih besar, replace target (backup)
    if old_size > tgt_size + 200:
        backup = txt_dir / (target_path.stem + ".bak.txt")
        try:
            if backup.exists():
                backup.unlink()
            target_path.rename(backup)
        except Exception:
            pass
        old.rename(target_path)
        return rel_target

    # kalau target lebih bagus, hapus old duplikat
    try:
        old.unlink()
    except Exception:
        pass
    return rel_target


# =========================
# Main pipeline
# =========================
def video_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"

def main():
    ap = argparse.ArgumentParser(
        description="Download subtitle->TXT dari channel YouTube, simpan status ke SQLite, dan rename mengikuti seq_num."
    )
    ap.add_argument("channel_url", help="contoh: https://www.youtube.com/@azhealthid")
    ap.add_argument("--db", default="channels.db", help="path sqlite db (default: channels.db)")
    ap.add_argument("-o", "--out", default="out", help="folder output (default: out)")
    ap.add_argument("--limit", type=int, default=0, help="batasi jumlah video (0=semua)")
    ap.add_argument(
        "--update",
        action="store_true",
        help="Mode update: retry video berstatus pending/error/no_subtitle (atau file TXT hilang) sambil tetap cek video baru.",
    )
    ap.add_argument(
        "--pending-only",
        action="store_true",
        help="Hanya proses video berstatus pending dari DB (tanpa scan playlist).",
    )
    ap.add_argument(
        "--video-id",
        default="",
        help="Proses hanya 1 video_id (umumnya untuk retry pending).",
    )
    ap.add_argument(
        "--stop-at-known",
        action="store_true",
        help="mode cepat: saat scan playlist, berhenti setelah menemukan deretan video yang sudah ada di DB (umumnya cukup untuk cek video terbaru).",
    )
    ap.add_argument(
        "--stop-at-known-after",
        type=int,
        default=25,
        help="Jika --stop-at-known aktif: berhenti setelah menemukan N video berturut-turut yang sudah ada di DB (default 25).",
    )
    ap.add_argument(
        "--stop-at-known-min-scan",
        type=int,
        default=200,
        help="Jika --stop-at-known aktif: minimal scan N entry playlist sebelum boleh berhenti (default 200).",
    )
    ap.add_argument("--cookies-from-browser", default=None,
                    help='contoh: firefox / chrome (opsional jika kena bot-check/login)')
    ap.add_argument("--ytdlp-sleep-requests", type=float, default=0.0,
                    help="Detik jeda antar HTTP request yt-dlp (untuk mengurangi 429).")
    ap.add_argument("--ytdlp-min-sleep-interval", type=float, default=0.0,
                    help="Detik jeda sebelum setiap download item (min).")
    ap.add_argument("--ytdlp-max-sleep-interval", type=float, default=0.0,
                    help="Detik jeda sebelum setiap download item (max).")
    ap.add_argument("--ytdlp-retries", type=int, default=10,
                    help="Jumlah retries yt-dlp (default 10).")
    args = ap.parse_args()

    channel_url = args.channel_url.strip().rstrip("/")
    videos_url = ensure_videos_url(channel_url)
    slug = parse_channel_slug(channel_url)
    target_video_id = (args.video_id or "").strip() or None
    pending_only = bool(args.pending_only or target_video_id)
    if pending_only:
        # Pending-only/single-video implies update mode.
        args.update = True

    out_root = Path(args.out) / slug
    txt_dir = out_root / "text"
    tmp_dir = out_root / "_tmp"
    out_root.mkdir(parents=True, exist_ok=True)
    txt_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db)
    ytdlp_extra: List[str] = []
    if args.ytdlp_sleep_requests and args.ytdlp_sleep_requests > 0:
        ytdlp_extra += ["--sleep-requests", str(args.ytdlp_sleep_requests)]
    if args.ytdlp_min_sleep_interval and args.ytdlp_min_sleep_interval > 0:
        ytdlp_extra += ["--min-sleep-interval", str(args.ytdlp_min_sleep_interval)]
    if args.ytdlp_max_sleep_interval and args.ytdlp_max_sleep_interval > 0:
        ytdlp_extra += ["--max-sleep-interval", str(args.ytdlp_max_sleep_interval)]
    if args.ytdlp_retries is not None and args.ytdlp_retries != 10:
        ytdlp_extra += ["--retries", str(args.ytdlp_retries)]
    log(f"[{slug}] Step 1/7: DB connect + migrate")
    con = db_connect(db_path)
    try:
        with con:
            channel_id = upsert_channel(con, channel_url, slug)
        existing_ids = {
            r[0] for r in con.execute("SELECT video_id FROM videos WHERE channel_id=?", (channel_id,))
        }

        flat_videos: List[Tuple[str, str]] = []
        do_scan = not pending_only and not target_video_id
        if do_scan:
            # 1) Scan list video
            log(f"[{slug}] Step 2/7: Scan playlist video (yt-dlp)")
            data = ytdlp_flat_list(videos_url, cookies_from_browser=args.cookies_from_browser, extra_args=ytdlp_extra)
            entries = data.get("entries", []) or []
            if args.limit and args.limit > 0:
                entries = entries[:args.limit]

            known_streak = 0
            scanned = 0
            for e in entries:
                vid = e.get("id")
                title = e.get("title") or ""
                if vid:
                    scanned += 1
                    flat_videos.append((vid, title))
                    if args.stop_at_known:
                        if vid in existing_ids:
                            known_streak += 1
                        else:
                            known_streak = 0

                        if scanned >= int(args.stop_at_known_min_scan) and known_streak >= int(args.stop_at_known_after):
                            log(
                                f"[{slug}]   stop-at-known: {known_streak} known berturut-turut (last={vid}), stop scan playlist"
                            )
                            break
        else:
            mode_msg = "pending-only" if pending_only and not target_video_id else f"video-id={target_video_id}"
            log(f"[{slug}] Step 2/7: Skip scan playlist ({mode_msg})")

        if not flat_videos:
            if args.update and existing_ids:
                log(f"[{slug}] Scan playlist kosong/terbatas; lanjut mode update pakai DB yang sudah ada.")
            elif args.stop_at_known and existing_ids:
                log(f"[{slug}] Tidak ada video baru terdeteksi (stop-at-known).")
            else:
                log("Tidak ada video ditemukan (atau akses dibatasi).")
                return

        did_scan = do_scan and bool(flat_videos)
        if did_scan:
            # 2) Ambil upload_date per video & upsert DB (title selalu mengikuti terbaru)
            log(f"[{slug}] Step 3/7: Update metadata (upload_date + title) ke DB")
            existing_dates = {
                r[0]: (r[1] or "")
                for r in con.execute("SELECT video_id, upload_date FROM videos WHERE channel_id=?", (channel_id,))
            }
            need_fetch = {
                vid
                for (vid, _) in flat_videos
                if not re.fullmatch(r"\d{8}", existing_dates.get(vid, ""))
            }
            log(
                f"[{slug}]   upload_date: fetch={len(need_fetch)} skip={len(flat_videos) - len(need_fetch)} "
                "(skip jika sudah ada di DB)"
            )
            for i, (vid, title) in enumerate(flat_videos, start=1):
                if i == 1 or i % 25 == 0 or i == len(flat_videos):
                    log(f"[{slug}]   metadata: {i}/{len(flat_videos)}")
                upload_date = (
                    ytdlp_print_upload_date(video_watch_url(vid), args.cookies_from_browser, extra_args=ytdlp_extra)
                    if vid in need_fetch
                    else None
                )
                with con:
                    upsert_video(con, channel_id, vid, title, upload_date)

        # 3) Recompute seq_num (oldest->newest) — only when we scanned playlist.
        if did_scan:
            log(f"[{slug}] Step 4/7: Recompute nomor (seq_num) oldest->newest")
            with con:
                recompute_seq_nums(con, channel_id)
                con.execute("UPDATE channels SET last_scanned=? WHERE id=?", (now_iso(), channel_id))
        else:
            log(f"[{slug}] Step 4/7: Skip recompute seq_num (no playlist scan)")

        # 4) Sync rename semua file TXT agar mengikuti seq_num terbaru
        #    (ini memastikan file lama dirapikan sebelum download)
        log(f"[{slug}] Step 5/7: Sync/rename file TXT sesuai seq_num")
        target_statuses = ["pending"] if pending_only else None
        rows = get_all_videos(con, channel_id, statuses=target_statuses, video_id=target_video_id)
        with con:
            for (vid, title, upload_date, seq_num, status, link_file, link_resume) in rows:
                if seq_num is None:
                    continue
                new_link = smart_rename_to_latest(
                    txt_dir=txt_dir,
                    seq_num=int(seq_num),
                    video_id=vid,
                    latest_title=title or "",
                    current_db_link_file=link_file
                )
                if new_link:
                    # Jika file ada setelah rename, status minimal downloaded
                    # (kalau sebelumnya error/pending, sekarang dianggap downloaded karena file ada)
                    update_video(con, channel_id, vid, status="downloaded", link_file=new_link, error_msg=None)

            # Refresh rows after sync
            rows = get_all_videos(con, channel_id, statuses=target_statuses, video_id=target_video_id)
            total_count = len(rows)
            if target_video_id and total_count == 0:
                log(f"[{slug}] video-id {target_video_id} tidak ditemukan di DB untuk channel ini.")
                return
            if pending_only and not target_video_id and total_count == 0:
                log(f"[{slug}] Tidak ada video pending. Selesai.")
                return
    
            # 5) Proses download untuk yang belum ada file (urut oldest->newest)
            log(f"[{slug}] Step 6/7: Download subtitle -> TXT (yang belum ada)")
            if pending_only:
                retry_statuses = {"pending"}
            else:
                retry_statuses = {"pending", "error", "no_subtitle"} if args.update else set()
            if args.update and not pending_only:
                pending_n = sum(1 for _vid, _t, _ud, _sn, st, _lf, _lr in rows if st == "pending")
                err_n = sum(1 for _vid, _t, _ud, _sn, st, _lf, _lr in rows if st == "error")
                nosub_n = sum(1 for _vid, _t, _ud, _sn, st, _lf, _lr in rows if st == "no_subtitle")
                log(f"[{slug}]   update: retry pending={pending_n} error={err_n} no_subtitle={nosub_n}")
    
            csv_rows: List[List[str]] = []
            for idx, (vid, title, upload_date, seq_num, status, link_file, link_resume) in enumerate(rows, start=1):
                seq_num_i = int(seq_num or 0)
                title = title or ""
                link = video_watch_url(vid)
                desired_name = desired_txt_name(seq_num_i, vid, title)
                desired_path = txt_dir / desired_name
                rel_desired = str(Path("text") / desired_name)
    
                # Jika sudah ada file sesuai nama terbaru, skip
                if txt_exists_ok(desired_path):
                    with con:
                        update_video(con, channel_id, vid, status="downloaded", link_file=rel_desired, error_msg=None)
                    if not args.update:
                        print(f"[{slug}] ({idx}/{total_count}) SKIP (sudah ada): {desired_name}")
                    csv_rows.append([str(seq_num_i), title, link, rel_desired])
                    continue
    
                # Kalau belum ada, coba cari file lama (harusnya sudah di-rename pada tahap sync, tapi kita jaga)
                existing_old = find_any_txt_by_video_id(txt_dir, vid)
                if existing_old and txt_exists_ok(existing_old):
                    # rename ke terbaru
                    try:
                        if not desired_path.exists():
                            existing_old.rename(desired_path)
                        else:
                            # kalau target ada tapi kecil, replace
                            if desired_path.stat().st_size < 80 and existing_old.stat().st_size >= 80:
                                desired_path.unlink()
                                existing_old.rename(desired_path)
                            else:
                                # target lebih baik, hapus duplikat
                                existing_old.unlink()
                    except Exception:
                        pass
                    if txt_exists_ok(desired_path):
                        with con:
                            update_video(con, channel_id, vid, status="downloaded", link_file=rel_desired, error_msg=None)
                        if not args.update:
                            print(f"[{slug}] ({idx}/{total_count}) SKIP (rename): {desired_name}")
                        csv_rows.append([str(seq_num_i), title, link, rel_desired])
                        continue
    
                # Mode update: untuk channel besar, jangan spam "SKIP". Tapi tetap retry status tertentu.
                if args.update and status == "downloaded":
                    # downloaded tapi file hilang -> akan dicoba download ulang di bawah
                    pass
                elif args.update and status and status not in retry_statuses:
                    # status lain jarang; tetap catat ke CSV dan skip network.
                    csv_rows.append([str(seq_num_i), title, link, ""])
                    continue
    
                if args.update and idx % 200 == 0:
                    log(f"[{slug}]   progress: {idx}/{total_count} (scan DB)")
    
                print(f"[{slug}] ({idx}/{total_count}) Download: {seq_num_i:04d} | {vid} | {title}")
    
                # bersihkan tmp untuk video ini
                for p in tmp_dir.glob(f"{vid}_*.vtt"):
                    try:
                        p.unlink()
                    except Exception:
                        pass
    
                try:
                    list_out = ytdlp_list_subs(link, args.cookies_from_browser, extra_args=ytdlp_extra)
                    langs = extract_lang_codes(list_out)
                    candidates = choose_lang_candidates(langs)
                    chosen_lang = choose_best_lang(langs) or (candidates[0] if candidates else "en")
    
                    # Jangan memaksakan banyak bahasa sekaligus:
                    # coba satu-per-satu, stop setelah dapat subtitle yang valid.
                    attempt_langs = candidates[:] if candidates else ([chosen_lang] if chosen_lang else [])
                    if not attempt_langs:
                        attempt_langs = ["en"]
    
                    vtts: List[Path] = []
                    last_dl_err: Optional[str] = None
                    for lang in attempt_langs:
                        sub_langs = f"{lang}.*,{lang}"
                        last_dl_err = (
                            download_subs_to_tmp(
                            link,
                            tmp_dir,
                            sub_langs,
                            args.cookies_from_browser,
                            extra_args=ytdlp_extra,
                            video_id=vid,
                        )
                            or last_dl_err
                        )
                        vtts = pick_vtts_sorted(tmp_dir, vid, [lang])
                        if vtts:
                            break
    
                    # fallback terakhir: coba "all" (kadang parsing/regex miss)
                    if not vtts:
                        last_dl_err = (
                            download_subs_to_tmp(
                            link,
                            tmp_dir,
                            "all",
                            args.cookies_from_browser,
                            extra_args=ytdlp_extra,
                            video_id=vid,
                        )
                            or last_dl_err
                        )
                        vtts = pick_vtts_sorted(tmp_dir, vid, attempt_langs)

                    best_txt = ""
                    best_vtt: Optional[Path] = None
                    for p in vtts:
                        if not p.exists():
                            continue
                        txt = vtt_to_text(p.read_text(encoding="utf-8", errors="ignore"))
                        if txt:
                            best_txt = txt
                            best_vtt = p
                            break

                    if not best_vtt:
                        # Jika sebelumnya ada error download (mis. 429), tandai sebagai error,
                        # bukan no_subtitle.
                        if last_dl_err:
                            with con:
                                update_video(
                                    con,
                                    channel_id,
                                    vid,
                                    status="error",
                                    link_file=None,
                                    error_msg=(last_dl_err[-2000:] if len(last_dl_err) > 2000 else last_dl_err),
                                )
                            print("  -> ERROR: yt-dlp gagal download subtitle (lihat error_msg)")
                            csv_rows.append([str(seq_num_i), title, link, ""])
                            continue
                        with con:
                            update_video(
                                con,
                                channel_id,
                                vid,
                                status="no_subtitle",
                                link_file=None,
                                error_msg=f"No subtitle/empty (lang={chosen_lang})",
                            )
                        print(f"  -> NO_SUBTITLE (lang={chosen_lang})")
                        csv_rows.append([str(seq_num_i), title, link, ""])
                    else:
                        desired_path.write_text(best_txt, encoding="utf-8")
                        with con:
                            update_video(
                                con,
                                channel_id,
                                vid,
                                status="downloaded",
                                link_file=rel_desired,
                                error_msg=None,
                            )
                        print(f"  -> OK: {rel_desired} (lang={chosen_lang})")
                        csv_rows.append([str(seq_num_i), title, link, rel_desired])
    
                except Exception as ex:
                    with con:
                        update_video(con, channel_id, vid, status="error", link_file=None, error_msg=str(ex))
                    print(f"  -> ERROR: {ex}")
                    csv_rows.append([str(seq_num_i), title, link, ""])
    
                # hapus VTT sementara
                for p in tmp_dir.glob(f"{vid}_*.vtt"):
                    try:
                        p.unlink()
                    except Exception:
                        pass

        # 6) Tulis CSV per channel
        if not pending_only and not target_video_id:
            log(f"[{slug}] Step 7/7: Tulis CSV ringkasan")
            csv_path = out_root / "videos_text.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["no", "title", "link", "text_link"])
                for r in csv_rows:
                    w.writerow(r)
        else:
            log(f"[{slug}] Step 7/7: Skip tulis CSV (mode pending-only/single video)")

        # 7) Report progress
        total, downloaded, no_sub, err = channel_progress(con, channel_id)
        processed = downloaded + no_sub
        complete = (processed == total)

        print("\n=== STATUS CHANNEL ===")
        print(f"Channel : {slug}")
        print(f"DB      : {db_path.resolve()}")
        print(f"Output  : {out_root.resolve()}")
        print(f"Total   : {total}")
        print(f"OK      : {downloaded}")
        print(f"NoSub   : {no_sub}")
        print(f"Error   : {err}")
        print(f"Selesai : {'YA' if complete else 'BELUM'} (processed={processed}/{total})")
    finally:
        try:
            con.close()
        except Exception:
            pass
        blocker = _db_maintenance_blocker_reason(db_path)
        if blocker:
            log(f"[{slug}] DB cleanup skipped: {blocker}")
        else:
            try:
                log(f"[{slug}] DB: checkpoint WAL + cleanup (-wal/-shm)")
                con_maint = sqlite3.connect(str(db_path), check_same_thread=False)
                try:
                    con_maint.execute(f"PRAGMA busy_timeout={_busy_timeout_ms()};")
                    _db_checkpoint_truncate(con_maint)
                finally:
                    con_maint.close()
            except Exception:
                pass
            try:
                _cleanup_wal_shm_files(db_path)
            except Exception:
                pass


if __name__ == "__main__":
    main()
