#!/usr/bin/env python3
import csv
import os
import re
import subprocess
import sys
import shutil
from pathlib import Path
from typing import List

# ====== KONFIG ======
CSV_PATH = "the_diary_of_a_ceo_videos_categorized.csv"   # ubah jika berbeda
OUT_DIR  = Path("subs")
SUB_LANGS = "en.*,en"    # bisa ganti: "id.*,id,en.*,en" jika mau coba ID dulu
SUB_FORMAT = "vtt"       # vtt paling umum dari YouTube
# ====================

def safe_folder_name(name: str) -> str:
    """Buat nama folder aman untuk filesystem."""
    name = (name or "Tanpa_Kategori").strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)  # karakter terlarang Windows
    name = re.sub(r"\s+", " ", name).strip()
    return name[:80] if len(name) > 80 else name

def ytdlp_base_cmd() -> List[str]:
    override = (os.getenv("YTDLP_BIN") or "").strip()
    if override:
        return [override]
    # Prefer module invocation so it works even when PATH does not include venv/bin.
    try:
        import yt_dlp  # noqa: F401

        return [sys.executable, "-m", "yt_dlp"]
    except Exception:
        return [shutil.which("yt-dlp") or "yt-dlp"]

def run_yt_dlp(url: str, outtmpl: str) -> int:
    """
    Coba download subtitle manual + auto.
    Return code yt-dlp (0 biasanya sukses; non-zero tidak selalu fatal).
    """
    cmd = [
        *ytdlp_base_cmd(),
        "--skip-download",
        "--write-subs",
        "--write-auto-subs",
        "--sub-langs", SUB_LANGS,
        "--sub-format", SUB_FORMAT,
        "--no-warnings",
        "-o", outtmpl,
        url,
    ]
    p = subprocess.run(cmd)
    return p.returncode

def main():
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV tidak ditemukan: {csv_path.resolve()}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    ok = 0
    failed = 0

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Pastikan kolom minimal ada
        for col in ["url", "kategori_5"]:
            if col not in reader.fieldnames:
                raise ValueError(f"Kolom '{col}' tidak ada di CSV. Kolom yang ada: {reader.fieldnames}")

        for row in reader:
            total += 1
            url = (row.get("url") or "").strip()
            kategori = safe_folder_name(row.get("kategori_5") or "Tanpa_Kategori")

            if not url:
                print(f"[SKIP] Baris {total}: url kosong")
                failed += 1
                continue

            cat_dir = OUT_DIR / kategori
            cat_dir.mkdir(parents=True, exist_ok=True)

            # Template output: simpan per kategori, file pakai metadata yt-dlp
            outtmpl = str(cat_dir / "%(upload_date)s_%(id)s_%(title)s.%(ext)s")

            rc = run_yt_dlp(url, outtmpl)

            # yt-dlp bisa return non-zero pada beberapa kasus walau sebagian file tersimpan,
            # jadi kita anggap "ok" bila ada file .vtt baru? (di sini kita pakai heuristik sederhana)
            if rc == 0:
                ok += 1
                print(f"[OK]   {kategori} | {url}")
            else:
                # tetap lanjut, karena mungkin video memang tidak punya subtitles
                failed += 1
                print(f"[FAIL] {kategori} | {url} (rc={rc})")

    print("\n=== RINGKASAN ===")
    print(f"Total: {total}")
    print(f"OK:    {ok}")
    print(f"Fail:  {failed}")
    print(f"Output: {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
