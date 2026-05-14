import argparse
import json
import html
import re
import shutil
import subprocess
import tempfile
import os
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from playwright.sync_api import sync_playwright


SAVESUBS_URL = "https://savesubs.com/"
SAVESUBS_PROCESS_URL = "https://savesubs.com/process?url={url}"
DEFAULT_VENV_DIR = Path("/media/harry/DATA120B/venv_youtube")

NO_SUBTITLE_MARKERS = (
    "no subtitles",
    "no subtitle",
    "subtitle not found",
    "subtitles unavailable",
    "subtitles not available",
    "no transcript",
    "no captions",
    "captions unavailable",
    "captions not available",
)

SAVE_SUBS_BLOCKED_MARKERS = (
    "this page has no download links or temporarily blocked",
    "no download links",
    "temporarily blocked",
    "please try later",
)

HTML_ERROR_MARKERS = (
    "<!doctype html",
    "cloudflare used to restrict access",
    "you are being rate limited",
    "access denied | savesubs.com",
    "error 1015",
    "ray id:",
)


def yt_dlp_command() -> list[str]:
    venv_dir = Path(
        os.environ.get("YOUTUBE_VENV_DIR")
        or os.environ.get("EXTERNAL_VENV_DIR")
        or str(DEFAULT_VENV_DIR)
    )
    candidates = [
        [sys.executable, "-m", "yt_dlp"],
        [shutil.which("yt-dlp") or ""],
        [str(venv_dir / "bin" / "yt-dlp")],
    ]
    for candidate in candidates:
        if not candidate or not candidate[0]:
            continue
        if candidate[0] == sys.executable:
            return candidate
        if Path(candidate[0]).exists():
            return candidate
    return [sys.executable, "-m", "yt_dlp"]


def normalize_text(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def seconds_to_timestamp(seconds: float) -> str:
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"[{hours:02d}:{minutes:02d}:{secs:02d}.000]"


def parse_subtitle_text(raw_text: str) -> str:
    """Parse SRT/VTT subtitle text into timestamped TXT output."""
    text = raw_text.replace("\ufeff", "").strip()
    if not text:
        raise ValueError("Subtitle file kosong.")

    lines = [line.rstrip("\n") for line in text.splitlines()]
    output_lines: list[str] = []
    current_text: list[str] = []
    current_timestamp: Optional[str] = None

    timestamp_re = re.compile(
        r"^\s*(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*"
    )

    def flush() -> None:
        nonlocal current_text, current_timestamp
        if current_timestamp and current_text:
            joined = normalize_text(" ".join(current_text))
            if joined:
                output_lines.append(f"{current_timestamp} {joined}")
        current_text = []
        current_timestamp = None

    for line in lines:
        stripped = line.strip()
        if not stripped:
            flush()
            continue
        if stripped.upper() == "WEBVTT":
            continue
        if stripped.isdigit() and current_timestamp is None and not current_text:
            continue

        match = timestamp_re.match(stripped)
        if match:
            flush()
            hours = int(match.group(1))
            minutes = int(match.group(2))
            seconds = int(match.group(3))
            current_timestamp = f"[{hours:02d}:{minutes:02d}:{seconds:02d}.000]"
            continue

        if current_timestamp is not None:
            current_text.append(stripped)

    flush()

    if not output_lines:
        raise ValueError("Tidak ada timestamp subtitle yang berhasil diparsing.")

    return "\n".join(output_lines)


def save_text_file(output_dir: Path, source_name: str, content: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / source_name
    target.write_text(content, encoding="utf-8")
    return target


def body_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000).lower()
    except Exception:
        return ""


def has_no_subtitle_marker(page) -> bool:
    text = body_text(page)
    return any(marker in text for marker in NO_SUBTITLE_MARKERS)


def has_blocked_marker(page) -> bool:
    text = body_text(page)
    return any(marker in text for marker in SAVE_SUBS_BLOCKED_MARKERS)


def looks_like_html_error_page(text: str) -> bool:
    normalized = (text or "").strip().lower()
    if not normalized:
        return False
    if normalized.startswith("<html") or normalized.startswith("<!doctype html"):
        return True
    return any(marker in normalized for marker in HTML_ERROR_MARKERS)


def wait_for_savesubs_result(page, timeout_seconds: int = 45) -> str:
    """Wait until SaveSubs exposes a subtitle download link or an explicit no-subtitle state."""
    deadline = timeout_seconds
    elapsed = 0
    while elapsed < deadline:
        if page.locator("a[href*='ext=srt']").count() > 0:
            return "available"
        if page.locator("a[href*='ext=vtt']").count() > 0:
            return "available"
        if page.locator("a[href*='ext=txt']").count() > 0:
            return "available"
        if has_blocked_marker(page):
            return "blocked"
        if has_no_subtitle_marker(page):
            return "no_subtitle"
        page.wait_for_timeout(1000)
        elapsed += 1

    return "unknown"


def download_savesubs_subtitle(
    youtube_url: str,
    output_dir: str,
    headless: bool = False,
    timeout_seconds: int = 45,
) -> tuple[Optional[Path], str]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    timeout_ms = max(5000, int(timeout_seconds) * 1000)
    wait_short = max(500, min(2000, timeout_ms // 10))
    wait_medium = max(1000, min(3000, timeout_ms // 5))

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()
        page.goto(SAVESUBS_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(wait_short)

        possible_cookie_buttons = [
            "button:has-text('Accept')",
            "button:has-text('I agree')",
            "button:has-text('Got it')",
            "button:has-text('OK')",
        ]
        for selector in possible_cookie_buttons:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=1000):
                    btn.click()
                    break
            except Exception:
                pass

        process_url = SAVESUBS_PROCESS_URL.format(url=quote(youtube_url, safe=""))
        page.goto(process_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(wait_medium)

        extraction = page.evaluate(
            """
            async ({ url }) => {
              function enc64(e) {
                if (/([^\\u0000-\\u00ff])/.test(e)) throw Error(`Can't base64 encode non-ASCII characters.`);
                var t = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`, n = 0, r, i, a, o = [];
                while (n < e.length) {
                  switch (r = e.charCodeAt(n), a = n % 3, a) {
                    case 0: o.push(t.charAt(r >> 2)); break;
                    case 1: o.push(t.charAt((i & 3) << 4 | r >> 4)); break;
                    case 2: o.push(t.charAt((i & 15) << 2 | r >> 6)), o.push(t.charAt(r & 63)); break;
                  }
                  i = r; n++;
                }
                return a == 0 ? (o.push(t.charAt((i & 3) << 4)), o.push(`==`)) : a == 1 && (o.push(t.charAt((i & 15) << 2)), o.push(`=`)), o.join(``);
              }
              function ord(e) {
                let t = `${e}`, n = t.charCodeAt(0);
                if (n >= 55296 && n <= 56319) {
                  let e = n;
                  if (t.length === 1) return n;
                  let r = t.charCodeAt(1);
                  return (e - 55296) * 1024 + (r - 56320) + 65536;
                }
                return n;
              }
              class F {
                encrypt(e, t) {
                  for (var n = ``, r = 0; r < e.length; r++) {
                    var i = e.substr(r, 1), a = t.substr(r % t.length - 1, 1);
                    i = Math.floor(ord(i) + ord(a)), i = String.fromCharCode(i), n += i;
                  }
                  return enc64(n);
                }
              }
              let e = document.head.innerHTML;
              let n = /\\/build\\/(?:assets\\/)?main\\.([^\"]+?).css/g.exec(e);
              if (!n) return { stage: "token", status: "unknown", error: "main css hash not found" };
              let r = n[1].split('').reverse().join(''),
                  i = window.__INIT__.ua.split('').reverse().join('').substr(0, 15),
                  a = [69,84,65,77,95,89,82,82,79,83].map(e => String.fromCharCode(e)).join('').split('').reverse().join(''),
                  o = window.location.hostname + r + i + a,
                  token = new F().encrypt(o, r);

              const resp = await fetch('/action/extract', {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json; charset=UTF-8',
                  'X-Requested-With': 'xmlhttprequest',
                  'x-auth-token': token,
                  'x-requested-domain': window.location.hostname,
                },
                body: JSON.stringify({ data: { url } }),
              });
              return { stage: "extract", status: resp.status, text: await resp.text(), token };
            }
            """,
            {"url": youtube_url},
        )

        if not isinstance(extraction, dict):
            browser.close()
            return None, "unknown"
        if int(extraction.get("status") or 0) != 200:
            browser.close()
            return None, "blocked"

        raw_response = str(extraction.get("text") or "").strip()
        if not raw_response:
            browser.close()
            return None, "unknown"

        try:
            payload = json.loads(raw_response)
        except Exception:
            browser.close()
            return None, "unknown"

        if not payload.get("status", True):
            message = str(payload.get("message") or "").strip().lower()
            if any(marker in message for marker in NO_SUBTITLE_MARKERS):
                browser.close()
                return None, "no_subtitle"
            if any(marker in message for marker in SAVE_SUBS_BLOCKED_MARKERS) or "blocked" in message:
                browser.close()
                return None, "blocked"
            browser.close()
            return None, "unknown"

        response = payload.get("response") or {}
        formats = response.get("formats") or []
        if not formats:
            message = str(payload.get("message") or "").strip().lower()
            if any(marker in message for marker in NO_SUBTITLE_MARKERS):
                browser.close()
                return None, "no_subtitle"
            browser.close()
            return None, "unknown"

        chosen = formats[0] if isinstance(formats[0], dict) else {}
        subtitle_url = str(chosen.get("url") or "").strip()
        if not subtitle_url:
            browser.close()
            return None, "unknown"

        ext = str(chosen.get("ext") or "srt").strip().lower() or "srt"
        fetch_url = subtitle_url if subtitle_url.startswith("http") else f"https://savesubs.com{subtitle_url}"
        raw_payload = page.evaluate(
            """
            async ({ url }) => {
              const resp = await fetch(url, { credentials: 'include' });
              return {
                status: resp.status,
                text: await resp.text(),
                content_length: resp.headers.get('content-length') || '',
                content_type: resp.headers.get('content-type') || '',
              };
            }
            """,
            {"url": fetch_url},
        )
        if not isinstance(raw_payload, dict):
            browser.close()
            return None, "unknown"

        raw_text = str(raw_payload.get("text") or "")
        content_length = str(raw_payload.get("content_length") or "").strip()
        content_type = str(raw_payload.get("content_type") or "").strip().lower()
        if content_length == "0":
            browser.close()
            return None, "unknown"
        if not raw_text.strip():
            browser.close()
            return None, "unknown"
        if content_type.startswith("text/html") or looks_like_html_error_page(raw_text):
            browser.close()
            return None, "blocked"

        try:
            converted_text = parse_subtitle_text(raw_text) if ext in {"srt", "vtt"} else raw_text.strip()
        except Exception:
            converted_text = raw_text.strip()

        if looks_like_html_error_page(converted_text):
            browser.close()
            return None, "blocked"

        final_target = save_text_file(output_path, "subtitle.txt", converted_text)
        try:
            if final_target.stat().st_size == 0:
                final_target.unlink(missing_ok=True)
                browser.close()
                return None, "unknown"
        except Exception:
            browser.close()
            return None, "unknown"
        print(f"Berhasil download SaveSubs: {final_target}")
        browser.close()
        return final_target, "ok"


def yt_dlp_subtitle_inventory(youtube_url: str) -> tuple[str, dict[str, list[str]], str]:
    cmd = [
        *yt_dlp_command(),
        "--dump-single-json",
        "--no-warnings",
        "--skip-download",
        youtube_url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=60)
    except Exception as exc:
        return "unknown", {"subtitles": [], "automatic_captions": []}, str(exc)

    raw = str(proc.stdout or "").strip()
    if proc.returncode != 0 or not raw:
        detail = (proc.stderr or proc.stdout or "").strip()[:300]
        return "unknown", {"subtitles": [], "automatic_captions": []}, detail

    import json

    try:
        payload = json.loads(raw)
    except Exception as exc:
        return "unknown", {"subtitles": [], "automatic_captions": []}, str(exc)

    subtitles = sorted((payload.get("subtitles") or {}).keys())
    automatic_captions = sorted((payload.get("automatic_captions") or {}).keys())
    if subtitles or automatic_captions:
        return "available", {"subtitles": subtitles, "automatic_captions": automatic_captions}, ""
    return "missing", {"subtitles": [], "automatic_captions": []}, ""


def parse_ytdlp_subtitle_file(path: Path) -> str:
    raw_text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() in {".srt", ".vtt"}:
        return parse_subtitle_text(raw_text)
    if path.suffix.lower() == ".json3":
        import json

        payload = json.loads(raw_text)
        output_lines: list[str] = []
        for event in payload.get("events", []):
            if not event.get("segs"):
                continue
            text = normalize_text("".join(seg.get("utf8", "") for seg in event["segs"]))
            if not text:
                continue
            start_ms = int(event.get("tStartMs", 0))
            output_lines.append(f"{seconds_to_timestamp(start_ms / 1000)} {text}")
        if not output_lines:
            raise ValueError("File json3 tidak berisi subtitle yang bisa dipakai.")
        return "\n".join(output_lines)
    return raw_text.strip()


def download_with_ytdlp(youtube_url: str, output_dir: str) -> Optional[Path]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    inventory_state, inventory, inventory_detail = yt_dlp_subtitle_inventory(youtube_url)
    if inventory_state == "missing":
        return None
    if inventory_state == "unknown" and inventory_detail:
        if any(marker in inventory_detail.lower() for marker in NO_SUBTITLE_MARKERS):
            return None

    langs_to_try: list[str] = []
    for lang in ["id", "en"]:
        if lang not in langs_to_try:
            langs_to_try.append(lang)
    for lang in inventory.get("subtitles", []):
        if lang not in langs_to_try:
            langs_to_try.append(lang)
    for lang in inventory.get("automatic_captions", []):
        if lang not in langs_to_try:
            langs_to_try.append(lang)

    if not langs_to_try:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_base = Path(tmpdir) / "subtitle"
        for lang in langs_to_try[:5]:
            cmd = [
                *yt_dlp_command(),
                "--write-subs",
                "--write-auto-subs",
                "--sub-lang",
                lang,
                "--skip-download",
                "--output",
                str(temp_base),
                youtube_url,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=120)

            candidates = []
            for pattern in ("*.srt", "*.vtt", "*.json3"):
                candidates.extend(Path(tmpdir).glob(f"subtitle*{pattern[1:]}"))

            if not candidates:
                detail = (proc.stderr or proc.stdout or "").lower()
                if any(marker in detail for marker in NO_SUBTITLE_MARKERS):
                    return None
                continue

            candidates.sort(key=lambda p: (p.suffix.lower() not in {".srt", ".vtt", ".json3"}, p.name))
            source_file = candidates[0]
            try:
                converted_text = parse_ytdlp_subtitle_file(source_file)
            except Exception:
                continue

            final_name = re.sub(r"\.(srt|vtt|txt|json3)$", "", source_file.name, flags=re.I) + ".txt"
            return save_text_file(output_path, final_name, converted_text)

    return None


def download_txt_with_timestamp(youtube_url: str, output_dir: str, headless: bool = False):
    """Primary flow: SaveSubs first, yt-dlp fallback, no_subtitle handled explicitly."""
    savesubs_path = None
    try:
        print("Mencoba SaveSubs sebagai jalur utama...")
        savesubs_path, savesubs_status = download_savesubs_subtitle(
            youtube_url=youtube_url,
            output_dir=output_dir,
            headless=headless,
        )
        if savesubs_path is not None:
            return savesubs_path
        if savesubs_status == "no_subtitle":
            print("SaveSubs tidak menemukan subtitle. Ditandai no_subtitle tanpa fallback.")
            return None
        if savesubs_status == "blocked":
            print("SaveSubs blocked. Ditandai blocked tanpa fallback.")
            return None
        print("SaveSubs tidak menemukan subtitle. Coba yt-dlp sebagai fallback...")
    except Exception as exc:
        print(f"SaveSubs gagal: {exc}")

    try:
        print("Mencoba yt-dlp sebagai fallback...")
        ytdlp_path = download_with_ytdlp(youtube_url=youtube_url, output_dir=output_dir)
        if ytdlp_path is not None:
            print(f"Berhasil download via yt-dlp: {ytdlp_path}")
            return ytdlp_path
        print("yt-dlp tidak menemukan subtitle. Ditandai no_subtitle.")
        return None
    except Exception as exc:
        raise RuntimeError(f"SaveSubs dan yt-dlp sama-sama gagal: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(
        description="Download subtitle YouTube via SaveSubs, dengan yt-dlp fallback."
    )
    parser.add_argument("youtube_url", help="Link video YouTube")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="downloads",
        help="Folder output. Default: downloads",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Jalankan browser tanpa tampilan.",
    )

    args = parser.parse_args()

    result = download_txt_with_timestamp(
        youtube_url=args.youtube_url,
        output_dir=args.output_dir,
        headless=args.headless,
    )

    if result is None:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
