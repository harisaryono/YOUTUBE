#!/usr/bin/env python3
import base64
import re
import json
import os
import sys
import logging
from pathlib import Path
from typing import Optional, Tuple
from curl_cffi import requests
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HTML_ERROR_MARKERS = (
    "<!doctype html",
    "cloudflare used to restrict access",
    "you are being rate limited",
    "access denied | savesubs.com",
    "error 1015",
    "ray id:",
)

def encrypt_token(o: str, r: str) -> str:
    """Algorithm F.encrypt from savesubs.com JS."""
    n = ""
    for idx in range(len(o)):
        char = o[idx]
        key_idx = (idx % len(r)) - 1
        if key_idx < 0:
            key_idx = len(r) + key_idx
        a = r[key_idx]
        n += chr(ord(char) + ord(a))
    return base64.b64encode(n.encode('latin-1')).decode('ascii')


def looks_like_html_error_page(text: str) -> bool:
    normalized = (text or "").strip().lower()
    if not normalized:
        return False
    if normalized.startswith("<html") or normalized.startswith("<!doctype html"):
        return True
    return any(marker in normalized for marker in HTML_ERROR_MARKERS)

def download_savesubs_subtitle(
    youtube_url: str, 
    output_dir: str, 
    headless: bool = True # Kept for compatibility with savesubs_playwright interface
) -> Tuple[Optional[Path], str]:
    """
    Directly download subtitles from savesubs.com using curl-cffi to bypass Cloudflare.
    Does not require Playwright or a real browser binary.
    """
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    
    session = requests.Session(impersonate="chrome")
    
    try:
        # 1. Fetch main page to get session cookies and token variables
        logger.info("📡 Connecting to savesubs.com...")
        r_init = session.get("https://savesubs.com", timeout=30)
        if r_init.status_code != 200:
            return None, f"failed_init_{r_init.status_code}"
        
        html = r_init.text
        
        # 2. Extract CSS hash (used as encryption key 'r')
        css_match = re.search(r'/build/(?:assets/)?main\.([a-f0-9]+)\.css', html)
        if not css_match:
            return None, "css_hash_not_found"
        css_hash = css_match.group(1)
        
        # 3. Extract window.__INIT__.ua (used as entropy 'i')
        ua_match = re.search(r'"ua":"([^"]+)"', html)
        if not ua_match:
            return None, "init_ua_not_found"
        init_ua = ua_match.group(1).replace('\\/', '/')
        
        # 4. Generate x-auth-token
        r = css_hash[::-1]
        i = init_ua[::-1][:15]
        a = "SORRY_MATE"[::-1]
        o = "savesubs.com" + r + i + a
        token = encrypt_token(o, r)
        
        # 5. Determine User-Agent for headers to match the token entropy
        try:
            decoded_ua = base64.b64decode(init_ua.replace(',,', '==')).decode('ascii')
            if decoded_ua.startswith('1005'):
                decoded_ua = decoded_ua[4:]
        except Exception:
            decoded_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        # 6. POST to /action/extract
        logger.info(f"🔍 Extracting transcript for: {youtube_url}")
        extract_url = "https://savesubs.com/action/extract"
        headers = {
            "User-Agent": decoded_ua,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "xmlhttprequest",
            "x-auth-token": token,
            "x-requested-domain": "savesubs.com",
            "Origin": "https://savesubs.com",
            "Referer": f"https://savesubs.com/process?url={quote(youtube_url, safe='')}"
        }
        
        payload = {"data": {"url": youtube_url}}
        r_extract = session.post(extract_url, headers=headers, json=payload, timeout=30)
        
        if r_extract.status_code != 200:
            return None, f"extract_failed_{r_extract.status_code}"
        
        data = r_extract.json()
        if not data.get("status", True):
            message = str(data.get("message") or "").lower()
            if "blocked" in message or "temporarily blocked" in message:
                return None, "blocked"
            if "no subtitles" in message or "not found" in message:
                return None, "no_subtitle"
            return None, "extract_status_false"
        
        response = data.get("response") or {}
        formats = response.get("formats") or []
        if not formats:
            return None, "no_subtitle"
        
        # 7. Pick format and download
        # Preference: txt -> srt -> vtt
        chosen = None
        for fmt in formats:
            if fmt.get("ext") == "txt":
                chosen = fmt
                break
        if not chosen:
            for fmt in formats:
                if fmt.get("ext") in ["srt", "vtt"]:
                    chosen = fmt
                    break
        if not chosen:
            chosen = formats[0]
            
        sub_url = chosen.get("url")
        if not sub_url:
            return None, "no_url"
        if not sub_url.startswith("http"):
            sub_url = "https://savesubs.com" + sub_url
            
        logger.info(f"📥 Downloading subtitle from: {sub_url}")
        r_sub = session.get(sub_url, timeout=30)
        if r_sub.status_code != 200:
            return None, f"download_failed_{r_sub.status_code}"
        if looks_like_html_error_page(r_sub.text):
            return None, "blocked"
            
        final_file = output_path / "subtitle.txt"
        final_file.write_text(r_sub.text, encoding="utf-8")
        
        logger.info(f"✅ Success! Saved to {final_file}")
        return final_file, "ok"

    except Exception as exc:
        logger.error(f"💥 Direct extraction failed: {exc}")
        return None, f"error:{exc}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <youtube_url> [output_dir]")
        sys.exit(1)
        
    yt_url = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "downloads"
    
    path, status = download_savesubs_subtitle(yt_url, out_dir)
    if path:
        print(f"SUCCESS: {path}")
    else:
        print(f"FAILED: {status}")
        sys.exit(2)
