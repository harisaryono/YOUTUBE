#!/usr/bin/env python3
"""
recover_transcripts.py
Script untuk memulihkan transkrip yang hilang atau belum didownload.
Menggunakan database_optimized.py, YouTubeTranscriptApi dengan requests.Session (cookies), dan yt-dlp fallback.
"""

import os
import sys
import time
import json
import random
import logging
import hashlib
import subprocess
import tempfile
import shutil
import glob
import http.cookiejar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api.proxies import GenericProxyConfig
except Exception:
    from youtube_transcript_api import YouTubeTranscriptApi
    GenericProxyConfig = None
import yt_dlp

# Load environment variables
load_dotenv()
_ENV_CANDIDATES = (
    (Path(__file__).resolve().parent / ".env.local", False),
    (Path(__file__).resolve().parent / ".env", False),
    (Path("/media/harry/DATA120B/GIT/youtube_transcript_bundle/.env.local"), True),
)
for _env_candidate, _override in _ENV_CANDIDATES:
    if _env_candidate.exists():
        load_dotenv(_env_candidate, override=_override)

# Dashboard / Database
from database_optimized import OptimizedDatabase
from local_services import (
    describe_youtube_auth_source,
    load_webshare_proxy_blocks,
    yt_dlp_command,
    yt_dlp_auth_args,
    yt_dlp_auth_mode,
    upsert_webshare_proxy_block,
    youtube_api_key_pool,
    youtube_cookie_files,
    youtube_cookies_from_browser,
)

# Konfigurasi
DB_PATH = "youtube_transcripts.db"
BASE_DIR = "uploads"
LOG_FILE = str(os.getenv("YT_TRANSCRIPT_LOG_FILE", "recover_transcripts.log") or "recover_transcripts.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _read_env_file_value(path: Path, key: str) -> str:
    if not path.exists():
        return ""
    try:
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            found_key, value = line.split("=", 1)
            if found_key.strip() != key:
                continue
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            return value.strip()
    except Exception:
        return ""
    return ""


class TranscriptRecoverer:
    def __init__(self):
        self.db = OptimizedDatabase(DB_PATH, BASE_DIR)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        ]
        self.cookie_files = youtube_cookie_files()
        self.cookies_from_browser = youtube_cookies_from_browser()
        self.auth_source = describe_youtube_auth_source()
        self.youtube_api_keys = youtube_api_key_pool()
        self.auth_sources = self._build_auth_sources()
        self._auth_cursor = 0
        self.active_cookie_file = ""

        # Inter-video pacing (applies to ALL callers, incl. update_latest_channel_videos.py)
        self._inter_video_delay_min = max(0.0, _env_float("YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN", 8.0))
        self._inter_video_delay_max = max(self._inter_video_delay_min, _env_float("YT_TRANSCRIPT_INTER_VIDEO_DELAY_MAX", 15.0))
        self._disable_inter_video_pacing = str(os.getenv("YT_TRANSCRIPT_DISABLE_INTER_VIDEO_PACING", "0")).strip().lower() not in {"0", "false", "no", "off"}
        self._next_video_earliest = 0.0  # time.monotonic() timestamp

        # Rate-limit protection
        self._consecutive_rate_limited = 0
        self._backoff_seconds = 0.0
        self._backoff_cap_seconds = max(60.0, _env_float("YT_TRANSCRIPT_BACKOFF_CAP_SECONDS", 1800.0))
        self._backoff_start_429_seconds = max(5.0, _env_float("YT_TRANSCRIPT_BACKOFF_START_429_SECONDS", 30.0))
        self._backoff_start_403_seconds = max(5.0, _env_float("YT_TRANSCRIPT_BACKOFF_START_403_SECONDS", 60.0))
        self._backoff_start_ip_blocked_seconds = max(5.0, _env_float("YT_TRANSCRIPT_BACKOFF_START_IP_BLOCKED_SECONDS", 300.0))
        self._webshare_proxy_cache: list[str] = []
        self._webshare_proxy_cache_loaded_at = 0.0
        self._webshare_proxy_cache_ttl_seconds = max(60, _env_int("WEBSHARE_PROXY_CACHE_TTL_SECONDS", 900))
        self._webshare_proxy_max_attempts = max(1, _env_int("WEBSHARE_PROXY_MAX_ATTEMPTS", 10))
        self._webshare_proxy_block_hours = max(1, _env_int("WEBSHARE_PROXY_BLOCK_HOURS", 24))
        self._webshare_escalate_after = max(1, _env_int("YT_TRANSCRIPT_WEBSHARE_ESCALATE_AFTER", 3))
        self._video_max_seconds = max(30.0, _env_float("YT_TRANSCRIPT_VIDEO_MAX_SECONDS", 75.0))
        self._savesubs_timeout_seconds = max(20, _env_int("YT_TRANSCRIPT_SAVESUBS_TIMEOUT_SECONDS", 45))
        self._yt_dlp_timeout_seconds = max(20, _env_int("YT_TRANSCRIPT_YT_DLP_TIMEOUT_SECONDS", 45))
        self._yt_dlp_max_langs = max(1, _env_int("YT_TRANSCRIPT_YT_DLP_MAX_LANGS", 3))
        self._yt_dlp_pre_inventory_sleep_min = max(0.0, _env_float("YT_TRANSCRIPT_YTDLP_PRE_INVENTORY_SLEEP_MIN", 1.0))
        self._yt_dlp_pre_inventory_sleep_max = max(self._yt_dlp_pre_inventory_sleep_min, _env_float("YT_TRANSCRIPT_YTDLP_PRE_INVENTORY_SLEEP_MAX", 2.5))
        self._yt_dlp_between_lang_sleep_min = max(0.0, _env_float("YT_TRANSCRIPT_YTDLP_BETWEEN_LANG_SLEEP_MIN", 1.0))
        self._yt_dlp_between_lang_sleep_max = max(self._yt_dlp_between_lang_sleep_min, _env_float("YT_TRANSCRIPT_YTDLP_BETWEEN_LANG_SLEEP_MAX", 2.5))
        self._skip_expensive_fallback = str(os.getenv("YT_TRANSCRIPT_SKIP_EXPENSIVE_FALLBACK", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._skip_savesubs = str(os.getenv("YT_TRANSCRIPT_SKIP_SAVESUBS", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._webshare_only = str(os.getenv("YT_TRANSCRIPT_WEBSHARE_ONLY", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._webshare_first = str(os.getenv("YT_TRANSCRIPT_WEBSHARE_FIRST", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._prefer_webshare = False
        self._non_webshare_fallback_streak = 0

        self.session = self.auth_sources[0]["session"] if self.auth_sources else self._create_session()
        self.last_transcript_failure_reason = ""
        self.last_savesubs_status = ""
        self._video_deadline_ts = 0.0
        
        if self.auth_source:
            logger.info(f"🍪 Auth YouTube terdeteksi: {self.auth_source}")
        else:
            logger.warning("⚠️ Auth YouTube tidak ditemukan. Video age-restricted kemungkinan gagal.")
        if self._webshare_only:
            logger.info("🌐 Transcript mode: webshare-only (SaveSubs/API/yt-dlp dilewati)")
        elif self._webshare_first:
            logger.info("🌐 Transcript mode: webshare-first (SaveSubs/API/yt-dlp tetap jadi fallback)")
        elif self._skip_savesubs:
            logger.info("🌐 Transcript mode: skip-savesubs (langsung API/yt-dlp, SaveSubs dilewati)")
        yt_mode = yt_dlp_auth_mode()
        if yt_mode == "browser":
            logger.info(f"🌐 yt-dlp auth mode: browser cookies ({', '.join(self.cookies_from_browser)})")
        elif yt_mode == "cookies":
            logger.info("🍪 yt-dlp auth mode: cookies only")
        if self.youtube_api_keys:
            count = len(self.youtube_api_keys)
            logger.info(f"🔑 YouTube API Keys terdeteksi: {count} key(s)")

        self.repo_root = Path(__file__).resolve().parent
        self.local_python = Path(sys.executable)

    def _build_auth_sources(self) -> list[dict]:
        cookie_files = list(self.cookie_files or [])
        if not cookie_files:
            return [
                {
                    "cookie_file": None,
                    "session": self._create_session(None),
                    "label": "anonymous",
                }
            ]

        sources: list[dict] = []
        for cookie_file in cookie_files:
            sources.append(
                {
                    "cookie_file": cookie_file,
                    "session": self._create_session(cookie_file),
                    "label": Path(cookie_file).name,
                }
            )
        return sources

    def _create_session(self, cookie_file: str | None = None):
        """Mengekspor session dengan cookies jika ada."""
        session = requests.Session()
        session.headers.update({'User-Agent': random.choice(self.user_agents)})

        if cookie_file:
            try:
                cj = http.cookiejar.MozillaCookieJar(cookie_file)
                cj.load(ignore_discard=True, ignore_expires=True)
                session.cookies = cj
                logger.info(f"✅ Cookies berhasil dimuat ke session: {cookie_file}")
            except Exception as e:
                logger.error(f"❌ Gagal memuat cookies {cookie_file}: {str(e)}")

        return session

    def _start_video_budget(self) -> None:
        if self._video_max_seconds > 0:
            self._video_deadline_ts = time.monotonic() + self._video_max_seconds
        else:
            self._video_deadline_ts = 0.0

    def _clear_video_budget(self) -> None:
        self._video_deadline_ts = 0.0

    def _video_budget_expired(self) -> bool:
        return bool(self._video_deadline_ts and time.monotonic() >= self._video_deadline_ts)

    def _maybe_bail_video_budget(self, video_id: str) -> bool:
        if self._video_budget_expired():
            self.last_transcript_failure_reason = "video_budget_exceeded"
            logger.warning(f"   ⏱️  Budget per video habis untuk {video_id}; lanjut video berikutnya.")
            return True
        return False

    def _fetch_webshare_proxy_urls(self) -> list[str]:
        """Fetch Webshare proxies only when everything else has failed."""
        api_key = str(os.getenv("WEBSHARE_API_KEY", "") or "").strip()
        if not api_key:
            api_key = _read_env_file_value(Path("/media/harry/DATA120B/GIT/youtube_transcript_bundle/.env.local"), "WEBSHARE_API_KEY")
        if not api_key:
            return []

        mode = str(os.getenv("WEBSHARE_PROXY_MODE", "") or "direct").strip().lower() or "direct"
        api_timeout = max(5.0, _env_float("WEBSHARE_PROXY_API_TIMEOUT_SECONDS", 20.0))
        api_retries = max(0, int(str(os.getenv("WEBSHARE_PROXY_API_RETRIES", "2")).strip() or "2"))

        session = requests.Session()
        headers = {
            "Authorization": f"Token {api_key}",
            "Accept": "application/json",
        }

        urls: list[str] = []
        page = 1
        last_error: Exception | None = None

        while True:
            payload = None
            for attempt in range(api_retries + 1):
                try:
                    response = session.get(
                        "https://proxy.webshare.io/api/v2/proxy/list/",
                        params={"mode": mode, "page": page, "page_size": 100},
                        headers=headers,
                        timeout=api_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt == api_retries:
                        payload = None
                        break
                    time.sleep(min(2 ** attempt, 8))

            if payload is None:
                break

            for proxy in payload.get("results") or []:
                if proxy.get("valid") is False:
                    continue
                username = quote(str(proxy.get("username") or "").strip(), safe="")
                password = quote(str(proxy.get("password") or "").strip(), safe="")
                host = str(proxy.get("proxy_address") or "").strip()
                port = proxy.get("port")
                if mode == "backbone":
                    host = "p.webshare.io"
                if not (username and password and host and port):
                    continue
                urls.append(f"http://{username}:{password}@{host}:{int(port)}")

            if not payload.get("next"):
                break
            page += 1

        if urls:
            return urls

        if last_error is not None:
            logger.warning(f"   ⚠️  Gagal mengambil daftar proxy Webshare: {last_error}")
        return []

    def _get_webshare_proxy_urls(self, video_id: str) -> list[str]:
        now = time.monotonic()
        cache_age = now - self._webshare_proxy_cache_loaded_at if self._webshare_proxy_cache_loaded_at > 0 else None
        if (
            self._webshare_proxy_cache_loaded_at > 0
            and cache_age is not None
            and cache_age < self._webshare_proxy_cache_ttl_seconds
        ):
            proxy_urls = list(self._webshare_proxy_cache)
        else:
            proxy_urls = self._fetch_webshare_proxy_urls()
            self._webshare_proxy_cache = list(proxy_urls)
            self._webshare_proxy_cache_loaded_at = now

        proxy_urls = self._filter_blocked_webshare_proxy_urls(proxy_urls, video_id=video_id)
        if not proxy_urls:
            return []

        if len(proxy_urls) > 1 and video_id:
            seed = int(hashlib.sha1(video_id.encode("utf-8")).hexdigest(), 16)
            offset = seed % len(proxy_urls)
            if offset:
                proxy_urls = proxy_urls[offset:] + proxy_urls[:offset]
        return proxy_urls

    def _filter_blocked_webshare_proxy_urls(self, proxy_urls: list[str], *, video_id: str = "") -> list[str]:
        if not proxy_urls:
            return []
        blocked_state = load_webshare_proxy_blocks()
        if not blocked_state:
            return list(proxy_urls)
        filtered = [proxy for proxy in proxy_urls if proxy not in blocked_state]
        removed = len(proxy_urls) - len(filtered)
        if removed > 0:
            label = f" untuk {video_id}" if video_id else ""
            logger.info(f"   🧱 Skip {removed} proxy Webshare yang masih kena cooldown{label}.")
        return filtered

    def _block_webshare_proxy(self, proxy_url: str, reason: str, *, source: str = "") -> None:
        proxy_url = str(proxy_url or "").strip()
        if not proxy_url:
            return
        blocked_until = datetime.now(timezone.utc) + timedelta(hours=self._webshare_proxy_block_hours)
        upsert_webshare_proxy_block(
            proxy_url,
            blocked_until.isoformat(),
            reason=reason,
            source=source,
        )

    def _looks_like_proxy_block_error(self, text: str) -> bool:
        msg = str(text or "").lower()
        return any(
            phrase in msg
            for phrase in [
                "blocking your requests, despite you using proxies",
                "youtube is blocking your requests",
                "youtube is block",
                "blocked requests from your ip",
                "your ip has been blocked",
                "ip has been blocked",
                "ipblocked",
                "requestblocked",
            ]
        )

    def _reset_webshare_pressure(self) -> None:
        self._non_webshare_fallback_streak = 0
        self._prefer_webshare = False

    def _record_webshare_pressure(self, reason: str) -> None:
        self._non_webshare_fallback_streak += 1
        if self._non_webshare_fallback_streak >= self._webshare_escalate_after:
            if not self._prefer_webshare:
                logger.warning(
                    f"   ⚡ Fallback non-paid terlalu sering ({self._non_webshare_fallback_streak}x, "
                    f"reason={reason or 'unknown'}); Webshare diprioritaskan lebih dulu."
                )
            self._prefer_webshare = True

    def _looks_like_terminal_transcript_error(self, text: str) -> bool:
        msg = str(text or "").lower()
        return any(
            phrase in msg
            for phrase in [
                "video is unavailable",
                "the video is unavailable",
                "transcript is disabled",
                "subtitles are disabled",
                "no transcripts were found",
                "could not retrieve a transcript",
                "video is unp",
            ]
        )

    def _download_transcript_via_webshare(
        self,
        video_id: str,
        languages: list[str],
        cookie_file: str | None = None,
    ) -> tuple[Optional[Dict], str]:
        """Last-resort fallback using paid Webshare proxies."""
        proxy_urls = self._get_webshare_proxy_urls(video_id)
        if not proxy_urls:
            if load_webshare_proxy_blocks():
                self.last_transcript_failure_reason = "webshare_proxy_block_cooldown"
                return None, "proxy_block"
            return None, "webshare_unavailable"

        saw_retry_later = False
        saw_geo_blocked = False
        saw_proxy_block = False
        errors: list[str] = []

        for attempt_idx, proxy_url in enumerate(proxy_urls, start=1):
            if attempt_idx > self._webshare_proxy_max_attempts:
                logger.warning(
                    f"   ⚠️  Batas percobaan Webshare tercapai untuk {video_id} "
                    f"({self._webshare_proxy_max_attempts} proxy)."
                )
                break
            session = self._create_session(cookie_file)
            session.proxies.update({"http": proxy_url, "https": proxy_url})

            try:
                api_kwargs = {"http_client": session}
                if GenericProxyConfig is not None:
                    api_kwargs["proxy_config"] = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
                api = YouTubeTranscriptApi(**api_kwargs)
                transcript_list = api.list(video_id)

                found_transcript = None
                try:
                    found_transcript = transcript_list.find_transcript(languages)
                except Exception:
                    found_transcript = next(iter(transcript_list))

                data = found_transcript.fetch()
                lang = found_transcript.language_code

                formatted_text = ""
                clean_text = ""
                for item in data:
                    start = item.start
                    m, s = divmod(int(start), 60)
                    h, m = divmod(m, 60)
                    ts = f"[{h:02d}:{m:02d}:{s:02d}.000]"
                    formatted_text += f"{ts} {item.text}\n"
                    clean_text += f"{item.text} "

                logger.info(f"   ✅ Webshare fallback berhasil untuk {video_id} ({lang}).")
                self.reset_rate_limit()
                return {
                    "formatted": formatted_text.strip(),
                    "language": lang,
                    "word_count": len(clean_text.split()),
                    "line_count": len(data),
                }, "ok"
            except Exception as exc:
                err = str(exc)
                errors.append(err)
                failure_kind = self._transcript_failure_kind(err)
                if failure_kind == "geo_blocked":
                    saw_geo_blocked = True
                elif failure_kind == "retry_later":
                    saw_retry_later = True
                if self._looks_like_proxy_block_error(err):
                    saw_proxy_block = True
                    self._block_webshare_proxy(proxy_url, err[:500], source=video_id)
                    logger.warning(
                        f"   🧱 Webshare proxy diblok untuk {video_id}; cooldown {self._webshare_proxy_block_hours} jam."
                    )
                    continue
                if self._looks_like_terminal_transcript_error(err):
                    self.last_transcript_failure_reason = err[:500] or "webshare_terminal_error"
                    logger.warning(
                        f"   ⚠️  Webshare terminal untuk {video_id} [{type(exc).__name__}]: {err[:140]}"
                    )
                    return None, "fatal"
                logger.warning(
                    f"   ⚠️  Webshare fallback gagal ({video_id}) [{type(exc).__name__}]: {err[:140]}"
                )

        if saw_proxy_block:
            self.last_transcript_failure_reason = "; ".join(errors[:3]) or "webshare_proxy_block"
            return None, "proxy_block"

        if saw_geo_blocked:
            self.last_transcript_failure_reason = "; ".join(errors[:3]) or "geo_blocked"
            return None, "geo_blocked"

        if saw_retry_later:
            return None, "retry_later"

        self.last_transcript_failure_reason = "; ".join(errors[:3]) or "webshare_fallback_failed"
        return None, "fatal"

    def _inter_video_wait(self) -> None:
        """Ensure a minimum delay between transcript attempts to reduce YouTube blocks.

        This is intentionally inside the recoverer so callers don't need to remember to sleep.
        """
        if self._disable_inter_video_pacing or self._inter_video_delay_max <= 0.0:
            return

        now = time.monotonic()
        if self._next_video_earliest and now < self._next_video_earliest:
            wait = self._next_video_earliest - now
            # Small jitter to desynchronize multi-process starts.
            wait += random.uniform(0.0, min(0.75, max(0.1, wait * 0.1)))
            if wait >= 0.25:
                logger.info(f"   💤 Pacing: tunggu {wait:.1f}s sebelum request berikutnya.")
            time.sleep(wait)

    def _schedule_next_video_delay(self, factor: float = 1.0) -> None:
        if self._disable_inter_video_pacing or self._inter_video_delay_max <= 0.0:
            return
        safe_factor = max(0.0, float(factor or 1.0))
        delay = random.uniform(self._inter_video_delay_min, self._inter_video_delay_max) * safe_factor
        self._next_video_earliest = max(self._next_video_earliest, time.monotonic() + delay)

    def _rate_limit_wait(self) -> None:
        """Exponential backoff with jitter when rate-limited."""
        if self._consecutive_rate_limited == 0:
            return
        base = min(self._backoff_seconds, self._backoff_cap_seconds)
        jitter = random.uniform(0, base * 0.3)
        wait = base + jitter
        logger.warning(
            f"   ⏳ Rate-limit backoff: tunggu {wait:.1f}s "
            f"(consecutive={self._consecutive_rate_limited}, base={base:.1f}s)"
        )
        time.sleep(wait)
        self.session = self._create_session(self.active_cookie_file or None)

    def _looks_like_retry_later_error(self, text: str) -> bool:
        msg = str(text or "").lower()
        return any(
            phrase in msg
            for phrase in [
                "sign in to confirm you're not a bot",
                "sign in to confirm you’re not a bot",
                "too many requests",
                "429",
                "403",
                "forbidden",
                "ip has been blocked",
                "blocked requests from your ip",
                "ipblocked",
                "requestblocked",
                "captcha",
                "unusual traffic",
                "rate limit",
            ]
        )

    def _looks_like_geo_region_block_error(self, text: str) -> bool:
        msg = " ".join(str(text or "").lower().split())
        return any(
            phrase in msg
            for phrase in [
                "not made this video available in your country",
                "not available in your country",
                "not available in your region",
                "this video is not available in your country",
                "video is not available in your country",
                "video unavailable in your country",
                "available only in",
                "only available in",
                "country restricted",
                "region restricted",
                "region locked",
                "geo blocked",
                "geo-blocked",
                "country block",
            ]
        )

    def _transcript_failure_kind(self, text: str) -> str:
        msg = str(text or "")
        if self._looks_like_geo_region_block_error(msg):
            return "geo_blocked"
        if self._looks_like_retry_later_error(msg):
            return "retry_later"
        return ""

    def _looks_like_video_too_long_error(self, text: str) -> bool:
        msg = str(text or "").lower()
        return any(
            phrase in msg
            for phrase in [
                "too long",
                "request too large",
                "payload too large",
                "entity too large",
                "content too large",
                "413",
                "maximum length",
                "length exceeded",
                "exceeds the limit",
                "video too long",
                "transcript too long",
                "audio too long",
            ]
        )

    def _log_transcript_exception(self, stage: str, video_id: str, exc: Exception) -> str:
        """Log transcript-stage exceptions with their concrete exception class."""
        exc_name = type(exc).__name__
        detail = str(exc).strip()
        if detail:
            logger.warning(f"   ⚠️  {stage} gagal ({video_id}) [{exc_name}]: {detail[:180]}")
        else:
            logger.warning(f"   ⚠️  {stage} gagal ({video_id}) [{exc_name}]")
        return detail or exc_name

    def reset_rate_limit(self) -> None:
        self._consecutive_rate_limited = 0
        self._backoff_seconds = 0.0

    def _yt_dlp_subtitle_inventory(self, video_id: str, cookie_file: str | None = None) -> Tuple[str, Dict[str, List[str]], str]:
        """Return subtitle availability from yt-dlp metadata.

        States:
        - available: yt-dlp metadata explicitly lists subtitles or automatic captions
        - missing: yt-dlp metadata loaded successfully and lists no subtitles/captions
        - unknown: yt-dlp metadata could not be fetched/parsing failed
        """
        cmd = [
            *yt_dlp_command(),
            '--dump-single-json',
            '--no-warnings',
            '--skip-download',
        ]
        cmd.extend(yt_dlp_auth_args(cookie_file=cookie_file))
        cmd.append(f"https://www.youtube.com/watch?v={video_id}")

        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=60)
        except Exception as exc:
            logger.warning(f"   ⚠️  Inventory yt-dlp gagal ({video_id}): {exc}")
            return "unknown", {"subtitles": [], "automatic_captions": []}, str(exc)

        raw = str(proc.stdout or "").strip()
        if proc.returncode != 0 or not raw:
            detail = (proc.stderr or proc.stdout or "").strip()[:300]
            logger.warning(f"   ⚠️  Inventory yt-dlp tidak memberi JSON ({video_id}): {detail}")
            return "unknown", {"subtitles": [], "automatic_captions": []}, detail

        try:
            payload = json.loads(raw)
        except Exception as exc:
            logger.warning(f"   ⚠️  Inventory yt-dlp JSON invalid ({video_id}): {exc}")
            return "unknown", {"subtitles": [], "automatic_captions": []}, str(exc)

        subtitles = sorted((payload.get('subtitles') or {}).keys())
        automatic_captions = sorted((payload.get('automatic_captions') or {}).keys())
        if subtitles or automatic_captions:
            return "available", {
                "subtitles": subtitles,
                "automatic_captions": automatic_captions,
            }, ""
        return "missing", {"subtitles": [], "automatic_captions": []}, ""

    def _pick_language_from_inventory(self, video_id: str, cookie_file: str | None = None) -> str:
        """Choose a stable transcript language code from yt-dlp metadata.

        SaveSubs is the primary fetch path, but the DB still needs a compact language code.
        We keep the existing id/en preference, then fall back to the first available code.
        """
        inventory_state, inventory, _detail = self._yt_dlp_subtitle_inventory(video_id, cookie_file=cookie_file)
        preferred = ["id", "en"]
        seen: list[str] = []
        for key in ("subtitles", "automatic_captions"):
            for lang in inventory.get(key, []):
                lang = str(lang or "").strip()
                if lang and lang not in seen:
                    seen.append(lang)
        for lang in preferred + seen:
            if lang:
                return lang
        if inventory_state == "available":
            return "und"
        return "id"

    def _download_savesubs(self, video_id: str) -> tuple[Optional[str], str]:
        """Call the SaveSubs downloader using the repo-local virtualenv.

        The production wrapper may run under an external venv that does not include
        Playwright. This helper keeps the dependency isolated to the repo-local venv.
        """
        if not self.local_python.exists():
            return None, "unknown"

        helper_code = r"""
import json
import sys
from pathlib import Path
from savesubs_playwright import download_savesubs_subtitle

video_url = sys.argv[1]
output_dir = sys.argv[2]
timeout_seconds = int(sys.argv[3])
path, status = download_savesubs_subtitle(video_url, output_dir, headless=True, timeout_seconds=timeout_seconds)
print(json.dumps({"status": status, "path": str(path) if path else ""}))
"""
        tmpdir = tempfile.mkdtemp(prefix=f"savesubs_{video_id}_")
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        def _cleanup_tmpdir() -> None:
            shutil.rmtree(tmpdir, ignore_errors=True)
        if self._video_budget_expired():
            _cleanup_tmpdir()
            return None, "retry_later"
        try:
            proc = subprocess.run(
                [str(self.local_python), "-c", helper_code, video_url, tmpdir, str(self._savesubs_timeout_seconds)],
                capture_output=True,
                text=True,
                timeout=self._savesubs_timeout_seconds + 20,
                check=False,
            )
        except Exception as exc:
            _cleanup_tmpdir()
            return None, f"unknown:{exc}"

        raw = str(proc.stdout or "").strip().splitlines()
        if not raw:
            detail = (proc.stderr or "").strip()[:200]
            _cleanup_tmpdir()
            return None, f"unknown:{detail or 'empty_output'}"

        try:
            payload = json.loads(raw[-1])
        except Exception as exc:
            detail = (proc.stderr or proc.stdout or "").strip()[:200]
            _cleanup_tmpdir()
            return None, f"unknown:{detail or str(exc)}"

        status = str(payload.get("status") or "unknown")
        path = str(payload.get("path") or "").strip() or None
        return path, status

    def _download_transcript_once(
        self,
        video_id: str,
        languages: list[str],
        cookie_file: str | None,
    ) -> tuple[Optional[Dict], str]:
        """Coba satu profile auth untuk satu video."""
        delay_factor = 1.0
        retry_later = False
        geo_blocked = False
        proxy_block = False
        save_subs_blocked = False
        self.last_transcript_failure_reason = ""
        prefer_webshare = self._prefer_webshare
        webshare_attempted_early = False

        if self._webshare_only:
            logger.info(f"   🌐 Mode Webshare-only aktif untuk {video_id}; lewati SaveSubs/API/yt-dlp.")
            result, outcome = self._download_transcript_via_webshare(video_id, languages, cookie_file=cookie_file)
            if result:
                self.reset_rate_limit()
                self._reset_webshare_pressure()
                return result, "ok"
            if outcome == "proxy_block":
                self.last_transcript_failure_reason = self.last_transcript_failure_reason or "proxy_block"
                return None, "proxy_block"
            if outcome == "geo_blocked":
                self.last_transcript_failure_reason = self.last_transcript_failure_reason or "geo_blocked"
                return None, "geo_blocked"
            if outcome == "retry_later":
                self.last_transcript_failure_reason = self.last_transcript_failure_reason or "retry_later"
                return None, "retry_later"
            if outcome == "webshare_unavailable":
                self.last_transcript_failure_reason = "webshare_unavailable"
            return None, "fatal"

        if self._webshare_first or prefer_webshare:
            if self._maybe_bail_video_budget(video_id):
                return None, "retry_later"
            webshare_attempted_early = True
            logger.info(f"   ⚡ Webshare diprioritaskan lebih dulu untuk {video_id}.")
            result, outcome = self._download_transcript_via_webshare(video_id, languages, cookie_file=cookie_file)
            if result:
                self.reset_rate_limit()
                self._reset_webshare_pressure()
                return result, "ok"
            if outcome == "proxy_block":
                logger.warning(f"   🧱 Webshare proxy diblok untuk {video_id}; lanjut ke jalur non-paid.")
                proxy_block = True
            elif outcome == "geo_blocked":
                geo_blocked = True
                self.last_transcript_failure_reason = self.last_transcript_failure_reason or "geo_blocked"
            elif outcome == "retry_later":
                retry_later = True
            elif outcome == "webshare_unavailable":
                logger.warning(f"   ⚠️  Webshare belum tersedia untuk {video_id}; lanjut ke jalur non-paid.")
            else:
                logger.warning(f"   ⚠️  Webshare awal gagal untuk {video_id}; lanjut ke jalur non-paid.")

        # 0. Coba YouTubeTranscriptApi via session
        try:
            if self._maybe_bail_video_budget(video_id):
                return None, "retry_later"
            logger.info(f"   📡 Menaik transkrip {video_id} via API (Session)...")

            api = YouTubeTranscriptApi(http_client=self.session)
            transcript_list = api.list(video_id)

            found_transcript = None
            try:
                found_transcript = transcript_list.find_transcript(languages)
            except Exception:
                found_transcript = next(iter(transcript_list))

            data = found_transcript.fetch()
            lang = found_transcript.language_code

            formatted_text = ""
            clean_text = ""
            for item in data:
                start = item.start
                m, s = divmod(int(start), 60)
                h, m = divmod(m, 60)
                ts = f"[{h:02d}:{m:02d}:{s:02d}.000]"
                formatted_text += f"{ts} {item.text}\n"
                clean_text += f"{item.text} "

            self.reset_rate_limit()
            self._reset_webshare_pressure()
            return {
                "formatted": formatted_text.strip(),
                "language": lang,
                "word_count": len(clean_text.split()),
                "line_count": len(data),
            }, "ok"
        except Exception as e:
            err_msg = str(e).lower()
            self.last_transcript_failure_reason = self._log_transcript_exception("API", video_id, e)
            failure_kind = self._transcript_failure_kind(err_msg)
            if failure_kind == "geo_blocked":
                geo_blocked = True
                delay_factor = max(delay_factor, 1.5)
            elif failure_kind == "retry_later":
                retry_later = True
                delay_factor = max(delay_factor, 2.0)

        if self._skip_expensive_fallback:
            if self._looks_like_terminal_transcript_error(err_msg):
                logger.info(
                    f"   ℹ️  API mengarah ke transcript terminal untuk {video_id}; "
                    f"cek inventory yt-dlp sebelum pulang."
                )
                if self._maybe_bail_video_budget(video_id):
                    return None, "retry_later"
                delay_factor = max(delay_factor, 1.5)
                time.sleep(random.uniform(self._yt_dlp_pre_inventory_sleep_min, self._yt_dlp_pre_inventory_sleep_max))
                inventory_state, inventory, inventory_detail = self._yt_dlp_subtitle_inventory(video_id, cookie_file=cookie_file)
                if inventory_state == "missing":
                    logger.info(
                        f"   ℹ️  Inventory yt-dlp eksplisit kosong untuk {video_id}; "
                        f"no_subtitle valid walau fallback mahal dimatikan."
                    )
                    self.last_transcript_failure_reason = self.last_transcript_failure_reason or "subtitle_inventory_missing"
                    return None, "no_subtitle"
                if inventory_state == "unknown":
                    if self._looks_like_geo_region_block_error(inventory_detail):
                        geo_blocked = True
                        self.last_transcript_failure_reason = inventory_detail or self.last_transcript_failure_reason or "geo_blocked"
                        logger.warning(
                            f"   🌍 Inventory subtitle mengarah ke geo/region block untuk {video_id}; "
                            f"akan dijadwalkan ulang dengan region/proxy yang cocok."
                        )
                    elif self._looks_like_retry_later_error(inventory_detail):
                        retry_later = True
                        self.last_transcript_failure_reason = inventory_detail or self.last_transcript_failure_reason or "retry_later"

            if geo_blocked:
                self.last_transcript_failure_reason = self.last_transcript_failure_reason or "geo_blocked"
                logger.warning(
                    f"   ⏭️  Expensive fallback dimatikan untuk {video_id}; tandai geo_blocked tanpa yt-dlp/Webshare."
                )
                return None, "geo_blocked"

            self.last_transcript_failure_reason = self.last_transcript_failure_reason or "expensive_fallback_skipped"
            logger.warning(
                f"   ⏭️  Expensive fallback dimatikan untuk {video_id}; tandai retry_later tanpa yt-dlp/Webshare."
            )
            return None, "retry_later"

        # 1. Inventory yt-dlp: no_subtitle hanya boleh jika metadata eksplisit kosong
        if self._maybe_bail_video_budget(video_id):
            return None, "retry_later"
        delay_factor = max(delay_factor, 1.5)
        time.sleep(random.uniform(self._yt_dlp_pre_inventory_sleep_min, self._yt_dlp_pre_inventory_sleep_max))
        logger.info(f"   📡 Mencoba fallback yt-dlp untuk {video_id}...")
        inventory_state, inventory, inventory_detail = self._yt_dlp_subtitle_inventory(video_id, cookie_file=cookie_file)
        if inventory_state == "missing":
            logger.info(f"   ℹ️  yt-dlp inventory eksplisit kosong untuk {video_id}; no_subtitle valid.")
            return None, "no_subtitle"
        if inventory_state == "unknown":
            failure_kind = self._transcript_failure_kind(inventory_detail)
            if failure_kind == "geo_blocked":
                geo_blocked = True
                self.last_transcript_failure_reason = inventory_detail or "geo_blocked"
                logger.warning(
                    f"   🌍 Inventory subtitle mengarah ke geo/region block untuk {video_id}; "
                    f"akan dijadwalkan ulang dengan region/proxy yang cocok."
                )
            elif failure_kind == "retry_later":
                retry_later = True
                self.last_transcript_failure_reason = inventory_detail or "retry_later"
                logger.warning(
                    f"   ⚠️  Inventory subtitle memicu challenge/rate-limit untuk {video_id}; "
                    f"akan dijadwalkan ulang nanti."
                )
            else:
                logger.warning(f"   ⚠️  Inventory subtitle tidak meyakinkan untuk {video_id}; lanjut ke fallback terakhir.")
                self.last_transcript_failure_reason = inventory_detail or "inventory_unknown"

        # Delay between inventory check and actual subtitle download
        if self._maybe_bail_video_budget(video_id):
            return None, "retry_later"
        time.sleep(random.uniform(self._yt_dlp_pre_inventory_sleep_min, self._yt_dlp_pre_inventory_sleep_max))

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_base = os.path.join(tmpdir, f"sub_{video_id}")
            langs_to_try: List[str] = []
            # Priority: user-requested languages first, then inventory
            priority_langs = list(languages)
            for lang in priority_langs:
                lang = str(lang or "").strip()
                if lang and lang not in langs_to_try:
                    langs_to_try.append(lang)
            for lang in inventory.get("subtitles", []):
                lang = str(lang or "").strip()
                if lang and lang not in langs_to_try:
                    langs_to_try.append(lang)
            for lang in inventory.get("automatic_captions", []):
                lang = str(lang or "").strip()
                if lang and lang not in langs_to_try:
                    langs_to_try.append(lang)
            MAX_FALLBACK_LANGS = self._yt_dlp_max_langs
            if len(langs_to_try) > MAX_FALLBACK_LANGS:
                logger.info(
                    f"   ℹ️  Membatasi bahasa dari {len(langs_to_try)} ke {MAX_FALLBACK_LANGS} untuk menghindari rate-limit."
                )
                langs_to_try = langs_to_try[:MAX_FALLBACK_LANGS]

            for l in langs_to_try:
                try:
                    if self._maybe_bail_video_budget(video_id):
                        return None, "retry_later"
                    cmd = [
                        *yt_dlp_command(),
                        "--write-auto-subs",
                        "--write-subs",
                        "--sub-lang",
                        l,
                        "--sub-format",
                        "json3",
                        "--skip-download",
                        "--output",
                        temp_base,
                    ]
                    cmd.extend(yt_dlp_auth_args(cookie_file=cookie_file))
                    cmd.append(f"https://www.youtube.com/watch?v={video_id}")
                    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=self._yt_dlp_timeout_seconds)

                    pattern = temp_base + "*.json3"
                    files = glob.glob(pattern)
                    if files:
                        sub_file = files[0]
                        logger.info(f"   💾 Membaca file subtitle (yt-dlp): {os.path.basename(sub_file)}")
                        with open(sub_file, "r", encoding="utf-8") as f:
                            sub_data = json.load(f)

                        formatted_text = ""
                        clean_text = ""
                        line_count = 0

                        for event in sub_data.get("events", []):
                            if event.get("segs"):
                                text = "".join([seg.get("utf8", "") for seg in event["segs"]])
                                if text.strip():
                                    start_ms = event.get("tStartMs", 0)
                                    s = start_ms / 1000
                                    m, s_int = divmod(int(s), 60)
                                    h, m = divmod(m, 60)
                                    ts = f"[{h:02d}:{m:02d}:{s_int:02d}.000]"

                                    formatted_text += f"{ts} {text.strip()}\n"
                                    clean_text += f"{text.strip()} "
                                    line_count += 1

                        if line_count > 0:
                            self.reset_rate_limit()
                            return {
                                "formatted": formatted_text.strip(),
                                "language": l,
                                "word_count": len(clean_text.split()),
                                "line_count": line_count,
                            }, "ok"
                    if proc.returncode != 0:
                        detail = (proc.stderr or proc.stdout or "").strip()[:300]
                        logger.warning(f"   ⚠️  yt-dlp {l} returncode={proc.returncode}: {detail}")
                        self.last_transcript_failure_reason = detail or self.last_transcript_failure_reason or "yt_dlp_subtitle_download_failed"
                        failure_kind = self._transcript_failure_kind(detail)
                        if failure_kind == "geo_blocked":
                            geo_blocked = True
                            break
                        if failure_kind == "retry_later":
                            retry_later = True
                            break
                except Exception as e_yt:
                    self.last_transcript_failure_reason = self._log_transcript_exception(f"yt-dlp {l}", video_id, e_yt)
                    failure_kind = self._transcript_failure_kind(str(e_yt))
                    if failure_kind == "geo_blocked":
                        geo_blocked = True
                        break
                    if failure_kind == "retry_later":
                        retry_later = True
                        break
                    continue

                time.sleep(random.uniform(self._yt_dlp_between_lang_sleep_min, self._yt_dlp_between_lang_sleep_max))

        # 2. SaveSubs via Playwright helper.
        if self._skip_savesubs:
            logger.info(f"   ⏭️  SaveSubs dilewati untuk {video_id}; lanjut Webshare/final fallback.")
        else:
            try:
                if self._maybe_bail_video_budget(video_id):
                    return None, "retry_later"
                logger.info(f"   🌐 Mencoba SaveSubs untuk {video_id}...")
                saved_path, saved_status = self._download_savesubs(video_id)
                self.last_savesubs_status = str(saved_status or "")
                if saved_status == "no_subtitle":
                    logger.info(
                        f"   ℹ️  SaveSubs menandai no_subtitle untuk {video_id}; "
                        f"lanjut verifikasi via fallback lain."
                    )
                    self.last_transcript_failure_reason = "save_subs_no_subtitle"
                if saved_status == "blocked":
                    save_subs_blocked = True
                    self.last_transcript_failure_reason = "save_subs_blocked_member_only"
                    logger.info(
                        f"   ⛔ SaveSubs blocked untuk {video_id}; diduga false positive, lanjut fallback terakhir."
                    )
                if saved_status == "ok" and saved_path:
                    saved_file = Path(saved_path)
                    try:
                        transcript_text = saved_file.read_text(encoding="utf-8", errors="replace").strip()
                    finally:
                        shutil.rmtree(saved_file.parent, ignore_errors=True)
                    lowered = transcript_text.lower()
                    if (
                        lowered.startswith("<!doctype html")
                        or lowered.startswith("<html")
                        or "cloudflare used to restrict access" in lowered
                        or "access denied | savesubs.com" in lowered
                        or "you are being rate limited" in lowered
                        or "error 1015" in lowered
                    ):
                        save_subs_blocked = True
                        self.last_transcript_failure_reason = "save_subs_html_error"
                        logger.warning(f"   ⚠️  SaveSubs mengembalikan HTML error untuk {video_id}; lanjut fallback.")
                        transcript_text = ""
                    if transcript_text:
                        lang = self._pick_language_from_inventory(video_id, cookie_file=cookie_file)
                        line_count = sum(1 for line in transcript_text.splitlines() if line.strip())
                        word_count = len(transcript_text.split())
                        self.reset_rate_limit()
                        self._reset_webshare_pressure()
                        logger.info(f"   ✅ SaveSubs berhasil untuk {video_id} ({lang}).")
                        return {
                            "formatted": transcript_text,
                            "language": lang,
                            "word_count": word_count,
                            "line_count": line_count,
                        }, "ok"
                    self.last_transcript_failure_reason = "save_subs_empty_output"
                    logger.warning(f"   ⚠️  SaveSubs menghasilkan file kosong untuk {video_id}.")
            except Exception as e:
                self.last_transcript_failure_reason = self._log_transcript_exception("SaveSubs", video_id, e)

        if str(os.getenv("WEBSHARE_API_KEY", "") or "").strip():
            if self._maybe_bail_video_budget(video_id):
                return None, "retry_later"
            if webshare_attempted_early:
                logger.info(f"   ℹ️  Webshare sudah dicoba lebih awal untuk {video_id}; skip retry akhir.")
            else:
                logger.info(f"   🌐 Mencoba fallback Webshare terakhir untuk {video_id}...")
                result, outcome = self._download_transcript_via_webshare(video_id, languages, cookie_file=cookie_file)
                if result:
                    self._reset_webshare_pressure()
                    return result, "ok"
                if outcome == "geo_blocked":
                    geo_blocked = True
                elif outcome == "retry_later":
                    retry_later = True
                elif outcome == "webshare_unavailable" and self.last_transcript_failure_reason:
                    logger.warning(f"   ⚠️  Webshare tidak tersedia untuk {video_id}; lanjut pakai status direct.")

        if geo_blocked:
            logger.warning(
                f"   🌍 Subtitle fetch untuk {video_id} terdeteksi geo/region block; "
                f"akan dicoba lagi dengan region/proxy yang cocok."
            )
            return None, "geo_blocked"

        if proxy_block:
            logger.warning(
                f"   🧱 Subtitle fetch untuk {video_id} kena proxy block; akan dicoba lagi nanti setelah cooldown."
            )
            return None, "proxy_block"

        if retry_later:
            logger.warning(
                f"   ⚠️  Subtitle fetch untuk {video_id} memicu challenge/rate-limit; "
                f"akan dicoba lagi nanti."
            )
            return None, "retry_later"

        if save_subs_blocked:
            logger.warning(
                f"   ⛔ SaveSubs blocked tetap gagal setelah fallback lain untuk {video_id}."
            )
            return None, "blocked"

        logger.warning(f"   ⚠️  Subtitle inventory ada, tapi file subtitle tidak berhasil diunduh untuk {video_id}.")
        self.last_transcript_failure_reason = "subtitle_download_failed"
        return None, "fatal"

    def download_transcript(self, video_id: str, languages=['id', 'en']) -> tuple[Optional[Dict], str]:
        """
        Mencoba mendownload transkrip menggunakan YouTubeTranscriptApi dan fallback yt-dlp.
        Returns: (result_dict, outcome)
        outcome:
        - ok
        - no_subtitle
        - blocked
        - proxy_block
        - geo_blocked
        - retry_later
        - fatal
        """
        try:
            # Apply (1) rate-limit backoff and (2) baseline pacing before any request
            self._rate_limit_wait()
            self._inter_video_wait()
            self._start_video_budget()
            sources = list(self.auth_sources or [])
            if not sources:
                sources = [{"cookie_file": None, "session": self.session, "label": "anonymous"}]

            start_index = self._auth_cursor % len(sources)
            saw_retry_later = False
            saw_geo_blocked = False
            saw_proxy_block = False
            saw_fatal = False
            last_reason = ""

            for offset in range(len(sources)):
                source = sources[(start_index + offset) % len(sources)]
                self.session = source["session"]
                self.active_cookie_file = str(source.get("cookie_file") or "")
                logger.info(
                    f"   🔐 Coba auth {offset + 1}/{len(sources)}: {source.get('label') or 'anonymous'}"
                )
                result, outcome = self._download_transcript_once(
                    video_id=video_id,
                    languages=list(languages),
                    cookie_file=source.get("cookie_file"),
                )
                if outcome == "ok":
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    self.reset_rate_limit()
                    self._reset_webshare_pressure()
                    return result, "ok"
                if outcome == "no_subtitle":
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    self._reset_webshare_pressure()
                    return None, "no_subtitle"
                if outcome == "blocked":
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    self._reset_webshare_pressure()
                    return None, "blocked"
                if outcome == "proxy_block":
                    saw_proxy_block = True
                    last_reason = self.last_transcript_failure_reason or "proxy_block"
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    continue
                if outcome == "geo_blocked":
                    saw_geo_blocked = True
                    last_reason = self.last_transcript_failure_reason or "geo_blocked"
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    continue
                if outcome == "retry_later":
                    saw_retry_later = True
                    last_reason = self.last_transcript_failure_reason or last_reason
                    self._auth_cursor = (start_index + offset + 1) % len(sources)
                    continue
                saw_fatal = True
                last_reason = self.last_transcript_failure_reason or last_reason
                self._auth_cursor = (start_index + offset + 1) % len(sources)

            if saw_proxy_block:
                self.last_transcript_failure_reason = last_reason or "proxy_block"
                self._record_webshare_pressure("proxy_block")
                return None, "proxy_block"

            if saw_geo_blocked:
                self.last_transcript_failure_reason = last_reason or "geo_blocked"
                self._record_webshare_pressure("geo_blocked")
                return None, "geo_blocked"

            if saw_retry_later:
                self.last_transcript_failure_reason = last_reason or "retry_later"
                self._record_webshare_pressure("retry_later")
                return None, "retry_later"

            if saw_fatal:
                self.last_transcript_failure_reason = last_reason or "fatal"
                self._record_webshare_pressure("fatal")
            return None, "fatal"
        finally:
            self._clear_video_budget()
            if not self._disable_inter_video_pacing:
                self._schedule_next_video_delay(1.0)

    def run(self, limit: int = 100):
        """Menjalankan proses pemulihan untuk N video"""
        with self.db._get_cursor() as cursor:
            cursor.execute("""
                SELECT v.video_id, v.title, c.channel_name, c.channel_id
                FROM videos v
                JOIN channels c ON v.channel_id = c.id
                WHERE v.transcript_downloaded = 0 
                  AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
                  AND COALESCE(v.is_short, 0) = 0
                ORDER BY v.created_at DESC
                LIMIT ?
            """, (limit,))
            videos = cursor.fetchall()

        if not videos:
            logger.info("✅ Tidak ada video yang perlu dipulihkan transkripnya.")
            return

        logger.info(f"🚀 Memproses {len(videos)} video backlog dengan Session + Cookies...")
        
        success_count = 0
        fail_count = 0
        consecutive_fatal_errors = 0
        consecutive_hard_blocks = 0
        geo_blocked_count = 0
        MAX_CONSECUTIVE_FATAL = 30
        MAX_CONSECUTIVE_HARD_BLOCKS = max(
            1,
            int(str(os.getenv("YT_TRANSCRIPT_MAX_CONSECUTIVE_HARD_BLOCKS", "3")).strip() or "3"),
        )
        stopped_early = False
        
        for i, row in enumerate(videos, 1):
            vid = row['video_id']
            title = row['title']
            ch_name = row['channel_name']
            
            logger.info(f"[{i}/{len(videos)}] 📹 {title[:50]} ({vid}) - {ch_name}")
            
            # Reset session occasionally to clear any sticky error states
            if i % 50 == 0:
                self.session = self._create_session()
                logger.info("🔄 Session refreshed.")

            try:
                result, outcome = self.download_transcript(vid)
            except Exception as e:
                logger.error(f"💥 Error tak terduga saat download: {str(e)}")
                result, outcome = None, "fatal"

            if result:
                consecutive_fatal_errors = 0
                consecutive_hard_blocks = 0
                safe_ch = row['channel_id'].replace('@', '').replace(' ', '_').replace('?', '_').replace(':', '_')
                text_dir = Path(BASE_DIR) / safe_ch / "text"
                text_dir.mkdir(parents=True, exist_ok=True)
                
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                file_name = f"{vid}_transcript_{timestamp}.txt"
                file_path = text_dir / file_name
                
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(result['formatted'])
                
                # Buffer update to JSON instead of direct DB write
                try:
                    buffer_dir = Path("pending_updates")
                    buffer_dir.mkdir(exist_ok=True)
                    
                    update_data = {
                        "video_id": vid,
                        "type": "transcript",
                        "status": "ok",
                        "file_path": str(file_path),
                        "content": result['formatted'],
                        "metadata": {
                            "language": result['language'],
                            "word_count": result['word_count'],
                            "line_count": result['line_count']
                        },
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    
                    buffer_file = buffer_dir / f"update_transcript_{vid}_{int(time.time())}.json"
                    buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                    logger.info(f"   [BUFFERED] Result saved to {buffer_file.name}")
                except Exception as e_buffer:
                    logger.warning(f"   ⚠️  Gagal menyimpan buffer JSON ({vid}): {e_buffer}")

                logger.info(f"   ✅ Berhasil! {result['word_count']} kata. Disimpan ke {file_name}")
                success_count += 1
            else:
                fail_count += 1

                if outcome == "proxy_block":
                    consecutive_hard_blocks += 1
                    logger.info(
                        "   ⏭️  Dipindahkan ke retry later karena proxy block. "
                        "State video tidak diubah."
                    )
                elif outcome == "geo_blocked":
                    consecutive_hard_blocks += 1
                    geo_blocked_count += 1
                    try:
                        buffer_dir = Path("pending_updates")
                        buffer_dir.mkdir(exist_ok=True)

                        update_data = {
                            "video_id": vid,
                            "type": "transcript",
                            "status": "geo_blocked",
                            "note": str(getattr(self, "last_transcript_failure_reason", "") or "geo_blocked"),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        buffer_file = buffer_dir / f"update_geo_blocked_{vid}_{int(time.time())}.json"
                        buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                        logger.info(f"   [BUFFERED] Geo-blocked status saved to {buffer_file.name}")
                    except Exception as e_buffer:
                        logger.warning(f"   ⚠️  Gagal menyimpan buffer geo-blocked ({vid}): {e_buffer}")

                    logger.info(
                        "   🌍 Geo/region block terdeteksi. Dijadwalkan ulang dengan region/proxy yang sesuai."
                    )
                elif outcome == "retry_later":
                    consecutive_hard_blocks = 0
                    logger.info(
                        "   ⏭️  Dipindahkan ke retry later karena challenge/rate-limit. "
                        "State video tidak diubah."
                    )
                elif outcome == "blocked":
                    consecutive_hard_blocks += 1
                    try:
                        buffer_dir = Path("pending_updates")
                        buffer_dir.mkdir(exist_ok=True)

                        update_data = {
                            "video_id": vid,
                            "type": "transcript",
                            "status": "blocked",
                            "note": str(getattr(self, "last_transcript_failure_reason", "") or "blocked_member_only"),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        buffer_file = buffer_dir / f"update_blocked_{vid}_{int(time.time())}.json"
                        buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                        logger.info(f"   [BUFFERED] Blocked status saved to {buffer_file.name}")
                    except Exception as e_buffer:
                        logger.warning(f"   ⚠️  Gagal menyimpan buffer blocked ({vid}): {e_buffer}")

                    logger.info(
                        "   ⛔ SaveSubs blocked/member-only. Ditandai terminal dan tidak dilanjutkan."
                    )
                elif outcome == "fatal":
                    consecutive_fatal_errors += 1
                    consecutive_hard_blocks = 0
                    logger.info(
                        f"   ❌ Gagal (FATAL/Rate Limit). State video tidak diubah. "
                        f"[{consecutive_fatal_errors}/{MAX_CONSECUTIVE_FATAL}]"
                    )
                else:
                    # Buffer no_subtitle update
                    try:
                        buffer_dir = Path("pending_updates")
                        buffer_dir.mkdir(exist_ok=True)
                        
                        update_data = {
                            "video_id": vid,
                            "type": "transcript",
                            "status": "no_subtitle",
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        
                        buffer_file = buffer_dir / f"update_no_subtitle_{vid}_{int(time.time())}.json"
                        buffer_file.write_text(json.dumps(update_data), encoding="utf-8")
                        logger.info(f"   [BUFFERED] No-subtitle status saved to {buffer_file.name}")
                    except Exception as e_buffer:
                        logger.warning(f"   ⚠️  Gagal menyimpan buffer no-subtitle ({vid}): {e_buffer}")
                    
                    consecutive_fatal_errors = 0 
                    consecutive_hard_blocks = 0
                    logger.info(f"   ❌ Tidak ada subtitle. Ditandai 'no_subtitle' di buffer.")
            
            if consecutive_fatal_errors >= MAX_CONSECUTIVE_FATAL:
                logger.error(f"🛑 BERHENTI: Terdeteksi {MAX_CONSECUTIVE_FATAL} kegagalan FATAL berturut-turut. Kemungkinan rate-limited.")
                break
            if consecutive_hard_blocks >= MAX_CONSECUTIVE_HARD_BLOCKS:
                logger.error(
                    f"🛑 BERHENTI: Terdeteksi {consecutive_hard_blocks} hard block berturut-turut "
                    f"(threshold={MAX_CONSECUTIVE_HARD_BLOCKS})."
                )
                stopped_early = True
                break

            # Inter-video pacing is handled inside download_transcript()

        logger.info(
            f"\n📊 RINGKASAN: {success_count} Berhasil, {fail_count} Gagal/No Subtitle, "
            f"geo_blocked={geo_blocked_count}."
        )
        if stopped_early:
            logger.error("🛑 Batch dihentikan lebih awal karena hard block berulang.")
        return 2 if stopped_early else 0

if __name__ == "__main__":
    limit_val = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    recoverer = TranscriptRecoverer()
    try:
        raise SystemExit(recoverer.run(limit=limit_val))
    except KeyboardInterrupt:
        logger.info("🛑 Dihentikan oleh pengguna.")
    except Exception as e:
        logger.error(f"💥 Fatal Error: {str(e)}")
