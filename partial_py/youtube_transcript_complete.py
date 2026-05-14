#!/usr/bin/env python3
"""
YouTube Transcript Framework Complete Version
Framework lengkap dengan database dan kemampuan download channel
"""

import http.cookiejar
import hashlib
import json
import os
import random
import re
import time
import sys
import urllib.request

import yt_dlp
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from local_services import yt_dlp_auth_args, youtube_cookie_file

try:
    from database import TranscriptDatabase
except Exception:
    from partial_py.database import TranscriptDatabase

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


load_dotenv()
_ENV_CANDIDATES = (
    (REPO_ROOT / ".env.local", False),
    (REPO_ROOT / ".env", False),
    (Path("/media/harry/DATA120B/GIT/youtube_transcript_bundle/.env.local"), True),
)
for _env_candidate, _override in _ENV_CANDIDATES:
    if _env_candidate.exists():
        load_dotenv(_env_candidate, override=_override)


class YouTubeTranscriptComplete:
    def __init__(self, database: TranscriptDatabase = None, db_path: str = "youtube_transcripts.db"):
        """
        Inisialisasi YouTube Transcript Complete
        
        Args:
            database: TranscriptDatabase instance (opsional)
            db_path: Path ke file database
        """
        self.db = database or TranscriptDatabase(db_path)
        self.language = 'id'
        self.retry_count = 5
        self.base_delay = 5.0
        self.max_delay = 30.0
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        ]
        
        self.output_dir = REPO_ROOT / "uploads"
        self.output_dir.mkdir(exist_ok=True)
        self.repo_root = REPO_ROOT
        self._webshare_proxy_cache: list[str] = []
        self._webshare_proxy_cache_loaded_at = 0.0
        self._webshare_proxy_cache_ttl_seconds = max(60, int(str(os.getenv("WEBSHARE_PROXY_CACHE_TTL_SECONDS", "900")).strip() or "900"))
        self._webshare_proxy_max_attempts = max(1, int(str(os.getenv("WEBSHARE_PROXY_MAX_ATTEMPTS", "10")).strip() or "10"))
        self._webshare_escalate_after = max(1, int(str(os.getenv("YT_TRANSCRIPT_WEBSHARE_ESCALATE_AFTER", "3")).strip() or "3"))
        self._prefer_webshare = False
        self._non_webshare_fallback_streak = 0
    
    def _get_random_user_agent(self) -> str:
        """Mendapatkan user agent random"""
        return random.choice(self.user_agents)

    def _read_env_file_value(self, path: Path, key: str) -> str:
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

    def _create_session(self, cookie_file: str | None = None):
        """Buat session requests dengan cookies jika tersedia."""
        session = requests.Session()
        session.headers.update({'User-Agent': self._get_random_user_agent()})

        if cookie_file:
            try:
                cj = http.cookiejar.MozillaCookieJar(cookie_file)
                cj.load(ignore_discard=True, ignore_expires=True)
                session.cookies = cj
            except Exception:
                pass

        return session

    def _normalize_transcript_items(self, items) -> list[dict]:
        normalized: list[dict] = []
        for item in items or []:
            if isinstance(item, dict):
                start = item.get("start", item.get("tStartMs", 0))
                duration = item.get("duration", item.get("dDurationMs", 0))
                text = item.get("text", "")
            else:
                start = getattr(item, "start", 0)
                duration = getattr(item, "duration", 0)
                text = getattr(item, "text", "")

            try:
                start_value = float(start) / 1000 if float(start) > 1000 else float(start)
            except Exception:
                start_value = 0.0
            try:
                duration_value = float(duration) / 1000 if float(duration) > 1000 else float(duration)
            except Exception:
                duration_value = 0.0

            text_value = str(text).strip()
            if not text_value:
                continue

            normalized.append({
                "start": start_value,
                "duration": duration_value,
                "text": text_value,
            })

        return normalized

    def _fetch_webshare_proxy_urls(self) -> list[str]:
        """Fetch Webshare proxies only as last-resort fallback."""
        api_key = str(os.getenv("WEBSHARE_API_KEY", "") or "").strip()
        if not api_key:
            api_key = self._read_env_file_value(self.repo_root.parent / "youtube_transcript_bundle/.env.local", "WEBSHARE_API_KEY")
        if not api_key:
            api_key = self._read_env_file_value(Path("/media/harry/DATA120B/GIT/youtube_transcript_bundle/.env.local"), "WEBSHARE_API_KEY")
        if not api_key:
            return []

        mode = str(os.getenv("WEBSHARE_PROXY_MODE", "") or "direct").strip().lower() or "direct"
        api_timeout = max(5.0, float(os.getenv("WEBSHARE_PROXY_API_TIMEOUT_SECONDS", "20") or 20))
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
                username = str(proxy.get("username") or "").strip()
                password = str(proxy.get("password") or "").strip()
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
            print(f"   ⚠️  Gagal mengambil daftar proxy Webshare: {last_error}")
        return []

    def _get_webshare_proxy_urls(self, video_id: str) -> list[str]:
        now = time.monotonic()
        cache_age = now - self._webshare_proxy_cache_loaded_at if self._webshare_proxy_cache_loaded_at > 0 else None
        if self._webshare_proxy_cache_loaded_at > 0 and cache_age is not None and cache_age < self._webshare_proxy_cache_ttl_seconds:
            proxy_urls = list(self._webshare_proxy_cache)
        else:
            proxy_urls = self._fetch_webshare_proxy_urls()
            self._webshare_proxy_cache = list(proxy_urls)
            self._webshare_proxy_cache_loaded_at = now

        if not proxy_urls:
            return []

        if len(proxy_urls) > 1 and video_id:
            seed = int(hashlib.sha1(video_id.encode("utf-8")).hexdigest(), 16)
            offset = seed % len(proxy_urls)
            if offset:
                proxy_urls = proxy_urls[offset:] + proxy_urls[:offset]
        return proxy_urls

    def _reset_webshare_pressure(self) -> None:
        self._non_webshare_fallback_streak = 0
        self._prefer_webshare = False

    def _record_webshare_pressure(self) -> None:
        self._non_webshare_fallback_streak += 1
        if self._non_webshare_fallback_streak >= self._webshare_escalate_after:
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

    def _download_transcript_via_webshare(self, video_id: str, languages: list[str], cookie_file: str | None = None) -> tuple[Optional[List[Dict]], Optional[str]]:
        """Last-resort fallback using paid Webshare proxies."""
        proxy_urls = self._get_webshare_proxy_urls(video_id)
        if not proxy_urls:
            return None, None

        errors: list[str] = []

        for attempt_idx, proxy_url in enumerate(proxy_urls, start=1):
            if attempt_idx > self._webshare_proxy_max_attempts:
                print(f"   ⚠️  Batas percobaan Webshare tercapai untuk {video_id} ({self._webshare_proxy_max_attempts} proxy).")
                break
            session = self._create_session(cookie_file)
            session.proxies.update({"http": proxy_url, "https": proxy_url})

            try:
                api_kwargs = {"http_client": session}
                if GenericProxyConfig is not None:
                    api_kwargs["proxy_config"] = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
                api = YouTubeTranscriptApi(**api_kwargs)
                transcript_list = api.list(video_id)

                try:
                    found_transcript = transcript_list.find_transcript(languages)
                except Exception:
                    found_transcript = next(iter(transcript_list))

                data = found_transcript.fetch()
                raw_items = data.to_raw_data() if hasattr(data, "to_raw_data") else data
                transcript = self._normalize_transcript_items(raw_items)
                if transcript:
                    return transcript, found_transcript.language_code
            except Exception as exc:
                err = str(exc)
                errors.append(err)
                if self._looks_like_terminal_transcript_error(err):
                    print(f"   ⚠️ Webshare terminal untuk {video_id}: {err[:140]}")
                    return None, None

        if errors:
            print(f"   ⚠️ Webshare fallback gagal: {errors[0][:140]}")
        return None, None
    
    def _calculate_delay(self, retry_attempt: int) -> float:
        """Hitung delay dengan exponential backoff"""
        delay = self.base_delay * (2 ** retry_attempt) + random.uniform(0.5, 2.0)
        return min(delay, self.max_delay)
    
    def _make_request_with_retry(self, request_func, *args, **kwargs):
        """Melakukan request dengan retry mechanism"""
        last_error = None
        
        for attempt in range(self.retry_count):
            try:
                if attempt > 0:
                    delay = self._calculate_delay(attempt)
                    print(f"  ⏳ Menunggu {delay:.1f} detik sebelum retry #{attempt + 1}...")
                    time.sleep(delay)
                
                return request_func(*args, **kwargs)
                
            except Exception as e:
                last_error = e
                error_msg = str(e)
                
                if any(keyword in error_msg.lower() for keyword in ['429', 'too many requests', 'rate limit']):
                    print(f"  ⚠️  Rate limit terdeteksi (attempt {attempt + 1}/{self.retry_count})")
                    continue
                else:
                    raise
        
        raise last_error
    
    def extract_video_id(self, url: str) -> str:
        """Extract video ID dari URL"""
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)',
            r'youtube\.com\/shorts\/([^&\n?#]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        raise ValueError(f"Tidak bisa extract video ID dari URL: {url}")
    
    def extract_channel_id(self, url: str) -> str:
        """Extract channel ID atau handle dari URL"""
        patterns = [
            r'youtube\.com\/@([^\/\n?#]+)',
            r'youtube\.com\/channel\/([^\/\n?#]+)',
            r'youtube\.com\/c\/([^\/\n?#]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        raise ValueError(f"Tidak bisa extract channel ID dari URL: {url}")
    
    def get_channel_info(self, channel_url: str) -> Dict:
        """
        Mengambil informasi channel dan daftar video
        
        Returns:
            Dict berisi info channel dan list video
        """
        def _get_channel_data():
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'user_agent': self._get_random_user_agent(),
                'extract_flat': True,
                'playlistend': 200,  # Max 200 videos per request
            }
            cookie_file = youtube_cookie_file()
            if cookie_file:
                ydl_opts['cookiefile'] = cookie_file
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    info = ydl.extract_info(channel_url, download=False)
                    
                    if info.get('_type') == 'playlist':
                        # Playlist/Channel result
                        channel_data = {
                            'channel_id': info.get('uploader_id', info.get('channel_id', 'unknown')),
                            'channel_name': info.get('uploader', info.get('channel', 'Unknown')),
                            'channel_url': channel_url,
                            'subscriber_count': info.get('uploader_sub_count', 0),
                            'video_count': len(info.get('entries', [])),
                            'videos': []
                        }
                        
                        for video in info.get('entries', []):
                            if video:
                                channel_data['videos'].append({
                                    'video_id': video.get('id'),
                                    'title': video.get('title', 'Unknown'),
                                    'url': video.get('url', f"https://www.youtube.com/watch?v={video.get('id')}"),
                                    'duration': video.get('duration', 0),
                                    'upload_date': video.get('upload_date', ''),
                                    'view_count': video.get('view_count', 0),
                                    'thumbnail': video.get('thumbnail') or f"https://i.ytimg.com/vi/{video.get('id')}/maxresdefault.jpg"
                                })
                        
                        return channel_data
                    
                    else:
                        # Single video result
                        return {
                            'channel_id': info.get('channel_id', info.get('uploader_id', 'unknown')),
                            'channel_name': info.get('channel', info.get('uploader', 'Unknown')),
                            'channel_url': channel_url,
                            'subscriber_count': info.get('channel_follower_count', 0),
                            'video_count': 1,
                            'videos': [{
                                'video_id': info.get('id'),
                                'title': info.get('title', 'Unknown'),
                                'url': info.get('webpage_url', channel_url),
                                'duration': info.get('duration', 0),
                                'upload_date': info.get('upload_date', ''),
                                'view_count': info.get('view_count', 0),
                                'thumbnail': info.get('thumbnail', '')
                            }]
                        }
                        
                except Exception as e:
                    raise Exception(f"Gagal mengambil info channel: {str(e)}")
        
        return self._make_request_with_retry(_get_channel_data)
    
    def save_channel_to_database(self, channel_data: Dict) -> int:
        """
        Menyimpan channel dan video ke database
        
        Returns:
            ID channel yang disimpan
        """
        # Save channel
        channel_id = self.db.add_channel(
            channel_id=channel_data['channel_id'],
            channel_name=channel_data['channel_name'],
            channel_url=channel_data['channel_url'],
            subscriber_count=channel_data['subscriber_count'],
            video_count=channel_data['video_count']
        )
        
        # Get internal channel ID
        channel_internal_id = self.db.get_channel_by_id(channel_data['channel_id'])['id']
        
        # Save videos
        for video in channel_data['videos']:
            # Update video
            self.db.add_video(
                video_id=video['video_id'],
                channel_id=channel_internal_id,
                title=video['title'],
                video_url=video['url'],
                duration=video.get('duration'),
                upload_date=video.get('upload_date'),
                view_count=video.get('view_count', 0),
                thumbnail_url=video.get('thumbnail') # External URL initially
            )
        
        return channel_id
    
    def download_single_transcript(self, video_url: str, language: str = 'id', 
                                   save_to_db: bool = True) -> Dict:
        """
        Download transcript dari single video
        
        Returns:
            Dict berisi transcript data
        """
        self.language = language
        video_id = self.extract_video_id(video_url)
        
        def _download_transcript():
            # Tambahkan delay sebelum request
            time.sleep(1 + random.uniform(0.5, 1.5))
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'writesubtitles': True,
                'writeautomaticsub': True,
                'user_agent': self._get_random_user_agent(),
            }
            cookie_file = youtube_cookie_file()
            if cookie_file:
                ydl_opts['cookiefile'] = cookie_file
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Get video info (process=False is faster and more robust against 403)
                info = ydl.extract_info(video_url, download=False, process=False)
                
                video_data = {
                    'video_id': info.get('id', video_id),
                    'title': info.get('title', 'Unknown'),
                    'channel': info.get('channel', info.get('uploader', 'Unknown')),
                    'duration': info.get('duration', 0),
                    'upload_date': info.get('upload_date', ''),
                    'view_count': info.get('view_count', 0),
                    'description': info.get('description', ''),
                    'thumbnail': info.get('thumbnail') or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
                }
                
                # Try YouTubeTranscriptApi first
                from youtube_transcript_api import YouTubeTranscriptApi
                print(f"   📡 Mengambil data transkrip melalui YouTubeTranscriptApi...")
                
                transcript = []
                found_lang = None
                prefer_webshare = self._prefer_webshare
                webshare_attempted_early = False

                if prefer_webshare:
                    webshare_attempted_early = True
                    print(f"   ⚡ Webshare diprioritaskan lebih dulu untuk {video_data['video_id']}.")
                    transcript, found_lang = self._download_transcript_via_webshare(
                        video_data['video_id'],
                        [language, 'en', 'en-US'],
                        cookie_file=cookie_file,
                    )
                    if transcript:
                        self._reset_webshare_pressure()
                        return {
                            'video_info': video_data,
                            'transcript': transcript,
                            'formatted': self._format_timestamp(transcript),
                            'clean': ' '.join([item['text'] for item in transcript]),
                            'summary': self._create_summary(' '.join([item['text'] for item in transcript])),
                            'stats': {
                                'total_duration': sum([item['duration'] for item in transcript]),
                                'word_count': len(' '.join([item['text'] for item in transcript]).split()),
                                'line_count': len(transcript)
                            },
                            'actual_lang': found_lang
                        }
                
                try:
                    api = YouTubeTranscriptApi()
                    transcript_list = api.list(video_data['video_id'])
                    
                    # Prioritize Indonesian, then English
                    try:
                        found_transcript = transcript_list.find_transcript([language])
                    except:
                        try:
                            found_transcript = transcript_list.find_transcript(['en', 'en-US'])
                        except:
                            found_transcript = next(iter(transcript_list))
                    
                    found_lang = found_transcript.language_code
                    print(f"   📝 Menggunakan transkrip bahasa: {found_lang}")
                    transcript = found_transcript.fetch().to_raw_data()
                    
                except Exception as e:
                    print(f"   ⚠️ YouTubeTranscriptApi gagal: {str(e)}, mencoba yt-dlp fallback...")
                    
                    # Fallback to yt-dlp subtitle links or manual download
                    import subprocess
                    import tempfile
                    import os
                    import glob
                    import json
                    
                    temp_dir = tempfile.gettempdir()
                    temp_base = os.path.join(temp_dir, f"sub_{video_id}")
                    
                    langs_to_try = [language, 'en', 'en-US']
                    
                    for l in langs_to_try:
                        try:
                            print(f"   📡 Mengambil data subtitle ({l}) melalui yt-dlp...")
                            cmd = [
                                'yt-dlp', 
                                '--write-auto-subs', '--write-subs',
                                '--sub-lang', l, 
                                '--sub-format', 'json3',
                                '--skip-download',
                                '--output', temp_base,
                            ]
                            cmd.extend(yt_dlp_auth_args(rotate=True))
                            cmd.append(video_url)
                            subprocess.run(cmd, capture_output=True)
                            
                            pattern = temp_base + "*.json3"
                            files = glob.glob(pattern)
                            if files:
                                sub_file = files[0]
                                print(f"   💾 Membaca file subtitle (yt-dlp): {os.path.basename(sub_file)}")
                                with open(sub_file, 'r', encoding='utf-8') as f:
                                    sub_data = json.load(f)
                                
                                # Parse transcript
                                for event in sub_data.get('events', []):
                                    if event.get('segs'):
                                        text = ''.join([seg.get('utf8', '') for seg in event['segs']])
                                        if text.strip():
                                            transcript.append({
                                                'start': event.get('tStartMs', 0) / 1000,
                                                'duration': event.get('dDurationMs', 0) / 1000,
                                                'text': text.strip()
                                            })
                                found_lang = l
                                # Clean up
                                for f in glob.glob(temp_base + "*"):
                                    try:
                                        os.remove(f)
                                    except:
                                        pass
                                break # Break after finding and processing subtitles for one language
                        except Exception as e_yt_dlp:
                            print(f"   ⚠️ yt-dlp for language {l} failed: {str(e_yt_dlp)}")
                            continue # Try next language
                    
                    if not transcript:
                        if webshare_attempted_early:
                            print(f"   ℹ️ Webshare sudah dicoba lebih awal untuk {video_id}; skip retry akhir.")
                        else:
                            print(f"   🌐 Mencoba fallback Webshare terakhir untuk {video_id}...")
                            transcript, found_lang = self._download_transcript_via_webshare(
                                video_data['video_id'],
                                langs_to_try,
                                cookie_file=cookie_file,
                            )

                    if not transcript:
                        self._record_webshare_pressure()
                        raise Exception(f"Semua metode direct dan Webshare gagal menarik transkrip untuk {video_id}")
                    self._reset_webshare_pressure()
                
                # Format transcript
                formatted = self._format_timestamp(transcript)
                clean_text = ' '.join([item['text'] for item in transcript])
                summary = self._create_summary(clean_text)
                
                # Calculate stats
                total_duration = sum([item['duration'] for item in transcript])
                word_count = len(clean_text.split())
                
                return {
                    'video_info': video_data,
                    'transcript': transcript,
                    'formatted': formatted,
                    'clean': clean_text,
                    'summary': summary,
                    'stats': {
                        'total_duration': total_duration,
                        'word_count': word_count,
                        'line_count': len(transcript)
                    },
                    'actual_lang': found_lang
                }
        
        result = self._make_request_with_retry(_download_transcript)
        actual_lang = result.get('actual_lang', language)
        
        if save_to_db:
            self._save_transcript_to_db(video_url, result, actual_lang)
        
        return result
    
    def _format_timestamp(self, transcript: List[Dict]) -> str:
        """Format transkrip dengan timestamp"""
        formatted = []
        for item in transcript:
            timestamp = self._seconds_to_timestamp(item['start'])
            formatted.append(f"[{timestamp}] {item['text']}")
        return '\n'.join(formatted)
    
    def _create_summary(self, text: str, max_sentences: int = 5) -> str:
        """Buat ringkasan dari teks"""
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if not sentences:
            return "Tidak bisa membuat ringkasan."
        
        # Ambil kalimat awal dan akhir
        intro_sentences = sentences[:2]
        conclusion_sentences = sentences[-min(3, len(sentences)):]
        important_sentences = intro_sentences + conclusion_sentences
        summary_sentences = important_sentences[:max_sentences]
        
        return ' '.join(summary_sentences) + '.'
    
    def _seconds_to_timestamp(self, seconds: float) -> str:
        """Konversi detik ke format timestamp"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"
    
    def _save_transcript_to_db(self, video_url: str, result: Dict, language: str):
        """Simpan transcript ke database"""
        try:
            video_id = self.extract_video_id(video_url)
            video_data = result['video_info']
            
            # Update or add video
            existing_video = self.db.get_video_by_id(video_id)
            
            if not existing_video:
                # Get channel info first
                channel = self.db.get_channel_by_id(video_data['channel'])
                if not channel:
                    # Add channel if not exists
                    channel_id = self.db.add_channel(
                        channel_id=video_data['channel'],
                        channel_name=video_data['channel'],
                        channel_url=f"https://www.youtube.com/@{video_data['channel']}"
                    )
                    channel_internal_id = self.db.get_channel_by_id(video_data['channel'])['id']
                else:
                    channel_internal_id = channel['id']
                
                # Add video
                video_internal_id = self.db.add_video(
                    video_id=video_id,
                    channel_id=channel_internal_id,
                    title=video_data['title'],
                    video_url=video_url,
                    description=video_data.get('description'),
                    duration=video_data.get('duration'),
                    upload_date=video_data.get('upload_date'),
                    view_count=video_data.get('view_count', 0),
                    metadata=video_data
                )
            else:
                video_internal_id = existing_video['id']
            
            # Save transcript to files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Get channel info for subdirectory
            channel_id = video_data['channel']
            # Match sanitization in OptimizedDatabase
            safe_channel_id = channel_id.replace('@', '').replace(' ', '_').replace('?', '_').replace(':', '_')
            
            channel_dir = self.output_dir / safe_channel_id
            text_dir = channel_dir / "text"
            resume_dir = channel_dir / "resume"
            thumb_dir = channel_dir / "thumbnails"
            
            text_dir.mkdir(parents=True, exist_ok=True)
            resume_dir.mkdir(parents=True, exist_ok=True)
            thumb_dir.mkdir(parents=True, exist_ok=True)
            
            # Download thumbnail
            thumbnail_url = video_data.get('thumbnail')
            local_thumb_path = ""
            if thumbnail_url:
                try:
                    thumb_ext = thumbnail_url.split('.')[-1].split('?')[0]
                    if thumb_ext not in ['jpg', 'png', 'webp', 'jpeg']:
                        thumb_ext = 'jpg'
                    
                    thumb_filename = f"{video_id}.{thumb_ext}"
                    thumb_path = thumb_dir / thumb_filename
                    
                    # Download image
                    headers = {'User-Agent': self._get_random_user_agent()}
                    req = urllib.request.Request(thumbnail_url, headers=headers)
                    with urllib.request.urlopen(req) as response:
                        with open(thumb_path, 'wb') as out_file:
                            out_file.write(response.read())
                    
                    # Use relative path from uploads/ for database
                    local_thumb_path = f"{safe_channel_id}/thumbnails/{thumb_filename}"
                    print(f"  🖼️  Thumbnail didownload: {thumb_filename}")
                except Exception as e:
                    print(f"  ⚠️  Gagal download thumbnail: {str(e)}")
                    # Fallback to URL if download fails
                    local_thumb_path = thumbnail_url
            
            transcript_file = text_dir / f"{video_id}_transcript_{timestamp}.txt"
            summary_file = resume_dir / f"{video_id}_summary_{timestamp}.txt"
            
            with open(transcript_file, 'w', encoding='utf-8') as f:
                f.write(result['formatted'])
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(result['summary'])
            
            # Save transcript to database
            self.db.add_transcript(
                video_id=video_internal_id,
                language=language,
                transcript_data=result['formatted'],
                format_type='timestamp',
                word_count=result['stats']['word_count'],
                duration=result['stats']['total_duration']
            )
            
            # Save summary to database
            self.db.add_summary(
                video_id=video_internal_id,
                summary_text=result['summary'],
                word_count=len(result['summary'].split())
            )
            
            # Update video status
            self.db.update_transcript_status(
                video_id=video_id,
                downloaded=True,
                format_type='timestamp',
                transcript_path=str(transcript_file),
                summary_path=str(summary_file),
                word_count=result['stats']['word_count'],
                line_count=result['stats']['line_count'],
                language=language
            )
            
            # Update thumbnail to local path if downloaded
            if local_thumb_path:
                self.db.update_video_thumbnail(video_id, local_thumb_path)
            
            print(f"  💾 Disimpan ke database dan file: {transcript_file.name}")
            
        except Exception as e:
            print(f"  ❌ Gagal menyimpan ke database: {str(e)}")
    
    def download_channel_transcripts(self, channel_url: str, language: str = 'id',
                                    max_videos: int = None, skip_downloaded: bool = True) -> Dict:
        """
        Download transcript dari seluruh video dalam channel
        
        Returns:
            Dict berisi statistik download
        """
        print(f"🎬 Mengambil info channel: {channel_url}")
        
        # Get channel info and videos
        channel_data = self.get_channel_info(channel_url)
        
        print(f"\n📺 Channel: {channel_data['channel_name']}")
        print(f"📊 Total video: {len(channel_data['videos'])}")
        
        # Save channel to database
        self.save_channel_to_database(channel_data)
        
        # Get internal channel ID
        channel_internal_id = self.db.get_channel_by_id(channel_data['channel_id'])['id']
        
        # Filter videos
        videos_to_download = channel_data['videos']
        if max_videos:
            videos_to_download = videos_to_download[:max_videos]
        
        # Skip already downloaded if requested
        if skip_downloaded:
            existing_videos = self.db.get_videos_by_channel(
                channel_data['channel_id'], 
                transcript_downloaded=True
            )
            existing_video_ids = {v['video_id'] for v in existing_videos}
            
            filtered_videos = []
            for video in videos_to_download:
                if video['video_id'] not in existing_video_ids:
                    filtered_videos.append(video)
            
            skipped_count = len(videos_to_download) - len(filtered_videos)
            if skipped_count > 0:
                print(f"⏭️  Melewati {skipped_count} video yang sudah didownload")
            
            videos_to_download = filtered_videos
        
        print(f"\n🚀 Mulai download {len(videos_to_download)} video...")
        
        # Statistics
        stats = {
            'total': len(videos_to_download),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        # Download each video
        for i, video in enumerate(videos_to_download, 1):
            try:
                print(f"\n[{i}/{len(videos_to_download)}] 📹 {video['title'][:50]}...")
                print(f"   URL: {video['url']}")
                
                # Add to queue
                video_record = self.db.get_video_by_id(video['video_id'])
                if video_record:
                    video_internal_id = video_record['id']
                    queue_id = self.db.add_to_queue(video_internal_id, channel_internal_id)
                
                # Download transcript
                result = self.download_single_transcript(video['url'], language, save_to_db=True)
                
                # Update queue status
                self.db.update_queue_status(queue_id, 'completed')
                
                stats['success'] += 1
                print(f"   ✅ Berhasil ({result['stats']['word_count']} kata, {result['stats']['line_count']} baris)")
                
                # Random delay between downloads to avoid rate limiting
                if i < len(videos_to_download):
                    delay = 2 + random.uniform(1, 3)
                    print(f"   ⏸️  Menunggu {delay:.1f} detik sebelum video berikutnya...")
                    time.sleep(delay)
                
            except Exception as e:
                error_msg = f"{str(e)}"
                stats['failed'] += 1
                stats['errors'].append({
                    'video_id': video['video_id'],
                    'title': video['title'],
                    'error': error_msg
                })
                
                print(f"   ❌ Gagal: {error_msg}")
                
                # Update queue status
                try:
                    self.db.update_queue_status(queue_id, 'failed', error_msg)
                except:
                    pass
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 SUMMARY CHANNEL DOWNLOAD")
        print(f"{'='*60}")
        print(f"Total video: {stats['total']}")
        print(f"✅ Berhasil: {stats['success']}")
        print(f"❌ Gagal: {stats['failed']}")
        print(f"⏭️  Dilewati: {stats.get('skipped', 0)}")
        
        if stats['errors']:
            print(f"\n❌ Error Details:")
            for error in stats['errors']:
                print(f"   • {error['video_id']}: {error['error']}")
        
        print(f"{'='*60}")
        
        return stats
    
    def search_in_database(self, query: str, limit: int = 10) -> List[Dict]:
        """Cari video dalam database"""
        return self.db.search_videos(query, limit)
    
    def get_video_from_database(self, video_id: str) -> Optional[Dict]:
        """Dapatkan info video dari database"""
        return self.db.get_video_by_id(video_id)
    
    def get_transcript_from_database(self, video_id: str, language: str = None) -> Optional[Dict]:
        """Dapatkan transcript dari database"""
        return self.db.get_transcript_by_video_id(video_id, language)
    
    def get_summary_from_database(self, video_id: str) -> Optional[Dict]:
        """Dapatkan ringkasan dari database"""
        return self.db.get_summary_by_video_id(video_id)
    
    def get_database_statistics(self) -> Dict:
        """Dapatkan statistik database"""
        return self.db.get_statistics()
    
    def export_database(self, output_path: str = "database_export.json"):
        """Export database ke file JSON"""
        self.db.export_to_json(output_path)
        print(f"✅ Database exported to: {output_path}")
    
    def close(self):
        """Tutup koneksi database"""
        self.db.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Fungsi utama untuk command line usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("YouTube Transcript Framework Complete")
        print("=" * 50)
        print("\nUsage:")
        print("  python partial_py/youtube_transcript_complete.py <URL> [OPTIONS]")
        print("\nCommands:")
        print("  --channel    Download seluruh video dalam channel")
        print("  --video      Download single video transcript")
        print("  --search     Search in database")
        print("  --stats      Show database statistics")
        print("  --export     Export database to JSON")
        print("\nExamples:")
        print("  python partial_py/youtube_transcript_complete.py https://youtube.com/@KenapaItuYa --channel")
        print("  python partial_py/youtube_transcript_complete.py https://youtube.com/watch?v=VIDEO_ID --video")
        print("  python partial_py/youtube_transcript_complete.py --search \"tutorial python\"")
        print("  python partial_py/youtube_transcript_complete.py --stats")
        sys.exit(1)
    
    url_or_command = sys.argv[1]
    
    # Parse options
    options = {
        'language': 'id',
        'max_videos': None,
        'skip_downloaded': True
    }
    
    for i in range(2, len(sys.argv)):
        if sys.argv[i] == '--channel':
            options['mode'] = 'channel'
        elif sys.argv[i] == '--video':
            options['mode'] = 'video'
        elif sys.argv[i] == '--search':
            options['mode'] = 'search'
            if i + 1 < len(sys.argv):
                options['query'] = sys.argv[i + 1]
        elif sys.argv[i] == '--stats':
            options['mode'] = 'stats'
        elif sys.argv[i] == '--export':
            options['mode'] = 'export'
        elif sys.argv[i] == '--language':
            if i + 1 < len(sys.argv):
                options['language'] = sys.argv[i + 1]
        elif sys.argv[i] == '--max':
            if i + 1 < len(sys.argv):
                options['max_videos'] = int(sys.argv[i + 1])
        elif sys.argv[i] == '--all':
            options['skip_downloaded'] = False
    
    try:
        with YouTubeTranscriptComplete() as yt:
            # Handle different modes
            if options.get('mode') == 'channel':
                # Download channel
                if not url_or_command.startswith('http'):
                    print("Error: Channel URL harus dimulai dengan http/https")
                    sys.exit(1)
                
                stats = yt.download_channel_transcripts(
                    channel_url=url_or_command,
                    language=options['language'],
                    max_videos=options['max_videos'],
                    skip_downloaded=options['skip_downloaded']
                )
            
            elif options.get('mode') == 'video':
                # Download single video
                if not url_or_command.startswith('http'):
                    print("Error: Video URL harus dimulai dengan http/https")
                    sys.exit(1)
                
                result = yt.download_single_transcript(
                    video_url=url_or_command,
                    language=options['language'],
                    save_to_db=True
                )
                
                print(f"\n✅ Transkrip berhasil diambil!")
                print(f"   📝 {result['stats']['word_count']} kata")
                print(f"   📊 {result['stats']['line_count']} baris")
                print(f"   ⏱️  {result['stats']['total_duration']:.2f} detik")
            
            elif options.get('mode') == 'search':
                # Search in database
                query = options.get('query', url_or_command)
                results = yt.search_in_database(query, limit=10)
                
                print(f"\n🔍 Hasil pencarian: '{query}'")
                print(f"   Ditemukan {len(results)} video\n")
                
                for i, video in enumerate(results, 1):
                    print(f"{i}. {video['title']}")
                    print(f"   📺 Channel: {video['channel_name']}")
                    print(f"   📹 Video ID: {video['video_id']}")
                    print(f"   👁️  Views: {video['view_count']:,}")
                    print(f"   ✅ Transcript: {'Ya' if video['transcript_downloaded'] else 'Tidak'}")
                    print()
            
            elif options.get('mode') == 'stats':
                # Show database statistics
                stats = yt.get_database_statistics()
                
                print(f"\n📊 DATABASE STATISTICS")
                print(f"{'='*40}")
                print(f"📺 Total Channels: {stats['total_channels']}")
                print(f"📹 Total Videos: {stats['total_videos']}")
                print(f"✅ Videos with Transcript: {stats['videos_with_transcript']}")
                print(f"📝 Total Transcripts: {stats['total_transcripts']}")
                print(f"📄 Total Summaries: {stats['total_summaries']}")
                print(f"\n📥 Download Queue:")
                print(f"   ⏳ Pending: {stats['pending_downloads']}")
                print(f"   ✅ Completed: {stats['completed_downloads']}")
                print(f"   ❌ Failed: {stats['failed_downloads']}")
            
            elif options.get('mode') == 'export':
                # Export database
                output_file = url_or_command if url_or_command.endswith('.json') else "database_export.json"
                yt.export_database(output_file)
            
            else:
                # Auto-detect mode based on URL
                if 'channel' in url_or_command or '@' in url_or_command:
                    # Channel URL
                    stats = yt.download_channel_transcripts(
                        channel_url=url_or_command,
                        language=options['language'],
                        max_videos=options['max_videos'],
                        skip_downloaded=options['skip_downloaded']
                    )
                else:
                    # Video URL
                    result = yt.download_single_transcript(
                        video_url=url_or_command,
                        language=options['language'],
                        save_to_db=True
                    )
                    
                    print(f"\n✅ Transkrip berhasil diambil!")
                    print(f"   📝 {result['stats']['word_count']} kata")
                    print(f"   📊 {result['stats']['line_count']} baris")
                    print(f"   ⏱️  {result['stats']['total_duration']:.2f} detik")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
