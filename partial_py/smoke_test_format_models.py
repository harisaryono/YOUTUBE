#!/usr/bin/env python3
"""
Smoke test untuk membandingkan kualitas formatting transkrip antar model.

Model yang ditest:
- groq/llama-3.3-70b-versatile (belum dipakai, quota segar)
- cerebras/qwen-3-235b-a22b-instruct-2507 (belum dipakai)
- nvidia/openai/gpt-oss-120b (standar utama)
- gemini/gemini-2.5-flash (fallback alternatif)

Output:
- Report CSV dengan metrik kualitas
- File output per model untuk inspeksi manual
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from local_services import (
    coordinator_acquire_accounts,
    coordinator_acquire_specific_account,
    coordinator_heartbeat_lease,
    coordinator_release_lease,
    coordinator_report_provider_event,
    coordinator_status_accounts,
)


LEASE_TTL_SECONDS = 300
RUNS_DIR = Path("runs")

# Prompt formatting transkrip → artikel
FORMAT_PROMPT = """Anda bertugas memformat transcript YouTube mentah agar jauh lebih mudah dibaca dan dipahami, TANPA merangkum isi.

Tujuan:
- pertahankan isi pembahasan selengkap mungkin
- rapikan tanda baca, kapitalisasi, pemenggalan kalimat, dan paragraf
- kelompokkan pembahasan dengan heading seperlunya agar alur lebih jelas
- jika ada kalimat yang jelas patah karena hasil transcript, sambungkan seperlunya

Aturan ketat:
- jangan merangkum
- jangan menghilangkan detail penting
- jangan menambahkan fakta baru
- jangan mengubah makna pembicara
- jika ada istilah yang tidak jelas, pertahankan semirip mungkin dengan sumber
- abaikan header teknis seperti "Kind:" atau "Language:" jika ada
- anggap transcript yang diberikan SUDAH final; jangan pernah meminta "full transcript", file lain, URL lain, atau konteks tambahan
- jangan menulis komentar asisten, disclaimer, permintaan maaf, atau instruksi kepada pengguna
- jika transcript sangat pendek, terpotong, atau didominasi penanda seperti "[Music]" / "[Applause]", tetap kembalikan versi markdown terbaik dari isi yang ada
- jika isinya hanya fragmen, fokus pada pembersihan, pengelompokan seperlunya, dan keterbacaan; jangan mengarang isi yang tidak ada
- output hanya hasil akhir dalam Markdown, tanpa komentar tambahan
"""

# Prompt alternatif lebih strict untuk retry
STRICT_PROMPT = """Tugas Anda hanya satu: keluarkan transcript yang sudah diformat ulang agar lebih mudah dibaca.

Aturan keras:
- JANGAN membalas sebagai asisten.
- JANGAN meminta transcript lengkap, file lain, URL, atau konteks tambahan.
- JANGAN menulis komentar seperti "silakan kirim", "I notice", "please provide", "maaf", atau penjelasan apa pun.
- JANGAN merangkum.
- JANGAN menambah fakta baru.
- JANGAN mengubah makna pembicara.
- Pertahankan isi sedekat mungkin dengan sumber.
- Rapikan hanya tanda baca, kapitalisasi, paragraf, dan heading.
- Output HARUS langsung berupa Markdown hasil akhir saja.
"""


@dataclass(frozen=True)
class ProviderAccountLease:
    id: int
    provider: str
    account_name: str
    usage_method: str
    api_key: str
    endpoint_url: str
    model_name: str
    extra_headers: Dict[str, str]
    lease_token: str

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.account_name}"


class LeaseHeartbeat:
    def __init__(self, lease_token: str, lease_ttl_seconds: int) -> None:
        self.lease_token = str(lease_token or "").strip()
        self.lease_ttl_seconds = max(60, int(lease_ttl_seconds or LEASE_TTL_SECONDS))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.lease_token:
            return
        self._thread = threading.Thread(target=self._run, name="smoke-lease-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        interval = max(30, int(self.lease_ttl_seconds // 3))
        while not self._stop.wait(interval):
            try:
                coordinator_heartbeat_lease(self.lease_token, lease_ttl_seconds=self.lease_ttl_seconds)
            except Exception:
                pass

    def stop(self, *, final_state: str = "idle", note: str = "") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if not self.lease_token:
            return
        try:
            coordinator_release_lease(self.lease_token, final_state=final_state, note=note)
        except Exception:
            pass


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)


def call_provider_api(
    *,
    endpoint_url: str,
    api_key: str,
    model_name: str,
    prompt: str,
    transcript: str,
    max_tokens: int = 3200,
    temperature: float = 0.4,
    extra_headers: Optional[Dict[str, str]] = None,
    timeout: int = 120,
) -> Dict[str, Any]:
    """Call provider API directly.
    
    Note: endpoint_url dari coordinator sudah lengkap (termasuk /chat/completions).
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    if extra_headers:
        headers.update(extra_headers)

    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": transcript[:80000]},  # Limit input
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": False,
    }

    # endpoint_url dari coordinator sudah lengkap
    req = urllib_request.Request(
        endpoint_url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    start = time.time()
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw)
            elapsed = time.time() - start
            return {
                "ok": True,
                "result": result,
                "elapsed_seconds": round(elapsed, 2),
            }
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = time.time() - start
        return {
            "ok": False,
            "http_status": exc.code,
            "error_body": raw,
            "elapsed_seconds": round(elapsed, 2),
        }
    except urllib_error.URLError as exc:
        elapsed = time.time() - start
        return {
            "ok": False,
            "error": str(exc.reason),
            "elapsed_seconds": round(elapsed, 2),
        }


def extract_formatted_text(result: Dict[str, Any]) -> str:
    """Extract formatted text from API response."""
    try:
        choices = result.get("choices") or []
        if not choices:
            return ""
        message = choices[0].get("message") or {}
        return str(message.get("content") or "").strip()
    except Exception:
        return ""


def count_words(text: str) -> int:
    """Count words in text."""
    return len(re.findall(r"\b\w+\b", text or ""))


def check_output_quality(formatted: str, original: str) -> Dict[str, Any]:
    """Check output quality metrics."""
    original_words = count_words(original)
    formatted_words = count_words(formatted)
    
    # Check for bad patterns
    issues = []
    
    # Too short (summarized?)
    if formatted_words < original_words * 0.5:
        issues.append("too_short_maybe_summarized")
    
    # Too meta / chatbot-like
    lower = formatted.lower()
    if any(phrase in lower for phrase in [
        "here is the formatted",
        "i have formatted",
        "please provide",
        "silakan kirim",
        "i notice",
        "as an ai",
    ]):
        issues.append("too_meta_chatbot")
    
    # Empty or very short
    if formatted_words < 50:
        issues.append("too_short_empty")
    
    # No markdown structure at all
    if not any(c in formatted for c in ["#", "\n", "**", "- "]):
        issues.append("no_markdown_structure")
    
    # Word retention ratio
    retention_ratio = formatted_words / max(1, original_words)
    
    return {
        "original_words": original_words,
        "formatted_words": formatted_words,
        "retention_ratio": round(retention_ratio, 2),
        "issues": issues,
        "quality_score": 100 - (len(issues) * 20),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Smoke test formatting models: Groq Llama, Cerebras Qwen vs GPT-oss-120b"
    )
    ap.add_argument("--db", default="youtube_transcripts.db")
    ap.add_argument("--video-id", action="append", default=[], help="Specific video IDs to test")
    ap.add_argument("--limit", type=int, default=3, help="Number of videos to test")
    ap.add_argument("--min-words", type=int, default=1000, help="Minimum transcript words")
    ap.add_argument("--max-words", type=int, default=8000, help="Maximum transcript words")
    ap.add_argument("--max-tokens", type=int, default=3200, help="Max tokens for generation")
    ap.add_argument("--run-dir", default="", help="Custom run directory")
    ap.add_argument("--dry-run", action="store_true", help="Don't call APIs, just prepare")
    return ap.parse_args()


def run_dir_from_args(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(args.run_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return RUNS_DIR / f"smoke_format_test_{stamp}"


def pick_test_videos(
    con: sqlite3.Connection,
    *,
    video_ids: List[str],
    limit: int,
    min_words: int,
    max_words: int,
) -> List[sqlite3.Row]:
    """Pick videos for testing."""
    con.row_factory = sqlite3.Row
    
    if video_ids:
        placeholders = ",".join("?" for _ in video_ids)
        sql = f"""
            SELECT v.video_id, v.title, v.word_count, v.transcript_file_path, c.channel_name
            FROM videos v
            JOIN channels c ON v.channel_id = c.id
            WHERE v.video_id IN ({placeholders})
              AND v.transcript_file_path IS NOT NULL
              AND v.word_count >= ?
              AND v.word_count <= ?
            LIMIT ?
        """
        params = list(video_ids) + [min_words, max_words, limit]
    else:
        sql = """
            SELECT v.video_id, v.title, v.word_count, v.transcript_file_path, c.channel_name
            FROM videos v
            JOIN channels c ON v.channel_id = c.id
            WHERE v.transcript_file_path IS NOT NULL
              AND v.word_count >= ?
              AND v.word_count <= ?
            ORDER BY v.word_count DESC
            LIMIT ?
        """
        params = [min_words, max_words, limit]
    
    return list(con.execute(sql, params))


def read_transcript(file_path: str, project_root: Path) -> Optional[str]:
    """Read transcript file."""
    # Handle both absolute and relative paths
    if not Path(file_path).is_absolute():
        full_path = project_root / file_path
    else:
        full_path = Path(file_path)
    
    if not full_path.exists():
        return None
    
    try:
        content = full_path.read_text(encoding="utf-8")
        # Remove any header lines like "Kind:", "Language:"
        lines = content.splitlines()
        cleaned_lines = []
        for line in lines:
            if line.startswith(("Kind:", "Language:", "Duration:")):
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines)
    except Exception:
        return None


def find_available_accounts() -> Dict[str, List[Dict[str, Any]]]:
    """Find available accounts for each model we want to test.
    
    Note: We check provider availability, not runtime_model_name match,
    because accounts can use any model they support.
    """
    try:
        all_accounts = coordinator_status_accounts(include_inactive=True)
    except Exception as e:
        log(f"Failed to get accounts: {e}")
        return {}
    
    models_to_test = {
        "groq_llama": {"provider": "groq", "model": "llama-3.3-70b-versatile", "required_state": "idle"},
        "cerebras_qwen": {"provider": "cerebras", "model": "qwen-3-235b-a22b-instruct-2507", "required_state": "idle"},
        "nvidia_gpt": {"provider": "nvidia", "model": "openai/gpt-oss-120b", "required_state": "idle"},
        "gemini_flash": {"provider": "gemini", "model": "gemini-2.5-flash", "required_state": "idle"},
    }
    
    available = {}
    for key, config in models_to_test.items():
        # Match by provider and state
        matching = [
            acc for acc in all_accounts
            if acc.get("provider") == config["provider"]
            and acc.get("is_active") == 1
            and acc.get("state") == config["required_state"]
        ]
        available[key] = matching
        
        # Also count blocked/error accounts for info
        blocked = [
            acc for acc in all_accounts
            if acc.get("provider") == config["provider"]
            and acc.get("is_active") == 1
            and acc.get("state") != config["required_state"]
        ]
        
        status_msg = f"{len(matching)} idle"
        if blocked:
            status_msg += f" ({len(blocked)} blocked/error)"
        log(f"Model {key} ({config['provider']}): {status_msg} accounts")
    
    return available


def acquire_lease_for_model(
    provider: str,
    model_name: str,
    eligible_ids: Optional[List[int]] = None,
) -> Optional[Dict[str, Any]]:
    """Acquire lease for specific model."""
    try:
        leases = coordinator_acquire_accounts(
            provider=provider,
            model_name=model_name,
            count=1,
            eligible_account_ids=eligible_ids,
            task_type="format_smoke_test",
            lease_ttl_seconds=LEASE_TTL_SECONDS,
        )
        if leases:
            return leases[0]
    except Exception as e:
        log(f"Failed to acquire lease: {e}")
    return None


def test_model(
    *,
    lease: Dict[str, Any],
    transcript: str,
    prompt: str = FORMAT_PROMPT,
    max_tokens: int = 3200,
) -> Dict[str, Any]:
    """Test a model with given transcript."""
    provider = lease.get("provider", "")
    model_name = lease.get("model_name", "")
    api_key = lease.get("api_key", "")
    endpoint = lease.get("endpoint_url", "")
    extra_headers = lease.get("extra_headers") or {}
    account_name = lease.get("account_name", "")
    
    log(f"Testing {provider}/{model_name} (account: {account_name})...")
    
    result = call_provider_api(
        endpoint_url=endpoint,
        api_key=api_key,
        model_name=model_name,
        prompt=prompt,
        transcript=transcript,
        max_tokens=max_tokens,
        extra_headers=extra_headers,
    )
    
    if not result["ok"]:
        log(f"  ❌ API error: HTTP {result.get('http_status', 'N/A')} - {result.get('error', result.get('error_body', 'Unknown'))[:200]}")
        return {
            "success": False,
            "error": result.get("error", result.get("error_body", "Unknown error")),
            "http_status": result.get("http_status"),
        }
    
    formatted = extract_formatted_text(result["result"])
    quality = check_output_quality(formatted, transcript)
    
    log(f"  ✅ Success in {result['elapsed_seconds']}s | Words: {quality['formatted_words']} | Quality: {quality['quality_score']}")
    
    return {
        "success": True,
        "formatted_text": formatted,
        "elapsed_seconds": result["elapsed_seconds"],
        "quality": quality,
        "usage": result["result"].get("usage", {}),
    }


def run_smoke_test(args: argparse.Namespace) -> Path:
    """Run the smoke test."""
    run_dir = run_dir_from_args(args)
    run_dir.mkdir(parents=True, exist_ok=True)
    
    log(f"Run directory: {run_dir}")
    
    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log(f"Database not found: {db_path}")
        return run_dir
    
    con = sqlite3.connect(str(db_path))
    project_root = Path(__file__).resolve().parent
    
    # Pick test videos
    videos = pick_test_videos(
        con,
        video_ids=args.video_id,
        limit=args.limit,
        min_words=args.min_words,
        max_words=args.max_words,
    )
    
    if not videos:
        log("No suitable videos found for testing")
        return run_dir
    
    log(f"Selected {len(videos)} videos for testing")
    
    # Find available accounts
    available_accounts = find_available_accounts()
    
    # Determine which models to actually test based on availability
    models_to_test = []
    all_models_config = [
        ("groq_llama", "groq", "llama-3.3-70b-versatile"),
        ("cerebras_qwen", "cerebras", "qwen-3-235b-a22b-instruct-2507"),
        ("nvidia_gpt", "nvidia", "openai/gpt-oss-120b"),
        ("gemini_flash", "gemini", "gemini-2.5-flash"),
    ]
    
    for model_key, provider, model_name in all_models_config:
        accounts = available_accounts.get(model_key, [])
        if accounts:
            models_to_test.append((model_key, provider, model_name))
        else:
            log(f"⚠️  Skipping {provider}/{model_name}: no idle accounts available")
    
    if not models_to_test:
        log("❌ No models available for testing!")
        return run_dir
    
    log(f"✅ Will test {len(models_to_test)} models: {[m[0] for m in models_to_test]}")
    
    # Prepare report
    report_rows = []
    model_outputs: Dict[str, Dict[str, str]] = {}
    
    for video in videos:
        video_id = video["video_id"]
        title = video["title"][:50]
        channel = video["channel_name"]
        word_count = video["word_count"]
        transcript_path = video["transcript_file_path"]
        
        log(f"\n📹 Video: {video_id} | {title} | {word_count} words")
        
        # Read transcript
        transcript = read_transcript(transcript_path, project_root)
        if not transcript:
            log(f"  ⚠️  Cannot read transcript from {transcript_path}")
            continue
        
        # Save original transcript for reference
        transcript_file = run_dir / f"{video_id}_original.txt"
        transcript_file.write_text(transcript, encoding="utf-8")
        
        # Test each available model
        video_results = {"video_id": video_id, "title": title, "channel": channel, "words": word_count}
        
        for model_key, provider, model_name in models_to_test:
            accounts = available_accounts.get(model_key, [])
            if not accounts:
                video_results[f"{model_key}_status"] = "no_accounts"
                continue
            
            account = accounts[0]  # Use first available account
            
            if args.dry_run:
                log(f"  [DRY-RUN] Would test {provider}/{model_name}")
                video_results[f"{model_key}_status"] = "dry_run"
                continue
            
            # Acquire lease
            lease = acquire_lease_for_model(
                provider=provider,
                model_name=model_name,
                eligible_ids=[account["provider_account_id"]],
            )
            
            if not lease:
                log(f"  ❌ Failed to acquire lease for {provider}/{model_name}")
                video_results[f"{model_key}_status"] = "lease_failed"
                continue
            
            lease_token = lease.get("lease_token", "")
            
            # Start heartbeat
            heartbeat = LeaseHeartbeat(lease_token, LEASE_TTL_SECONDS)
            heartbeat.start()
            
            try:
                # Test the model
                result = test_model(
                    lease=lease,
                    transcript=transcript,
                    max_tokens=args.max_tokens,
                )
                
                if result["success"]:
                    video_results[f"{model_key}_status"] = "ok"
                    video_results[f"{model_key}_words"] = result["quality"]["formatted_words"]
                    video_results[f"{model_key}_retention"] = result["quality"]["retention_ratio"]
                    video_results[f"{model_key}_quality"] = result["quality"]["quality_score"]
                    video_results[f"{model_key}_elapsed"] = result["elapsed_seconds"]
                    video_results[f"{model_key}_issues"] = ",".join(result["quality"]["issues"]) or "none"
                    
                    # Save output
                    output_file = run_dir / f"{video_id}_{model_key}.txt"
                    output_file.write_text(result["formatted_text"], encoding="utf-8")
                    
                    # Track for comparison
                    if model_key not in model_outputs:
                        model_outputs[model_key] = {}
                    model_outputs[model_key][video_id] = result["formatted_text"]
                else:
                    video_results[f"{model_key}_status"] = "error"
                    video_results[f"{model_key}_error"] = str(result.get("error", ""))[:100]
                
            finally:
                # Stop heartbeat and release lease
                heartbeat.stop(final_state="idle", note="smoke_test_complete")
            
            # Small delay between tests
            time.sleep(1)
        
        report_rows.append(video_results)
    
    con.close()
    
    # Write report CSV
    report_file = run_dir / "report.csv"
    if report_rows:
        # Build dynamic fieldnames based on models tested
        base_fields = ["video_id", "title", "channel", "words"]
        model_fields = []
        for model_key, _, _ in models_to_test:
            model_fields.extend([
                f"{model_key}_status",
                f"{model_key}_words",
                f"{model_key}_retention",
                f"{model_key}_quality",
                f"{model_key}_elapsed",
                f"{model_key}_issues",
            ])
        fieldnames = base_fields + model_fields
        
        with open(report_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(report_rows)
        log(f"\n📊 Report written to: {report_file}")
    
    # Write summary
    summary_file = run_dir / "summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(f"Smoke Test Summary\n")
        f.write(f"==================\n\n")
        f.write(f"Videos tested: {len(report_rows)}\n")
        f.write(f"Models tested: {[m[0] for m in models_to_test]}\n")
        f.write(f"Run directory: {run_dir}\n\n")
        
        for model_key, _, _ in models_to_test:
            ok_count = sum(1 for row in report_rows if row.get(f"{model_key}_status") == "ok")
            avg_quality = sum(
                row.get(f"{model_key}_quality", 0) or 0 
                for row in report_rows
            ) / max(1, ok_count)
            avg_elapsed = sum(
                row.get(f"{model_key}_elapsed", 0) or 0 
                for row in report_rows
            ) / max(1, ok_count)
            
            f.write(f"{model_key}:\n")
            f.write(f"  Success: {ok_count}/{len(report_rows)}\n")
            f.write(f"  Avg Quality Score: {avg_quality:.1f}\n")
            f.write(f"  Avg Elapsed: {avg_elapsed:.1f}s\n\n")
    
    log(f"📝 Summary written to: {summary_file}")
    
    return run_dir


def main() -> None:
    args = parse_args()
    run_dir = run_smoke_test(args)
    
    if not args.dry_run:
        log(f"\n✅ Smoke test completed!")
        log(f"📁 Results: {run_dir}")
        log(f"\nTo view results:")
        log(f"  cat {run_dir / 'summary.txt'}")
        log(f"  code {run_dir / 'report.csv'}")


if __name__ == "__main__":
    main()
