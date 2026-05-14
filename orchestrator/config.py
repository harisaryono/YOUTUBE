"""
Orchestrator Configuration
Loads orchestrator.yaml with .env overrides.
"""

from __future__ import annotations

import os
import yaml
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "orchestrator.yaml"


def _load_dotenv() -> dict[str, str]:
    """Load .env file manually (avoid python-dotenv dependency)."""
    env_path = PROJECT_ROOT / ".env"
    result: dict[str, str] = {}
    if not env_path.exists():
        return result
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("\"'")
        if key:
            result[key] = val
    return result


def _merge_env(config: dict[str, Any], dotenv: dict[str, str]) -> dict[str, Any]:
    """Override YAML config with .env values where applicable."""
    # System
    if "MIN_FREE_DISK_GB" in dotenv:
        config.setdefault("system", {})["min_free_disk_gb"] = float(dotenv["MIN_FREE_DISK_GB"])
    if "MIN_MEMORY_MB_RESUME" in dotenv:
        config.setdefault("system", {})["min_memory_mb_resume"] = int(dotenv["MIN_MEMORY_MB_RESUME"])
    if "MIN_MEMORY_MB_FORMAT" in dotenv:
        config.setdefault("system", {})["min_memory_mb_format"] = int(dotenv["MIN_MEMORY_MB_FORMAT"])

    # YouTube
    if "YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN" in dotenv:
        config.setdefault("youtube", {})["inter_video_delay_min"] = float(dotenv["YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN"])
    if "YT_TRANSCRIPT_INTER_VIDEO_DELAY_MAX" in dotenv:
        config.setdefault("youtube", {})["inter_video_delay_max"] = float(dotenv["YT_TRANSCRIPT_INTER_VIDEO_DELAY_MAX"])
    if "YT_TRANSCRIPT_MAX_CONSECUTIVE_HARD_BLOCKS" in dotenv:
        config.setdefault("youtube", {})["max_consecutive_hard_blocks"] = int(dotenv["YT_TRANSCRIPT_MAX_CONSECUTIVE_HARD_BLOCKS"])

    # Audio download
    if "AUDIO_DOWNLOAD_WORKERS" in dotenv:
        config.setdefault("audio_download", {})["workers"] = int(dotenv["AUDIO_DOWNLOAD_WORKERS"])
    if "AUDIO_DOWNLOAD_BATCH_LIMIT" in dotenv:
        config.setdefault("audio_download", {})["batch_limit"] = int(dotenv["AUDIO_DOWNLOAD_BATCH_LIMIT"])
    if "AUDIO_DOWNLOAD_RATE_LIMIT_SAFE" in dotenv:
        config.setdefault("audio_download", {})["yt_dlp_rate_limit_safe"] = dotenv["AUDIO_DOWNLOAD_RATE_LIMIT_SAFE"].strip().lower() in {"1", "true", "yes", "on"}
    if "AUDIO_DOWNLOAD_AUDIO_DIR" in dotenv:
        config.setdefault("audio_download", {})["audio_dir"] = dotenv["AUDIO_DOWNLOAD_AUDIO_DIR"]

    # Resume
    if "RESUME_MAX_WORKERS" in dotenv:
        config.setdefault("resume", {})["max_workers"] = int(dotenv["RESUME_MAX_WORKERS"])
    if "RESUME_PROVIDER_PRIORITY" in dotenv:
        config.setdefault("resume", {})["provider_plan"] = dotenv["RESUME_PROVIDER_PRIORITY"]

    # Format
    if "FORMAT_MAX_WORKERS" in dotenv:
        config.setdefault("format", {})["max_workers"] = int(dotenv["FORMAT_MAX_WORKERS"])

    # ASR
    if "ASR_MODEL_GROQ" in dotenv:
        config.setdefault("asr", {})["groq_model"] = dotenv["ASR_MODEL_GROQ"]
    if "ASR_MODEL_NVIDIA_RIVA" in dotenv:
        config.setdefault("asr", {})["nvidia_model"] = dotenv["ASR_MODEL_NVIDIA_RIVA"]
    elif "ASR_MODEL_NVIDIA" in dotenv:
        config.setdefault("asr", {})["nvidia_model"] = dotenv["ASR_MODEL_NVIDIA"]
    if "ASR_REQUIRE_LOCAL_AUDIO" in dotenv:
        config.setdefault("asr", {})["require_local_audio"] = dotenv["ASR_REQUIRE_LOCAL_AUDIO"].strip().lower() in {"1", "true", "yes", "on"}
    if "ASR_DELETE_AUDIO_AFTER_SUCCESS" in dotenv:
        config.setdefault("asr", {})["delete_audio_after_success"] = dotenv["ASR_DELETE_AUDIO_AFTER_SUCCESS"].strip().lower() in {"1", "true", "yes", "on"}

    # Timeouts
    timeout_map = {
        "ORCH_TIMEOUT_DEFAULT_SECONDS": "default_seconds",
        "ORCH_TIMEOUT_DISCOVERY_SECONDS": "discovery_seconds",
        "ORCH_TIMEOUT_TRANSCRIPT_SECONDS": "transcript_seconds",
        "ORCH_TIMEOUT_AUDIO_DOWNLOAD_SECONDS": "audio_download_seconds",
        "ORCH_TIMEOUT_RESUME_SECONDS": "resume_seconds",
        "ORCH_TIMEOUT_ASR_SECONDS": "asr_seconds",
        "ORCH_TIMEOUT_FORMAT_SECONDS": "format_seconds",
    }
    for env_name, timeout_key in timeout_map.items():
        if env_name in dotenv:
            try:
                config.setdefault("timeouts", {})[timeout_key] = int(dotenv[env_name])
            except (TypeError, ValueError):
                continue

    return config


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load orchestrator config from YAML, merged with .env overrides."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    defaults: dict[str, Any] = {
        "profile": "safe",
        "loop": {
            "min_sleep_seconds": 5,
            "idle_sleep_seconds": 60,
            "error_sleep_seconds": 300,
        },
        "orchestrator": {
            "mode": "work_conserving",
            "max_jobs_per_cycle": 7,
            "max_parallel_jobs": 3,
            "parallel_groups": {
                "discovery": 1,
                "youtube": 1,
                "local": 2,
                "provider": 1,
            },
            "short_sleep_seconds": 5,
            "idle_sleep_seconds": 900,
            "youtube_backlog_boost_threshold": 500,
        },
        "parallel": {
            "enabled": True,
            "max_total_jobs": 8,
            "groups": {
                "discovery": {
                    "max_running": 1,
                    "stages": ["discovery"],
                },
                "youtube": {
                    "max_running": 2,
                    "stages": ["transcript", "audio_download"],
                },
                "provider": {
                    "max_running": 3,
                    "stages": ["resume", "asr"],
                },
                "local": {
                    "max_running": 2,
                    "stages": ["format", "janitor", "import_pending"],
                },
            },
            "stages": {
                "discovery": {"slots": 1, "prefer_never_discovered": True},
                "transcript": {"slots": 2},
                "audio_download": {"slots": 1},
                "resume": {"slots": 2},
                "asr": {"slots": 1},
                "format": {"slots": 1},
                "janitor": {"slots": 1},
            },
        },
        "system": {
            "min_free_disk_gb": 5,
            "min_memory_mb_resume": 1500,
            "min_memory_mb_format": 1200,
            "min_memory_mb_asr": 2500,
        },
        "timeouts": {
            "default_seconds": 7200,
            "discovery_seconds": 1800,
            "transcript_seconds": 3600,
            "audio_download_seconds": 3600,
            "resume_seconds": 7200,
            "asr_seconds": 7200,
            "format_seconds": 7200,
        },
        "youtube": {
            "discovery_interval_hours": 24,
            "batch_limit": 100,
            "safe_transcript_workers": 2,
            "normal_transcript_workers": 5,
            "hard_block_cooldown_hours": 6,
            "bot_detection_cooldown_hours": 24,
            "max_consecutive_hard_blocks": 3,
            "inter_video_delay_min": 8,
            "inter_video_delay_max": 15,
        },
        "audio_download": {
            "enabled": True,
            "batch_limit": 50,
            "workers": 1,
            "max_duration_minutes": 60,
            "yt_dlp_rate_limit_safe": True,
            "keep_audio_after_asr": False,
            "audio_dir": "uploads/audio",
        },
        "resume": {
            "enabled": True,
            "max_workers": 4,
            "batch_limit": 100,
            "require_lease": True,
            "provider_plan": "nvidia_first",
        },
        "format": {
            "enabled": True,
            "max_workers": 4,
            "batch_limit": 500,
            "prefer_idle_hours": True,
            "idle_hours": {"start": "22:00", "end": "05:00"},
        },
        "asr": {
            "enabled": True,
            "groq_model": "whisper-large-v3",
            "nvidia_model": "whisper-large-v3-multi-asr-offline",
            "max_video_workers": 2,
            "batch_limit": 20,
            "max_duration_minutes": 60,
            "require_local_audio": True,
            "delete_audio_after_success": True,
            "require_cached_audio": False,
        },
        "report": {
            "write_markdown": True,
            "notify_on_blocking": True,
        },
        "adaptive": {
            "transcript": {
                "enabled": True,
                "min_batch": 20,
                "max_batch": 300,
                "step": 50,
                "increase_after_success_batches": 3,
                "decrease_on_block": True,
            },
            "audio_download": {
                "enabled": True,
                "min_batch": 10,
                "max_batch": 100,
                "step": 20,
                "increase_after_success_batches": 3,
                "decrease_on_block": True,
            },
        },
        "janitor": {
            "enabled": True,
            "interval_minutes": 60,
            "keep_events_days": 30,
            "keep_logs_days": 14,
            "keep_run_dirs_days": 7,
            "keep_reports_days": 14,
            "cleanup_audio_orphans": True,
        },
    }

    if path.exists():
        with open(path) as f:
            yaml_config = yaml.safe_load(f) or {}
        # Deep merge
        for section, values in yaml_config.items():
            if section in defaults and isinstance(values, dict):
                defaults[section].update(values)
            else:
                defaults[section] = values

    # .env overrides
    dotenv = _load_dotenv()
    defaults = _merge_env(defaults, dotenv)

    return defaults
