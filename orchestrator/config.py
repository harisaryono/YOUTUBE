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

    # Resume
    if "RESUME_MAX_WORKERS" in dotenv:
        config.setdefault("resume", {})["max_workers"] = int(dotenv["RESUME_MAX_WORKERS"])
    if "RESUME_PROVIDER_PRIORITY" in dotenv:
        config.setdefault("resume", {})["provider_plan"] = dotenv["RESUME_PROVIDER_PRIORITY"]

    # Format
    if "FORMAT_MAX_WORKERS" in dotenv:
        config.setdefault("format", {})["max_workers"] = int(dotenv["FORMAT_MAX_WORKERS"])

    return config


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load orchestrator config from YAML, merged with .env overrides."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    defaults: dict[str, Any] = {
        "profile": "safe",
        "loop": {
            "min_sleep_seconds": 60,
            "idle_sleep_seconds": 900,
            "error_sleep_seconds": 1800,
        },
        "system": {
            "min_free_disk_gb": 5,
            "min_memory_mb_resume": 1500,
            "min_memory_mb_format": 1200,
            "min_memory_mb_asr": 2500,
        },
        "youtube": {
            "discovery_interval_hours": 24,
            "safe_transcript_workers": 2,
            "normal_transcript_workers": 5,
            "hard_block_cooldown_hours": 6,
            "bot_detection_cooldown_hours": 24,
            "max_consecutive_hard_blocks": 3,
            "inter_video_delay_min": 8,
            "inter_video_delay_max": 15,
        },
        "resume": {
            "enabled": True,
            "max_workers": 8,
            "require_lease": True,
            "provider_plan": "nvidia_first",
        },
        "format": {
            "enabled": True,
            "max_workers": 6,
            "prefer_idle_hours": True,
            "idle_hours": {"start": "22:00", "end": "05:00"},
        },
        "asr": {
            "enabled": False,
            "max_video_workers": 1,
            "max_duration_minutes": 60,
            "require_cached_audio": False,
        },
        "report": {
            "write_markdown": True,
            "notify_on_blocking": True,
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
