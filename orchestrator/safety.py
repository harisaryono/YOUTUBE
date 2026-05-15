"""
Safety Gate — System health checks and job safety decisions.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .policies import policy_blockers_for_job
from .state import OrchestratorState


# --- Health Data ---

class SystemHealth:
    """Snapshot of system health at a point in time."""

    def __init__(self) -> None:
        self.disk_free_gb: float = 0.0
        self.mem_available_mb: float = 0.0
        self.mem_total_mb: float = 0.0
        self.load_1min: float = 0.0
        self.load_5min: float = 0.0
        self.load_15min: float = 0.0
        self.errors: list[str] = []

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


class ProviderHealth:
    """Snapshot of provider health."""

    def __init__(self) -> None:
        self.coordinator_available: bool = False
        self.available_leases: int = 0
        self.total_leases: int = 0
        self.blocked_providers: list[str] = []
        self.errors: list[str] = []

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


class YouTubeHealth:
    """Snapshot of YouTube accessibility."""

    def __init__(self) -> None:
        self.global_cooldown_active: bool = False
        self.cooldown_reason: str = ""
        self.cooldown_until: str = ""
        self.consecutive_hard_blocks: int = 0
        self.errors: list[str] = []

    @property
    def ok(self) -> bool:
        return not self.global_cooldown_active and len(self.errors) == 0


# --- Safety Decision ---

class SafetyDecision:
    """Result of a safety gate check for a job."""

    def __init__(
        self,
        verdict: str = "RUN",
        reason: str = "",
        cooldown_seconds: int = 0,
        recommendation: str = "",
        reason_code: str = "",
    ):
        self.verdict = verdict  # RUN | WAIT | SKIP_PERMANENT | REPORT
        self.reason = reason
        self.cooldown_seconds = cooldown_seconds
        self.recommendation = recommendation
        self.reason_code = reason_code

    @classmethod
    def run(cls) -> "SafetyDecision":
        return cls("RUN")

    @classmethod
    def wait(
        cls,
        reason: str,
        cooldown_seconds: int = 300,
        recommendation: str = "",
        reason_code: str = "",
    ) -> "SafetyDecision":
        return cls("WAIT", reason, cooldown_seconds, recommendation, reason_code)

    @classmethod
    def skip_permanent(cls, reason: str, recommendation: str = "") -> "SafetyDecision":
        return cls("SKIP_PERMANENT", reason, recommendation=recommendation)

    @classmethod
    def report(cls, reason: str, recommendation: str = "") -> "SafetyDecision":
        return cls("REPORT", reason, recommendation=recommendation)


# --- Helpers ---

def _is_idle_hours(config: dict[str, Any]) -> bool:
    """Check if current time (Asia/Jakarta) falls within idle hours."""
    idle = config.get("format", {}).get("idle_hours", {})
    start_str = idle.get("start", "22:00")
    end_str = idle.get("end", "05:00")

    try:
        # Current time in Asia/Jakarta (UTC+7)
        now = datetime.now(timezone.utc)
        jakarta_hour = (now.hour + 7) % 24
        jakarta_min = now.minute

        start_parts = start_str.split(":")
        end_parts = end_str.split(":")
        start_h = int(start_parts[0])
        start_m = int(start_parts[1]) if len(start_parts) > 1 else 0
        end_h = int(end_parts[0])
        end_m = int(end_parts[1]) if len(end_parts) > 1 else 0

        current_minutes = jakarta_hour * 60 + jakarta_min
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m

        if start_minutes <= end_minutes:
            # e.g. 05:00 - 22:00
            return start_minutes <= current_minutes <= end_minutes
        else:
            # e.g. 22:00 - 05:00 (overnight)
            return current_minutes >= start_minutes or current_minutes <= end_minutes
    except (ValueError, IndexError):
        return True  # If config is malformed, allow


def _pause_reason(state: OrchestratorState, key: str) -> str:
    """Return the pause reason for a pause key, if any."""
    raw = str(state.get(f"pause:{key}", "") or "").strip()
    if not raw:
        return ""
    try:
        payload = json.loads(raw)
    except Exception:
        return raw
    if isinstance(payload, dict):
        reason = str(payload.get("reason") or "").strip()
        if reason:
            return reason
    return raw


# --- Health Checkers ---

def check_system_health(config: dict[str, Any]) -> SystemHealth:
    """Check disk and memory health."""
    health = SystemHealth()

    # Disk
    try:
        usage = shutil.disk_usage("/")
        health.disk_free_gb = usage.free / (1024 ** 3)
        min_disk = config.get("system", {}).get("min_free_disk_gb", 5)
        if health.disk_free_gb < min_disk:
            health.errors.append(
                f"Disk low: {health.disk_free_gb:.1f} GB free (min {min_disk} GB)"
            )
    except Exception as e:
        health.errors.append(f"Disk check failed: {e}")

    # Memory
    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()
        for line in meminfo.splitlines():
            if line.startswith("MemAvailable:"):
                health.mem_available_mb = int(line.split()[1]) / 1024
            elif line.startswith("MemTotal:"):
                health.mem_total_mb = int(line.split()[1]) / 1024
    except Exception:
        # Fallback: try psutil if available
        try:
            import psutil
            health.mem_available_mb = psutil.virtual_memory().available / (1024 * 1024)
            health.mem_total_mb = psutil.virtual_memory().total / (1024 * 1024)
        except ImportError:
            health.errors.append("Cannot check memory (no /proc/meminfo or psutil)")

    # Load
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().split()
            if len(parts) >= 3:
                health.load_1min = float(parts[0])
                health.load_5min = float(parts[1])
                health.load_15min = float(parts[2])
    except Exception:
        pass

    return health


def check_provider_health(config: dict[str, Any], state: OrchestratorState) -> ProviderHealth:
    """Check provider/coordinator health via real coordinator health check."""
    health = ProviderHealth()

    # Try real coordinator health check
    try:
        from local_services import coordinator_status_accounts
        accounts = coordinator_status_accounts(include_inactive=False)
        health.coordinator_available = True
        health.total_leases = len(accounts)
        # leaseable=None means account exists but status unknown — treat as available
        health.available_leases = sum(
            1 for a in accounts
            if a.get("leaseable") is None or a.get("leaseable") is True
        )
        state.set("coordinator_last_ok", "1")
    except ImportError:
        # Fallback: try direct HTTP health check via env var
        try:
            import urllib.request
            import json as _json
            coordinator_url = os.getenv("YT_PROVIDER_COORDINATOR_URL", "http://127.0.0.1:8788").rstrip("/")
            req = urllib.request.Request(
                f"{coordinator_url}/health",
                method="GET",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = _json.loads(resp.read().decode())
                health.coordinator_available = True
                health.total_leases = data.get("total_accounts", 0)
                health.available_leases = data.get("available_accounts", 0)
                state.set("coordinator_last_ok", "1")
        except Exception as e:
            # Fallback: check via state
            coordinator_ok = state.get("coordinator_last_ok", "0")
            health.coordinator_available = coordinator_ok == "1"
            if not health.coordinator_available:
                health.errors.append(f"Coordinator unavailable: {e}")
    except Exception as e:
        health.coordinator_available = False
        state.set("coordinator_last_ok", "0")
        health.errors.append(f"Coordinator unavailable: {e}")

    # Check provider-specific cooldowns
    for cd in state.list_active_cooldowns():
        scope = cd["scope"]
        if scope.startswith("provider:"):
            provider_name = scope.replace("provider:", "")
            health.blocked_providers.append(provider_name)
            health.errors.append(f"Provider {provider_name} blocked: {cd['reason']}")

    return health


def check_youtube_health(config: dict[str, Any], state: OrchestratorState) -> YouTubeHealth:
    """Check YouTube cooldown status."""
    health = YouTubeHealth()

    cd = state.get_cooldown("youtube")
    if cd is not None:
        health.global_cooldown_active = True
        health.cooldown_reason = cd["reason"]
        health.cooldown_until = cd["cooldown_until"]
        health.errors.append(f"YouTube cooldown: {cd['reason']} until {cd['cooldown_until']}")

    health.consecutive_hard_blocks = state.get_int("youtube_consecutive_hard_blocks", 0)

    return health


# --- Safety Gate ---

def safety_gate_for_job(
    job: dict[str, Any],
    config: dict[str, Any],
    sys_health: SystemHealth,
    provider_health: ProviderHealth,
    youtube_health: YouTubeHealth,
    state: OrchestratorState,
) -> SafetyDecision:
    """
    Check if a specific job can run safely.
    Returns a SafetyDecision with verdict: RUN, WAIT, SKIP_PERMANENT, or REPORT.
    """
    stage = job.get("stage", "")
    scope = job.get("scope", "")

    # --- Global checks ---

    # Disk
    min_disk = config.get("system", {}).get("min_free_disk_gb", 5)
    if sys_health.disk_free_gb < min_disk:
        return SafetyDecision.wait(
            f"Disk low: {sys_health.disk_free_gb:.1f} GB free",
            cooldown_seconds=1800,
            recommendation="Clean up runs/uploads/logs/cache",
            reason_code="DEFER_DISK_LOW",
        )

    # Pause controls
    blockers = policy_blockers_for_job(state, stage=stage, scope=scope)
    pause_reason = next((str(item.get("reason") or "").strip() for item in blockers if item.get("type") in {"pause", "quarantine"} and str(item.get("reason") or "").strip()), "")
    if pause_reason:
        return SafetyDecision.wait(
            f"Paused: {pause_reason}",
            cooldown_seconds=0,
            recommendation="Resume the stage or scope when ready",
            reason_code="DEFER_STAGE_PAUSED",
        )

    def _trim_channel_cooldown_reason(value: str) -> str:
        text = str(value or "").strip()
        prefix = "Channel cooldown: "
        while text.startswith(prefix):
            text = text[len(prefix) :].strip()
        return text

    # --- Stage-specific checks ---

    if stage in ("discovery", "transcript", "audio_download"):
        # YouTube-dependent stages
        youtube_cd = state.get_cooldown("youtube")
        if stage == "discovery":
            discovery_cd = state.get_cooldown("youtube:discovery")
            if youtube_cd is not None:
                return SafetyDecision.wait(
                    f"YouTube cooldown active: {youtube_cd['reason']}",
                    cooldown_seconds=3600,
                    recommendation="Wait for YouTube cooldown to expire",
                    reason_code="DEFER_YOUTUBE_COOLDOWN",
                )
            if discovery_cd is not None:
                return SafetyDecision.wait(
                    f"Discovery cooldown active: {discovery_cd['reason']}",
                    recommendation=discovery_cd.get("recommendation", ""),
                    reason_code="DEFER_DISCOVERY_COOLDOWN",
                )
        else:
            content_cd = state.get_cooldown("youtube:content")
            if youtube_cd is not None:
                return SafetyDecision.wait(
                    f"YouTube cooldown active: {youtube_cd['reason']}",
                    cooldown_seconds=3600,
                    recommendation="Wait for YouTube cooldown to expire",
                    reason_code="DEFER_YOUTUBE_COOLDOWN",
                )
            if content_cd is not None:
                return SafetyDecision.wait(
                    f"Content cooldown active: {content_cd['reason']}",
                    recommendation=content_cd.get("recommendation", ""),
                    reason_code="DEFER_YOUTUBE_CONTENT_COOLDOWN",
                )

        # Channel-specific cooldown (only if scope is a channel)
        if scope and scope.startswith("channel:") and state.is_cooldown_active(scope):
            cd = state.get_cooldown(scope)
            return SafetyDecision.wait(
                f"Channel cooldown: {_trim_channel_cooldown_reason(cd['reason'])}",
                recommendation=cd.get("recommendation", ""),
                reason_code="DEFER_CHANNEL_COOLDOWN",
            )

    if stage == "asr":
        if config.get("asr", {}).get("require_local_audio", True):
            audio_path = str(job.get("audio_file_path") or "").strip()
            if audio_path and not Path(audio_path).exists():
                return SafetyDecision.wait(
                    f"Local audio missing: {audio_path}",
                    recommendation="Requeue audio_download for the video",
                    reason_code="DEFER_NO_LOCAL_AUDIO",
                )

    if stage in ("asr", "resume", "format"):
        # Memory check for LLM stages
        min_mem = config.get("system", {}).get(
            f"min_memory_mb_{stage}", 1200
        )
        if stage == "asr":
            min_mem = config.get("system", {}).get("min_memory_mb_asr", 2500)
        if sys_health.mem_available_mb < min_mem:
            return SafetyDecision.wait(
                f"Memory low for {stage}: {sys_health.mem_available_mb:.0f} MB available (need {min_mem} MB)",
                cooldown_seconds=900,
                recommendation="Reduce workers or wait for other jobs to finish",
                reason_code="DEFER_MEMORY_LOW",
            )

        # Provider check — only warn, don't block.
        # Lease acquisition is handled by the worker script itself (launch_resume_queue.py etc).
        # Blocking here based on coordinator_status_accounts is unreliable because
        # leaseable=None means "status unknown" not "unavailable".
        if stage in ("resume", "format", "asr") and config.get(stage, {}).get("require_lease", True):
            if not provider_health.coordinator_available:
                return SafetyDecision.wait(
                    "Coordinator unavailable, cannot get provider lease",
                    cooldown_seconds=300,
                    recommendation="Check coordinator service",
                    reason_code="DEFER_PROVIDER_UNAVAILABLE",
                )
            if provider_health.available_leases == 0:
                # Don't block — let the worker script try to acquire.
                # Just log a warning via the report.
                pass

    if stage == "format":
        # Idle hours check for format
        if config.get("format", {}).get("prefer_idle_hours", False):
            if not _is_idle_hours(config):
                # Aggressive mode: idle hours are advisory, not a hard block.
                # If the machine has room, keep formatting instead of cooling down.
                return SafetyDecision.run()

    return SafetyDecision.run()
