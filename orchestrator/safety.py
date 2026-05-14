"""
Safety Gate — System health checks and job safety decisions.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

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
    ):
        self.verdict = verdict  # RUN | WAIT | SKIP_PERMANENT | REPORT
        self.reason = reason
        self.cooldown_seconds = cooldown_seconds
        self.recommendation = recommendation

    @classmethod
    def run(cls) -> "SafetyDecision":
        return cls("RUN")

    @classmethod
    def wait(cls, reason: str, cooldown_seconds: int = 300, recommendation: str = "") -> "SafetyDecision":
        return cls("WAIT", reason, cooldown_seconds, recommendation)

    @classmethod
    def skip_permanent(cls, reason: str, recommendation: str = "") -> "SafetyDecision":
        return cls("SKIP_PERMANENT", reason, recommendation=recommendation)

    @classmethod
    def report(cls, reason: str, recommendation: str = "") -> "SafetyDecision":
        return cls("REPORT", reason, recommendation=recommendation)


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
    """Check provider/coordinator health via state cooldowns."""
    health = ProviderHealth()

    # Check coordinator availability via state
    coordinator_ok = state.get("coordinator_last_ok", "0")
    health.coordinator_available = coordinator_ok == "1"

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

    # Check channel-specific cooldowns
    for cd in state.list_active_cooldowns():
        if cd["scope"].startswith("channel:"):
            pass  # Channel cooldowns are checked per-job, not globally

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
        )

    # --- Stage-specific checks ---

    if stage in ("discovery", "transcript"):
        # YouTube-dependent stages
        if youtube_health.global_cooldown_active:
            return SafetyDecision.wait(
                f"YouTube cooldown active: {youtube_health.cooldown_reason}",
                cooldown_seconds=3600,
                recommendation="Wait for YouTube cooldown to expire",
            )

        # Channel-specific cooldown
        if scope and state.is_cooldown_active(scope):
            cd = state.get_cooldown(scope)
            return SafetyDecision.wait(
                f"Channel cooldown: {cd['reason']}",
                recommendation=cd.get("recommendation", ""),
            )

    if stage in ("resume", "format"):
        # Memory check for LLM stages
        min_mem = config.get("system", {}).get(
            f"min_memory_mb_{stage}", 1200
        )
        if sys_health.mem_available_mb < min_mem:
            return SafetyDecision.wait(
                f"Memory low for {stage}: {sys_health.mem_available_mb:.0f} MB available (need {min_mem} MB)",
                cooldown_seconds=900,
                recommendation="Reduce workers or wait for other jobs to finish",
            )

        # Provider check
        if config.get(stage, {}).get("require_lease", True):
            if not provider_health.coordinator_available:
                return SafetyDecision.wait(
                    "Coordinator unavailable, cannot get provider lease",
                    cooldown_seconds=300,
                    recommendation="Check coordinator service",
                )

    if stage == "asr":
        # ASR is expensive — be extra careful
        min_mem = config.get("system", {}).get("min_memory_mb_asr", 2500)
        if sys_health.mem_available_mb < min_mem:
            return SafetyDecision.wait(
                f"Memory low for ASR: {sys_health.mem_available_mb:.0f} MB available (need {min_mem} MB)",
                cooldown_seconds=1800,
            )

    return SafetyDecision.run()
