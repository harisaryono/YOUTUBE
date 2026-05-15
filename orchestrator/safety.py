"""
Safety Gate — System health checks, emergency stop, and job safety decisions.

This module provides:
- CLI commands: safety-status, emergency-stop, clear-emergency-stop
- Guard functions for launch_job and retry drain
- Integration with existing health checkers

Usage:
    python -m orchestrator.safety status [--json]
    python -m orchestrator.safety emergency-stop --reason "test"
    python -m orchestrator.safety clear-emergency-stop --reason "resume"
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .policies import policy_blockers_for_job
from .state import OrchestratorState
from .config import DEFAULT_CONFIG_PATH, load_config


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
            return start_minutes <= current_minutes <= end_minutes
        else:
            return current_minutes >= start_minutes or current_minutes <= end_minutes
    except (ValueError, IndexError):
        return True


# --- Health Checkers ---

def check_system_health(config: dict[str, Any]) -> SystemHealth:
    """Check disk and memory health."""
    health = SystemHealth()

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

    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()
        for line in meminfo.splitlines():
            if line.startswith("MemAvailable:"):
                health.mem_available_mb = int(line.split()[1]) / 1024
            elif line.startswith("MemTotal:"):
                health.mem_total_mb = int(line.split()[1]) / 1024
    except Exception:
        try:
            import psutil
            health.mem_available_mb = psutil.virtual_memory().available / (1024 * 1024)
            health.mem_total_mb = psutil.virtual_memory().total / (1024 * 1024)
        except ImportError:
            health.errors.append("Cannot check memory (no /proc/meminfo or psutil)")

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
    """Check provider/coordinator health."""
    health = ProviderHealth()
    try:
        from local_services import coordinator_status_accounts
        accounts = coordinator_status_accounts(include_inactive=False)
        health.coordinator_available = True
        health.total_leases = len(accounts)
        health.available_leases = sum(
            1 for a in accounts
            if a.get("leaseable") is None or a.get("leaseable") is True
        )
        state.set("coordinator_last_ok", "1")
    except ImportError:
        try:
            import urllib.request
            coordinator_url = os.getenv("YT_PROVIDER_COORDINATOR_URL", "http://127.0.0.1:8788").rstrip("/")
            req = urllib.request.Request(
                f"{coordinator_url}/health",
                method="GET",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                health.coordinator_available = True
                health.total_leases = data.get("total_accounts", 0)
                health.available_leases = data.get("available_accounts", 0)
                state.set("coordinator_last_ok", "1")
        except Exception as e:
            coordinator_ok = state.get("coordinator_last_ok", "0")
            health.coordinator_available = coordinator_ok == "1"
            if not health.coordinator_available:
                health.errors.append(f"Coordinator unavailable: {e}")
    except Exception as e:
        health.coordinator_available = False
        state.set("coordinator_last_ok", "0")
        health.errors.append(f"Coordinator unavailable: {e}")

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
    min_disk = config.get("system", {}).get("min_free_disk_gb", 5)
    if sys_health.disk_free_gb < min_disk:
        return SafetyDecision.wait(
            f"Disk low: {sys_health.disk_free_gb:.1f} GB free",
            cooldown_seconds=1800,
            recommendation="Clean up runs/uploads/logs/cache",
            reason_code="DEFER_DISK_LOW",
        )

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
            text = text[len(prefix):].strip()
        return text

    def _trim_stage_cooldown_reason(value: str) -> str:
        text = str(value or "").strip()
        prefix = "Stage cooldown: "
        while text.startswith(prefix):
            text = text[len(prefix):].strip()
        return text

    # Stage-scoped cooldown — prevents cross-contamination between unrelated stages.
    # e.g. stage:resume cooldown only blocks resume, not transcript or ASR.
    stage_scope = f"stage:{stage}"
    if state.is_cooldown_active(stage_scope):
        cd = state.get_cooldown(stage_scope)
        return SafetyDecision.wait(
            f"Stage cooldown: {_trim_stage_cooldown_reason(cd['reason'])}",
            cooldown_seconds=300,
            recommendation=cd.get("recommendation", ""),
            reason_code="DEFER_STAGE_COOLDOWN",
        )

    if stage in ("discovery", "transcript", "audio_download"):
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
        min_mem = config.get("system", {}).get(f"min_memory_mb_{stage}", 1200)
        if stage == "asr":
            min_mem = config.get("system", {}).get("min_memory_mb_asr", 2500)
        if sys_health.mem_available_mb < min_mem:
            return SafetyDecision.wait(
                f"Memory low for {stage}: {sys_health.mem_available_mb:.0f} MB available (need {min_mem} MB)",
                cooldown_seconds=900,
                recommendation="Reduce workers or wait for other jobs to finish",
                reason_code="DEFER_MEMORY_LOW",
            )

        if stage in ("resume", "format", "asr") and config.get(stage, {}).get("require_lease", True):
            if not provider_health.coordinator_available:
                return SafetyDecision.wait(
                    "Coordinator unavailable, cannot get provider lease",
                    cooldown_seconds=300,
                    recommendation="Check coordinator service",
                    reason_code="DEFER_PROVIDER_UNAVAILABLE",
                )
            if stage == "asr":
                # Granular ASR provider degraded check based on provider_plan
                blocked = set(provider_health.blocked_providers)
                provider_plan = str(config.get("asr", {}).get("provider_plan", "groq_first")).strip().lower()

                # Global ASR block — all providers down
                if "asr" in blocked:
                    return SafetyDecision.wait(
                        "ASR provider service degraded (asr), retry later",
                        cooldown_seconds=600,
                        recommendation="Wait for provider recovery or check provider cooldown",
                        reason_code="DEFER_ASR_PROVIDER_DEGRADED",
                    )

                # NVIDIA-only plan and NVIDIA is degraded — must wait
                if provider_plan == "nvidia_only" and "nvidia_riva" in blocked:
                    return SafetyDecision.wait(
                        "NVIDIA Riva degraded and ASR is configured as nvidia_only",
                        cooldown_seconds=600,
                        recommendation="Switch ASR provider_plan to groq_first or wait for NVIDIA recovery",
                        reason_code="DEFER_NVIDIA_RIVA_DEGRADED",
                    )

                # Groq-only plan and Groq is down — must wait
                if provider_plan == "groq_only" and "groq" in blocked:
                    return SafetyDecision.wait(
                        "Groq unavailable and ASR is configured as groq_only",
                        cooldown_seconds=600,
                        recommendation="Switch ASR provider_plan or wait for Groq recovery",
                        reason_code="DEFER_GROQ_UNAVAILABLE",
                    )

                # Multi-provider plans: allow ASR if at least one provider is healthy
                if provider_plan in {"groq_first", "nvidia_first"}:
                    nvidia_ok = "nvidia_riva" not in blocked
                    groq_ok = "groq" not in blocked
                    if nvidia_ok or groq_ok:
                        pass  # At least one provider available — allow ASR
                    else:
                        return SafetyDecision.wait(
                            "All ASR providers degraded, retry later",
                            cooldown_seconds=600,
                            recommendation="Wait for any provider recovery",
                            reason_code="DEFER_ASR_ALL_PROVIDERS_DEGRADED",
                        )

    if stage == "format":
        if config.get("format", {}).get("prefer_idle_hours", False):
            if not _is_idle_hours(config):
                return SafetyDecision.run()

    return SafetyDecision.run()


# --- Stage 15: Emergency Stop Guards ---

def ensure_launch_allowed(
    config: dict[str, Any],
    state: OrchestratorState,
    action: str = "launch_job",
) -> tuple[bool, list[str]]:
    """
    Check if a launch action is allowed under current safety state.
    Returns (allowed: bool, blockers: list[str]).

    action can be:
      - "launch_job" — normal job launch
      - "retry_queue_drain" — real retry queue drain
    """
    safety_cfg = config.get("safety", {}) or {}
    es_blocks_launch = bool(safety_cfg.get("emergency_stop_blocks_launch", True))
    es_blocks_retry = bool(safety_cfg.get("emergency_stop_blocks_retry_drain", True))

    if state.is_emergency_stop_active():
        if action == "launch_job" and es_blocks_launch:
            return False, ["Emergency stop is active. No new job launches allowed."]
        if action == "retry_queue_drain" and es_blocks_retry:
            return False, ["Emergency stop is active. Real retry queue drain is blocked."]

    return True, []


# --- CLI Implementation ---

def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def cmd_status(config: dict[str, Any], args: argparse.Namespace) -> int:
    """Show safety status."""
    state = OrchestratorState()
    try:
        safety_status = state.get_safety_status()

        # Build enriched status
        enriched = dict(safety_status)
        enriched["recent_safety_events"] = state.list_safety_events(limit=args.recent_events or 20)

        # Add system health context
        sys_health = check_system_health(config)
        enriched["system"] = {
            "disk_free_gb": round(sys_health.disk_free_gb, 1),
            "mem_available_mb": round(sys_health.mem_available_mb, 0),
            "ok": sys_health.ok,
        }

        if args.json:
            _print_json(enriched)
        else:
            es = safety_status.get("emergency_stop", {})
            ro = safety_status.get("readonly", {})
            print("SAFETY STATUS")
            print("")
            print(f"Emergency stop: {'ACTIVE' if es.get('active') else 'inactive'}")
            if es.get("active"):
                print(f"  reason: {es.get('reason', '')}")
                print(f"  actor: {es.get('actor', '')}")
                print(f"  updated_at: {es.get('updated_at', '')}")
            print(f"Readonly: {'ACTIVE' if ro.get('active') else 'inactive'}")
            print(f"System: disk {sys_health.disk_free_gb:.1f} GB, mem {sys_health.mem_available_mb:.0f} MB")
            events = enriched.get("recent_safety_events", [])
            if events:
                print("")
                print("Recent safety events:")
                for ev in events[:5]:
                    print(f"  - {ev.get('created_at', '')} {ev.get('event_type', '')}: {ev.get('message', '')[:120]}")
        return 0
    finally:
        state.close()


def cmd_emergency_stop(config: dict[str, Any], args: argparse.Namespace) -> int:
    """Activate emergency stop."""
    state = OrchestratorState()
    try:
        reason = str(args.reason or "").strip()
        if not reason:
            print("Error: --reason is required for emergency stop", file=sys.stderr)
            return 1
        actor = str(args.actor or "cli").strip()
        state.set_emergency_stop(reason=reason, actor=actor)
        result = {
            "ok": True,
            "action": "emergency_stop",
            "reason": reason,
            "actor": actor,
            "safety_status": state.get_safety_status(),
        }
        if args.json:
            _print_json(result)
        else:
            print(f"Emergency stop activated.")
            print(f"  reason: {reason}")
            print(f"  actor: {actor}")
            print("No new job launches will be allowed.")
            print("Running jobs are still being monitored.")
        return 0
    finally:
        state.close()


def cmd_clear_emergency_stop(config: dict[str, Any], args: argparse.Namespace) -> int:
    """Clear emergency stop."""
    state = OrchestratorState()
    try:
        reason = str(args.reason or "").strip()
        if not reason:
            print("Error: --reason is required to clear emergency stop", file=sys.stderr)
            return 1
        actor = str(args.actor or "cli").strip()
        state.clear_emergency_stop(reason=reason, actor=actor)
        result = {
            "ok": True,
            "action": "clear_emergency_stop",
            "reason": reason,
            "actor": actor,
            "safety_status": state.get_safety_status(),
        }
        if args.json:
            _print_json(result)
        else:
            print(f"Emergency stop cleared.")
            print(f"  reason: {reason}")
            print(f"  actor: {actor}")
            print("Job launches are now allowed.")
        return 0
    finally:
        state.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safety guard CLI for orchestrator")
    parser.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser("status", help="Show safety status")
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    p.add_argument("--recent-events", type=int, default=20, help="Number of recent safety events")

    p = subparsers.add_parser("emergency-stop", help="Activate emergency stop")
    p.add_argument("--reason", required=True, help="Reason for emergency stop")
    p.add_argument("--actor", default="cli", help="Who is activating the stop")
    p.add_argument("--json", action="store_true", help="Emit JSON output")

    p = subparsers.add_parser("clear-emergency-stop", help="Clear emergency stop")
    p.add_argument("--reason", required=True, help="Reason for clearing emergency stop")
    p.add_argument("--actor", default="cli", help="Who is clearing the stop")
    p.add_argument("--json", action="store_true", help="Emit JSON output")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)

    if args.command == "status":
        return cmd_status(config, args)
    elif args.command == "emergency-stop":
        return cmd_emergency_stop(config, args)
    elif args.command == "clear-emergency-stop":
        return cmd_clear_emergency_stop(config, args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
