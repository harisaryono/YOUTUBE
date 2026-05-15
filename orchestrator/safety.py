"""
Safety Gate — system health checks, emergency stop, and job safety decisions.

Important behavior:
- Provider coordinator status checks are soft-fail tolerant after a previous OK.
  A transient /v1/status/accounts timeout must not block every provider worker.
- Provider lease acquisition remains the worker's responsibility.
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

from .config import DEFAULT_CONFIG_PATH, load_config
from .policies import policy_blockers_for_job
from .state import OrchestratorState


class SystemHealth:
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
    def __init__(self) -> None:
        self.coordinator_available: bool = False
        self.available_leases: int = 0
        self.total_leases: int = 0
        self.blocked_providers: list[str] = []
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.status_source: str = "unknown"

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


class YouTubeHealth:
    def __init__(self) -> None:
        self.global_cooldown_active: bool = False
        self.cooldown_reason: str = ""
        self.cooldown_until: str = ""
        self.consecutive_hard_blocks: int = 0
        self.errors: list[str] = []

    @property
    def ok(self) -> bool:
        return not self.global_cooldown_active and len(self.errors) == 0


class SafetyDecision:
    def __init__(
        self,
        verdict: str = "RUN",
        reason: str = "",
        cooldown_seconds: int = 0,
        recommendation: str = "",
        reason_code: str = "",
    ):
        self.verdict = verdict
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


def _is_idle_hours(config: dict[str, Any]) -> bool:
    idle = config.get("format", {}).get("idle_hours", {}) or {}
    start_str = str(idle.get("start", "22:00"))
    end_str = str(idle.get("end", "05:00"))
    try:
        now = datetime.now(timezone.utc)
        current_minutes = ((now.hour + 7) % 24) * 60 + now.minute
        start_h, start_m = (int(x) for x in start_str.split(":", 1))
        end_h, end_m = (int(x) for x in end_str.split(":", 1))
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m
        if start_minutes <= end_minutes:
            return start_minutes <= current_minutes <= end_minutes
        return current_minutes >= start_minutes or current_minutes <= end_minutes
    except Exception:
        return True


def _soft_coordinator_status_enabled(config: dict[str, Any]) -> bool:
    safety_cfg = config.get("safety", {}) or {}
    return bool(safety_cfg.get("soft_fail_coordinator_status", True) is not False)


def check_system_health(config: dict[str, Any]) -> SystemHealth:
    health = SystemHealth()
    try:
        usage = shutil.disk_usage("/")
        health.disk_free_gb = usage.free / (1024 ** 3)
        min_disk = config.get("system", {}).get("min_free_disk_gb", 5)
        if health.disk_free_gb < min_disk:
            health.errors.append(f"Disk low: {health.disk_free_gb:.1f} GB free (min {min_disk} GB)")
    except Exception as exc:
        health.errors.append(f"Disk check failed: {exc}")

    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    health.mem_available_mb = int(line.split()[1]) / 1024
                elif line.startswith("MemTotal:"):
                    health.mem_total_mb = int(line.split()[1]) / 1024
    except Exception:
        try:
            import psutil  # type: ignore
            vm = psutil.virtual_memory()
            health.mem_available_mb = vm.available / (1024 * 1024)
            health.mem_total_mb = vm.total / (1024 * 1024)
        except Exception:
            health.errors.append("Cannot check memory")

    try:
        with open("/proc/loadavg", encoding="utf-8") as fh:
            parts = fh.read().split()
        if len(parts) >= 3:
            health.load_1min = float(parts[0])
            health.load_5min = float(parts[1])
            health.load_15min = float(parts[2])
    except Exception:
        pass
    return health


def check_provider_health(config: dict[str, Any], state: OrchestratorState) -> ProviderHealth:
    """Check coordinator/provider status without poisoning all workers on one transient failure.

    The coordinator status endpoint is only a preflight hint. Workers still perform
    real lease acquisition. Therefore, if the coordinator was previously OK, a
    single status timeout/error is treated as soft warning, not as global provider
    unavailability. This prevents unrelated transcript/geo-block incidents from
    cascading into provider-wide worker blockage.
    """
    health = ProviderHealth()
    try:
        from local_services import coordinator_status_accounts
        accounts = coordinator_status_accounts(include_inactive=False)
        health.coordinator_available = True
        health.status_source = "coordinator_status_accounts"
        health.total_leases = len(accounts)
        health.available_leases = sum(
            1 for item in accounts
            if item.get("leaseable") is None or item.get("leaseable") is True
        )
        state.set("coordinator_last_ok", "1")
        state.set("coordinator_last_error", "")
    except ImportError:
        try:
            import urllib.request
            coordinator_url = os.getenv("YT_PROVIDER_COORDINATOR_URL", "http://127.0.0.1:8788").rstrip("/")
            req = urllib.request.Request(f"{coordinator_url}/health", method="GET", headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            health.coordinator_available = True
            health.status_source = "health_endpoint"
            health.total_leases = int(data.get("total_accounts", 0) or 0)
            health.available_leases = int(data.get("available_accounts", 0) or 0)
            state.set("coordinator_last_ok", "1")
            state.set("coordinator_last_error", "")
        except Exception as exc:
            if _soft_coordinator_status_enabled(config) and state.get("coordinator_last_ok", "0") == "1":
                health.coordinator_available = True
                health.status_source = "stale_last_ok_after_status_error"
                health.warnings.append(f"Coordinator status soft-failed: {exc}")
                state.set("coordinator_last_error", str(exc)[:1000])
            else:
                health.coordinator_available = False
                health.status_source = "unavailable"
                health.errors.append(f"Coordinator unavailable: {exc}")
                state.set("coordinator_last_ok", "0")
                state.set("coordinator_last_error", str(exc)[:1000])
    except Exception as exc:
        if _soft_coordinator_status_enabled(config) and state.get("coordinator_last_ok", "0") == "1":
            health.coordinator_available = True
            health.status_source = "stale_last_ok_after_status_error"
            health.warnings.append(f"Coordinator status soft-failed: {exc}")
            state.set("coordinator_last_error", str(exc)[:1000])
        else:
            health.coordinator_available = False
            health.status_source = "unavailable"
            health.errors.append(f"Coordinator unavailable: {exc}")
            state.set("coordinator_last_ok", "0")
            state.set("coordinator_last_error", str(exc)[:1000])

    for cd in state.list_active_cooldowns():
        scope = str(cd.get("scope") or "")
        if not scope.startswith("provider:"):
            continue
        provider_name = scope.replace("provider:", "", 1)
        health.blocked_providers.append(provider_name)
        health.errors.append(f"Provider {provider_name} blocked: {cd.get('reason', '')}")
    return health


def check_youtube_health(config: dict[str, Any], state: OrchestratorState) -> YouTubeHealth:
    health = YouTubeHealth()
    cd = state.get_cooldown("youtube")
    if cd is not None:
        health.global_cooldown_active = True
        health.cooldown_reason = cd["reason"]
        health.cooldown_until = cd["cooldown_until"]
        health.errors.append(f"YouTube cooldown: {cd['reason']} until {cd['cooldown_until']}")
    health.consecutive_hard_blocks = state.get_int("youtube_consecutive_hard_blocks", 0)
    return health


def _trim_prefixed_reason(value: str, prefix: str) -> str:
    text = str(value or "").strip()
    while text.startswith(prefix):
        text = text[len(prefix):].strip()
    return text


def safety_gate_for_job(
    job: dict[str, Any],
    config: dict[str, Any],
    sys_health: SystemHealth,
    provider_health: ProviderHealth,
    youtube_health: YouTubeHealth,
    state: OrchestratorState,
) -> SafetyDecision:
    stage = str(job.get("stage", "")).strip().lower()
    scope = str(job.get("scope", "")).strip()

    min_disk = config.get("system", {}).get("min_free_disk_gb", 5)
    if sys_health.disk_free_gb < min_disk:
        return SafetyDecision.wait(
            f"Disk low: {sys_health.disk_free_gb:.1f} GB free",
            cooldown_seconds=1800,
            recommendation="Clean up runs/uploads/logs/cache",
            reason_code="DEFER_DISK_LOW",
        )

    blockers = policy_blockers_for_job(state, stage=stage, scope=scope)
    pause_reason = next((str(x.get("reason") or "").strip() for x in blockers if x.get("type") in {"pause", "quarantine"} and str(x.get("reason") or "").strip()), "")
    if pause_reason:
        return SafetyDecision.wait(pause_reason, cooldown_seconds=0, recommendation="Resume the stage/scope when ready", reason_code="DEFER_STAGE_PAUSED")

    stage_scope = f"stage:{stage}"
    if state.is_cooldown_active(stage_scope):
        cd = state.get_cooldown(stage_scope) or {}
        return SafetyDecision.wait(
            f"Stage cooldown: {_trim_prefixed_reason(str(cd.get('reason') or ''), 'Stage cooldown: ')}",
            cooldown_seconds=300,
            recommendation=str(cd.get("recommendation") or ""),
            reason_code="DEFER_STAGE_COOLDOWN",
        )

    if stage in {"discovery", "transcript", "audio_download"}:
        youtube_cd = state.get_cooldown("youtube")
        if youtube_cd is not None:
            return SafetyDecision.wait(
                f"YouTube cooldown active: {youtube_cd['reason']}",
                cooldown_seconds=3600,
                recommendation="Wait for YouTube cooldown to expire",
                reason_code="DEFER_YOUTUBE_COOLDOWN",
            )
        scoped_youtube = "youtube:discovery" if stage == "discovery" else "youtube:content"
        scoped_cd = state.get_cooldown(scoped_youtube)
        if scoped_cd is not None:
            code = "DEFER_DISCOVERY_COOLDOWN" if stage == "discovery" else "DEFER_YOUTUBE_CONTENT_COOLDOWN"
            return SafetyDecision.wait(
                f"YouTube scoped cooldown active: {scoped_cd['reason']}",
                recommendation=str(scoped_cd.get("recommendation") or ""),
                reason_code=code,
            )
        if scope.startswith("channel:") and state.is_cooldown_active(scope):
            cd = state.get_cooldown(scope) or {}
            return SafetyDecision.wait(
                f"Channel cooldown: {_trim_prefixed_reason(str(cd.get('reason') or ''), 'Channel cooldown: ')}",
                recommendation=str(cd.get("recommendation") or ""),
                reason_code="DEFER_CHANNEL_COOLDOWN",
            )

    if stage == "asr" and config.get("asr", {}).get("require_local_audio", True):
        audio_path = str(job.get("audio_file_path") or "").strip()
        if audio_path and not Path(audio_path).exists():
            return SafetyDecision.wait(
                f"Local audio missing: {audio_path}",
                recommendation="Requeue audio_download for the video",
                reason_code="DEFER_NO_LOCAL_AUDIO",
            )

    if stage in {"asr", "resume", "format"}:
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

        if config.get(stage, {}).get("require_lease", True):
            if not provider_health.coordinator_available:
                return SafetyDecision.wait(
                    "Coordinator unavailable, cannot get provider lease",
                    cooldown_seconds=300,
                    recommendation="Check coordinator service",
                    reason_code="DEFER_PROVIDER_UNAVAILABLE",
                )
            if stage == "asr":
                blocked = set(provider_health.blocked_providers)
                provider_plan = str(config.get("asr", {}).get("provider_plan", "groq_first")).strip().lower()
                if "asr" in blocked:
                    return SafetyDecision.wait("ASR provider service degraded", cooldown_seconds=600, recommendation="Wait for ASR provider recovery", reason_code="DEFER_ASR_PROVIDER_DEGRADED")
                if provider_plan == "nvidia_only" and "nvidia_riva" in blocked:
                    return SafetyDecision.wait("NVIDIA Riva degraded and ASR is nvidia_only", cooldown_seconds=600, recommendation="Switch ASR provider_plan or wait", reason_code="DEFER_NVIDIA_RIVA_DEGRADED")
                if provider_plan == "groq_only" and "groq" in blocked:
                    return SafetyDecision.wait("Groq unavailable and ASR is groq_only", cooldown_seconds=600, recommendation="Switch ASR provider_plan or wait", reason_code="DEFER_GROQ_UNAVAILABLE")
                if provider_plan in {"groq_first", "nvidia_first"} and "nvidia_riva" in blocked and "groq" in blocked:
                    return SafetyDecision.wait("All ASR providers degraded", cooldown_seconds=600, recommendation="Wait for any provider recovery", reason_code="DEFER_ASR_ALL_PROVIDERS_DEGRADED")

    if stage == "format" and config.get("format", {}).get("prefer_idle_hours", False) and not _is_idle_hours(config):
        return SafetyDecision.run()
    return SafetyDecision.run()


def ensure_launch_allowed(config: dict[str, Any], state: OrchestratorState, action: str = "launch_job") -> tuple[bool, list[str]]:
    safety_cfg = config.get("safety", {}) or {}
    if state.is_emergency_stop_active():
        if action == "launch_job" and bool(safety_cfg.get("emergency_stop_blocks_launch", True)):
            return False, ["Emergency stop is active. No new job launches allowed."]
        if action == "retry_queue_drain" and bool(safety_cfg.get("emergency_stop_blocks_retry_drain", True)):
            return False, ["Emergency stop is active. Real retry queue drain is blocked."]
    return True, []


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def cmd_status(config: dict[str, Any], args: argparse.Namespace) -> int:
    state = OrchestratorState()
    try:
        safety_status = state.get_safety_status()
        sys_health = check_system_health(config)
        provider_health = check_provider_health(config, state)
        enriched = dict(safety_status)
        enriched["recent_safety_events"] = state.list_safety_events(limit=args.recent_events or 20)
        enriched["system"] = {
            "disk_free_gb": round(sys_health.disk_free_gb, 1),
            "mem_available_mb": round(sys_health.mem_available_mb, 0),
            "ok": sys_health.ok,
        }
        enriched["provider"] = {
            "coordinator_available": provider_health.coordinator_available,
            "status_source": provider_health.status_source,
            "total_leases": provider_health.total_leases,
            "available_leases": provider_health.available_leases,
            "blocked_providers": provider_health.blocked_providers,
            "warnings": provider_health.warnings,
            "errors": provider_health.errors,
        }
        if args.json:
            _print_json(enriched)
        else:
            es = safety_status.get("emergency_stop", {})
            ro = safety_status.get("readonly", {})
            print("SAFETY STATUS\n")
            print(f"Emergency stop: {'ACTIVE' if es.get('active') else 'inactive'}")
            if es.get("active"):
                print(f"  reason: {es.get('reason', '')}")
                print(f"  actor: {es.get('actor', '')}")
                print(f"  updated_at: {es.get('updated_at', '')}")
            print(f"Readonly: {'ACTIVE' if ro.get('active') else 'inactive'}")
            print(f"System: disk {sys_health.disk_free_gb:.1f} GB, mem {sys_health.mem_available_mb:.0f} MB")
            print(f"Provider coordinator: {'available' if provider_health.coordinator_available else 'unavailable'} ({provider_health.status_source})")
            for warning in provider_health.warnings[:3]:
                print(f"  warning: {warning}")
            events = enriched.get("recent_safety_events", [])
            if events:
                print("\nRecent safety events:")
                for ev in events[:5]:
                    print(f"  - {ev.get('created_at', '')} {ev.get('event_type', '')}: {str(ev.get('message', ''))[:120]}")
        return 0
    finally:
        state.close()


def cmd_emergency_stop(config: dict[str, Any], args: argparse.Namespace) -> int:
    state = OrchestratorState()
    try:
        reason = str(args.reason or "").strip()
        if not reason:
            print("Error: --reason is required for emergency stop", file=sys.stderr)
            return 1
        actor = str(args.actor or "cli").strip()
        state.set_emergency_stop(reason=reason, actor=actor)
        result = {"ok": True, "action": "emergency_stop", "reason": reason, "actor": actor, "safety_status": state.get_safety_status()}
        if args.json:
            _print_json(result)
        else:
            print("Emergency stop activated.")
            print(f"  reason: {reason}")
            print(f"  actor: {actor}")
            print("No new job launches will be allowed. Running jobs are still monitored.")
        return 0
    finally:
        state.close()


def cmd_clear_emergency_stop(config: dict[str, Any], args: argparse.Namespace) -> int:
    state = OrchestratorState()
    try:
        reason = str(args.reason or "").strip()
        if not reason:
            print("Error: --reason is required to clear emergency stop", file=sys.stderr)
            return 1
        actor = str(args.actor or "cli").strip()
        state.clear_emergency_stop(reason=reason, actor=actor)
        result = {"ok": True, "action": "clear_emergency_stop", "reason": reason, "actor": actor, "safety_status": state.get_safety_status()}
        if args.json:
            _print_json(result)
        else:
            print("Emergency stop cleared.")
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
    p.add_argument("--reason", required=True)
    p.add_argument("--actor", default="cli")
    p.add_argument("--json", action="store_true")

    p = subparsers.add_parser("clear-emergency-stop", help="Clear emergency stop")
    p.add_argument("--reason", required=True)
    p.add_argument("--actor", default="cli")
    p.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    if args.command == "status":
        return cmd_status(config, args)
    if args.command == "emergency-stop":
        return cmd_emergency_stop(config, args)
    if args.command == "clear-emergency-stop":
        return cmd_clear_emergency_stop(config, args)
    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
