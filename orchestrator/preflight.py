"""
Preflight checks for the orchestrator runtime.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import load_config
from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
YT_DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts.db"


@dataclass
class PreflightResult:
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: dict[str, bool] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)

    def add_error(self, message: str, check: str | None = None) -> None:
        self.ok = False
        self.errors.append(message)
        if check:
            self.checks[check] = False

    def add_warning(self, message: str, check: str | None = None) -> None:
        self.warnings.append(message)
        if check is not None:
            self.checks.setdefault(check, True)

    def mark_ok(self, check: str, detail: Any | None = None) -> None:
        self.checks[check] = True
        if detail is not None:
            self.details[check] = detail


def _check_sqlite_tables(db_path: Path, required_tables: list[str]) -> list[str]:
    if not db_path.exists():
        return [f"Database missing: {db_path}"]

    errors: list[str] = []
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        existing = {str(row["name"]) for row in rows}
        for table in required_tables:
            if table not in existing:
                errors.append(f"Missing table {table} in {db_path.name}")
        conn.close()
    except Exception as exc:
        errors.append(f"Cannot open {db_path}: {exc}")
    return errors


def _check_command_available(command: str) -> bool:
    return shutil.which(command) is not None


def _check_writeable_dir(path: Path) -> tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".preflight_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True, str(path)
    except Exception as exc:
        return False, f"{path}: {exc}"


def run_preflight(
    config: dict[str, Any] | None = None,
    *,
    require_coordinator: bool = False,
    state: OrchestratorState | None = None,
) -> PreflightResult:
    """Run orchestrator preflight checks."""
    cfg = config or load_config()
    result = PreflightResult()
    own_state = state is None
    local_state = state or OrchestratorState()

    try:
        # Virtualenv / python
        venv_python = os.getenv("VIRTUAL_ENV", "")
        result.mark_ok("venv", venv_python or sys.executable)

        # DB paths
        yt_errors = _check_sqlite_tables(
            YT_DB_PATH,
            ["channels", "videos", "video_audio_assets", "video_asr_chunks"],
        )
        if yt_errors:
            for err in yt_errors:
                result.add_error(err, "youtube_db")
        else:
            result.mark_ok("youtube_db", str(YT_DB_PATH))

        # Orchestrator DB
        try:
            local_state._connect()  # noqa: SLF001 - intentional preflight check
            result.mark_ok("orchestrator_db", str(local_state.db_path))
        except Exception as exc:
            result.add_error(f"Cannot open orchestrator DB: {exc}", "orchestrator_db")

        # Scripts
        scripts = {
            "audio_download_script": PROJECT_ROOT / "scripts" / "audio_download.sh",
            "audio_script": PROJECT_ROOT / "scripts" / "audio.sh",
            "asr_script": PROJECT_ROOT / "scripts" / "asr.sh",
        }
        for check_name, script_path in scripts.items():
            if script_path.exists():
                result.mark_ok(check_name, str(script_path))
            else:
                result.add_error(f"Missing script: {script_path}", check_name)

        # ASR local-only support
        asr_script = scripts["asr_script"]
        if asr_script.exists():
            try:
                text = asr_script.read_text(encoding="utf-8", errors="ignore")
                if "--local-audio-only" in text:
                    result.mark_ok("asr_local_only", True)
                else:
                    result.add_error(
                        f"{asr_script} does not advertise --local-audio-only",
                        "asr_local_only",
                    )
            except Exception as exc:
                result.add_error(f"Cannot read {asr_script}: {exc}", "asr_local_only")

        # yt-dlp availability
        yt_dlp_ok = _check_command_available("yt-dlp")
        if not yt_dlp_ok:
            try:
                import yt_dlp  # noqa: F401
                yt_dlp_ok = True
            except Exception:
                yt_dlp_ok = False
        if yt_dlp_ok:
            result.mark_ok("yt_dlp", True)
        else:
            result.add_error("yt-dlp is not available", "yt_dlp")

        # Audio dir writability
        audio_dir_cfg = str(cfg.get("audio_download", {}).get("audio_dir", "uploads/audio") or "uploads/audio")
        audio_dir = Path(audio_dir_cfg)
        if not audio_dir.is_absolute():
            audio_dir = PROJECT_ROOT / audio_dir
        audio_ok, audio_detail = _check_writeable_dir(audio_dir)
        if audio_ok:
            result.mark_ok("audio_dir", audio_detail)
        else:
            result.add_error(f"Audio dir not writable: {audio_detail}", "audio_dir")

        # Coordinator check (optional)
        coordinator_url = os.getenv("YT_PROVIDER_COORDINATOR_URL", "").strip()
        if coordinator_url:
            try:
                import urllib.request

                req = urllib.request.Request(
                    f"{coordinator_url.rstrip('/')}/health",
                    method="GET",
                    headers={"Accept": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if 200 <= getattr(resp, "status", 200) < 300:
                        result.mark_ok("coordinator", coordinator_url)
                    else:
                        msg = f"Coordinator unhealthy: HTTP {getattr(resp, 'status', 'unknown')}"
                        if require_coordinator:
                            result.add_error(msg, "coordinator")
                        else:
                            result.add_warning(msg, "coordinator")
            except Exception as exc:
                msg = f"Coordinator unreachable: {exc}"
                if require_coordinator:
                    result.add_error(msg, "coordinator")
                else:
                    result.add_warning(msg, "coordinator")
        else:
            result.add_warning("YT_PROVIDER_COORDINATOR_URL not set; coordinator check skipped", "coordinator")

        return result
    finally:
        if own_state:
            local_state.close()


def format_preflight(result: PreflightResult) -> str:
    """Render a concise text summary."""
    lines = ["Preflight"]
    lines.append(f"Status: {'OK' if result.ok else 'FAIL'}")
    if result.checks:
        lines.append("Checks:")
        for key, value in sorted(result.checks.items()):
            lines.append(f"  - {key}: {'ok' if value else 'fail'}")
    if result.warnings:
        lines.append("Warnings:")
        for warning in result.warnings:
            lines.append(f"  - {warning}")
    if result.errors:
        lines.append("Errors:")
        for error in result.errors:
            lines.append(f"  - {error}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run orchestrator preflight checks")
    parser.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    parser.add_argument(
        "--require-coordinator",
        action="store_true",
        help="Fail if the coordinator is not reachable",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print preflight result as JSON",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    result = run_preflight(config, require_coordinator=args.require_coordinator)

    if args.json:
        print(json.dumps(
            {
                "ok": result.ok,
                "checks": result.checks,
                "warnings": result.warnings,
                "errors": result.errors,
                "details": result.details,
            },
            indent=2,
            default=str,
        ))
    else:
        print(format_preflight(result))

    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
