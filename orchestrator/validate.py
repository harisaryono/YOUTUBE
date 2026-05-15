"""
Orchestrator validation helpers and CLI.

This module validates the control-plane configuration and the stage-context
artifacts used to keep AI changes bounded.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config


PROJECT_ROOT = Path(__file__).resolve().parent.parent
AI_CONTEXT_DIR = PROJECT_ROOT / "AI_CONTEXT"


def validate_config(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    parallel = config.get("parallel", {}) or {}
    groups = parallel.get("groups", {}) or {}
    stages = parallel.get("stages", {}) or {}

    required_groups: dict[str, set[str]] = {
        "discovery": {"discovery"},
        "youtube": {"transcript", "audio_download"},
        "provider": {"resume", "asr"},
        "local": {"format", "janitor", "import_pending"},
    }

    for group_name, required_stages in required_groups.items():
        actual = {str(item).strip().lower() for item in (groups.get(group_name, {}) or {}).get("stages", []) or []}
        missing = sorted(required_stages - actual)
        if missing:
            errors.append(f"group {group_name} missing stages: {', '.join(missing)}")

    for stage in ("discovery", "transcript", "audio_download", "resume", "asr", "format", "janitor"):
        if stage not in stages:
            errors.append(f"missing parallel.stages.{stage}")

    try:
        max_total_jobs = int(parallel.get("max_total_jobs", 0) or 0)
    except (TypeError, ValueError):
        max_total_jobs = 0
    if max_total_jobs < 1:
        errors.append("parallel.max_total_jobs must be >= 1")

    timeouts = config.get("timeouts", {}) or {}
    required_timeouts = (
        "default_seconds",
        "discovery_seconds",
        "transcript_seconds",
        "audio_download_seconds",
        "resume_seconds",
        "asr_seconds",
        "format_seconds",
    )
    for key in required_timeouts:
        try:
            value = int(timeouts.get(key, 0) or 0)
        except (TypeError, ValueError):
            value = 0
        if value < 1:
            errors.append(f"missing or invalid timeouts.{key}")

    return errors


def validate_ai_context() -> list[str]:
    errors: list[str] = []
    required = [
        AI_CONTEXT_DIR / "00_INDEX.md",
        AI_CONTEXT_DIR / "01_REPO_MAP.md",
        AI_CONTEXT_DIR / "02_STAGE_CONTRACTS.md",
        AI_CONTEXT_DIR / "03_ORCHESTRATOR_CONTROL_PLANE.md",
        AI_CONTEXT_DIR / "05_SAFE_PATCH_RULES.md",
        AI_CONTEXT_DIR / "06_STAGE11_SAFE_ACTIONS.md",
        AI_CONTEXT_DIR / "07_STAGE12_POLICY_REQUEUE.md",
    ]
    for path in required:
        if not path.exists():
            errors.append(f"missing AI_CONTEXT file: {path.relative_to(PROJECT_ROOT)}")
    return errors


def run_validation(config_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    config = load_config(path)
    errors = validate_config(config)
    errors.extend(validate_ai_context())
    return {
        "ok": not errors,
        "config_path": str(path),
        "errors": errors,
    }


def format_validation_report(result: dict[str, Any]) -> str:
    lines = [
        f"Config: {result.get('config_path', '')}",
        f"OK: {'yes' if result.get('ok') else 'no'}",
    ]
    errors = list(result.get("errors") or [])
    if errors:
        lines.append("Errors:")
        lines.extend(f"- {err}" for err in errors)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate orchestrator config and AI context")
    parser.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    parser.add_argument("--json", action="store_true", help="Print JSON result")
    args = parser.parse_args(argv)

    result = run_validation(args.config)
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(format_validation_report(result))

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
