"""
Dispatcher — Execute pipeline stages via subprocess.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _get_venv_python() -> str:
    """Get the Python executable from the virtual environment."""
    # Try get_venv.sh first
    get_venv = SCRIPTS_DIR / "get_venv.sh"
    if get_venv.exists():
        try:
            result = subprocess.run(
                ["bash", str(get_venv)],
                capture_output=True, text=True, timeout=10
            )
            python_path = result.stdout.strip()
            if python_path and Path(python_path).exists():
                return python_path
        except Exception:
            pass

    # Fallback: try .venv
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python3"
    if venv_python.exists():
        return str(venv_python)

    # Fallback: try external venv from env
    external = os.getenv("EXTERNAL_VENV_DIR", "")
    if external:
        candidate = Path(external) / "bin" / "python3"
        if candidate.exists():
            return str(candidate)

    # Last resort
    return sys.executable


def _make_run_dir(stage: str) -> Path:
    """Create a run directory for a stage execution."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = PROJECT_ROOT / "runs" / "orchestrator" / f"{stage}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def run_import_pending(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run import_pending_updates.py."""
    python = _get_venv_python()
    script = PROJECT_ROOT / "partial_py" / "import_pending_updates.py"

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    cmd = [python, str(script)]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "stdout": result.stdout[-500:],
            "stderr": result.stderr[-500:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Import timed out (300s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_discovery(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run discovery for a channel."""
    python = _get_venv_python()
    script = SCRIPTS_DIR / "discover.sh"
    channel_id = job.get("channel_id", "")

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    cmd = [
        "bash", str(script),
        "--latest-only",
        "--recent-per-channel", "50",
        "--rate-limit-safe",
    ]
    if channel_id:
        cmd.extend(["--channel-id", channel_id])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1800
        )
        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "stdout": result.stdout[-1000:],
            "stderr": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Discovery timed out (1800s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_transcript(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run transcript download for videos."""
    python = _get_venv_python()
    script = SCRIPTS_DIR / "transcript.sh"
    channel_id = job.get("channel_identifier", "")

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    workers = config.get("youtube", {}).get("safe_transcript_workers", 2)
    run_dir = _make_run_dir("transcript")

    cmd = [
        "bash", str(script),
        "--limit", "20",
        "--workers", str(workers),
        "--rate-limit-safe",
        "--run-dir", str(run_dir),
    ]
    if channel_id:
        cmd.extend(["--channel-id", channel_id])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600
        )
        success = result.returncode == 0

        # Analyze report CSV if exists
        report_csv = run_dir / "recover_report.csv"
        if report_csv.exists():
            from .error_analyzer import analyze_report_csv
            analyze_report_csv(str(report_csv), state)

        return {
            "success": success,
            "returncode": result.returncode,
            "run_dir": str(run_dir),
            "stdout": result.stdout[-1000:],
            "stderr": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Transcript timed out (3600s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_resume(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run resume generation for videos."""
    python = _get_venv_python()
    script = SCRIPTS_DIR / "resume.sh"

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    max_workers = config.get("resume", {}).get("max_workers", 4)
    provider_plan = config.get("resume", {}).get("provider_plan", "nvidia_first")
    run_dir = _make_run_dir("resume")

    cmd = [
        "bash", str(script),
        "--limit", "20",
        "--max-workers", str(max_workers),
        "--run-dir", str(run_dir),
    ]
    if provider_plan == "nvidia_first":
        cmd.append("--nvidia-only")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600
        )
        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "run_dir": str(run_dir),
            "stdout": result.stdout[-1000:],
            "stderr": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Resume timed out (3600s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_format(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run transcript formatting."""
    python = _get_venv_python()
    script = PROJECT_ROOT / "format_transcripts_pool.py"

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    max_workers = config.get("format", {}).get("max_workers", 4)
    run_dir = _make_run_dir("format")

    cmd = [
        python, str(script),
        "--limit", "20",
        "--workers", str(max_workers),
        "--run-dir", str(run_dir),
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600
        )
        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "run_dir": str(run_dir),
            "stdout": result.stdout[-1000:],
            "stderr": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Format timed out (3600s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_asr(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run ASR for a video."""
    python = _get_venv_python()
    script = SCRIPTS_DIR / "asr.sh"
    video_id = job.get("video_id", "")

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    cmd = ["bash", str(script)]
    if video_id:
        cmd.extend(["--video-id", video_id])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600
        )
        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "stdout": result.stdout[-1000:],
            "stderr": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "ASR timed out (3600s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# Dispatch table
DISPATCH_TABLE: dict[str, Any] = {
    "import_pending": run_import_pending,
    "discovery": run_discovery,
    "transcript": run_transcript,
    "resume": run_resume,
    "format": run_format,
    "asr": run_asr,
}


def dispatch_job(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """
    Dispatch a single job to the appropriate runner.
    Returns result dict with success/error info.
    """
    stage = job.get("stage", "")
    runner = DISPATCH_TABLE.get(stage)

    if runner is None:
        return {"success": False, "error": f"No dispatcher for stage: {stage}"}

    state.add_event(
        event_type="dispatch",
        message=f"Dispatching {stage} job",
        stage=stage,
        scope=job.get("scope", ""),
        severity="info",
        payload={"job": job},
    )

    result = runner(job, config, state)

    if result.get("success"):
        state.add_event(
            event_type="dispatch_success",
            message=f"{stage} completed successfully",
            stage=stage,
            scope=job.get("scope", ""),
            severity="info",
        )
    else:
        error_msg = result.get("error", result.get("stderr", "Unknown error"))[:200]
        state.add_event(
            event_type="dispatch_failure",
            message=f"{stage} failed: {error_msg}",
            stage=stage,
            scope=job.get("scope", ""),
            severity="warning",
            recommendation="Check logs for details",
            payload={"result": result},
        )

    return result
