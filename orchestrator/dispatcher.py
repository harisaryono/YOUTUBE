"""
Dispatcher — Execute pipeline stages via subprocess.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import uuid
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


def _append_limit(cmd: list[str], limit: Any) -> None:
    """Append --limit only when the limit is positive."""
    try:
        limit_value = int(limit)
    except (TypeError, ValueError):
        return
    if limit_value > 0:
        cmd.extend(["--limit", str(limit_value)])


def _config_audio_dir(config: dict[str, Any]) -> str:
    audio_dir = str(config.get("audio_download", {}).get("audio_dir", "uploads/audio") or "uploads/audio").strip()
    if not audio_dir:
        audio_dir = "uploads/audio"
    path = Path(audio_dir)
    if path.is_absolute():
        return str(path)
    return str((PROJECT_ROOT / path).resolve())


def _parallel_group_for_stage(stage: str) -> str:
    stage = str(stage or "").strip().lower()
    if stage == "discovery":
        return "discovery"
    if stage in {"transcript", "audio_download"}:
        return "youtube"
    if stage in {"resume", "asr"}:
        return "provider"
    return "local"


def _stage_needs_scope_lock(stage: str) -> bool:
    return str(stage or "").strip().lower() in {"transcript", "audio_download"}


def _shell_wrap_command(cmd: list[str], exit_code_path: Path) -> list[str]:
    quoted = " ".join(shlex.quote(str(part)) for part in cmd)
    shell = (
        "set +e;"
        f" {quoted};"
        " code=$?;"
        f" printf '%s\\n' \"$code\" > {shlex.quote(str(exit_code_path))};"
        " exit $code"
    )
    return ["bash", "-lc", shell]


def _build_stage_launch_command(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> tuple[list[str], dict[str, str], Path, Path]:
    stage = str(job.get("stage", "")).strip()
    env = os.environ.copy()

    if stage == "import_pending":
        python = _get_venv_python()
        script = PROJECT_ROOT / "partial_py" / "import_pending_updates.py"
        run_dir = _make_run_dir("import_pending")
        log_path = run_dir / "stdout_stderr.log"
        cmd = [python, str(script)]
        return cmd, env, run_dir, log_path

    if stage == "discovery":
        script = SCRIPTS_DIR / "discover.sh"
        run_dir = _make_run_dir("discovery")
        log_path = run_dir / "stdout_stderr.log"
        channel_id = job.get("channel_id", "") or job.get("channel_identifier", "")
        scan_mode = str(job.get("scan_mode") or "latest_only").strip().lower()
        cmd = [
            "bash", str(script),
            "--rate-limit-safe",
        ]
        if scan_mode == "full_history":
            cmd.append("--scan-all-missing")
        else:
            cmd.extend(["--latest-only", "--recent-per-channel", "50"])
        if channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        return cmd, env, run_dir, log_path

    if stage == "transcript":
        script = SCRIPTS_DIR / "transcript.sh"
        run_dir = _make_run_dir("transcript")
        log_path = run_dir / "stdout_stderr.log"
        workers = config.get("youtube", {}).get("safe_transcript_workers", 2)
        limit = job.get("limit", config.get("youtube", {}).get("batch_limit", 100))
        channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
        cmd = [
            "bash", str(script),
            "--workers", str(workers),
            "--rate-limit-safe",
            "--run-dir", str(run_dir),
        ]
        _append_limit(cmd, limit)
        if channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        return cmd, env, run_dir, log_path

    if stage == "audio_download":
        script = SCRIPTS_DIR / "audio_download.sh"
        if not script.exists():
            script = SCRIPTS_DIR / "audio.sh"
        run_dir = _make_run_dir("audio_download")
        log_path = run_dir / "stdout_stderr.log"
        workers = config.get("audio_download", {}).get("workers", 1)
        limit = job.get("limit", config.get("audio_download", {}).get("batch_limit", 50))
        channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
        env["ASR_AUDIO_DIR"] = _config_audio_dir(config)
        nvidia_model = str(config.get("asr", {}).get("nvidia_model", "") or "").strip()
        if nvidia_model:
            env["ASR_MODEL_NVIDIA_RIVA"] = nvidia_model
        cmd = [
            "bash", str(script),
            "--workers", str(workers),
            "--run-dir", str(run_dir),
        ]
        if config.get("audio_download", {}).get("yt_dlp_rate_limit_safe", True):
            cmd.append("--rate-limit-safe")
        _append_limit(cmd, limit)
        if channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        return cmd, env, run_dir, log_path

    if stage == "resume":
        script = SCRIPTS_DIR / "resume.sh"
        run_dir = _make_run_dir("resume")
        log_path = run_dir / "stdout_stderr.log"
        max_workers = config.get("resume", {}).get("max_workers", 4)
        limit = job.get("limit", config.get("resume", {}).get("batch_limit", 100))
        channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
        cmd = [
            "bash", str(script),
            "--max-workers", str(max_workers),
            "--run-dir", str(run_dir),
        ]
        _append_limit(cmd, limit)
        if channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        if config.get("resume", {}).get("provider_plan", "nvidia_first") == "nvidia_first":
            cmd.append("--nvidia-only")
        return cmd, env, run_dir, log_path

    if stage == "format":
        script = SCRIPTS_DIR / "format.sh"
        run_dir = _make_run_dir("format")
        log_path = run_dir / "stdout_stderr.log"
        max_workers = config.get("format", {}).get("max_workers", 4)
        limit = job.get("limit", config.get("format", {}).get("batch_limit", 500))
        channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
        cmd = [
            "bash", str(script),
            "--workers", str(max_workers),
            "--run-dir", str(run_dir),
        ]
        _append_limit(cmd, limit)
        if channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        return cmd, env, run_dir, log_path

    if stage == "asr":
        script = SCRIPTS_DIR / "asr.sh"
        run_dir = _make_run_dir("asr")
        log_path = run_dir / "stdout_stderr.log"
        limit = job.get("limit", config.get("asr", {}).get("batch_limit", 20))
        channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
        video_id = job.get("video_id", "")
        env["ASR_AUDIO_DIR"] = _config_audio_dir(config)
        groq_model = str(config.get("asr", {}).get("groq_model", "") or "").strip()
        nvidia_model = str(config.get("asr", {}).get("nvidia_model", "") or "").strip()
        if groq_model:
            env["ASR_MODEL_GROQ"] = groq_model
        if nvidia_model:
            env["ASR_MODEL_NVIDIA_RIVA"] = nvidia_model
        cmd = ["bash", str(script), "--local-audio-only"]
        if video_id:
            cmd.extend(["--video-id", str(video_id)])
        elif channel_id:
            cmd.extend(["--channel-id", str(channel_id)])
        _append_limit(cmd, limit)
        if config.get("asr", {}).get("delete_audio_after_success", True):
            cmd.append("--delete-audio-after-success")
        return cmd, env, run_dir, log_path

    raise ValueError(f"Unsupported launch stage: {stage}")


def launch_job(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
    *,
    slot_index: int,
    lock_key: str,
) -> dict[str, Any]:
    """Launch a stage job asynchronously and register it in orchestrator_active_jobs."""
    stage = str(job.get("stage", "")).strip()
    if not stage:
        return {"success": False, "error": "Missing stage"}

    scope_lock_key = ""
    if _stage_needs_scope_lock(stage):
        scope = str(job.get("scope") or "").strip()
        if scope:
            scope_lock_key = f"scope:{scope}"
            if not state.acquire_lock(scope_lock_key, owner=f"pending:{stage}", ttl_seconds=7200):
                return {
                    "success": True,
                    "launched": False,
                    "deferred": True,
                    "reason": f"scope lock busy: {scope_lock_key}",
                    "scope_lock_key": scope_lock_key,
                }

    try:
        cmd, env, run_dir, log_path = _build_stage_launch_command(job, config, state)
    except Exception as e:
        if scope_lock_key:
            state.release_lock(scope_lock_key)
        return {"success": False, "error": f"Failed to build command for {stage}: {e}"}
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    exit_code_path = run_dir / "exit_code.txt"
    shell_cmd = _shell_wrap_command(cmd, exit_code_path)
    command_text = " ".join(shlex.quote(str(part)) for part in cmd)
    job_id = f"{stage}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    env["JOB_ID"] = job_id
    env["JOB_RUN_DIR"] = str(run_dir)
    env["JOB_LOG_PATH"] = str(log_path)
    env["JOB_SOURCE"] = "orchestrator"

    try:
        log_fh = open(log_path, "ab", buffering=0)
    except Exception as e:
        if scope_lock_key:
            state.release_lock(scope_lock_key)
        return {"success": False, "error": f"Failed to open log file: {e}"}

    try:
        process = subprocess.Popen(
            shell_cmd,
            cwd=str(PROJECT_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
    except Exception as e:
        try:
            log_fh.close()
        except Exception:
            pass
        if scope_lock_key:
            state.release_lock(scope_lock_key)
        return {"success": False, "error": f"Failed to launch {stage}: {e}"}
    finally:
        try:
            log_fh.close()
        except Exception:
            pass

    group_name = _parallel_group_for_stage(stage)
    try:
        state.register_active_job(
            job_id=job_id,
            stage=stage,
            scope=str(job.get("scope", "")),
            group_name=group_name,
            slot_index=slot_index,
            lock_key=lock_key,
            pid=process.pid,
            command=command_text,
            run_dir=str(run_dir),
            log_path=str(log_path),
            payload={
                "job": job,
                "exit_code_path": str(exit_code_path),
                "scope_lock_key": scope_lock_key,
            },
        )
    except Exception as e:
        try:
            process.terminate()
        except Exception:
            pass
        if scope_lock_key:
            state.release_lock(scope_lock_key)
        return {"success": False, "error": f"Failed to register active job: {e}"}
    state.add_event(
        event_type="dispatch",
        stage=stage,
        scope=str(job.get("scope", "")),
        message=f"Launched {stage} slot {slot_index} pid={process.pid}",
        severity="info",
        payload={
            "job_id": job_id,
            "pid": process.pid,
            "run_dir": str(run_dir),
            "log_path": str(log_path),
            "command": command_text,
            "slot_index": slot_index,
            "lock_key": lock_key,
        },
    )
    return {
        "success": True,
        "launched": True,
        "started": True,
        "job_id": job_id,
        "pid": process.pid,
        "run_dir": str(run_dir),
        "log_path": str(log_path),
        "slot_index": slot_index,
        "lock_key": lock_key,
        "scope_lock_key": scope_lock_key,
    }


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
    channel_id = job.get("channel_id", "") or job.get("channel_identifier", "")
    scan_mode = str(job.get("scan_mode") or "latest_only").strip().lower()

    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    cmd = [
        "bash", str(script),
        "--rate-limit-safe",
    ]
    if scan_mode == "full_history":
        cmd.append("--scan-all-missing")
    else:
        cmd.extend(["--latest-only", "--recent-per-channel", "50"])
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


def run_audio_download(
    job: dict[str, Any],
    config: dict[str, Any],
    state: OrchestratorState,
) -> dict[str, Any]:
    """Run local audio download for no_subtitle videos."""
    python = _get_venv_python()
    script = SCRIPTS_DIR / "audio_download.sh"

    if not script.exists():
        script = SCRIPTS_DIR / "audio.sh"
    if not script.exists():
        return {"success": False, "error": f"Script not found: {script}"}

    workers = config.get("audio_download", {}).get("workers", 1)
    run_dir = _make_run_dir("audio_download")
    limit = job.get("limit", config.get("audio_download", {}).get("batch_limit", 50))
    rate_limit_safe = config.get("audio_download", {}).get("yt_dlp_rate_limit_safe", True)
    env = os.environ.copy()
    env["ASR_AUDIO_DIR"] = _config_audio_dir(config)
    nvidia_model = str(config.get("asr", {}).get("nvidia_model", "") or "").strip()
    if nvidia_model:
        env["ASR_MODEL_NVIDIA_RIVA"] = nvidia_model

    cmd = [
        "bash", str(script),
        "--workers", str(workers),
        "--run-dir", str(run_dir),
    ]
    if rate_limit_safe:
        cmd.append("--rate-limit-safe")
    _append_limit(cmd, limit)

    channel_id = job.get("channel_identifier", "") or job.get("channel_id", "")
    if channel_id:
        cmd.extend(["--channel-id", channel_id])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600, env=env
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
        return {"success": False, "error": "Audio download timed out (3600s)"}
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

    limit = job.get("limit", config.get("youtube", {}).get("batch_limit", 100))
    cmd = [
        "bash", str(script),
        "--workers", str(workers),
        "--rate-limit-safe",
        "--run-dir", str(run_dir),
    ]
    _append_limit(cmd, limit)
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

    limit = job.get("limit", config.get("resume", {}).get("batch_limit", 0))
    cmd = [
        "bash", str(script),
        "--max-workers", str(max_workers),
        "--run-dir", str(run_dir),
    ]
    _append_limit(cmd, limit)
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

    limit = job.get("limit", config.get("format", {}).get("batch_limit", 500))
    cmd = [
        python, str(script),
        "--workers", str(max_workers),
        "--run-dir", str(run_dir),
    ]
    _append_limit(cmd, limit)

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

    limit = job.get("limit", config.get("asr", {}).get("batch_limit", 20))
    env = os.environ.copy()
    env["ASR_AUDIO_DIR"] = _config_audio_dir(config)
    groq_model = str(config.get("asr", {}).get("groq_model", "") or "").strip()
    nvidia_model = str(config.get("asr", {}).get("nvidia_model", "") or "").strip()
    if groq_model:
        env["ASR_MODEL_GROQ"] = groq_model
    if nvidia_model:
        env["ASR_MODEL_NVIDIA_RIVA"] = nvidia_model

    cmd = ["bash", str(script), "--local-audio-only"]
    if video_id:
        cmd.extend(["--video-id", video_id])
    _append_limit(cmd, limit)
    if config.get("asr", {}).get("delete_audio_after_success", True):
        cmd.append("--delete-audio-after-success")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600, env=env
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
    "audio_download": run_audio_download,
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
