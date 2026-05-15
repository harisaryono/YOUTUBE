#!/bin/bash
# Smoke-test the orchestrator rule set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"

if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    exit 1
fi

cd "$REPO_DIR"

exec "$VENV_PYTHON" - <<'PY'
from orchestrator.config import load_config
from orchestrator.state import OrchestratorState
from orchestrator.safety import (
    SystemHealth,
    ProviderHealth,
    YouTubeHealth,
    check_provider_health,
    check_system_health,
    check_youtube_health,
    safety_gate_for_job,
)

cfg = load_config()
state = OrchestratorState()

try:
    # Scenario 1: YouTube cooldown only blocks YouTube-sensitive stages.
    state.set_cooldown("youtube", "test youtube cooldown", 60)
    sys_h = check_system_health(cfg)
    prov_h = check_provider_health(cfg, state)
    yt_h = check_youtube_health(cfg, state)
    assert safety_gate_for_job({"stage": "transcript", "scope": "youtube"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_YOUTUBE_COOLDOWN"
    assert safety_gate_for_job({"stage": "audio_download", "scope": "youtube"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_YOUTUBE_COOLDOWN"
    assert safety_gate_for_job({"stage": "discovery", "scope": "channel:demo"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_YOUTUBE_COOLDOWN"
    assert safety_gate_for_job({"stage": "asr", "scope": "local:asr"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "resume", "scope": "provider"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "format", "scope": "global"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    state.clear_cooldown("youtube")

    # Scenario 2: low memory should block ASR/Resume/Format but not transcript.
    sys_h = SystemHealth()
    sys_h.disk_free_gb = 100.0
    sys_h.mem_available_mb = 100.0
    prov_h = ProviderHealth()
    prov_h.coordinator_available = True
    prov_h.available_leases = 1
    yt_h = YouTubeHealth()
    assert safety_gate_for_job({"stage": "transcript", "scope": "youtube"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "asr", "scope": "local:asr"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_MEMORY_LOW"
    assert safety_gate_for_job({"stage": "resume", "scope": "provider"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_MEMORY_LOW"
    assert safety_gate_for_job({"stage": "format", "scope": "global"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_MEMORY_LOW"

    # Scenario 3: pause gate should block only the targeted stage.
    state.set_pause("stage:transcript", "test pause")
    sys_h = check_system_health(cfg)
    prov_h = check_provider_health(cfg, state)
    yt_h = check_youtube_health(cfg, state)
    assert safety_gate_for_job({"stage": "transcript", "scope": "youtube"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_STAGE_PAUSED"
    assert safety_gate_for_job({"stage": "audio_download", "scope": "youtube"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    state.clear_pause("stage:transcript")

    # Scenario 4: stage cooldowns must not cross-contaminate unrelated stages.
    state.set_cooldown("stage:resume", "test resume cooldown", 60)
    sys_h = check_system_health(cfg)
    prov_h = check_provider_health(cfg, state)
    yt_h = check_youtube_health(cfg, state)
    assert safety_gate_for_job({"stage": "transcript", "scope": "channel:demo"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "audio_download", "scope": "channel:demo"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "resume", "scope": "channel:demo"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_STAGE_COOLDOWN"
    state.clear_cooldown("stage:resume")

    state.set_cooldown("stage:transcript", "test transcript cooldown", 60)
    sys_h = check_system_health(cfg)
    prov_h = check_provider_health(cfg, state)
    yt_h = check_youtube_health(cfg, state)
    assert safety_gate_for_job({"stage": "resume", "scope": "provider"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "format", "scope": "global"}, cfg, sys_h, prov_h, yt_h, state).verdict == "RUN"
    assert safety_gate_for_job({"stage": "transcript", "scope": "channel:demo"}, cfg, sys_h, prov_h, yt_h, state).reason_code == "DEFER_STAGE_COOLDOWN"

    print("orchestrator rules smoke-test: OK")
finally:
    state.clear_cooldown("youtube")
    state.clear_cooldown("stage:resume")
    state.clear_cooldown("stage:transcript")
    state.clear_pause("stage:transcript")
    state.close()
PY
