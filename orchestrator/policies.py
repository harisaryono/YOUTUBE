"""
Orchestrator policy helpers.

This module keeps the control-plane rules for pauses, group scoping, and
quarantine decisions in one place so daemon, doctor, and actions can share the
same semantics.

Quarantine rule:
- A channel quarantine may be global, but it should usually be stage-scoped.
- Example: a transcript/history failure on a channel must not block resume or
  format jobs for the same channel.
"""

from __future__ import annotations

import json
from typing import Any

from .state import OrchestratorState


def stage_to_group(stage: str) -> str:
    stage = str(stage or "").strip().lower()
    if stage == "discovery":
        return "discovery"
    if stage in {"transcript", "audio_download"}:
        return "youtube"
    if stage in {"resume", "asr"}:
        return "provider"
    return "local"


def pause_keys_for_stage(stage: str) -> list[str]:
    stage = str(stage or "").strip().lower()
    group = stage_to_group(stage)
    keys = ["scope:all", f"stage:{stage}", f"group:{group}"]
    if group in {"youtube", "youtube_download", "provider", "local", "discovery"}:
        keys.append(f"scope:{group}")
    return list(dict.fromkeys(keys))


def channel_id_from_scope(scope: str) -> str:
    raw = str(scope or "").strip()
    if raw.startswith("channel:"):
        return raw.split(":", 1)[1].strip()
    return ""


def _read_pause_reason(state: OrchestratorState, key: str) -> str:
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


def _decode_control_payload(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {"reason": text}
    return payload if isinstance(payload, dict) else {}


def _normalize_stage_list(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.replace(";", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        raw_items = [str(item).strip() for item in value]
    else:
        raw_items = [str(value).strip()]
    result: set[str] = set()
    aliases = {
        "history": "discovery",
        "channel_history": "discovery",
        "channel-history": "discovery",
        "youtube_history": "discovery",
        "youtube-history": "discovery",
        "transkrip": "transcript",
        "subtitle": "transcript",
        "audio": "audio_download",
        "download": "audio_download",
        "summarize": "resume",
        "summary": "resume",
        "ringkasan": "resume",
        "formatting": "format",
    }
    known = {"discovery", "transcript", "audio_download", "resume", "asr", "format", "janitor", "import_pending"}
    for item in raw_items:
        key = item.lower().replace(" ", "_").strip()
        if not key:
            continue
        key = aliases.get(key, key)
        if key in known:
            result.add(key)
    return result


def quarantine_stages_from_payload(payload: dict[str, Any]) -> set[str]:
    """Return stages affected by a quarantine payload.

    Empty set means legacy/global channel quarantine. New quarantines should set
    `stages` explicitly so a transcript-specific channel issue does not block
    unrelated provider/local work for that channel.
    """
    stages = set()
    for key in ("stages", "stage", "target_stages", "target_stage", "affected_stages", "quarantine_stages"):
        stages.update(_normalize_stage_list(payload.get(key)))
    if stages:
        return stages

    # Backward-compatible inference for old quarantine records that only stored
    # a human reason. This prevents existing "transcript/transkrip" quarantines
    # from continuing to block resume/format for the same channel.
    reason = str(payload.get("reason") or "").lower()
    inferred: set[str] = set()
    if any(token in reason for token in ("transcript", "transkrip", "subtitle", "caption")):
        inferred.add("transcript")
    if any(token in reason for token in ("audio_download", "audio download", "audio", "yt-dlp")):
        inferred.add("audio_download")
    if any(token in reason for token in ("discovery", "channel history", "full history", "scan-all-missing")):
        inferred.add("discovery")
    if any(token in reason for token in ("resume", "summary", "ringkasan")):
        inferred.add("resume")
    if "asr" in reason:
        inferred.add("asr")
    if "format" in reason:
        inferred.add("format")
    return inferred


def quarantine_applies_to_stage(payload: dict[str, Any], stage: str) -> bool:
    stages = quarantine_stages_from_payload(payload)
    if not stages:
        return True
    return str(stage or "").strip().lower() in stages


def policy_blockers_for_job(
    state: OrchestratorState,
    *,
    stage: str,
    scope: str = "",
) -> list[dict[str, Any]]:
    stage = str(stage or "").strip().lower()
    scope = str(scope or "").strip()
    blockers: list[dict[str, Any]] = []

    for pause_key in pause_keys_for_stage(stage):
        reason = _read_pause_reason(state, pause_key)
        if reason:
            blockers.append(
                {
                    "type": "pause",
                    "key": pause_key,
                    "reason": reason,
                    "message": f"{stage} blocked by {pause_key}: {reason}",
                }
            )

    channel_id = channel_id_from_scope(scope)
    if channel_id and state.is_quarantined_channel(channel_id):
        raw = str(state.get(f"quarantine:channel:{channel_id}", "") or "")
        payload = _decode_control_payload(raw)
        if quarantine_applies_to_stage(payload, stage):
            reason = str(payload.get("reason") or "channel quarantined").strip()
            stages = sorted(quarantine_stages_from_payload(payload))
            stage_note = f" for stage(s) {','.join(stages)}" if stages else ""
            blockers.append(
                {
                    "type": "quarantine",
                    "key": f"quarantine:channel:{channel_id}",
                    "reason": reason,
                    "channel_id": channel_id,
                    "stages": stages,
                    "message": f"channel {channel_id} quarantined{stage_note}: {reason}",
                }
            )

    return blockers


def policy_blockers_summary(
    state: OrchestratorState,
    *,
    stage: str,
    scope: str = "",
) -> list[str]:
    return [str(item.get("message") or "") for item in policy_blockers_for_job(state, stage=stage, scope=scope)]
