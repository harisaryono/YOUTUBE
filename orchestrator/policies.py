"""
Orchestrator policy helpers.

This module keeps the control-plane rules for pauses, group scoping, and
quarantine decisions in one place so daemon, doctor, and actions can share the
same semantics.
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
    if group in {"youtube", "provider", "local", "discovery"}:
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
        payload = {}
        try:
            payload = json.loads(str(state.get(f"quarantine:channel:{channel_id}", "") or "{}"))
        except Exception:
            payload = {}
        reason = str(payload.get("reason") or "channel quarantined").strip()
        blockers.append(
            {
                "type": "quarantine",
                "key": f"quarantine:channel:{channel_id}",
                "reason": reason,
                "channel_id": channel_id,
                "message": f"channel {channel_id} quarantined: {reason}",
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
