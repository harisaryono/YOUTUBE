#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import hmac
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
from urllib import error as urllib_error
from urllib import request as urllib_request

from local_services import (
    DEFAULT_PROVIDERS_DB,
    PROVIDER_MODEL_BLOCKS_TABLE,
    PROVIDER_MODEL_LIMITS_TABLE,
    PROVIDER_MODELS_TABLE,
    ensure_provider_model_limits_table,
    ensure_provider_models_table,
    extract_business_error_code,
    is_provider_blocking_enabled,
    is_transient_provider_limit_error,
    parse_provider_quota_block,
    seed_provider_model_limits,
    service_env,
    set_provider_account_active,
)

# Import encryption module for API key security
try:
    from provider_encryption import decrypt_api_key, encrypt_api_key
except Exception:
    decrypt_api_key = lambda x: x  # type: ignore[assignment]
    encrypt_api_key = lambda x: x  # type: ignore[assignment]


RUNTIME_TABLE = "provider_account_runtime_state"
EVENTS_TABLE = "provider_account_runtime_events"
ADMIN_AUDIT_TABLE = "provider_admin_audit_events"
AUTH_FATAL_PATTERNS = (
    "error code: 401",
    "error code: 403",
    "user not found",
    "invalid api key",
    "invalid_api_key",
    "missing authentication header",
    "authentication failed",
    "authorization token is invalid",
    "authorization token expired",
    "unauthorized",
    "forbidden",
    '"code":"1000"',
    '"code":"1001"',
    '"code":"1002"',
    '"code":"1003"',
    '"code":"1004"',
    '"code":"1110"',
    '"code":"1111"',
    '"code":"1112"',
    '"code":"1113"',
    '"code":"1121"',
)
ADMIN_COOKIE_NAME = "provider_coord_admin"
ADMIN_COOKIE_MAX_AGE = 60 * 60 * 12
# For these providers, "tokens per day" / quota exhaustion is typically account-wide.
# To preserve the "block per provider-model" storage model, we fan-out the block across
# all registered models for the provider (instead of using a special wildcard model).
ACCOUNT_WIDE_QUOTA_PROVIDERS = {"groq", "cerebras", "openrouter"}


def h(text: Any) -> str:
    return html.escape(str(text or ""))


def compare_secret(expected: str, provided: str) -> bool:
    exp = str(expected or "")
    got = str(provided or "")
    return bool(exp) and hmac.compare_digest(exp, got)


def admin_token() -> str:
    return (
        service_env("YT_PROVIDER_COORDINATOR_ADMIN_TOKEN", "")
        or service_env("OPS_DASH_TOKEN", "")
        or service_env("YT_PROVIDER_COORDINATOR_SECRET", "")
    )


def summarize_payload(payload_json: str) -> str:
    raw = str(payload_json or "").strip()
    if not raw:
        return ""
    try:
        obj = json.loads(raw)
    except Exception:
        return raw[:240]
    if isinstance(obj, dict):
        reason = str(obj.get("reason") or "").strip()
        note = str(obj.get("note") or "").strip()
        action = str(((obj.get("decision") or {}) if isinstance(obj.get("decision"), dict) else {}).get("action") or "").strip()
        bits = [part for part in (action, reason, note) if part]
        if bits:
            return " | ".join(bits)[:240]
    return raw[:240]


def friendly_dt(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if dt.tzinfo is None:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def state_badge_class(state: str) -> str:
    key = str(state or "").strip().lower()
    if key in {"in_use", "busy"}:
        return "state-busy"
    if key in {"blocked"}:
        return "state-blocked"
    if key in {"disabled", "inactive"}:
        return "state-disabled"
    if key in {"error"}:
        return "state-error"
    return "state-idle"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utc_now().isoformat()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Provider coordinator HTTP service backed by SQLite.")
    ap.add_argument("--db", default=str(DEFAULT_PROVIDERS_DB))
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8787)
    return ap.parse_args()


def ensure_runtime_tables(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RUNTIME_TABLE} (
                provider_account_id INTEGER NOT NULL,
                provider TEXT NOT NULL,
                model_name TEXT NOT NULL,
                lease_token TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'idle',
                holder TEXT NOT NULL DEFAULT '',
                host TEXT NOT NULL DEFAULT '',
                pid INTEGER NOT NULL DEFAULT 0,
                task_type TEXT NOT NULL DEFAULT '',
                lease_started_at TEXT NOT NULL DEFAULT '',
                lease_expires_at TEXT NOT NULL DEFAULT '',
                last_heartbeat_at TEXT NOT NULL DEFAULT '',
                released_at TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (provider_account_id, model_name)
            )
            """
        )
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_account_id INTEGER NOT NULL,
                provider TEXT NOT NULL,
                model_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                lease_token TEXT NOT NULL DEFAULT '',
                holder TEXT NOT NULL DEFAULT '',
                host TEXT NOT NULL DEFAULT '',
                pid INTEGER NOT NULL DEFAULT 0,
                task_type TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{{}}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{RUNTIME_TABLE}_state ON {RUNTIME_TABLE}(state)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{RUNTIME_TABLE}_lease_expires_at ON {RUNTIME_TABLE}(lease_expires_at)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{EVENTS_TABLE}_acct_model ON {EVENTS_TABLE}(provider_account_id, model_name)"
        )
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ADMIN_AUDIT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ok',
                provider_account_id INTEGER NOT NULL DEFAULT 0,
                provider TEXT NOT NULL DEFAULT '',
                model_name TEXT NOT NULL DEFAULT '',
                actor_addr TEXT NOT NULL DEFAULT '',
                actor_user_agent TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{{}}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{ADMIN_AUDIT_TABLE}_acct_action ON {ADMIN_AUDIT_TABLE}(provider_account_id, action_type)"
        )
        con.commit()
    finally:
        con.close()


def append_event(
    con: sqlite3.Connection,
    *,
    provider_account_id: int,
    provider: str,
    model_name: str,
    event_type: str,
    lease_token: str = "",
    holder: str = "",
    host: str = "",
    pid: int = 0,
    task_type: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    con.execute(
        f"""
        INSERT INTO {EVENTS_TABLE} (
            provider_account_id, provider, model_name, event_type,
            lease_token, holder, host, pid, task_type, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(provider_account_id),
            str(provider).strip(),
            str(model_name).strip(),
            str(event_type).strip(),
            str(lease_token or "").strip(),
            str(holder or "").strip(),
            str(host or "").strip(),
            int(pid or 0),
            str(task_type or "").strip(),
            json.dumps(payload or {}, ensure_ascii=True),
        ),
    )


def append_admin_audit(
    con: sqlite3.Connection,
    *,
    action_type: str,
    status: str = "ok",
    provider_account_id: int = 0,
    provider: str = "",
    model_name: str = "",
    actor_addr: str = "",
    actor_user_agent: str = "",
    message: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    con.execute(
        f"""
        INSERT INTO {ADMIN_AUDIT_TABLE} (
            action_type, status, provider_account_id, provider, model_name,
            actor_addr, actor_user_agent, message, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(action_type or "").strip()[:80],
            str(status or "ok").strip()[:40],
            int(provider_account_id or 0),
            str(provider or "").strip()[:120],
            str(model_name or "").strip()[:200],
            str(actor_addr or "").strip()[:200],
            str(actor_user_agent or "").strip()[:300],
            str(message or "").strip()[:2000],
            json.dumps(payload or {}, ensure_ascii=True),
        ),
    )


def decode_json_bytes(raw: bytes) -> dict[str, Any]:
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def run_provider_connectivity_test(account_row: sqlite3.Row) -> dict[str, Any]:
    provider = str(account_row["provider"] or "").strip()
    model_name = str(account_row["model_name"] or "").strip()
    endpoint_url = str(account_row["endpoint_url"] or "").strip()
    usage_method = str(account_row["usage_method"] or "").strip()
    encrypted_key = str(account_row["api_key"] or "").strip()
    if not endpoint_url:
        return {"ok": False, "message": "endpoint_url kosong"}
    if usage_method != "http_chat":
        return {"ok": False, "message": f"usage_method {usage_method} belum didukung untuk test"}
    if not encrypted_key:
        return {"ok": False, "message": "api_key kosong"}

    api_key = decrypt_api_key(encrypted_key)
    extra_headers_raw = str(account_row["extra_headers_json"] or "{}").strip() or "{}"
    try:
        extra_headers = json.loads(extra_headers_raw)
    except Exception:
        extra_headers = {}
    if not isinstance(extra_headers, dict):
        extra_headers = {}

    payload: dict[str, Any] = {
        "model": model_name,
        "messages": [{"role": "user", "content": "Reply with OK."}],
        "max_tokens": 24,
    }
    # Keep connectivity tests cheap and deterministic.
    if provider.lower() == "z.ai":
        payload["thinking"] = {"type": "disabled"}

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    for key, value in extra_headers.items():
        if key:
            headers[str(key)] = str(value)

    request = urllib_request.Request(
        endpoint_url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib_request.urlopen(request, timeout=25) as resp:
            raw = resp.read()
            body = decode_json_bytes(raw)
            choice0 = ((body.get("choices") or [{}])[0] if isinstance(body.get("choices"), list) and body.get("choices") else {})
            message_obj = choice0.get("message") if isinstance(choice0, dict) else {}
            if not isinstance(message_obj, dict):
                message_obj = {}
            content = str(message_obj.get("content") or "").strip()
            reasoning = str(message_obj.get("reasoning_content") or "").strip()
            finish_reason = str(choice0.get("finish_reason") or "").strip() if isinstance(choice0, dict) else ""
            usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
            excerpt = content or reasoning or str(body.get("error") or "") or "response received"
            return {
                "ok": True,
                "http_status": int(getattr(resp, "status", 200) or 200),
                "finish_reason": finish_reason,
                "excerpt": excerpt[:240],
                "usage": usage,
                "payload": payload,
            }
    except urllib_error.HTTPError as exc:
        raw = exc.read()
        body = decode_json_bytes(raw)
        err = body.get("error") if isinstance(body.get("error"), dict) else {}
        message = str(err.get("message") or body or raw.decode("utf-8", errors="replace") or exc.reason)
        code = str(err.get("code") or extract_business_error_code(message) or "")
        return {
            "ok": False,
            "http_status": int(exc.code or 0),
            "message": message[:400],
            "error_code": code,
            "payload": payload,
        }
    except Exception as exc:
        return {"ok": False, "message": str(exc)[:400], "payload": payload}


def cleanup_expired_leases(con: sqlite3.Connection) -> int:
    now = now_iso()
    rows = con.execute(
        f"""
        SELECT provider_account_id, provider, model_name, lease_token, holder, host, pid, task_type
        FROM {RUNTIME_TABLE}
        WHERE state='in_use'
          AND COALESCE(lease_token,'') <> ''
          AND COALESCE(lease_expires_at,'') <> ''
          AND lease_expires_at < ?
        """,
        (now,),
    ).fetchall()
    if not rows:
        return 0
    con.executemany(
        f"""
        UPDATE {RUNTIME_TABLE}
        SET state='idle',
            lease_token='',
            released_at=?,
            note='lease expired',
            updated_at=?
        WHERE provider_account_id=? AND model_name=?
        """,
        [(now, now, int(r[0]), str(r[2])) for r in rows],
    )
    for r in rows:
        append_event(
            con,
            provider_account_id=int(r[0]),
            provider=str(r[1]),
            model_name=str(r[2]),
            event_type="lease_expired",
            lease_token=str(r[3] or ""),
            holder=str(r[4] or ""),
            host=str(r[5] or ""),
            pid=int(r[6] or 0),
            task_type=str(r[7] or ""),
            payload={"expired_at": now},
        )
    return len(rows)


def is_fatal_auth_error(reason: str) -> bool:
    low = str(reason or "").strip().lower()
    if not low:
        return False
    return any(pattern in low for pattern in AUTH_FATAL_PATTERNS)


def upsert_model_block_record(
    con: sqlite3.Connection,
    *,
    provider_account_id: int,
    provider: str,
    model_name: str,
    blocked_until: str,
    limit_value: int = 0,
    used_value: int = 0,
    requested_value: int = 0,
    reason: str = "",
    source: str = "",
) -> None:
    now = datetime.now().isoformat()
    con.execute(
        f"""
        INSERT INTO {PROVIDER_MODEL_BLOCKS_TABLE} (
            provider_account_id, provider, model_name, blocked_until,
            limit_value, used_value, requested_value, reason, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_account_id, model_name) DO UPDATE SET
            provider=excluded.provider,
            blocked_until=excluded.blocked_until,
            limit_value=excluded.limit_value,
            used_value=excluded.used_value,
            requested_value=excluded.requested_value,
            reason=excluded.reason,
            source=excluded.source,
            updated_at=excluded.updated_at
        """,
        (
            int(provider_account_id),
            str(provider).strip(),
            str(model_name).strip(),
            str(blocked_until).strip(),
            int(limit_value),
            int(used_value),
            int(requested_value),
            str(reason or "")[:2000],
            str(source or "")[:200],
            now,
        ),
    )


def set_provider_account_active_tx(
    con: sqlite3.Connection,
    provider_account_id: int,
    is_active: bool,
    *,
    notes_suffix: str = "",
) -> None:
    if notes_suffix:
        row = con.execute(
            "SELECT notes FROM provider_accounts WHERE id=?",
            (int(provider_account_id),),
        ).fetchone()
        current_notes = str(row[0] or "") if row else ""
        merged_notes = current_notes.strip()
        if notes_suffix not in merged_notes:
            merged_notes = (merged_notes + "\n" + notes_suffix).strip() if merged_notes else notes_suffix
        con.execute(
            "UPDATE provider_accounts SET is_active=?, notes=? WHERE id=?",
            (1 if is_active else 0, merged_notes[:4000], int(provider_account_id)),
        )
    else:
        con.execute(
            "UPDATE provider_accounts SET is_active=? WHERE id=?",
            (1 if is_active else 0, int(provider_account_id)),
        )


def active_models_for_provider(con: sqlite3.Connection, provider: str) -> list[str]:
    con.row_factory = None
    rows = con.execute(
        """
        SELECT model_name
        FROM provider_models
        WHERE provider = ?
          AND COALESCE(is_deprecated, 0) = 0
        ORDER BY model_name ASC
        """,
        (str(provider).strip(),),
    ).fetchall()
    out: list[str] = []
    for r in rows or []:
        name = str(r[0] or "").strip()
        if name:
            out.append(name)
    return out


def cleanup_expired_blocks(con: sqlite3.Connection) -> int:
    now = now_iso()
    rows = con.execute(
        f"""
        SELECT provider_account_id, provider, model_name, blocked_until, reason
        FROM {PROVIDER_MODEL_BLOCKS_TABLE}
        WHERE COALESCE(blocked_until,'') <> ''
          AND datetime(blocked_until) <= datetime(?)
        """,
        (now,),
    ).fetchall()
    if not rows:
        return 0
    con.executemany(
        f"DELETE FROM {PROVIDER_MODEL_BLOCKS_TABLE} WHERE provider_account_id=? AND model_name=?",
        [(int(r[0]), str(r[2])) for r in rows],
    )
    # Keep runtime state consistent by only clearing rows that no longer have any active block.
    con.executemany(
        f"""
        UPDATE {RUNTIME_TABLE} AS rs
        SET state='idle',
            note='block expired',
            updated_at=?
        WHERE rs.provider_account_id=?
          AND rs.state='blocked'
          AND NOT EXISTS (
            SELECT 1
            FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
            WHERE blk.provider_account_id = rs.provider_account_id
              AND datetime(blk.blocked_until) > datetime(?)
              AND blk.model_name = rs.model_name
          )
        """,
        [(now, int(r[0]), now) for r in rows],
    )
    for r in rows:
        append_event(
            con,
            provider_account_id=int(r[0]),
            provider=str(r[1]),
            model_name=str(r[2]),
            event_type="unblocked",
            payload={
                "previous_blocked_until": str(r[3] or ""),
                "reason": str(r[4] or "")[:1000],
            },
        )
    return len(rows)


def sort_key_sql() -> str:
    return """
        CASE
            WHEN pa.account_name LIKE 'nvidia %'
            THEN CAST(substr(pa.account_name, 8, instr(substr(pa.account_name, 8), '|') - 1) AS INT)
            WHEN pa.account_name LIKE 'groq %'
            THEN CAST(substr(pa.account_name, 6, instr(substr(pa.account_name, 6), '|') - 1) AS INT)
            WHEN pa.account_name LIKE 'gemini %'
            THEN CAST(substr(pa.account_name, 8, instr(substr(pa.account_name, 8), '|') - 1) AS INT)
            WHEN pa.account_name LIKE 'z.ai %'
            THEN CAST(substr(pa.account_name, 6, instr(substr(pa.account_name, 6), '|') - 1) AS INT)
            ELSE pa.id
        END,
        pa.id
    """


def acquire_order_sql() -> str:
    return f"""
        CASE WHEN rs.provider_account_id IS NULL THEN 0 ELSE 1 END,
        COALESCE(
            NULLIF(rs.released_at, ''),
            NULLIF(rs.updated_at, ''),
            '1970-01-01T00:00:00+00:00'
        ) ASC,
        {sort_key_sql()}
    """


def acquire_candidates(
    con: sqlite3.Connection,
    *,
    provider: str,
    model_name: str,
    count: int,
    eligible_account_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    where = ["pa.provider = ?", "pa.is_active = 1"]
    params: list[Any] = [str(provider).strip()]
    where.append(
        """
        EXISTS (
            SELECT 1
            FROM provider_models pm
            WHERE pm.provider = pa.provider
              AND pm.model_name = ?
              AND COALESCE(pm.is_deprecated, 0) = 0
        )
        """
    )
    params.append(str(model_name).strip())
    if eligible_account_ids:
        where.append(f"pa.id IN ({','.join('?' for _ in eligible_account_ids)})")
        params.extend(int(x) for x in eligible_account_ids)
    where.append(
        """
        NOT EXISTS (
            SELECT 1
            FROM provider_account_model_blocks blk
            WHERE blk.provider_account_id = pa.id
              AND blk.model_name = ?
              AND datetime(blk.blocked_until) > datetime(?)
        )
        """
    )
    params.extend([str(model_name).strip(), now_iso()])
    sql = f"""
        SELECT pa.id,
               pa.provider,
               pa.account_name,
               pa.model_name,
               pa.api_key,
               pa.endpoint_url,
               pa.usage_method,
               pa.extra_headers_json,
               rs.state,
               rs.lease_token,
               rs.lease_expires_at,
               rs.holder,
               rs.host,
               rs.pid,
               rs.task_type,
               rs.released_at,
               rs.updated_at
        FROM provider_accounts pa
        LEFT JOIN {RUNTIME_TABLE} rs
          ON rs.provider_account_id = pa.id
         AND rs.model_name = ?
        WHERE {' AND '.join(where)}
          AND (
                rs.provider_account_id IS NULL
             OR COALESCE(rs.state, 'idle') <> 'in_use'
             OR COALESCE(rs.lease_token,'') = ''
             OR COALESCE(rs.lease_expires_at,'') = ''
             OR rs.lease_expires_at <= ?
          )
        ORDER BY {acquire_order_sql()}
        LIMIT ?
    """
    params = [str(model_name).strip(), *params, now_iso(), int(count)]
    con.row_factory = sqlite3.Row
    return con.execute(sql, params).fetchall()


def provider_model_limits_payload(
    con: sqlite3.Connection,
    *,
    provider: str,
    model_name: str,
) -> dict[str, Any]:
    row = con.execute(
        f"""
        SELECT provider, model_name,
               context_window_tokens, max_output_tokens,
               recommended_prompt_tokens, recommended_completion_tokens,
               chars_per_token, notes, updated_at
        FROM {PROVIDER_MODEL_LIMITS_TABLE}
        WHERE provider = ? AND model_name = ?
        LIMIT 1
        """,
        (str(provider).strip(), str(model_name).strip()),
    ).fetchone()
    if row is None:
        return {}
    return {
        "provider": str(row["provider"]),
        "model_name": str(row["model_name"]),
        "context_window_tokens": int(row["context_window_tokens"] or 0),
        "max_output_tokens": int(row["max_output_tokens"] or 0),
        "recommended_prompt_tokens": int(row["recommended_prompt_tokens"] or 0),
        "recommended_completion_tokens": int(row["recommended_completion_tokens"] or 0),
        "chars_per_token": float(row["chars_per_token"] or 4.0),
        "notes": str(row["notes"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def lease_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "provider_account_id": int(row["provider_account_id"]),
        "provider": str(row["provider"]),
        "account_name": str(row["account_name"]),
        "model_name": str(row["model_name"]),
        "lease_token": str(row["lease_token"]),
        "state": str(row["state"]),
        "holder": str(row["holder"] or ""),
        "host": str(row["host"] or ""),
        "pid": int(row["pid"] or 0),
        "task_type": str(row["task_type"] or ""),
        "lease_started_at": str(row["lease_started_at"] or ""),
        "lease_expires_at": str(row["lease_expires_at"] or ""),
        "last_heartbeat_at": str(row["last_heartbeat_at"] or ""),
        "released_at": str(row["released_at"] or ""),
        "note": str(row["note"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def lease_bundle_from_account_row(
    con: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    model_name: str,
    lease_token: str,
    holder: str,
    host: str,
    pid: int,
    task_type: str,
    lease_started_at: str,
    lease_expires_at: str,
) -> dict[str, Any]:
    raw_headers = str(row["extra_headers_json"] or "").strip()
    try:
        extra_headers = json.loads(raw_headers) if raw_headers else {}
    except Exception:
        extra_headers = {}
    if not isinstance(extra_headers, dict):
        extra_headers = {}
    api_key = decrypt_api_key(str(row["api_key"] or "").strip())
    model_limits = provider_model_limits_payload(
        con,
        provider=str(row["provider"]),
        model_name=str(model_name),
    )
    return {
        "provider_account_id": int(row["id"]),
        "provider": str(row["provider"]),
        "account_name": str(row["account_name"]),
        "model_name": str(model_name),
        "usage_method": str(row["usage_method"] or ""),
        "endpoint_url": str(row["endpoint_url"] or ""),
        "extra_headers": {str(k): str(v) for k, v in extra_headers.items()},
        "api_key": str(api_key or ""),
        "lease_token": str(lease_token),
        "state": "in_use",
        "holder": str(holder or ""),
        "host": str(host or ""),
        "pid": int(pid or 0),
        "task_type": str(task_type or ""),
        "lease_started_at": str(lease_started_at),
        "lease_expires_at": str(lease_expires_at),
        "model_limits": model_limits,
    }


def ensure_account_model_links(
    con: sqlite3.Connection,
    *,
    provider_account_id: int,
    provider: str,
    model_name: str,
) -> None:
    con.execute(
        f"""
        INSERT INTO {PROVIDER_MODELS_TABLE} (
            provider, model_name, capability, is_deprecated, notes
        ) VALUES (?, ?, 'chat', 0, '')
        ON CONFLICT(provider, model_name) DO NOTHING
        """,
        (str(provider).strip(), str(model_name).strip()),
    )


def build_admin_snapshot(
    con: sqlite3.Connection,
    *,
    selected_account_id: int = 0,
    search_query: str = "",
    provider_filter: str = "",
    state_filter: str = "",
    event_query: str = "",
) -> dict[str, Any]:
    now = now_iso()
    con.row_factory = sqlite3.Row
    search_query = str(search_query or "").strip().lower()
    provider_filter = str(provider_filter or "").strip().lower()
    state_filter = str(state_filter or "").strip().lower()
    event_query = str(event_query or "").strip().lower()
    account_where = ["1=1"]
    account_params: list[Any] = []
    if provider_filter:
        account_where.append("lower(pa.provider)=?")
        account_params.append(provider_filter)
    if search_query:
        like_value = f"%{search_query}%"
        account_where.append(
            "(lower(pa.provider) LIKE ? OR lower(pa.account_name) LIKE ? OR lower(pa.model_name) LIKE ? OR lower(pa.endpoint_url) LIKE ? OR lower(pa.notes) LIKE ?)"
        )
        account_params.extend([like_value, like_value, like_value, like_value, like_value])
    accounts_rows = con.execute(
        f"""
        WITH last_events AS (
            SELECT e.provider_account_id, e.event_type, e.created_at, e.payload_json
            FROM {EVENTS_TABLE} e
            INNER JOIN (
                SELECT provider_account_id, MAX(id) AS max_id
                FROM {EVENTS_TABLE}
                GROUP BY provider_account_id
            ) x ON x.max_id = e.id
        )
        SELECT
            pa.id AS provider_account_id,
            pa.provider,
            pa.account_name,
            pa.model_name,
            pa.endpoint_url,
            pa.usage_method,
            pa.is_active,
            pa.notes,
            pa.updated_at,
            pa.extra_headers_json,
            CASE
                WHEN pa.is_active = 0 THEN 'disabled'
                WHEN EXISTS (
                    SELECT 1
                    FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
                    WHERE blk.provider_account_id = pa.id
                      AND datetime(blk.blocked_until) > datetime(?)
                ) THEN 'blocked'
                WHEN EXISTS (
                    SELECT 1
                    FROM {RUNTIME_TABLE} rs
                    WHERE rs.provider_account_id = pa.id
                      AND rs.state = 'in_use'
                      AND COALESCE(rs.lease_token,'') <> ''
                      AND COALESCE(rs.lease_expires_at,'') > ?
                ) THEN 'in_use'
                ELSE 'idle'
            END AS admin_state,
            (
                SELECT COUNT(*)
                FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
                WHERE blk.provider_account_id = pa.id
                  AND datetime(blk.blocked_until) > datetime(?)
            ) AS active_block_count,
            (
                SELECT COUNT(*)
                FROM {RUNTIME_TABLE} rs
                WHERE rs.provider_account_id = pa.id
                  AND rs.state = 'in_use'
                  AND COALESCE(rs.lease_token,'') <> ''
                  AND COALESCE(rs.lease_expires_at,'') > ?
            ) AS active_lease_count,
            (
                SELECT blk.blocked_until
                FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
                WHERE blk.provider_account_id = pa.id
                  AND datetime(blk.blocked_until) > datetime(?)
                ORDER BY blk.blocked_until ASC
                LIMIT 1
            ) AS blocked_until,
            (
                SELECT blk.reason
                FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
                WHERE blk.provider_account_id = pa.id
                  AND datetime(blk.blocked_until) > datetime(?)
                ORDER BY blk.blocked_until ASC
                LIMIT 1
            ) AS block_reason,
            le.event_type AS last_event_type,
            le.created_at AS last_event_at,
            le.payload_json AS last_event_payload
        FROM provider_accounts pa
        LEFT JOIN last_events le
          ON le.provider_account_id = pa.id
        WHERE {' AND '.join(account_where)}
        ORDER BY {sort_key_sql()}
        """,
        (now, now, now, now, now, now, *account_params),
    ).fetchall()

    accounts = []
    for row in accounts_rows:
        accounts.append(
            {
                "provider_account_id": int(row["provider_account_id"]),
                "provider": str(row["provider"]),
                "account_name": str(row["account_name"]),
                "model_name": str(row["model_name"]),
                "endpoint_url": str(row["endpoint_url"]),
                "usage_method": str(row["usage_method"]),
                "is_active": int(row["is_active"] or 0),
                "notes": str(row["notes"] or ""),
                "updated_at": str(row["updated_at"] or ""),
                "extra_headers_json": str(row["extra_headers_json"] or "{}"),
                "admin_state": str(row["admin_state"] or "idle"),
                "active_block_count": int(row["active_block_count"] or 0),
                "active_lease_count": int(row["active_lease_count"] or 0),
                "blocked_until": str(row["blocked_until"] or ""),
                "block_reason": str(row["block_reason"] or ""),
                "last_event_type": str(row["last_event_type"] or ""),
                "last_event_at": str(row["last_event_at"] or ""),
                "last_event_payload": str(row["last_event_payload"] or ""),
            }
        )

    if state_filter:
        accounts = [item for item in accounts if item["admin_state"] == state_filter]

    visible_account_ids = [item["provider_account_id"] for item in accounts]
    account_id_clause = ""
    account_id_params: list[Any] = []
    if visible_account_ids:
        account_id_clause = f" AND pa.id IN ({','.join('?' for _ in visible_account_ids)})"
        account_id_params = [int(item) for item in visible_account_ids]
    elif search_query or provider_filter or state_filter:
        account_id_clause = " AND 1=0"

    blocks_rows = con.execute(
        f"""
        SELECT
            blk.provider_account_id,
            blk.provider,
            pa.account_name,
            blk.model_name,
            blk.blocked_until,
            blk.reason,
            blk.source,
            blk.updated_at
        FROM {PROVIDER_MODEL_BLOCKS_TABLE} blk
        INNER JOIN provider_accounts pa
          ON pa.id = blk.provider_account_id
        WHERE datetime(blk.blocked_until) > datetime(?)
          {account_id_clause}
        ORDER BY blk.blocked_until ASC, {sort_key_sql()}
        """,
        (now, *account_id_params),
    ).fetchall()
    blocks = [
        {
            "provider_account_id": int(row["provider_account_id"]),
            "provider": str(row["provider"]),
            "account_name": str(row["account_name"]),
            "model_name": str(row["model_name"]),
            "blocked_until": str(row["blocked_until"] or ""),
            "reason": str(row["reason"] or ""),
            "source": str(row["source"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in blocks_rows
    ]

    leases_rows = con.execute(
        f"""
        SELECT
            rs.provider_account_id,
            rs.provider,
            pa.account_name,
            rs.model_name,
            rs.state,
            rs.lease_token,
            rs.holder,
            rs.host,
            rs.pid,
            rs.task_type,
            rs.lease_started_at,
            rs.lease_expires_at,
            rs.last_heartbeat_at,
            rs.note,
            rs.updated_at
        FROM {RUNTIME_TABLE} rs
        INNER JOIN provider_accounts pa
          ON pa.id = rs.provider_account_id
        WHERE (
               rs.state <> 'idle'
            OR COALESCE(rs.lease_token,'') <> ''
            OR COALESCE(rs.note,'') <> ''
        )
          {account_id_clause.replace('pa.id', 'rs.provider_account_id')}
        ORDER BY
            CASE rs.state
                WHEN 'in_use' THEN 0
                WHEN 'blocked' THEN 1
                WHEN 'disabled' THEN 2
                ELSE 3
            END,
            rs.updated_at DESC
        """,
        tuple(account_id_params),
    ).fetchall()
    leases = [
        {
            "provider_account_id": int(row["provider_account_id"]),
            "provider": str(row["provider"]),
            "account_name": str(row["account_name"]),
            "model_name": str(row["model_name"]),
            "state": str(row["state"] or ""),
            "lease_token": str(row["lease_token"] or ""),
            "holder": str(row["holder"] or ""),
            "host": str(row["host"] or ""),
            "pid": int(row["pid"] or 0),
            "task_type": str(row["task_type"] or ""),
            "lease_started_at": str(row["lease_started_at"] or ""),
            "lease_expires_at": str(row["lease_expires_at"] or ""),
            "last_heartbeat_at": str(row["last_heartbeat_at"] or ""),
            "note": str(row["note"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in leases_rows
    ]

    events_rows = con.execute(
        f"""
        SELECT
            e.id,
            e.provider_account_id,
            e.provider,
            pa.account_name,
            e.model_name,
            e.event_type,
            e.lease_token,
            e.holder,
            e.host,
            e.pid,
            e.task_type,
            e.payload_json,
            e.created_at
        FROM {EVENTS_TABLE} e
        LEFT JOIN provider_accounts pa
          ON pa.id = e.provider_account_id
        WHERE 1=1
          {account_id_clause.replace('pa.id', 'e.provider_account_id')}
        ORDER BY e.id DESC
        LIMIT 200
        """,
        tuple(account_id_params),
    ).fetchall()
    events = [
        {
            "id": int(row["id"]),
            "provider_account_id": int(row["provider_account_id"]),
            "provider": str(row["provider"]),
            "account_name": str(row["account_name"] or ""),
            "model_name": str(row["model_name"]),
            "event_type": str(row["event_type"]),
            "lease_token": str(row["lease_token"] or ""),
            "holder": str(row["holder"] or ""),
            "host": str(row["host"] or ""),
            "pid": int(row["pid"] or 0),
            "task_type": str(row["task_type"] or ""),
            "payload_json": str(row["payload_json"] or ""),
            "created_at": str(row["created_at"] or ""),
        }
        for row in events_rows
    ]
    if event_query:
        events = [
            row
            for row in events
            if event_query in str(row["provider"]).lower()
            or event_query in str(row["account_name"]).lower()
            or event_query in str(row["model_name"]).lower()
            or event_query in str(row["event_type"]).lower()
            or event_query in str(row["payload_json"]).lower()
        ]
    events = events[:80]

    audit_rows = con.execute(
        f"""
        SELECT id, action_type, status, provider_account_id, provider, model_name,
               actor_addr, actor_user_agent, message, payload_json, created_at
        FROM {ADMIN_AUDIT_TABLE}
        ORDER BY id DESC
        LIMIT 120
        """
    ).fetchall()
    audit_logs = [
        {
            "id": int(row["id"]),
            "action_type": str(row["action_type"] or ""),
            "status": str(row["status"] or ""),
            "provider_account_id": int(row["provider_account_id"] or 0),
            "provider": str(row["provider"] or ""),
            "model_name": str(row["model_name"] or ""),
            "actor_addr": str(row["actor_addr"] or ""),
            "actor_user_agent": str(row["actor_user_agent"] or ""),
            "message": str(row["message"] or ""),
            "payload_json": str(row["payload_json"] or ""),
            "created_at": str(row["created_at"] or ""),
        }
        for row in audit_rows
    ]
    if visible_account_ids:
        visible_account_id_set = set(visible_account_ids)
        audit_logs = [
            row
            for row in audit_logs
            if not row["provider_account_id"] or row["provider_account_id"] in visible_account_id_set
        ]
    elif search_query or provider_filter or state_filter:
        audit_logs = []
    if event_query:
        audit_logs = [
            row
            for row in audit_logs
            if event_query in str(row["action_type"]).lower()
            or event_query in str(row["provider"]).lower()
            or event_query in str(row["model_name"]).lower()
            or event_query in str(row["message"]).lower()
            or event_query in str(row["payload_json"]).lower()
        ]
    audit_logs = audit_logs[:60]

    account_map = {item["provider_account_id"]: item for item in accounts}
    selected_account = account_map.get(int(selected_account_id or 0))
    model_rows: list[dict[str, Any]] = []
    if selected_account is not None:
        selected_provider = str(selected_account.get("provider") or "").strip()
        selected_default_model = str(selected_account.get("default_model_name") or "").strip()
        model_detail_rows = con.execute(
            f"""
            WITH model_set AS (
                SELECT model_name FROM provider_models WHERE provider = ?
                UNION
                SELECT model_name FROM {RUNTIME_TABLE} WHERE provider_account_id = ?
                UNION
                SELECT model_name FROM {PROVIDER_MODEL_BLOCKS_TABLE} WHERE provider_account_id = ?
                UNION
                SELECT model_name FROM provider_accounts WHERE id = ?
            ),
            last_model_events AS (
                SELECT e.provider_account_id, e.model_name, e.event_type, e.created_at, e.payload_json
                FROM {EVENTS_TABLE} e
                INNER JOIN (
                    SELECT provider_account_id, model_name, MAX(id) AS max_id
                    FROM {EVENTS_TABLE}
                    WHERE provider_account_id = ?
                    GROUP BY provider_account_id, model_name
                ) x ON x.max_id = e.id
            )
            SELECT
                m.model_name,
                CASE WHEN m.model_name = ? THEN 1 ELSE 0 END AS is_default,
                COALESCE(pm.is_deprecated, 0) AS is_deprecated,
                rs.state,
                rs.lease_token,
                rs.holder,
                rs.host,
                rs.pid,
                rs.task_type,
                rs.lease_started_at,
                rs.lease_expires_at,
                rs.last_heartbeat_at,
                rs.note,
                blk.blocked_until,
                blk.reason AS block_reason,
                le.event_type AS last_event_type,
                le.created_at AS last_event_at,
                le.payload_json AS last_event_payload
            FROM model_set m
            LEFT JOIN provider_models pm
              ON pm.provider = ?
             AND pm.model_name = m.model_name
            LEFT JOIN {RUNTIME_TABLE} rs
              ON rs.provider_account_id = ?
             AND rs.model_name = m.model_name
            LEFT JOIN {PROVIDER_MODEL_BLOCKS_TABLE} blk
              ON blk.provider_account_id = ?
             AND blk.model_name = m.model_name
             AND datetime(blk.blocked_until) > datetime(?)
            LEFT JOIN last_model_events le
              ON le.provider_account_id = ?
             AND le.model_name = m.model_name
            ORDER BY CASE WHEN m.model_name = ? THEN 1 ELSE 0 END DESC, m.model_name ASC
            """,
            (
                selected_provider,
                int(selected_account_id),
                int(selected_account_id),
                int(selected_account_id),
                int(selected_account_id),
                selected_default_model,
                selected_provider,
                int(selected_account_id),
                int(selected_account_id),
                now,
                int(selected_account_id),
                selected_default_model,
            ),
        ).fetchall()
        model_rows = [
            {
                "model_name": str(row["model_name"]),
                "is_default": int(row["is_default"] or 0),
                "is_deprecated": int(row["is_deprecated"] or 0),
                "state": str(row["state"] or "idle"),
                "lease_token": str(row["lease_token"] or ""),
                "holder": str(row["holder"] or ""),
                "host": str(row["host"] or ""),
                "pid": int(row["pid"] or 0),
                "task_type": str(row["task_type"] or ""),
                "lease_started_at": str(row["lease_started_at"] or ""),
                "lease_expires_at": str(row["lease_expires_at"] or ""),
                "last_heartbeat_at": str(row["last_heartbeat_at"] or ""),
                "note": str(row["note"] or ""),
                "blocked_until": str(row["blocked_until"] or ""),
                "block_reason": str(row["block_reason"] or ""),
                "last_event_type": str(row["last_event_type"] or ""),
                "last_event_at": str(row["last_event_at"] or ""),
                "last_event_payload": str(row["last_event_payload"] or ""),
            }
            for row in model_detail_rows
        ]

    summary = {
        "total_accounts": len(accounts),
        "active_accounts": sum(1 for item in accounts if item["is_active"]),
        "inactive_accounts": sum(1 for item in accounts if not item["is_active"]),
        "blocked_accounts": sum(1 for item in accounts if item["active_block_count"] > 0),
        "leased_accounts": sum(1 for item in accounts if item["active_lease_count"] > 0),
        "event_count": len(events),
        "audit_count": len(audit_logs),
    }
    return {
        "summary": summary,
        "accounts": accounts,
        "blocks": blocks,
        "leases": leases,
        "events": events,
        "audit_logs": audit_logs,
        "selected_account": selected_account,
        "selected_model_rows": model_rows,
        "filters": {
            "search_query": search_query,
            "provider_filter": provider_filter,
            "state_filter": state_filter,
            "event_query": event_query,
        },
    }


def render_admin_login(error: str = "") -> str:
    error_html = f"<div class='banner banner-error'>{h(error)}</div>" if error else ""
    token_hint = "YT_PROVIDER_COORDINATOR_ADMIN_TOKEN / OPS_DASH_TOKEN"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Coordinator Admin Login</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --panel: #fffdf8;
      --ink: #1f2a2e;
      --muted: #6a7478;
      --accent: #0b6e4f;
      --danger: #a22c29;
      --line: #d7d1c7;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(11,110,79,0.14), transparent 28rem),
        linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
      display: grid;
      place-items: center;
      padding: 2rem;
    }}
    .card {{
      width: min(28rem, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 1.5rem;
      box-shadow: 0 16px 40px rgba(34, 36, 38, 0.08);
    }}
    h1 {{ margin: 0 0 0.5rem; font-size: 1.5rem; }}
    p {{ color: var(--muted); margin: 0 0 1rem; }}
    label {{ display: block; font-size: 0.92rem; margin-bottom: 0.4rem; }}
    input {{
      width: 100%;
      padding: 0.9rem 1rem;
      border: 1px solid var(--line);
      border-radius: 12px;
      font-size: 1rem;
      background: #fff;
    }}
    button {{
      margin-top: 1rem;
      width: 100%;
      border: 0;
      border-radius: 12px;
      padding: 0.95rem 1rem;
      background: var(--accent);
      color: #fff;
      font-size: 1rem;
      font-weight: 700;
      cursor: pointer;
    }}
    .banner {{
      border-radius: 12px;
      padding: 0.8rem 1rem;
      margin-bottom: 1rem;
    }}
    .banner-error {{
      background: rgba(162, 44, 41, 0.10);
      color: var(--danger);
    }}
    .hint {{
      margin-top: 0.9rem;
      font-size: 0.88rem;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <form class="card" method="post" action="/admin/login">
    <h1>Coordinator Admin</h1>
    <p>Masuk untuk mengelola akun provider, block, lease, dan event coordinator.</p>
    {error_html}
    <label for="token">Admin token</label>
    <input id="token" name="token" type="password" autocomplete="current-password" required>
    <button type="submit">Masuk</button>
    <div class="hint">Token dibaca dari env <code>{h(token_hint)}</code>. Jika tidak diatur, fallback ke <code>YT_PROVIDER_COORDINATOR_SECRET</code>.</div>
  </form>
</body>
</html>"""


def render_admin_page(
    snapshot: dict[str, Any],
    *,
    db_path: Path,
    flash: str = "",
    error: str = "",
) -> str:
    summary = snapshot["summary"]
    accounts = snapshot["accounts"]
    blocks = snapshot["blocks"]
    leases = snapshot["leases"]
    events = snapshot["events"]
    audit_logs = snapshot["audit_logs"]
    filters = snapshot["filters"]
    selected_account = snapshot["selected_account"] or {}
    model_rows = snapshot["selected_model_rows"]
    form_values = {
        "provider_account_id": selected_account.get("provider_account_id", 0),
        "provider": selected_account.get("provider", ""),
        "account_name": selected_account.get("account_name", ""),
        "model_name": selected_account.get("model_name", ""),
        "endpoint_url": selected_account.get("endpoint_url", ""),
        "usage_method": selected_account.get("usage_method", "http_chat"),
        "extra_headers_json": selected_account.get("extra_headers_json", "{}"),
        "is_active": int(selected_account.get("is_active", 1 if not selected_account else 0)),
        "notes": selected_account.get("notes", ""),
    }
    flash_html = f"<div class='banner banner-ok'>{h(flash)}</div>" if flash else ""
    error_html = f"<div class='banner banner-error'>{h(error)}</div>" if error else ""

    accounts_rows_html = "\n".join(
        f"""
        <tr>
          <td>{item['provider_account_id']}</td>
          <td><strong>{h(item['provider'])}</strong><div class="muted">{h(item['account_name'])}</div></td>
          <td>{h(item['model_name'])}</td>
          <td><span class="state {state_badge_class(item['admin_state'])}">{h(item['admin_state'])}</span></td>
          <td>{'yes' if item['is_active'] else 'no'}</td>
          <td>{h(friendly_dt(item['blocked_until']))}</td>
          <td>{h(item['last_event_type'])}<div class="muted">{h(friendly_dt(item['last_event_at']))}</div></td>
          <td class="notes">{h(item['notes'] or item['block_reason'])}</td>
          <td>
            <div class="actions">
              <a class="link-button" href="/admin?account_id={item['provider_account_id']}">Edit</a>
              <form method="post" action="/admin/accounts/toggle-active">
                <input type="hidden" name="provider_account_id" value="{item['provider_account_id']}">
                <input type="hidden" name="is_active" value="{0 if item['is_active'] else 1}">
                <button type="submit" class="small {'danger' if item['is_active'] else 'good'}">{'Disable' if item['is_active'] else 'Enable'}</button>
              </form>
              <form method="post" action="/admin/accounts/test">
                <input type="hidden" name="provider_account_id" value="{item['provider_account_id']}">
                <button type="submit" class="small">Test</button>
              </form>
            </div>
          </td>
        </tr>
        """
        for item in accounts
    ) or "<tr><td colspan='9' class='muted'>Tidak ada akun yang cocok dengan filter.</td></tr>"

    blocks_rows_html = "\n".join(
        f"""
        <tr>
          <td>{row['provider_account_id']}</td>
          <td><strong>{h(row['provider'])}</strong><div class="muted">{h(row['account_name'])}</div></td>
          <td>{h(row['model_name'])}</td>
          <td>{h(friendly_dt(row['blocked_until']))}</td>
          <td class="notes">{h(row['reason'])}</td>
          <td>
            <form method="post" action="/admin/blocks/clear">
              <input type="hidden" name="provider_account_id" value="{row['provider_account_id']}">
              <input type="hidden" name="model_name" value="{h(row['model_name'])}">
              <button type="submit" class="small">Clear</button>
            </form>
          </td>
        </tr>
        """
        for row in blocks
    ) or "<tr><td colspan='6' class='muted'>Tidak ada block aktif.</td></tr>"

    leases_rows_html = "\n".join(
        f"""
        <tr>
          <td>{row['provider_account_id']}</td>
          <td><strong>{h(row['provider'])}</strong><div class="muted">{h(row['account_name'])}</div></td>
          <td>{h(row['model_name'])}</td>
          <td><span class="state {state_badge_class(row['state'])}">{h(row['state'])}</span></td>
          <td>{h(row['holder'])}<div class="muted">{h(row['host'])} / {row['pid']}</div></td>
          <td>{h(friendly_dt(row['lease_expires_at']))}</td>
          <td class="notes">{h(row['note'])}</td>
          <td>
            <form method="post" action="/admin/leases/reset">
              <input type="hidden" name="provider_account_id" value="{row['provider_account_id']}">
              <input type="hidden" name="model_name" value="{h(row['model_name'])}">
              <button type="submit" class="small danger">Reset Lease</button>
            </form>
          </td>
        </tr>
        """
        for row in leases
    ) or "<tr><td colspan='8' class='muted'>Tidak ada lease aktif / runtime bermasalah.</td></tr>"

    events_rows_html = "\n".join(
        f"""
        <tr>
          <td>{row['id']}</td>
          <td><strong>{h(row['provider'])}</strong><div class="muted">{h(row['account_name'])}</div></td>
          <td>{h(row['model_name'])}</td>
          <td>{h(row['event_type'])}</td>
          <td>{h(friendly_dt(row['created_at']))}</td>
          <td class="notes">{h(summarize_payload(row['payload_json']))}</td>
        </tr>
        """
        for row in events
    ) or "<tr><td colspan='6' class='muted'>Belum ada event runtime.</td></tr>"

    audit_rows_html = "\n".join(
        f"""
        <tr>
          <td>{row['id']}</td>
          <td>{h(row['action_type'])}<div class="muted">{h(row['status'])}</div></td>
          <td>{row['provider_account_id'] or ''}<div class="muted">{h(row['provider'])} {h(row['model_name'])}</div></td>
          <td>{h(row['actor_addr'])}</td>
          <td>{h(friendly_dt(row['created_at']))}</td>
          <td class="notes">{h(row['message'] or summarize_payload(row['payload_json']))}</td>
        </tr>
        """
        for row in audit_logs
    ) or "<tr><td colspan='6' class='muted'>Belum ada audit admin.</td></tr>"

    model_rows_html = "\n".join(
        f"""
        <tr>
          <td>{h(row['model_name'])}{' <span class="tag">default</span>' if row['is_default'] else ''}</td>
          <td><span class="state {state_badge_class(row['state'])}">{h(row['state'])}</span></td>
          <td>{h(friendly_dt(row['blocked_until']))}</td>
          <td>{h(row['holder'])}<div class="muted">{h(row['task_type'])}</div></td>
          <td>{h(friendly_dt(row['last_event_at']))}<div class="muted">{h(row['last_event_type'])}</div></td>
          <td class="notes">{h(row['block_reason'] or summarize_payload(row['last_event_payload']) or row['note'])}</td>
          <td>
            <div class="actions">
              <form method="post" action="/admin/blocks/clear">
                <input type="hidden" name="provider_account_id" value="{form_values['provider_account_id']}">
                <input type="hidden" name="model_name" value="{h(row['model_name'])}">
                <button type="submit" class="small">Clear Block</button>
              </form>
              <form method="post" action="/admin/leases/reset">
                <input type="hidden" name="provider_account_id" value="{form_values['provider_account_id']}">
                <input type="hidden" name="model_name" value="{h(row['model_name'])}">
                <button type="submit" class="small danger">Reset Lease</button>
              </form>
            </div>
          </td>
        </tr>
        """
        for row in model_rows
    ) or "<tr><td colspan='7' class='muted'>Pilih akun untuk melihat status per model.</td></tr>"

    selected_title = (
        f"Edit akun #{form_values['provider_account_id']}"
        if form_values["provider_account_id"]
        else "Tambah akun provider"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Provider Coordinator Admin</title>
  <style>
    :root {{
      --bg: #efe8de;
      --paper: #fffdf8;
      --ink: #1e2528;
      --muted: #69757a;
      --line: #d5cabc;
      --accent: #0a6c74;
      --good: #0b6e4f;
      --warn: #9a6700;
      --danger: #9b2226;
      --shadow: 0 18px 50px rgba(22, 24, 28, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(10,108,116,0.14), transparent 28rem),
        radial-gradient(circle at top right, rgba(11,110,79,0.10), transparent 22rem),
        linear-gradient(180deg, #f7f1e8 0%, var(--bg) 100%);
    }}
    .shell {{
      width: min(1500px, calc(100% - 2rem));
      margin: 1rem auto 2rem;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: flex-start;
      padding: 1.2rem 1.4rem;
      background: rgba(255, 253, 248, 0.86);
      border: 1px solid rgba(213, 202, 188, 0.92);
      border-radius: 20px;
      backdrop-filter: blur(10px);
      box-shadow: var(--shadow);
    }}
    .topbar h1 {{ margin: 0; font-size: 1.7rem; }}
    .muted {{ color: var(--muted); font-size: 0.88rem; }}
    .grid {{
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      gap: 1rem;
      margin-top: 1rem;
    }}
    .panel {{
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 1rem;
      box-shadow: var(--shadow);
    }}
    .panel h2 {{
      margin: 0 0 0.8rem;
      font-size: 1.08rem;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 0.8rem;
      margin-top: 1rem;
    }}
    .stat {{
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 0.9rem 1rem;
    }}
    .stat strong {{
      display: block;
      font-size: 1.5rem;
      margin-top: 0.25rem;
    }}
    label {{
      display: block;
      margin-bottom: 0.8rem;
      font-size: 0.9rem;
      font-weight: 600;
    }}
    input, textarea, select {{
      width: 100%;
      margin-top: 0.35rem;
      padding: 0.72rem 0.85rem;
      border: 1px solid var(--line);
      border-radius: 12px;
      font: inherit;
      background: #fff;
      color: var(--ink);
    }}
    textarea {{ min-height: 110px; resize: vertical; }}
    .button-row {{
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      margin-top: 1rem;
    }}
    button, .link-button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 0;
      border-radius: 12px;
      padding: 0.68rem 0.95rem;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      text-decoration: none;
      background: var(--accent);
      color: #fff;
    }}
    button.small, .link-button {{
      min-height: 34px;
      padding: 0.45rem 0.72rem;
      font-size: 0.88rem;
    }}
    button.good {{ background: var(--good); }}
    button.danger {{ background: var(--danger); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
    }}
    th, td {{
      text-align: left;
      vertical-align: top;
      padding: 0.7rem 0.55rem;
      border-bottom: 1px solid rgba(213, 202, 188, 0.9);
    }}
    th {{
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }}
    .notes {{
      max-width: 30rem;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .state {{
      display: inline-block;
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .state-idle {{ background: rgba(10,108,116,0.10); color: var(--accent); }}
    .state-busy {{ background: rgba(154,103,0,0.13); color: var(--warn); }}
    .state-blocked {{ background: rgba(155,34,38,0.12); color: var(--danger); }}
    .state-disabled, .state-error {{ background: rgba(64,64,64,0.12); color: #404040; }}
    .tag {{
      display: inline-block;
      margin-left: 0.35rem;
      padding: 0.14rem 0.44rem;
      font-size: 0.72rem;
      border-radius: 999px;
      background: rgba(11,110,79,0.12);
      color: var(--good);
    }}
    .actions {{
      display: flex;
      gap: 0.4rem;
      flex-wrap: wrap;
    }}
    .banner {{
      border-radius: 14px;
      padding: 0.8rem 0.95rem;
      margin-top: 1rem;
      border: 1px solid transparent;
    }}
    .banner-ok {{
      background: rgba(11,110,79,0.10);
      color: var(--good);
      border-color: rgba(11,110,79,0.20);
    }}
    .banner-error {{
      background: rgba(155,34,38,0.10);
      color: var(--danger);
      border-color: rgba(155,34,38,0.20);
    }}
    .subtle {{
      margin-top: 0.5rem;
      font-size: 0.86rem;
      color: var(--muted);
    }}
    .section-stack {{
      display: grid;
      gap: 1rem;
    }}
    .tabs {{
      display: flex;
      gap: 0.4rem;
      margin: 1rem 0 0 0;
      border-bottom: 2px solid var(--line);
      padding-bottom: 0;
    }}
    .tab {{
      border: 0;
      border-radius: 12px 12px 0 0;
      padding: 0.7rem 1.1rem;
      font-size: 0.92rem;
      font-weight: 600;
      color: var(--muted);
      background: rgba(255,255,255,0.5);
      cursor: pointer;
      transition: all 0.15s;
      margin-bottom: -2px;
      border: 2px solid transparent;
    }}
    .tab:hover {{
      background: rgba(255,255,255,0.8);
      color: var(--ink);
    }}
    .tab.active {{
      color: var(--accent);
      background: var(--paper);
      border-color: var(--line);
      border-bottom-color: var(--paper);
    }}
    .tab-content {{
      display: none;
      animation: fadeIn 0.2s ease;
    }}
    .tab-content.active {{
      display: block;
    }}
    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(4px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .tabs {{ flex-wrap: wrap; }}
      .tab {{ flex: 1 1 auto; text-align: center; }}
    }}
    @media (max-width: 640px) {{
      .shell {{ width: min(100% - 1rem, 100%); }}
      .topbar {{ flex-direction: column; }}
      table, thead, tbody, th, td, tr {{ display: block; }}
      thead {{ display: none; }}
      tr {{
        border-bottom: 1px solid rgba(213, 202, 188, 0.9);
        padding: 0.35rem 0;
      }}
      td {{
        border: 0;
        padding: 0.28rem 0;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div>
        <h1>Provider Coordinator Admin</h1>
        <div class="muted">DB: {h(db_path)}</div>
        <div class="subtle">Pantau akun, lease, block, dan error provider tanpa edit SQLite manual.</div>
      </div>
      <form method="post" action="/admin/logout">
        <button type="submit" class="small">Logout</button>
      </form>
    </div>
    {flash_html}
    {error_html}
    <div class="summary">
      <div class="stat"><div class="muted">Total akun</div><strong>{summary['total_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun aktif</div><strong>{summary['active_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun nonaktif</div><strong>{summary['inactive_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun diblock</div><strong>{summary['blocked_accounts']}</strong></div>
      <div class="stat"><div class="muted">Lease aktif</div><strong>{summary['leased_accounts']}</strong></div>
      <div class="stat"><div class="muted">Event dimuat</div><strong>{summary['event_count']}</strong></div>
      <div class="stat"><div class="muted">Audit dimuat</div><strong>{summary['audit_count']}</strong></div>
    </div>

    <div class="tabs">
      <button class="tab active" data-tab="accounts">Accounts</button>
      <button class="tab" data-tab="blocks">Blocks</button>
      <button class="tab" data-tab="leases">Leases</button>
      <button class="tab" data-tab="events">Events</button>
      <button class="tab" data-tab="audit">Audit</button>
    </div>

    <div id="accounts" class="tab-content active">
      <div class="grid">
      <div class="panel">
        <h2>{h(selected_title)}</h2>
        <form method="post" action="/admin/accounts/save">
          <input type="hidden" name="provider_account_id" value="{form_values['provider_account_id']}">
          <label>Provider
            <input name="provider" value="{h(form_values['provider'])}" required>
          </label>
          <label>Account name
            <input name="account_name" value="{h(form_values['account_name'])}" required>
          </label>
          <label>Model default
            <input name="model_name" value="{h(form_values['model_name'])}" required>
          </label>
          <label>Endpoint URL
            <input name="endpoint_url" value="{h(form_values['endpoint_url'])}" required>
          </label>
          <label>Usage method
            <input name="usage_method" value="{h(form_values['usage_method'])}" required>
          </label>
          <label>API key
            <input name="api_key" type="password" value="" {'required' if not form_values['provider_account_id'] else ''}>
          </label>
          <div class="subtle">Kosongkan saat edit bila API key tidak ingin diganti.</div>
          <label>Extra headers JSON
            <textarea name="extra_headers_json">{h(form_values['extra_headers_json'])}</textarea>
          </label>
          <label>Notes
            <textarea name="notes">{h(form_values['notes'])}</textarea>
          </label>
          <label>
            <select name="is_active">
              <option value="1" {'selected' if form_values['is_active'] else ''}>Active</option>
              <option value="0" {'selected' if not form_values['is_active'] else ''}>Inactive</option>
            </select>
          </label>
          <div class="button-row">
            <button type="submit" class="good">Simpan akun</button>
            {'<button type="submit" formaction="/admin/accounts/test">Test koneksi</button>' if form_values['provider_account_id'] else ''}
            <a class="link-button" href="/admin">Reset form</a>
          </div>
        </form>
      </div>
      <div class="section-stack">
        <div class="panel">
          <h2>Filter</h2>
          <form method="get" action="/admin">
            <div class="button-row">
              <label style="flex:2 1 320px;">Cari akun
                <input name="q" value="{h(filters['search_query'])}" placeholder="provider, nama akun, model, endpoint, notes">
              </label>
              <label style="flex:1 1 180px;">Provider
                <input name="provider" value="{h(filters['provider_filter'])}" placeholder="mis. z.ai">
              </label>
              <label style="flex:1 1 160px;">State
                <select name="state">
                  <option value="" {'selected' if not filters['state_filter'] else ''}>Semua</option>
                  <option value="idle" {'selected' if filters['state_filter'] == 'idle' else ''}>idle</option>
                  <option value="in_use" {'selected' if filters['state_filter'] == 'in_use' else ''}>in_use</option>
                  <option value="blocked" {'selected' if filters['state_filter'] == 'blocked' else ''}>blocked</option>
                  <option value="disabled" {'selected' if filters['state_filter'] == 'disabled' else ''}>disabled</option>
                </select>
              </label>
              <label style="flex:2 1 280px;">Cari event/audit
                <input name="event_q" value="{h(filters['event_query'])}" placeholder="event_type, message, payload">
              </label>
            </div>
            {f'<input type="hidden" name="account_id" value="{form_values["provider_account_id"]}">' if form_values['provider_account_id'] else ''}
            <div class="button-row">
              <button type="submit">Terapkan filter</button>
              <a class="link-button" href="/admin">Reset filter</a>
            </div>
          </form>
        </div>
        <div class="panel">
          <h2>Accounts</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Account</th>
                <th>Model</th>
                <th>Status</th>
                <th>Active</th>
                <th>Blocked until</th>
                <th>Last event</th>
                <th>Reason / notes</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>{accounts_rows_html}</tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Status per Model</h2>
          <table>
            <thead>
              <tr>
                <th>Model</th>
                <th>Runtime</th>
                <th>Blocked until</th>
                <th>Holder</th>
                <th>Last event</th>
                <th>Reason / note</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>{model_rows_html}</tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Active Blocks</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Account</th>
                <th>Model</th>
                <th>Blocked until</th>
                <th>Reason</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>{blocks_rows_html}</tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Runtime / Lease</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Account</th>
                <th>Model</th>
                <th>State</th>
                <th>Holder</th>
                <th>Lease expires</th>
                <th>Note</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>{leases_rows_html}</tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Recent Events</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Account</th>
                <th>Model</th>
                <th>Event</th>
                <th>At</th>
                <th>Payload</th>
              </tr>
            </thead>
            <tbody>{events_rows_html}</tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Admin Audit</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Target</th>
                <th>Actor</th>
                <th>At</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>{audit_rows_html}</tbody>
          </table>
        </div>
      </div>
    </div>
    </div> <!-- Close audit tab-content -->

    <script>
      document.addEventListener('DOMContentLoaded', function() {{
        const tabs = document.querySelectorAll('.tab');
        const tabContents = document.querySelectorAll('.tab-content');

        tabs.forEach(tab => {{
          tab.addEventListener('click', function() {{
            const targetTab = this.dataset.tab;

            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));

            this.classList.add('active');
            const targetContent = document.getElementById(targetTab);
            if (targetContent) {{
              targetContent.classList.add('active');
            }}
          }});
        }});

        // Auto-switch to appropriate tab based on URL hash
        if (window.location.hash) {{
          const hash = window.location.hash.substring(1);
          const targetTab = document.querySelector(`.tab[data-tab="${{hash}}"]`);
          const targetContent = document.getElementById(hash);
          if (targetTab && targetContent) {{
            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));
            targetTab.classList.add('active');
            targetContent.classList.add('active');
          }}
        }}
      }});
    </script>
  </div>
</body>
</html>"""


class CoordinatorHandler(BaseHTTPRequestHandler):
    server_version = "ProviderCoordinator/1.0"

    @property
    def db_path(self) -> Path:
        return self.server.db_path  # type: ignore[attr-defined]

    def admin_cookie(self) -> str:
        raw = self.headers.get("Cookie") or ""
        jar = SimpleCookie()
        try:
            jar.load(raw)
        except Exception:
            return ""
        morsel = jar.get(ADMIN_COOKIE_NAME)
        return str(morsel.value).strip() if morsel else ""

    def admin_authenticated(self) -> bool:
        expected = admin_token()
        if not expected:
            return False
        return compare_secret(expected, self.admin_cookie())

    def read_form(self) -> dict[str, str]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b""
        parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        return {key: (values[0] if values else "") for key, values in parsed.items()}

    def html_response(
        self,
        status: int,
        body_text: str,
        *,
        cookies: list[str] | None = None,
    ) -> None:
        body = body_text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for cookie in cookies or []:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.wfile.write(body)

    def redirect(
        self,
        location: str,
        *,
        cookies: list[str] | None = None,
        status: int = HTTPStatus.SEE_OTHER,
    ) -> None:
        self.send_response(int(status))
        self.send_header("Location", location)
        for cookie in cookies or []:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def admin_cookie_header(self, token_value: str) -> str:
        return (
            f"{ADMIN_COOKIE_NAME}={token_value}; Path=/admin; Max-Age={ADMIN_COOKIE_MAX_AGE}; "
            "HttpOnly; SameSite=Lax"
        )

    def expired_admin_cookie_header(self) -> str:
        return f"{ADMIN_COOKIE_NAME}=; Path=/admin; Max-Age=0; HttpOnly; SameSite=Lax"

    def admin_actor_addr(self) -> str:
        forwarded_for = (self.headers.get("X-Forwarded-For") or "").strip()
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()[:200]
        return str(self.client_address[0] if self.client_address else "")[:200]

    def admin_actor_user_agent(self) -> str:
        return str(self.headers.get("User-Agent") or "").strip()[:300]

    def verify_client_secret(self) -> bool:
        """Verify X-Client-Secret header against coordinator secret."""
        expected_secret = service_env("YT_PROVIDER_COORDINATOR_SECRET", "")
        if not expected_secret:
            # Fallback: no auth if secret not set (dev mode)
            return True
        client_secret = (self.headers.get("X-Client-Secret") or "").strip()
        return compare_secret(expected_secret, client_secret)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        stamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{stamp}] {self.address_string()} {format % args}", flush=True)

    def json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"JSON invalid: {exc}") from exc

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/admin/login":
            self.handle_admin_login_page(parse_qs(parsed.query))
            return
        if parsed.path == "/admin":
            self.handle_admin_dashboard(parse_qs(parsed.query))
            return
        if parsed.path == "/health":
            self.json_response(200, {"ok": True, "db": str(self.db_path)})
            return
        if parsed.path.startswith("/v1/accounts/") and parsed.path.endswith("/api-key"):
            self.handle_get_api_key()
            return
        if parsed.path == "/v1/status/accounts":
            self.handle_status_accounts(parse_qs(parsed.query))
            return
        if parsed.path == "/v1/model-limits":
            self.handle_model_limits(parse_qs(parsed.query))
            return
        self.json_response(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/admin/login":
            self.handle_admin_login()
            return
        if self.path == "/admin/logout":
            self.handle_admin_logout()
            return
        if self.path == "/admin/accounts/save":
            self.handle_admin_account_save()
            return
        if self.path == "/admin/accounts/test":
            self.handle_admin_test_account()
            return
        if self.path == "/admin/accounts/toggle-active":
            self.handle_admin_toggle_active()
            return
        if self.path == "/admin/blocks/clear":
            self.handle_admin_clear_block()
            return
        if self.path == "/admin/leases/reset":
            self.handle_admin_reset_lease()
            return
        if self.path == "/v1/leases/acquire":
            self.handle_acquire()
            return
        if self.path == "/v1/leases/heartbeat":
            self.handle_heartbeat()
            return
        if self.path == "/v1/leases/release":
            self.handle_release()
            return
        if self.path == "/v1/blocks/upsert":
            self.handle_block_upsert()
            return
        if self.path == "/v1/provider-events/report":
            self.handle_provider_event_report()
            return
        if self.path == "/v1/accounts/set-active":
            self.handle_set_active()
            return
        self.json_response(404, {"ok": False, "error": "not_found"})

    def handle_admin_login_page(self, qs: dict[str, list[str]]) -> None:
        if self.admin_authenticated():
            self.redirect("/admin")
            return
        error = (qs.get("error") or [""])[0].strip()
        self.html_response(200, render_admin_login(error=error))

    def handle_admin_dashboard(self, qs: dict[str, list[str]]) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        selected_account_id = int(((qs.get("account_id") or ["0"])[0] or "0").strip() or "0")
        search_query = (qs.get("q") or [""])[0].strip()
        provider_filter = (qs.get("provider") or [""])[0].strip()
        state_filter = (qs.get("state") or [""])[0].strip()
        event_query = (qs.get("event_q") or [""])[0].strip()
        flash = (qs.get("flash") or [""])[0].strip()
        error = (qs.get("error") or [""])[0].strip()
        con = sqlite3.connect(str(self.db_path))
        try:
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)
            snapshot = build_admin_snapshot(
                con,
                selected_account_id=selected_account_id,
                search_query=search_query,
                provider_filter=provider_filter,
                state_filter=state_filter,
                event_query=event_query,
            )
            con.commit()
        finally:
            con.close()
        self.html_response(
            200,
            render_admin_page(snapshot, db_path=self.db_path, flash=flash, error=error),
        )

    def handle_admin_login(self) -> None:
        form = self.read_form()
        expected = admin_token()
        provided = str(form.get("token") or "").strip()
        if not expected:
            self.redirect(
                "/admin/login?" + urlencode({"error": "admin token belum diatur di environment"}),
            )
            return
        if not compare_secret(expected, provided):
            self.redirect("/admin/login?" + urlencode({"error": "token tidak valid"}))
            return
        self.redirect("/admin", cookies=[self.admin_cookie_header(expected)])

    def handle_admin_logout(self) -> None:
        self.redirect("/admin/login", cookies=[self.expired_admin_cookie_header()])

    def handle_admin_test_account(self) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        form = self.read_form()
        provider_account_id = int((form.get("provider_account_id") or "0").strip() or "0")
        if provider_account_id <= 0:
            self.redirect("/admin?" + urlencode({"error": "provider_account_id wajib untuk test koneksi"}))
            return
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                """
                SELECT id, provider, account_name, api_key, endpoint_url, model_name, usage_method, extra_headers_json
                FROM provider_accounts
                WHERE id=?
                LIMIT 1
                """,
                (provider_account_id,),
            ).fetchone()
            if row is None:
                self.redirect("/admin?" + urlencode({"error": "provider_account_id tidak ditemukan"}))
                return
            result = run_provider_connectivity_test(row)
            message = (
                f"test koneksi OK: HTTP {result.get('http_status', 200)} | {result.get('excerpt', '')}"
                if result.get("ok")
                else f"test koneksi gagal: HTTP {result.get('http_status', 0)} | {result.get('message', '')}"
            )[:500]
            con.execute("BEGIN IMMEDIATE")
            append_admin_audit(
                con,
                action_type="account_test",
                status="ok" if result.get("ok") else "error",
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=str(row["model_name"]),
                actor_addr=self.admin_actor_addr(),
                actor_user_agent=self.admin_actor_user_agent(),
                message=message,
                payload=result,
            )
            con.commit()
        finally:
            con.close()
        target_key = "flash" if result.get("ok") else "error"
        self.redirect(
            "/admin?" + urlencode({"account_id": str(provider_account_id), target_key: message})
        )

    def handle_admin_account_save(self) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        form = self.read_form()
        provider_account_id = int((form.get("provider_account_id") or "0").strip() or "0")
        provider = str(form.get("provider") or "").strip()
        account_name = str(form.get("account_name") or "").strip()
        model_name = str(form.get("model_name") or "").strip()
        endpoint_url = str(form.get("endpoint_url") or "").strip()
        usage_method = str(form.get("usage_method") or "").strip()
        api_key_input = str(form.get("api_key") or "").strip()
        extra_headers_json = str(form.get("extra_headers_json") or "{}").strip() or "{}"
        notes = str(form.get("notes") or "").strip()
        is_active = str(form.get("is_active") or "1").strip() == "1"
        if not provider or not account_name or not model_name or not endpoint_url or not usage_method:
            target = {"error": "provider/account_name/model_name/endpoint_url/usage_method wajib diisi"}
            if provider_account_id > 0:
                target["account_id"] = str(provider_account_id)
            self.redirect("/admin?" + urlencode(target))
            return
        try:
            extra_headers_obj = json.loads(extra_headers_json)
            if not isinstance(extra_headers_obj, dict):
                raise ValueError("extra_headers_json harus object")
            extra_headers_json = json.dumps(extra_headers_obj, ensure_ascii=False, sort_keys=True)
        except Exception as exc:
            target = {"error": f"extra_headers_json invalid: {exc}"}
            if provider_account_id > 0:
                target["account_id"] = str(provider_account_id)
            self.redirect("/admin?" + urlencode(target))
            return

        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = None
            stored_api_key = ""
            if provider_account_id > 0:
                existing = con.execute(
                    "SELECT * FROM provider_accounts WHERE id=? LIMIT 1",
                    (provider_account_id,),
                ).fetchone()
                if existing is None:
                    con.rollback()
                    self.redirect("/admin?" + urlencode({"error": "provider_account_id tidak ditemukan"}))
                    return
                stored_api_key = str(existing["api_key"] or "")
            if provider_account_id <= 0 and not api_key_input:
                con.rollback()
                self.redirect("/admin?" + urlencode({"error": "api_key wajib diisi untuk akun baru"}))
                return
            final_api_key = stored_api_key
            if api_key_input:
                final_api_key = api_key_input if api_key_input.startswith("ENC:") else encrypt_api_key(api_key_input)

            if provider_account_id > 0:
                con.execute(
                    """
                    UPDATE provider_accounts
                    SET provider=?, account_name=?, api_key=?, endpoint_url=?, model_name=?,
                        usage_method=?, extra_headers_json=?, is_active=?, notes=?
                    WHERE id=?
                    """,
                    (
                        provider,
                        account_name,
                        final_api_key,
                        endpoint_url,
                        model_name,
                        usage_method,
                        extra_headers_json,
                        1 if is_active else 0,
                        notes[:4000],
                        provider_account_id,
                    ),
                )
            else:
                cur = con.execute(
                    """
                    INSERT INTO provider_accounts (
                        provider, account_name, api_key, endpoint_url, model_name,
                        usage_method, extra_headers_json, is_active, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        provider,
                        account_name,
                        final_api_key,
                        endpoint_url,
                        model_name,
                        usage_method,
                        extra_headers_json,
                        1 if is_active else 0,
                        notes[:4000],
                    ),
                )
                provider_account_id = int(cur.lastrowid)

            ensure_account_model_links(
                con,
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
            )
            now = now_iso()
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state=CASE
                        WHEN ? = 0 THEN 'disabled'
                        WHEN state = 'disabled' AND COALESCE(lease_token,'') = '' THEN 'idle'
                        ELSE state
                    END,
                    lease_token=CASE WHEN ? = 0 THEN '' ELSE lease_token END,
                    released_at=CASE WHEN ? = 0 THEN ? ELSE released_at END,
                    note=CASE
                        WHEN ? = 0 THEN 'disabled by admin'
                        WHEN state = 'disabled' AND COALESCE(lease_token,'') = '' THEN 're-enabled by admin'
                        ELSE note
                    END,
                    updated_at=?
                WHERE provider_account_id=?
                """,
                (
                    1 if is_active else 0,
                    1 if is_active else 0,
                    1 if is_active else 0,
                    now,
                    1 if is_active else 0,
                    now,
                    provider_account_id,
                ),
            )
            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
                event_type="admin_account_saved",
                payload={
                    "account_name": account_name,
                    "endpoint_url": endpoint_url,
                    "usage_method": usage_method,
                    "is_active": is_active,
                    "api_key_updated": bool(api_key_input),
                },
            )
            append_admin_audit(
                con,
                action_type="account_save",
                status="ok",
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
                actor_addr=self.admin_actor_addr(),
                actor_user_agent=self.admin_actor_user_agent(),
                message=f"account saved: {account_name}",
                payload={
                    "account_name": account_name,
                    "is_active": is_active,
                    "api_key_updated": bool(api_key_input),
                },
            )
            con.commit()
        except sqlite3.IntegrityError as exc:
            con.rollback()
            self.redirect(
                "/admin?" + urlencode({"account_id": str(provider_account_id), "error": f"gagal simpan akun: {exc}"})
            )
            return
        finally:
            con.close()
        self.redirect(
            "/admin?" + urlencode({"account_id": str(provider_account_id), "flash": "akun berhasil disimpan"})
        )

    def handle_admin_toggle_active(self) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        form = self.read_form()
        provider_account_id = int((form.get("provider_account_id") or "0").strip() or "0")
        is_active = str(form.get("is_active") or "0").strip() == "1"
        if provider_account_id <= 0:
            self.redirect("/admin?" + urlencode({"error": "provider_account_id wajib"}))
            return
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT provider, model_name FROM provider_accounts WHERE id=? LIMIT 1",
                (provider_account_id,),
            ).fetchone()
            if row is None:
                con.rollback()
                self.redirect("/admin?" + urlencode({"error": "provider_account_id tidak ditemukan"}))
                return
            set_provider_account_active_tx(
                con,
                provider_account_id,
                is_active,
                notes_suffix=("enabled by admin" if is_active else "disabled by admin"),
            )
            now = now_iso()
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state=CASE
                        WHEN ? = 1 AND state = 'disabled' THEN 'idle'
                        WHEN ? = 0 THEN 'disabled'
                        ELSE state
                    END,
                    lease_token=CASE WHEN ? = 0 THEN '' ELSE lease_token END,
                    released_at=CASE WHEN ? = 0 THEN ? ELSE released_at END,
                    note=?,
                    updated_at=?
                WHERE provider_account_id=?
                """,
                (
                    1 if is_active else 0,
                    1 if is_active else 0,
                    1 if is_active else 0,
                    1 if is_active else 0,
                    now,
                    "enabled by admin" if is_active else "disabled by admin",
                    now,
                    provider_account_id,
                ),
            )
            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=str(row["model_name"]),
                event_type="admin_set_active",
                payload={"is_active": is_active},
            )
            append_admin_audit(
                con,
                action_type="account_toggle_active",
                status="ok",
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=str(row["model_name"]),
                actor_addr=self.admin_actor_addr(),
                actor_user_agent=self.admin_actor_user_agent(),
                message="account enabled by admin" if is_active else "account disabled by admin",
                payload={"is_active": is_active},
            )
            con.commit()
        finally:
            con.close()
        self.redirect(
            "/admin?" + urlencode({"account_id": str(provider_account_id), "flash": "status akun diperbarui"})
        )

    def handle_admin_clear_block(self) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        form = self.read_form()
        provider_account_id = int((form.get("provider_account_id") or "0").strip() or "0")
        model_name = str(form.get("model_name") or "").strip()
        if provider_account_id <= 0 or not model_name:
            self.redirect("/admin?" + urlencode({"error": "provider_account_id dan model_name wajib"}))
            return
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT provider FROM provider_accounts WHERE id=? LIMIT 1",
                (provider_account_id,),
            ).fetchone()
            if row is None:
                con.rollback()
                self.redirect("/admin?" + urlencode({"error": "provider_account_id tidak ditemukan"}))
                return
            con.execute(
                f"DELETE FROM {PROVIDER_MODEL_BLOCKS_TABLE} WHERE provider_account_id=? AND model_name=?",
                (provider_account_id, model_name),
            )
            now = now_iso()
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state='idle', note='block cleared by admin', updated_at=?
                WHERE provider_account_id=? AND model_name=? AND state='blocked'
                """,
                (now, provider_account_id, model_name),
            )
            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=model_name,
                event_type="admin_unblock",
                payload={"note": "block cleared by admin"},
            )
            append_admin_audit(
                con,
                action_type="block_clear",
                status="ok",
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=model_name,
                actor_addr=self.admin_actor_addr(),
                actor_user_agent=self.admin_actor_user_agent(),
                message=f"block cleared for {model_name}",
            )
            con.commit()
        finally:
            con.close()
        self.redirect(
            "/admin?" + urlencode({"account_id": str(provider_account_id), "flash": f"block {model_name} dibersihkan"})
        )

    def handle_admin_reset_lease(self) -> None:
        if not self.admin_authenticated():
            self.redirect("/admin/login")
            return
        form = self.read_form()
        provider_account_id = int((form.get("provider_account_id") or "0").strip() or "0")
        model_name = str(form.get("model_name") or "").strip()
        if provider_account_id <= 0 or not model_name:
            self.redirect("/admin?" + urlencode({"error": "provider_account_id dan model_name wajib"}))
            return
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                f"""
                SELECT rs.*, pa.provider
                FROM {RUNTIME_TABLE} rs
                INNER JOIN provider_accounts pa ON pa.id = rs.provider_account_id
                WHERE rs.provider_account_id=? AND rs.model_name=?
                LIMIT 1
                """,
                (provider_account_id, model_name),
            ).fetchone()
            if row is None:
                con.rollback()
                self.redirect(
                    "/admin?" + urlencode({"account_id": str(provider_account_id), "error": "runtime state tidak ditemukan"})
                )
                return
            has_block = con.execute(
                f"""
                SELECT 1
                FROM {PROVIDER_MODEL_BLOCKS_TABLE}
                WHERE provider_account_id=? AND model_name=? AND datetime(blocked_until) > datetime(?)
                LIMIT 1
                """,
                (provider_account_id, model_name, now_iso()),
            ).fetchone()
            now = now_iso()
            next_state = "blocked" if has_block else "idle"
            next_note = "lease reset by admin"
            if has_block:
                next_note = "lease reset by admin; block still active"
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state=?, lease_token='', holder='', host='', pid=0, task_type='',
                    released_at=?, note=?, updated_at=?
                WHERE provider_account_id=? AND model_name=?
                """,
                (next_state, now, next_note, now, provider_account_id, model_name),
            )
            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=model_name,
                event_type="admin_lease_reset",
                lease_token=str(row["lease_token"] or ""),
                holder=str(row["holder"] or ""),
                host=str(row["host"] or ""),
                pid=int(row["pid"] or 0),
                task_type=str(row["task_type"] or ""),
                payload={"next_state": next_state},
            )
            append_admin_audit(
                con,
                action_type="lease_reset",
                status="ok",
                provider_account_id=provider_account_id,
                provider=str(row["provider"]),
                model_name=model_name,
                actor_addr=self.admin_actor_addr(),
                actor_user_agent=self.admin_actor_user_agent(),
                message=f"lease reset for {model_name}",
                payload={"next_state": next_state},
            )
            con.commit()
        finally:
            con.close()
        self.redirect(
            "/admin?" + urlencode({"account_id": str(provider_account_id), "flash": f"lease {model_name} direset"})
        )

    def handle_status_accounts(self, qs: dict[str, list[str]]) -> None:
        provider = (qs.get("provider") or [""])[0].strip()
        model_name = (qs.get("model_name") or [""])[0].strip()
        include_inactive = ((qs.get("include_inactive") or ["0"])[0].strip() == "1")
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)
            where = ["1=1"]
            where_params: list[Any] = []
            if provider:
                where.append("pa.provider=?")
                where_params.append(provider)
            if not include_inactive:
                where.append("pa.is_active=1")
            if model_name:
                runtime_join = f"rs.model_name = ?"
                runtime_params = [model_name]
                model_join = """
                    LEFT JOIN provider_models pamq
                      ON pamq.provider = pa.provider
                     AND pamq.model_name = ?
                """
                model_params = [model_name]
                block_join = f"""
                    LEFT JOIN {PROVIDER_MODEL_BLOCKS_TABLE} blk
                      ON blk.provider_account_id = pa.id
                     AND blk.model_name = ?
                     AND datetime(blk.blocked_until) > datetime(?)
                """
                block_params = [model_name, now_iso()]
            else:
                runtime_join = """
                    rs.model_name = COALESCE(
                        (
                            SELECT rs2.model_name
                            FROM provider_account_runtime_state rs2
                            WHERE rs2.provider_account_id = pa.id
                              AND (
                                    rs2.state = 'in_use'
                                 OR COALESCE(rs2.lease_token,'') <> ''
                              )
                            ORDER BY rs2.updated_at DESC, rs2.model_name ASC
                            LIMIT 1
                        ),
                        pa.model_name
                    )
                """
                runtime_params = []
                model_join = """
                    LEFT JOIN provider_models pamq
                      ON pamq.provider = pa.provider
                     AND pamq.model_name = COALESCE(rs.model_name, pa.model_name)
                """
                model_params = []
                block_join = f"""
                    LEFT JOIN {PROVIDER_MODEL_BLOCKS_TABLE} blk
                      ON blk.provider_account_id = pa.id
                     AND blk.model_name = COALESCE(rs.model_name, pa.model_name)
                     AND datetime(blk.blocked_until) > datetime(?)
                """
                block_params = [now_iso()]
            rows = con.execute(
                f"""
                SELECT pa.id AS provider_account_id, pa.provider, pa.account_name, pa.model_name AS default_model_name,
                       pa.is_active, pa.notes,
                       rs.model_name, rs.state, rs.lease_token, rs.holder, rs.host, rs.pid, rs.task_type,
                       rs.lease_started_at, rs.lease_expires_at, rs.last_heartbeat_at, rs.released_at, rs.note, rs.updated_at,
                       pamq.model_name AS queried_model_name, COALESCE(pamq.is_deprecated, 0) AS model_is_deprecated,
                       blk.blocked_until, blk.reason AS block_reason, blk.limit_value, blk.used_value, blk.requested_value
                FROM provider_accounts pa
                LEFT JOIN {RUNTIME_TABLE} rs
                  ON rs.provider_account_id = pa.id
                 AND {runtime_join}
                {model_join}
                {block_join}
                WHERE {' AND '.join(where)}
                ORDER BY {sort_key_sql()}
                """,
                [*runtime_params, *model_params, *block_params, *where_params],
            ).fetchall()
            items = []
            for row in rows:
                current_model_name = str(row["model_name"] or model_name or row["default_model_name"] or "")
                account_active = int(row["is_active"] or 0) == 1
                model_registered = bool(str(row["queried_model_name"] or "").strip()) if model_name else True
                model_deprecated = int(row["model_is_deprecated"] or 0) == 1
                blocked = bool(str(row["blocked_until"] or "").strip())
                has_live_lease = (
                    str(row["lease_token"] or "").strip() != ""
                    and str(row["lease_expires_at"] or "").strip() != ""
                    and str(row["lease_expires_at"] or "").strip() > now_iso()
                    and str(row["state"] or "").strip().lower() == "in_use"
                )
                effective_state = str(row["state"] or "idle").strip() or "idle"
                leaseable = False
                lease_block_reason = ""
                if model_name:
                    if not account_active:
                        effective_state = "disabled"
                        lease_block_reason = "inactive_account"
                    elif not model_registered:
                        effective_state = "unsupported"
                        lease_block_reason = "model_not_registered"
                    elif model_deprecated:
                        effective_state = "deprecated"
                        lease_block_reason = "deprecated_model"
                    elif blocked:
                        effective_state = "blocked"
                        lease_block_reason = "blocked_model"
                    elif has_live_lease:
                        effective_state = "in_use"
                        lease_block_reason = "in_use"
                    else:
                        effective_state = "idle"
                        leaseable = True
                items.append(
                    {
                        "provider_account_id": int(row["provider_account_id"]),
                        "provider": str(row["provider"]),
                        "account_name": str(row["account_name"]),
                        "default_model_name": str(row["default_model_name"]),
                        "runtime_model_name": current_model_name,
                        "is_active": int(row["is_active"] or 0),
                        "state": effective_state,
                        "raw_state": str(row["state"] or "idle"),
                        "leaseable": leaseable,
                        "lease_block_reason": lease_block_reason,
                        "model_registered": model_registered,
                        "model_is_deprecated": model_deprecated,
                        "lease_token": str(row["lease_token"] or ""),
                        "holder": str(row["holder"] or ""),
                        "host": str(row["host"] or ""),
                        "pid": int(row["pid"] or 0),
                        "task_type": str(row["task_type"] or ""),
                        "lease_started_at": str(row["lease_started_at"] or ""),
                        "lease_expires_at": str(row["lease_expires_at"] or ""),
                        "last_heartbeat_at": str(row["last_heartbeat_at"] or ""),
                        "released_at": str(row["released_at"] or ""),
                        "note": str(row["note"] or ""),
                        "blocked_until": str(row["blocked_until"] or ""),
                        "block_reason": str(row["block_reason"] or ""),
                        "limit_value": int(row["limit_value"] or 0),
                        "used_value": int(row["used_value"] or 0),
                        "requested_value": int(row["requested_value"] or 0),
                        "model_limits": provider_model_limits_payload(
                            con,
                            provider=str(row["provider"]),
                            model_name=current_model_name,
                        ),
                    }
                )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True, "accounts": items})

    def handle_model_limits(self, qs: dict[str, list[str]]) -> None:
        provider = (qs.get("provider") or [""])[0].strip()
        model_name = (qs.get("model_name") or [""])[0].strip()
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            where = ["1=1"]
            params: list[Any] = []
            if provider:
                where.append("provider = ?")
                params.append(provider)
            if model_name:
                where.append("model_name = ?")
                params.append(model_name)
            rows = con.execute(
                f"""
                SELECT provider, model_name,
                       context_window_tokens, max_output_tokens,
                       recommended_prompt_tokens, recommended_completion_tokens,
                       chars_per_token, notes, updated_at
                FROM {PROVIDER_MODEL_LIMITS_TABLE}
                WHERE {' AND '.join(where)}
                ORDER BY provider ASC, model_name ASC
                """,
                params,
            ).fetchall()
            items = [
                {
                    "provider": str(row["provider"]),
                    "model_name": str(row["model_name"]),
                    "context_window_tokens": int(row["context_window_tokens"] or 0),
                    "max_output_tokens": int(row["max_output_tokens"] or 0),
                    "recommended_prompt_tokens": int(row["recommended_prompt_tokens"] or 0),
                    "recommended_completion_tokens": int(row["recommended_completion_tokens"] or 0),
                    "chars_per_token": float(row["chars_per_token"] or 4.0),
                    "notes": str(row["notes"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                }
                for row in rows
            ]
        finally:
            con.close()
        self.json_response(200, {"ok": True, "model_limits": items})

    def handle_acquire(self) -> None:
        # Verify client secret first
        if not self.verify_client_secret():
            self.json_response(401, {"ok": False, "error": "invalid_client_secret"})
            return

        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        provider_account_id = int(payload.get("provider_account_id") or 0)
        provider = str(payload.get("provider") or "").strip()
        model_name = str(payload.get("model_name") or "").strip()
        if not model_name:
            self.json_response(400, {"ok": False, "error": "model_name wajib diisi"})
            return
        holder = str(payload.get("holder") or "").strip()
        host = str(payload.get("host") or "").strip()
        pid = int(payload.get("pid") or 0)
        task_type = str(payload.get("task_type") or "").strip()
        lease_ttl_seconds = max(60, int(payload.get("lease_ttl_seconds") or 300))
        eligible_account_ids = [int(x) for x in (payload.get("eligible_account_ids") or []) if int(x) > 0]
        count = max(1, int(payload.get("count") or 1))
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)
            if provider_account_id > 0:
                row = con.execute(
                    "SELECT provider FROM provider_accounts WHERE id=? AND is_active=1 LIMIT 1",
                    (provider_account_id,),
                ).fetchone()
                if row is None:
                    con.rollback()
                    self.json_response(404, {"ok": False, "error": "provider_account_id tidak aktif/tidak ada"})
                    return
                provider = str(row["provider"])
                eligible_account_ids = [provider_account_id]
                count = 1
            if not provider:
                con.rollback()
                self.json_response(400, {"ok": False, "error": "provider wajib diisi"})
                return
            rows = acquire_candidates(
                con,
                provider=provider,
                model_name=model_name,
                count=count,
                eligible_account_ids=eligible_account_ids,
            )
            if provider_account_id > 0 and not rows:
                con.rollback()
                self.json_response(409, {"ok": False, "error": "akun sedang dipakai atau diblok"})
                return
            granted: list[dict[str, Any]] = []
            for row in rows:
                lease_token = uuid.uuid4().hex
                started = now_iso()
                expires = (utc_now() + timedelta(seconds=lease_ttl_seconds)).isoformat()
                con.execute(
                    f"""
                    INSERT INTO {RUNTIME_TABLE} (
                        provider_account_id, provider, model_name, lease_token, state,
                        holder, host, pid, task_type,
                        lease_started_at, lease_expires_at, last_heartbeat_at, released_at, note, updated_at
                    ) VALUES (?, ?, ?, ?, 'in_use', ?, ?, ?, ?, ?, ?, ?, '', '', ?)
                    ON CONFLICT(provider_account_id, model_name) DO UPDATE SET
                        provider=excluded.provider,
                        lease_token=excluded.lease_token,
                        state='in_use',
                        holder=excluded.holder,
                        host=excluded.host,
                        pid=excluded.pid,
                        task_type=excluded.task_type,
                        lease_started_at=excluded.lease_started_at,
                        lease_expires_at=excluded.lease_expires_at,
                        last_heartbeat_at=excluded.last_heartbeat_at,
                        released_at='',
                        note='',
                        updated_at=excluded.updated_at
                    """,
                    (
                        int(row["id"]),
                        str(row["provider"]),
                        str(model_name),
                        lease_token,
                        holder,
                        host,
                        pid,
                        task_type,
                        started,
                        expires,
                        started,
                        started,
                    ),
                )
                append_event(
                    con,
                    provider_account_id=int(row["id"]),
                    provider=str(row["provider"]),
                    model_name=str(model_name),
                    event_type="lease_acquired",
                    lease_token=lease_token,
                    holder=holder,
                    host=host,
                    pid=pid,
                    task_type=task_type,
                    payload={"lease_expires_at": expires},
                )
                granted.append(
                    lease_bundle_from_account_row(
                        con,
                        row,
                        model_name=str(model_name),
                        lease_token=lease_token,
                        holder=holder,
                        host=host,
                        pid=pid,
                        task_type=task_type,
                        lease_started_at=started,
                        lease_expires_at=expires,
                    )
                )
            con.commit()
        finally:
            con.close()
        if provider_account_id > 0:
            self.json_response(200, {"ok": True, "lease": granted[0] if granted else None})
            return
        self.json_response(200, {"ok": True, "leases": granted})

    def handle_heartbeat(self) -> None:
        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        lease_token = str(payload.get("lease_token") or "").strip()
        if not lease_token:
            self.json_response(400, {"ok": False, "error": "lease_token wajib diisi"})
            return
        ttl = max(60, int(payload.get("lease_ttl_seconds") or 300))
        now = now_iso()
        expires = (utc_now() + timedelta(seconds=ttl)).isoformat()
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)
            row = con.execute(
                f"SELECT * FROM {RUNTIME_TABLE} WHERE lease_token=? LIMIT 1",
                (lease_token,),
            ).fetchone()
            if row is None:
                con.rollback()
                self.json_response(404, {"ok": False, "error": "lease_token tidak ditemukan"})
                return
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET last_heartbeat_at=?, lease_expires_at=?, updated_at=?
                WHERE lease_token=?
                """,
                (now, expires, now, lease_token),
            )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True, "lease_expires_at": expires})

    def handle_release(self) -> None:
        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        lease_token = str(payload.get("lease_token") or "").strip()
        final_state = str(payload.get("final_state") or "idle").strip() or "idle"
        note = str(payload.get("note") or "")[:1000]
        if not lease_token:
            self.json_response(400, {"ok": False, "error": "lease_token wajib diisi"})
            return
        now = now_iso()
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            cleanup_expired_blocks(con)
            row = con.execute(
                f"SELECT * FROM {RUNTIME_TABLE} WHERE lease_token=? LIMIT 1",
                (lease_token,),
            ).fetchone()
            if row is None:
                con.rollback()
                self.json_response(404, {"ok": False, "error": "lease_token tidak ditemukan"})
                return
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state=?, lease_token='', released_at=?, note=?, updated_at=?
                WHERE provider_account_id=? AND model_name=?
                """,
                (
                    final_state,
                    now,
                    note,
                    now,
                    int(row["provider_account_id"]),
                    str(row["model_name"]),
                ),
            )
            append_event(
                con,
                provider_account_id=int(row["provider_account_id"]),
                provider=str(row["provider"]),
                model_name=str(row["model_name"]),
                event_type="lease_released",
                lease_token=lease_token,
                holder=str(row["holder"] or ""),
                host=str(row["host"] or ""),
                pid=int(row["pid"] or 0),
                task_type=str(row["task_type"] or ""),
                payload={"final_state": final_state, "note": note},
            )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True})

    def handle_block_upsert(self) -> None:
        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        provider_account_id = int(payload.get("provider_account_id") or 0)
        provider = str(payload.get("provider") or "").strip()
        model_name = str(payload.get("model_name") or "").strip()
        blocked_until = str(payload.get("blocked_until") or "").strip()
        if provider_account_id <= 0 or not provider or not model_name or not blocked_until:
            self.json_response(400, {"ok": False, "error": "provider_account_id/provider/model_name/blocked_until wajib"})
            return
        con = sqlite3.connect(str(self.db_path))
        try:
            con.execute("BEGIN IMMEDIATE")
            cleanup_expired_blocks(con)
            upsert_model_block_record(
                con,
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
                blocked_until=blocked_until,
                limit_value=int(payload.get("limit_value") or 0),
                used_value=int(payload.get("used_value") or 0),
                requested_value=int(payload.get("requested_value") or 0),
                reason=str(payload.get("reason") or ""),
                source=str(payload.get("source") or ""),
            )
            now = now_iso()
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state='blocked',
                    note=?,
                    updated_at=?
                WHERE provider_account_id=? AND model_name=?
                """,
                (
                    str(payload.get("reason") or "")[:1000],
                    now,
                    provider_account_id,
                    model_name,
                ),
            )
            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
                event_type="blocked",
                payload={
                    "blocked_until": blocked_until,
                    "limit_value": int(payload.get("limit_value") or 0),
                    "used_value": int(payload.get("used_value") or 0),
                    "requested_value": int(payload.get("requested_value") or 0),
                    "reason": str(payload.get("reason") or "")[:1000],
                },
            )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True})

    def handle_provider_event_report(self) -> None:
        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        provider_account_id = int(payload.get("provider_account_id") or 0)
        provider = str(payload.get("provider") or "").strip()
        model_name = str(payload.get("model_name") or "").strip()
        reason = str(payload.get("reason") or "")[:4000].strip()
        source = str(payload.get("source") or "")[:200].strip()
        http_status = int(payload.get("http_status") or 0)
        error_code = str(payload.get("error_code") or "").strip() or str(extract_business_error_code(reason) or "")
        limit_value = int(payload.get("limit_value") or 0)
        used_value = int(payload.get("used_value") or 0)
        requested_value = int(payload.get("requested_value") or 0)
        extra_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        if provider_account_id <= 0 or not provider or not model_name or not reason:
            self.json_response(400, {"ok": False, "error": "provider_account_id/provider/model_name/reason wajib"})
            return

        decision: dict[str, Any] = {
            "action": "ignore",
            "provider_account_id": provider_account_id,
            "provider": provider,
            "model_name": model_name,
        }
        con = sqlite3.connect(str(self.db_path))
        try:
            con.execute("BEGIN IMMEDIATE")
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)
            now = now_iso()
            event_payload: dict[str, Any] = {
                "reason": reason[:1000],
                "source": source,
                "http_status": http_status,
                "error_code": error_code,
                "limit_value": limit_value,
                "used_value": used_value,
                "requested_value": requested_value,
                "payload": extra_payload,
            }

            if is_fatal_auth_error(reason):
                notes_suffix = (
                    f"auto-disabled by provider coordinator due to provider error on "
                    f"{now}: {reason[:300]}"
                )
                set_provider_account_active_tx(
                    con,
                    provider_account_id,
                    False,
                    notes_suffix=notes_suffix,
                )
                con.execute(
                    f"""
                    UPDATE {RUNTIME_TABLE}
                    SET state='disabled',
                        note=?,
                        updated_at=?
                    WHERE provider_account_id=?
                    """,
                    (reason[:1000], now, provider_account_id),
                )
                decision = {
                    **decision,
                    "action": "disabled",
                    "notes_suffix": notes_suffix,
                }
            else:
                block_payload = parse_provider_quota_block(provider, reason)
                if block_payload is not None and is_provider_blocking_enabled(provider):
                    blocked_until = str(block_payload.get("blocked_until") or "")
                    limit_final = int(block_payload.get("limit") or limit_value)
                    used_final = int(block_payload.get("used") or used_value)
                    requested_final = int(block_payload.get("requested") or requested_value)
                    provider_key = str(provider or "").strip().lower()
                    models_to_block = [model_name]
                    if provider_key in ACCOUNT_WIDE_QUOTA_PROVIDERS:
                        models = active_models_for_provider(con, provider)
                        # If provider_models is empty for some reason, fall back to the current model.
                        models_to_block = models or [model_name]
                    for mname in models_to_block:
                        upsert_model_block_record(
                            con,
                            provider_account_id=provider_account_id,
                            provider=provider,
                            model_name=mname,
                            blocked_until=blocked_until,
                            limit_value=limit_final,
                            used_value=used_final,
                            requested_value=requested_final,
                            reason=reason,
                            source=source or "provider_event_report",
                        )
                    con.execute(
                        f"""
                        UPDATE {RUNTIME_TABLE}
                        SET state='blocked',
                            note=?,
                            updated_at=?
                        WHERE provider_account_id=? AND model_name=?
                        """,
                        (reason[:1000], now, provider_account_id, model_name),
                    )
                    decision = {
                        **decision,
                        "action": "blocked",
                        "blocked_model_name": model_name,
                        "blocked_models_count": len(models_to_block),
                        "blocked_until": blocked_until,
                        "limit_value": limit_final,
                        "used_value": used_final,
                        "requested_value": requested_final,
                        "error_code": str(block_payload.get("code") or error_code),
                    }
                elif is_transient_provider_limit_error(reason, provider=provider):
                    decision = {
                        **decision,
                        "action": "retry",
                        "retryable": True,
                        "error_code": error_code,
                    }

            append_event(
                con,
                provider_account_id=provider_account_id,
                provider=provider,
                model_name=model_name,
                event_type="provider_error",
                payload={**event_payload, "decision": decision},
            )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True, "decision": decision})

    def handle_set_active(self) -> None:
        try:
            payload = self.read_json()
        except ValueError as exc:
            self.json_response(400, {"ok": False, "error": str(exc)})
            return
        provider_account_id = int(payload.get("provider_account_id") or 0)
        is_active = bool(payload.get("is_active"))
        notes_suffix = str(payload.get("notes_suffix") or "")[:2000]
        if provider_account_id <= 0:
            self.json_response(400, {"ok": False, "error": "provider_account_id wajib"})
            return
        set_provider_account_active(
            provider_account_id,
            is_active,
            notes_suffix=notes_suffix,
            db_path=self.db_path,
        )
        con = sqlite3.connect(str(self.db_path))
        try:
            now = now_iso()
            con.execute(
                f"""
                UPDATE {RUNTIME_TABLE}
                SET state=?, lease_token='', released_at=?, note=?, updated_at=?
                WHERE provider_account_id=?
                """,
                ("idle" if is_active else "disabled", now, notes_suffix[:1000], now, provider_account_id),
            )
            row = con.execute(
                "SELECT provider, model_name FROM provider_accounts WHERE id=? LIMIT 1",
                (provider_account_id,),
            ).fetchone()
            if row is not None:
                append_event(
                    con,
                    provider_account_id=provider_account_id,
                    provider=str(row[0]),
                    model_name=str(row[1]),
                    event_type="set_active",
                    payload={"is_active": is_active, "notes_suffix": notes_suffix},
                )
            con.commit()
        finally:
            con.close()
        self.json_response(200, {"ok": True})

    def handle_get_api_key(self) -> None:
        """Handle GET API key endpoint with auth validation."""
        # Verify client secret first
        if not self.verify_client_secret():
            self.json_response(401, {"ok": False, "error": "invalid_client_secret"})
            return

        # Extract provider_account_id from path: /v1/accounts/{id}/api-key
        try:
            parsed_path = urlparse(self.path).path
            path_parts = parsed_path.strip("/").split("/")
            if len(path_parts) != 4 or path_parts[0] != "v1" or path_parts[1] != "accounts" or path_parts[3] != "api-key":
                self.json_response(400, {"ok": False, "error": "invalid_path_format"})
                return
            provider_account_id = int(path_parts[2])
        except (ValueError, IndexError):
            self.json_response(400, {"ok": False, "error": "invalid_provider_account_id"})
            return

        # Get API key from database and decrypt it
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT provider, api_key FROM provider_accounts WHERE id=? LIMIT 1",
                (provider_account_id,),
            ).fetchone()
            if row is None:
                self.json_response(404, {"ok": False, "error": "provider_account_id tidak ditemukan"})
                return

            encrypted_key = str(row["api_key"] or "")
            if not encrypted_key:
                self.json_response(404, {"ok": False, "error": "api_key tidak ada"})
                return

            # Decrypt the API key
            decrypted_key = decrypt_api_key(encrypted_key)

            self.json_response(200, {"ok": True, "api_key": decrypted_key})
        finally:
            con.close()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).resolve()
    ensure_runtime_tables(db_path)
    ensure_provider_models_table(db_path)
    ensure_provider_model_limits_table(db_path)
    seed_provider_model_limits(db_path)
    server = ThreadingHTTPServer((args.host, int(args.port)), CoordinatorHandler)
    server.db_path = db_path  # type: ignore[attr-defined]
    print(
        f"[provider-coordinator] listening on http://{args.host}:{args.port} db={db_path}",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
