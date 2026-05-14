from __future__ import annotations


import json
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


PROJECT_ROOT = Path(__file__).resolve().parent
PREFERRED_SERVICES_ROOT = Path(str(Path(__file__).resolve().parent.parent / "services"))
PROVIDERS_WITH_TPD_BLOCKING = {"groq", "cerebras", "openrouter", "z.ai"}
PROVIDERS_WITHOUT_BLOCKING = {"nvidia"}
TPD_RE = re.compile(r"tokens per day \(TPD\): Limit (\d+), Used (\d+), Requested (\d+)", re.I)
BUSINESS_CODE_PATTERNS = (
    re.compile(r"""['"]code['"]\s*:\s*['"]?(\d{4})['"]?""", re.I),
    re.compile(r"""\berror\s+code\b\s*[:=]?\s*(\d{4})\b""", re.I),
)
RESET_TIME_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
)
ZAI_TRANSIENT_LIMIT_CODES = {"1302", "1303", "1305", "1312"}
ZAI_BLOCK_LIMIT_CODES = {"1304", "1308", "1309", "1310", "1313"}


def _services_root() -> Path:
    raw = (os.getenv("YT_SERVICES_DIR") or "").strip()
    candidate = Path(raw).expanduser() if raw else PREFERRED_SERVICES_ROOT
    if candidate.exists():
        return candidate.resolve()
    return PROJECT_ROOT


def _service_path(filename: str, env_name: str) -> Path:
    raw = (os.getenv(env_name) or "").strip()
    if raw:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        return candidate.resolve()
    return (_services_root() / filename).resolve()


SERVICES_ROOT = _services_root()
DEFAULT_PROVIDERS_DB = _service_path("provider_accounts.sqlite3", "YT_PROVIDERS_DB")
DEFAULT_LOCAL_ENV = _service_path(".env", "YT_LOCAL_ENV_PATH")
DEFAULT_LOCAL_NOTES = _service_path("yt_channel_local_notes.md", "YT_LOCAL_NOTES_PATH")
LEGACY_QUOTA_STATE_JSON = _service_path("provider_quota_state.json", "YT_PROVIDER_QUOTA_STATE_JSON")
PROVIDER_MODEL_BLOCKS_TABLE = "provider_account_model_blocks"
WEBSHARE_PROXY_BLOCKS_TABLE = "webshare_proxy_blocks"
PROVIDER_MODELS_TABLE = "provider_models"
PROVIDER_MODEL_LIMITS_TABLE = "provider_model_limits"
_DOTENV_CACHE: dict[str, str] | None = None
_COOKIE_ROTATION_INDEX = 0

DEFAULT_PROVIDER_MODEL_LIMITS: tuple[dict[str, Any], ...] = (
    {
        "provider": "nvidia",
        "model_name": "openai/gpt-oss-120b",
        "context_window_tokens": 131072,
        "max_output_tokens": 65536,
        "recommended_prompt_tokens": 12000,
        "recommended_completion_tokens": 2400,
        "chars_per_token": 4.0,
        "notes": "Baseline formatting/resume backbone. Operationally stable.",
    },
    {
        "provider": "groq",
        "model_name": "openai/gpt-oss-20b",
        "context_window_tokens": 131072,
        "max_output_tokens": 65536,
        "recommended_prompt_tokens": 12000,
        "recommended_completion_tokens": 2200,
        "chars_per_token": 4.0,
        "notes": "Groq docs: 131072 context, 65536 max output. reasoning_effort supported.",
    },
    {
        "provider": "groq",
        "model_name": "qwen/qwen3-32b",
        "context_window_tokens": 131072,
        "max_output_tokens": 40960,
        "recommended_prompt_tokens": 3200,
        "recommended_completion_tokens": 1200,
        "chars_per_token": 4.0,
        "notes": "Conservative due observed 413/request-too-large and org TPM sensitivity. reasoning_effort default/none only.",
    },
    {
        "provider": "groq",
        "model_name": "llama-3.3-70b-versatile",
        "context_window_tokens": 131072,
        "max_output_tokens": 32768,
        "recommended_prompt_tokens": 8000,
        "recommended_completion_tokens": 1800,
        "chars_per_token": 4.0,
        "notes": "Groq docs: 131072 context, 32768 max output.",
    },
    {
        "provider": "groq",
        "model_name": "meta-llama/llama-4-scout-17b-16e-instruct",
        "context_window_tokens": 131072,
        "max_output_tokens": 8192,
        "recommended_prompt_tokens": 5000,
        "recommended_completion_tokens": 1200,
        "chars_per_token": 4.0,
        "notes": "Groq docs: 131072 context, 8192 max output.",
    },
    {
        "provider": "groq",
        "model_name": "moonshotai/kimi-k2-instruct",
        "context_window_tokens": 262144,
        "max_output_tokens": 16384,
        "recommended_prompt_tokens": 12000,
        "recommended_completion_tokens": 2000,
        "chars_per_token": 4.0,
        "notes": "Operational fallback. Prefer 0905 where available.",
    },
    {
        "provider": "groq",
        "model_name": "moonshotai/kimi-k2-instruct-0905",
        "context_window_tokens": 262144,
        "max_output_tokens": 16384,
        "recommended_prompt_tokens": 16000,
        "recommended_completion_tokens": 2000,
        "chars_per_token": 4.0,
        "notes": "Groq docs: 262144 context, 16384 max output.",
    },
    {
        "provider": "cerebras",
        "model_name": "llama3.1-8b",
        "context_window_tokens": 8192,
        "max_output_tokens": 8192,
        "recommended_prompt_tokens": 3000,
        "recommended_completion_tokens": 1200,
        "chars_per_token": 4.0,
        "notes": "Cerebras docs: 8k free tier context, 32k paid tiers. Use conservative prompt budget.",
    },
    {
        "provider": "cerebras",
        "model_name": "qwen-3-235b-a22b-instruct-2507",
        "context_window_tokens": 32768,
        "max_output_tokens": 8192,
        "recommended_prompt_tokens": 6000,
        "recommended_completion_tokens": 1500,
        "chars_per_token": 4.0,
        "notes": "Cerebras docs incomplete for context window on instruct page. Conservative operational budget.",
    },
    {
        "provider": "z.ai",
        "model_name": "glm-4.7",
        "context_window_tokens": 128000,
        "max_output_tokens": 16000,
        "recommended_prompt_tokens": 10000,
        "recommended_completion_tokens": 2200,
        "chars_per_token": 4.0,
        "notes": "Operational fallback for transcript formatting.",
    },
)


def _load_dotenv_map(path: Path | None = None) -> dict[str, str]:
    target = (path or DEFAULT_LOCAL_ENV).resolve()
    if not target.exists():
        return {}
    values: dict[str, str] = {}
    try:
        for raw_line in target.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            values[key] = value
    except Exception:
        return {}
    return values


def service_env(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is not None and str(raw).strip():
        return str(raw).strip()
    global _DOTENV_CACHE
    if _DOTENV_CACHE is None:
        _DOTENV_CACHE = _load_dotenv_map()
    return str((_DOTENV_CACHE or {}).get(name, default) or default).strip()


def _project_env_value(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is not None and str(raw).strip():
        return str(raw).strip()
    project_env = PROJECT_ROOT / ".env"
    if not project_env.exists():
        return default
    try:
        for line in project_env.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != name:
                continue
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            return value.strip() or default
    except Exception:
        return default
    return default


def coordinator_base_url() -> str:
    project_value = _project_env_value("YT_PROVIDER_COORDINATOR_URL", "")
    if project_value:
        return project_value
    return service_env("YT_PROVIDER_COORDINATOR_URL", "")


def coordinator_enabled() -> bool:
    return bool(coordinator_base_url())


def is_provider_blocking_enabled(provider: str) -> bool:
    return str(provider or "").strip().lower() in PROVIDERS_WITH_TPD_BLOCKING


def extract_business_error_code(reason: str) -> str | None:
    text = str(reason or "")
    for pattern in BUSINESS_CODE_PATTERNS:
        match = pattern.search(text)
        if match:
            return str(match.group(1))
    return None


def _next_local_midnight_iso() -> str:
    now = datetime.now().astimezone()
    next_day = (now + timedelta(days=1)).date()
    dt_local = datetime(next_day.year, next_day.month, next_day.day, 0, 5, 0, tzinfo=now.tzinfo)
    return dt_local.astimezone(timezone.utc).isoformat()


def _extract_reset_time_iso(reason: str) -> str | None:
    match = RESET_TIME_RE.search(str(reason or ""))
    if not match:
        return None
    raw = match.group(1).strip()
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return dt.astimezone(timezone.utc).isoformat()


def parse_provider_quota_block(provider: str, reason: str) -> dict[str, Any] | None:
    provider_key = str(provider or "").strip().lower()
    text = str(reason or "")
    if not provider_key or not text:
        return None

    if provider_key in {"groq", "cerebras", "openrouter"}:
        match = TPD_RE.search(text)
        if not match:
            low = text.lower()
            if provider_key == "groq" and (
                "429" in low
                or "too many requests" in low
                or "rate limit" in low
                or "asph" in low
                or "seconds of audio per hour" in low
            ):
                blocked_until = _extract_reset_time_iso(text)
                if not blocked_until:
                    blocked_until = (datetime.now().astimezone() + timedelta(hours=6)).isoformat()
                return {
                    "blocked_until": blocked_until,
                    "limit": 0,
                    "used": 0,
                    "requested": 0,
                    "code": "",
                }
            return None
        limit, used, requested = (int(v) for v in match.groups())
        threshold = int(limit * 0.95)
        if used < threshold and (used + requested) < threshold:
            return None
        return {
            "blocked_until": _next_local_midnight_iso(),
            "limit": limit,
            "used": used,
            "requested": requested,
            "code": "",
        }

    if provider_key != "z.ai":
        return None

    code = extract_business_error_code(text) or ""
    if code not in ZAI_BLOCK_LIMIT_CODES:
        return None

    blocked_until = _extract_reset_time_iso(text)
    if not blocked_until:
        now = datetime.now().astimezone()
        if code == "1304":
            blocked_until = _next_local_midnight_iso()
        elif code == "1308":
            blocked_until = (now + timedelta(hours=6)).isoformat()
        elif code == "1309":
            blocked_until = (now + timedelta(days=32)).isoformat()
        elif code == "1310":
            blocked_until = (now + timedelta(days=8)).isoformat()
        else:
            blocked_until = (now + timedelta(days=8)).isoformat()

    return {
        "blocked_until": blocked_until,
        "limit": 0,
        "used": 0,
        "requested": 0,
        "code": code,
    }


def is_transient_provider_limit_error(reason: str, *, provider: str = "") -> bool:
    text = str(reason or "")
    low = text.lower()
    code = extract_business_error_code(text) or ""
    if code in ZAI_BLOCK_LIMIT_CODES:
        return False
    if ("429" in low) or ("too many requests" in low) or ("rate limit" in low):
        return True
    provider_key = str(provider or "").strip().lower()
    if provider_key == "z.ai" and code in ZAI_TRANSIENT_LIMIT_CODES:
        return True
    if code in ZAI_TRANSIENT_LIMIT_CODES:
        return True
    phrases = (
        "high concurrency usage of this api",
        "high frequency usage of this api",
        "the api has triggered a rate limit",
        "this model is currently experiencing high traffic",
    )
    return any(phrase in low for phrase in phrases)


def coordinator_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 20,
) -> dict[str, Any]:
    base = coordinator_base_url().rstrip("/")
    if not base:
        raise RuntimeError("YT_PROVIDER_COORDINATOR_URL belum diatur.")
    url = base + path
    body = None
    headers = {"Content-Type": "application/json"}
    
    # Add client secret header if configured
    client_secret = _project_env_value("YT_PROVIDER_COORDINATOR_SECRET") or service_env("YT_PROVIDER_COORDINATOR_SECRET", "")
    if client_secret:
        headers["X-Client-Secret"] = client_secret
    
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        detail = raw.strip() or exc.reason
        raise RuntimeError(f"Coordinator HTTP {exc.code}: {detail}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Coordinator tidak bisa dihubungi: {exc.reason}") from exc


def coordinator_acquire_accounts(
    *,
    provider: str,
    model_name: str,
    count: int,
    eligible_account_ids: list[int] | None = None,
    holder: str = "",
    host: str = "",
    pid: int = 0,
    task_type: str = "",
    lease_ttl_seconds: int = 300,
) -> list[dict[str, Any]]:
    payload = {
        "provider": str(provider).strip(),
        "model_name": str(model_name).strip(),
        "count": int(count),
        "eligible_account_ids": [int(x) for x in (eligible_account_ids or [])],
        "holder": str(holder or "").strip(),
        "host": str(host or "").strip(),
        "pid": int(pid or 0),
        "task_type": str(task_type or "").strip(),
        "lease_ttl_seconds": int(lease_ttl_seconds),
    }
    data = coordinator_request("POST", "/v1/leases/acquire", payload)
    items = data.get("leases") or []
    return [dict(item) for item in items if isinstance(item, dict)]


def coordinator_status_accounts(
    *,
    provider: str | None = None,
    model_name: str | None = None,
    include_inactive: bool = False,
) -> list[dict[str, Any]]:
    """Get status of provider accounts from coordinator.
    
    Args:
        provider: Filter by provider (optional)
        model_name: Filter by model name (optional)
        include_inactive: Include inactive accounts (default: False)
    
    Returns:
        List of account status dictionaries
    """
    params = {}
    if provider:
        params["provider"] = str(provider).strip()
    if model_name:
        params["model_name"] = str(model_name).strip()
    params["include_inactive"] = 1 if include_inactive else 0

    query = urllib_parse.urlencode({k: v for k, v in params.items() if v})
    endpoint = "/v1/status/accounts"
    if query:
        endpoint = f"{endpoint}?{query}"
    
    data = coordinator_request("GET", endpoint)
    items = data.get("accounts") or []
    return [dict(item) for item in items if isinstance(item, dict)]


def coordinator_acquire_first_available(
    *,
    candidates: list[tuple[str, str]],
    eligible_account_ids: list[int] | None = None,
    holder: str = "",
    host: str = "",
    pid: int = 0,
    task_type: str = "",
    lease_ttl_seconds: int = 300,
) -> tuple[dict[str, Any], tuple[str, str]]:
    last_error = "tidak ada kandidat provider-model"
    for provider, model_name in candidates:
        try:
            leases = coordinator_acquire_accounts(
                provider=provider,
                model_name=model_name,
                count=1,
                eligible_account_ids=eligible_account_ids,
                holder=holder,
                host=host,
                pid=pid,
                task_type=task_type,
                lease_ttl_seconds=lease_ttl_seconds,
            )
        except Exception as exc:
            last_error = str(exc).strip() or repr(exc)
            continue
        if leases:
            return leases[0], (str(provider).strip(), str(model_name).strip())
        last_error = f"tidak ada lease untuk {provider}:{model_name}"
    raise RuntimeError(last_error)


def coordinator_acquire_specific_account(
    *,
    provider_account_id: int,
    model_name: str,
    holder: str = "",
    host: str = "",
    pid: int = 0,
    task_type: str = "",
    lease_ttl_seconds: int = 300,
) -> dict[str, Any] | None:
    payload = {
        "provider_account_id": int(provider_account_id),
        "model_name": str(model_name).strip(),
        "holder": str(holder or "").strip(),
        "host": str(host or "").strip(),
        "pid": int(pid or 0),
        "task_type": str(task_type or "").strip(),
        "lease_ttl_seconds": int(lease_ttl_seconds),
    }
    data = coordinator_request("POST", "/v1/leases/acquire", payload)
    item = data.get("lease")
    return dict(item) if isinstance(item, dict) else None


def coordinator_heartbeat_lease(
    lease_token: str,
    *,
    lease_ttl_seconds: int = 300,
) -> dict[str, Any]:
    return coordinator_request(
        "POST",
        "/v1/leases/heartbeat",
        {
            "lease_token": str(lease_token).strip(),
            "lease_ttl_seconds": int(lease_ttl_seconds),
        },
    )


def coordinator_release_lease(
    lease_token: str,
    *,
    final_state: str = "idle",
    note: str = "",
) -> dict[str, Any]:
    return coordinator_request(
        "POST",
        "/v1/leases/release",
        {
            "lease_token": str(lease_token).strip(),
            "final_state": str(final_state).strip() or "idle",
            "note": str(note or "")[:1000],
        },
    )


def coordinator_report_model_block(
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
) -> dict[str, Any]:
    if not is_provider_blocking_enabled(provider):
        return {"skipped": True, "reason": f"{provider} does not support blocking"}
    return coordinator_request(
        "POST",
        "/v1/blocks/upsert",
        {
            "provider_account_id": int(provider_account_id),
            "provider": str(provider).strip(),
            "model_name": str(model_name).strip(),
            "blocked_until": str(blocked_until).strip(),
            "limit_value": int(limit_value),
            "used_value": int(used_value),
            "requested_value": int(requested_value),
            "reason": str(reason or "")[:2000],
            "source": str(source or "")[:200],
        },
    )


def coordinator_report_provider_event(
    *,
    provider_account_id: int,
    provider: str,
    model_name: str,
    reason: str,
    source: str = "",
    http_status: int = 0,
    error_code: str = "",
    limit_value: int = 0,
    used_value: int = 0,
    requested_value: int = 0,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return coordinator_request(
        "POST",
        "/v1/provider-events/report",
        {
            "provider_account_id": int(provider_account_id),
            "provider": str(provider).strip(),
            "model_name": str(model_name).strip(),
            "reason": str(reason or "")[:4000],
            "source": str(source or "")[:200],
            "http_status": int(http_status or 0),
            "error_code": str(error_code or "").strip(),
            "limit_value": int(limit_value or 0),
            "used_value": int(used_value or 0),
            "requested_value": int(requested_value or 0),
            "payload": payload or {},
        },
    )


def coordinator_set_provider_account_active(
    provider_account_id: int,
    is_active: bool,
    *,
    notes_suffix: str = "",
) -> dict[str, Any]:
    return coordinator_request(
        "POST",
        "/v1/accounts/set-active",
        {
            "provider_account_id": int(provider_account_id),
            "is_active": bool(is_active),
            "notes_suffix": str(notes_suffix or "")[:2000],
        },
    )


def ensure_provider_blocks_table(db_path: Path | None = None) -> Path:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PROVIDER_MODEL_BLOCKS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_account_id INTEGER NOT NULL,
                provider TEXT NOT NULL,
                model_name TEXT NOT NULL,
                blocked_until TEXT NOT NULL,
                limit_value INTEGER NOT NULL DEFAULT 0,
                used_value INTEGER NOT NULL DEFAULT 0,
                requested_value INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider_account_id, model_name)
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODEL_BLOCKS_TABLE}_provider_model "
            f"ON {PROVIDER_MODEL_BLOCKS_TABLE}(provider, model_name)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODEL_BLOCKS_TABLE}_blocked_until "
            f"ON {PROVIDER_MODEL_BLOCKS_TABLE}(blocked_until)"
        )
        con.commit()
    finally:
        con.close()
    return target


def ensure_webshare_proxy_blocks_table(db_path: Path | None = None) -> Path:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {WEBSHARE_PROXY_BLOCKS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_url TEXT NOT NULL UNIQUE,
                blocked_until TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{WEBSHARE_PROXY_BLOCKS_TABLE}_blocked_until "
            f"ON {WEBSHARE_PROXY_BLOCKS_TABLE}(blocked_until)"
        )
        con.commit()
    finally:
        con.close()
    return target


def ensure_provider_models_table(db_path: Path | None = None) -> Path:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PROVIDER_MODELS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                model_name TEXT NOT NULL,
                capability TEXT NOT NULL DEFAULT 'chat',
                is_deprecated INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider, model_name)
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODELS_TABLE}_provider "
            f"ON {PROVIDER_MODELS_TABLE}(provider)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODELS_TABLE}_provider_model "
            f"ON {PROVIDER_MODELS_TABLE}(provider, model_name)"
        )
        con.commit()
    finally:
        con.close()
    return target


def ensure_provider_model_limits_table(db_path: Path | None = None) -> Path:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PROVIDER_MODEL_LIMITS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                model_name TEXT NOT NULL,
                context_window_tokens INTEGER NOT NULL DEFAULT 0,
                max_output_tokens INTEGER NOT NULL DEFAULT 0,
                recommended_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                recommended_completion_tokens INTEGER NOT NULL DEFAULT 0,
                chars_per_token REAL NOT NULL DEFAULT 4.0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider, model_name)
            )
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODEL_LIMITS_TABLE}_provider "
            f"ON {PROVIDER_MODEL_LIMITS_TABLE}(provider)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PROVIDER_MODEL_LIMITS_TABLE}_provider_model "
            f"ON {PROVIDER_MODEL_LIMITS_TABLE}(provider, model_name)"
        )
        con.commit()
    finally:
        con.close()
    return target


def seed_provider_model_limits(db_path: Path | None = None) -> int:
    target = ensure_provider_model_limits_table(db_path)
    con = sqlite3.connect(str(target))
    try:
        changed = 0
        for item in DEFAULT_PROVIDER_MODEL_LIMITS:
            con.execute(
                f"""
                INSERT INTO {PROVIDER_MODEL_LIMITS_TABLE} (
                    provider, model_name,
                    context_window_tokens, max_output_tokens,
                    recommended_prompt_tokens, recommended_completion_tokens,
                    chars_per_token, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(provider, model_name) DO UPDATE SET
                    context_window_tokens=excluded.context_window_tokens,
                    max_output_tokens=excluded.max_output_tokens,
                    recommended_prompt_tokens=excluded.recommended_prompt_tokens,
                    recommended_completion_tokens=excluded.recommended_completion_tokens,
                    chars_per_token=excluded.chars_per_token,
                    notes=excluded.notes,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(item["provider"]).strip(),
                    str(item["model_name"]).strip(),
                    int(item.get("context_window_tokens") or 0),
                    int(item.get("max_output_tokens") or 0),
                    int(item.get("recommended_prompt_tokens") or 0),
                    int(item.get("recommended_completion_tokens") or 0),
                    float(item.get("chars_per_token") or 4.0),
                    str(item.get("notes") or "")[:4000],
                ),
            )
            changed += 1
        con.commit()
        return changed
    finally:
        con.close()


def load_provider_model_limit(
    provider: str,
    model_name: str,
    *,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    target = ensure_provider_model_limits_table(db_path)
    con = sqlite3.connect(str(target))
    con.row_factory = sqlite3.Row
    try:
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
    finally:
        con.close()
    if row is None:
        return None
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


def migrate_provider_models_from_account_models(db_path: Path | None = None) -> int:
    target = ensure_provider_models_table(db_path)
    con = sqlite3.connect(str(target))
    try:
        rows = con.execute(
            """
            SELECT
                pa.provider,
                pam.model_name,
                COALESCE(NULLIF(TRIM(pam.capability), ''), 'chat') AS capability,
                MAX(COALESCE(pam.is_deprecated, 0)) AS is_deprecated,
                MAX(COALESCE(pam.notes, '')) AS notes
            FROM provider_account_models pam
            JOIN provider_accounts pa ON pa.id = pam.provider_account_id
            GROUP BY pa.provider, pam.model_name, COALESCE(NULLIF(TRIM(pam.capability), ''), 'chat')
            """
        ).fetchall()
        moved = 0
        for provider, model_name, capability, is_deprecated, notes in rows:
            con.execute(
                f"""
                INSERT INTO {PROVIDER_MODELS_TABLE} (
                    provider, model_name, capability, is_deprecated, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(provider, model_name) DO UPDATE SET
                    capability=excluded.capability,
                    is_deprecated=excluded.is_deprecated,
                    notes=CASE
                        WHEN length(excluded.notes) > length({PROVIDER_MODELS_TABLE}.notes) THEN excluded.notes
                        ELSE {PROVIDER_MODELS_TABLE}.notes
                    END,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(provider).strip(),
                    str(model_name).strip(),
                    str(capability or "chat").strip(),
                    int(is_deprecated or 0),
                    str(notes or "")[:4000],
                ),
            )
            moved += 1
        con.commit()
        return moved
    finally:
        con.close()


def compact_provider_account_models_to_defaults(db_path: Path | None = None) -> int:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        current = con.execute("SELECT COUNT(*) FROM provider_account_models").fetchone()[0]
        con.execute(
            """
            DELETE FROM provider_account_models
            WHERE id NOT IN (
                SELECT MIN(pam.id)
                FROM provider_account_models pam
                JOIN provider_accounts pa ON pa.id = pam.provider_account_id
                WHERE pam.model_name = pa.model_name
                GROUP BY pam.provider_account_id, pam.model_name
            )
            """
        )
        con.execute(
            """
            INSERT OR IGNORE INTO provider_account_models (
                provider_account_id, model_name, is_default, capability, is_deprecated, notes
            )
            SELECT
                pa.id,
                pa.model_name,
                1,
                COALESCE(pm.capability, 'chat'),
                COALESCE(pm.is_deprecated, 0),
                COALESCE(pm.notes, '')
            FROM provider_accounts pa
            LEFT JOIN provider_models pm
              ON pm.provider = pa.provider
             AND pm.model_name = pa.model_name
            WHERE COALESCE(TRIM(pa.model_name), '') <> ''
            """
        )
        con.execute(
            """
            UPDATE provider_account_models
            SET is_default = CASE
                WHEN model_name = (
                    SELECT pa.model_name
                    FROM provider_accounts pa
                    WHERE pa.id = provider_account_models.provider_account_id
                ) THEN 1
                ELSE 0
            END
            """
        )
        con.commit()
        remaining = con.execute("SELECT COUNT(*) FROM provider_account_models").fetchone()[0]
        return int(current) - int(remaining)
    finally:
        con.close()


def load_provider_model_blocks(db_path: Path | None = None) -> dict[str, dict[str, Any]]:
    target = ensure_provider_blocks_table(db_path)
    con = sqlite3.connect(str(target))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            f"""
            SELECT provider_account_id, provider, model_name, blocked_until,
                   limit_value, used_value, requested_value, reason, source, updated_at
            FROM {PROVIDER_MODEL_BLOCKS_TABLE}
            """
        ).fetchall()
    finally:
        con.close()
    state: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = f"{row['provider']}|{row['provider_account_id']}|{row['model_name']}"
        state[key] = {
            "provider": row["provider"],
            "account_id": int(row["provider_account_id"]),
            "model": row["model_name"],
            "blocked_until": row["blocked_until"],
            "limit": int(row["limit_value"] or 0),
            "used": int(row["used_value"] or 0),
            "requested": int(row["requested_value"] or 0),
            "reason": row["reason"] or "",
            "source": row["source"] or "",
            "updated_at": row["updated_at"] or "",
        }
    return state


def _parse_iso_datetime(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def load_webshare_proxy_blocks(db_path: Path | None = None) -> dict[str, dict[str, Any]]:
    target = ensure_webshare_proxy_blocks_table(db_path)
    now = datetime.now(timezone.utc)
    con = sqlite3.connect(str(target))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            f"""
            SELECT proxy_url, blocked_until, reason, source, updated_at
            FROM {WEBSHARE_PROXY_BLOCKS_TABLE}
            """
        ).fetchall()
    finally:
        con.close()
    state: dict[str, dict[str, Any]] = {}
    for row in rows:
        blocked_until = _parse_iso_datetime(str(row["blocked_until"] or ""))
        if blocked_until is not None and blocked_until.tzinfo is None:
            blocked_until = blocked_until.replace(tzinfo=timezone.utc)
        if blocked_until is not None and blocked_until <= now:
            continue
        proxy_url = str(row["proxy_url"] or "").strip()
        if not proxy_url:
            continue
        state[proxy_url] = {
            "proxy_url": proxy_url,
            "blocked_until": str(row["blocked_until"] or ""),
            "reason": str(row["reason"] or ""),
            "source": str(row["source"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
    return state


def upsert_webshare_proxy_block(
    proxy_url: str,
    blocked_until: str,
    *,
    reason: str = "",
    source: str = "",
    db_path: Path | None = None,
) -> None:
    target = ensure_webshare_proxy_blocks_table(db_path)
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"""
            INSERT INTO {WEBSHARE_PROXY_BLOCKS_TABLE} (
                proxy_url, blocked_until, reason, source, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(proxy_url) DO UPDATE SET
                blocked_until=excluded.blocked_until,
                reason=excluded.reason,
                source=excluded.source,
                updated_at=excluded.updated_at
            """,
            (
                str(proxy_url or "").strip(),
                str(blocked_until or "").strip(),
                str(reason or "")[:2000],
                str(source or "")[:200],
                now,
            ),
        )
        con.commit()
    finally:
        con.close()


def remove_webshare_proxy_block(
    proxy_url: str,
    *,
    db_path: Path | None = None,
) -> None:
    target = ensure_webshare_proxy_blocks_table(db_path)
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"DELETE FROM {WEBSHARE_PROXY_BLOCKS_TABLE} WHERE proxy_url=?",
            (str(proxy_url or "").strip(),),
        )
        con.commit()
    finally:
        con.close()


def upsert_provider_model_block(
    provider_account_id: int,
    provider: str,
    model_name: str,
    blocked_until: str,
    *,
    limit_value: int = 0,
    used_value: int = 0,
    requested_value: int = 0,
    reason: str = "",
    source: str = "",
    db_path: Path | None = None,
) -> None:
    target = ensure_provider_blocks_table(db_path)
    now = datetime.now().isoformat()
    con = sqlite3.connect(str(target))
    try:
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
        con.commit()
    finally:
        con.close()


def remove_provider_model_block(
    provider_account_id: int,
    model_name: str,
    *,
    db_path: Path | None = None,
) -> None:
    target = ensure_provider_blocks_table(db_path)
    con = sqlite3.connect(str(target))
    try:
        con.execute(
            f"DELETE FROM {PROVIDER_MODEL_BLOCKS_TABLE} WHERE provider_account_id=? AND model_name=?",
            (int(provider_account_id), str(model_name).strip()),
        )
        con.commit()
    finally:
        con.close()


def set_provider_account_active(
    provider_account_id: int,
    is_active: bool,
    *,
    notes_suffix: str = "",
    db_path: Path | None = None,
) -> None:
    target = (db_path or DEFAULT_PROVIDERS_DB).resolve()
    con = sqlite3.connect(str(target))
    try:
        if notes_suffix:
            cur = con.execute(
                "SELECT notes FROM provider_accounts WHERE id=?",
                (int(provider_account_id),),
            )
            row = cur.fetchone()
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
        con.commit()
    finally:
        con.close()


def migrate_legacy_quota_state_to_sqlite(
    *,
    db_path: Path | None = None,
    json_path: Path | None = None,
) -> int:
    target_json = (json_path or LEGACY_QUOTA_STATE_JSON).resolve()
    if not target_json.exists():
        return 0
    try:
        payload = json.loads(target_json.read_text(encoding="utf-8"))
    except Exception:
        return 0
    moved = 0
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        provider = str(value.get("provider") or "").strip()
        account_id = int(value.get("account_id") or 0)
        model_name = str(value.get("model") or "").strip()
        blocked_until = str(value.get("blocked_until") or "").strip()
        if not provider or account_id <= 0 or not model_name or not blocked_until:
            parts = str(key).split("|", 2)
            if len(parts) == 3:
                provider = provider or parts[0]
                try:
                    account_id = account_id or int(parts[1])
                except Exception:
                    account_id = account_id or 0
                model_name = model_name or parts[2]
        if not provider or account_id <= 0 or not model_name or not blocked_until:
            continue
        upsert_provider_model_block(
            account_id,
            provider,
            model_name,
            blocked_until,
            limit_value=int(value.get("limit") or 0),
            used_value=int(value.get("used") or 0),
            requested_value=int(value.get("requested") or 0),
            reason=str(value.get("reason") or ""),
            source="legacy_json_migration",
            db_path=db_path,
        )
        moved += 1
    return moved


# ---------------------------------------------------------------------------
# YouTube auth / cookie helpers  (used by recover_transcripts, yt_dlp, etc.)
# ---------------------------------------------------------------------------

def youtube_cookie_file() -> str | None:
    """Return path to cookies.txt if it exists and looks valid."""
    files = youtube_cookie_files()
    return files[0] if files else None


def _cookie_file_candidates() -> list[str]:
    candidates: list[str] = []

    def add(value: str | None) -> None:
        raw = str(value or "").strip()
        if not raw:
            return
        for part in raw.replace("\n", ",").replace(";", ",").split(","):
            item = part.strip()
            if item:
                candidates.append(item)

    add(os.getenv("YT_COOKIE_FILES"))
    add(os.getenv("YT_COOKIE_FILE_LIST"))

    indexed_keys = sorted(
        [
            key
            for key in os.environ
            if key.startswith("YT_COOKIE_FILE_") and key[len("YT_COOKIE_FILE_"):].isdigit()
        ],
        key=lambda key: int(key[len("YT_COOKIE_FILE_"):]),
    )
    for key in indexed_keys:
        add(os.getenv(key))

    add(os.getenv("YT_COOKIE_FILE"))
    return candidates


def youtube_cookie_files() -> list[str]:
    """Return all valid YouTube cookie files in priority order."""
    seen: set[str] = set()
    files: list[str] = []

    def add_path(path_value: str | Path | None) -> None:
        if path_value is None:
            return
        try:
            candidate = Path(str(path_value)).expanduser().resolve()
        except Exception:
            return
        raw = str(candidate)
        if raw in seen:
            return
        if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
            seen.add(raw)
            files.append(raw)

    for item in _cookie_file_candidates():
        add_path(item)

    default_candidate = PROJECT_ROOT / "cookies.txt"
    add_path(default_candidate)

    home_candidate = Path.home() / "cookies.txt"
    if home_candidate != default_candidate:
        add_path(home_candidate)

    for glob_candidate in sorted(PROJECT_ROOT.glob("cookies*.txt")):
        add_path(glob_candidate)

    home_glob = sorted(Path.home().glob("cookies*.txt"))
    for glob_candidate in home_glob:
        if glob_candidate.resolve() != default_candidate.resolve():
            add_path(glob_candidate)

    return files


def youtube_next_cookie_file() -> str | None:
    """Return the next valid cookie file in a round-robin cycle."""
    global _COOKIE_ROTATION_INDEX
    files = youtube_cookie_files()
    if not files:
        return None
    index = _COOKIE_ROTATION_INDEX % len(files)
    _COOKIE_ROTATION_INDEX = (_COOKIE_ROTATION_INDEX + 1) % len(files)
    return files[index]


def youtube_cookies_from_browser() -> list[str]:
    """Return browser names with a detectable cookie store for yt-dlp."""
    raw = (os.getenv("YT_DLP_BROWSER_NAMES") or "").strip()
    if raw:
        browsers = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    else:
        browsers = ["chrome", "firefox", "edge"]

    home = Path.home()
    browser_dirs = {
        "chrome": [home / ".config/google-chrome", home / ".config/chromium"],
        "chromium": [home / ".config/chromium", home / ".config/google-chrome"],
        "edge": [home / ".config/microsoft-edge", home / ".config/microsoft-edge-beta"],
        "firefox": [home / ".mozilla/firefox"],
    }

    detected: list[str] = []
    for browser in browsers:
        browser_key = browser.split(":", 1)[0].strip().lower()
        candidate_dirs = browser_dirs.get(browser_key)
        if not candidate_dirs:
            detected.append(browser)
            continue
        if any(path.exists() for path in candidate_dirs):
            detected.append(browser)
    return detected


def describe_youtube_auth_source() -> str | None:
    """Human-readable description of available YouTube auth."""
    auth_mode = (os.getenv("YT_DLP_AUTH_MODE") or "").strip().lower()
    cookies = youtube_cookie_files()
    if auth_mode == "browser":
        browsers = youtube_cookies_from_browser()
        if browsers:
            return f"browser cookies: {', '.join(browsers)}"
        if cookies:
            if len(cookies) == 1:
                return f"browser cookies unavailable; cookie file fallback: {cookies[0]}"
            return f"browser cookies unavailable; cookie file fallback: {', '.join(cookies)}"
    if cookies:
        if len(cookies) == 1:
            return f"cookie file: {cookies[0]}"
        return f"cookie files: {', '.join(cookies)}"
    return None


def yt_dlp_auth_mode() -> str:
    """Return yt-dlp auth mode: auto, cookies, or browser."""
    raw = (os.getenv("YT_DLP_AUTH_MODE") or os.getenv("YT_TRANSCRIPT_YT_DLP_AUTH_MODE") or "auto").strip().lower()
    if raw not in {"auto", "cookies", "browser"}:
        return "auto"
    return raw


def yt_dlp_auth_args(cookie_file: str | None = None, rotate: bool = False) -> list[str]:
    """Return yt-dlp arguments for auth (cookies or browser).

    Auth mode can be forced with YT_DLP_AUTH_MODE:
    - auto: prefer cookie files, fallback to browser
    - cookies: only cookie files
    - browser: only browser cookies
    """
    args: list[str] = []
    auth_mode = yt_dlp_auth_mode()
    cookie_path = str(cookie_file or "").strip()
    browser_candidates = youtube_cookies_from_browser()

    if auth_mode == "browser":
        if browser_candidates:
            args.extend(["--cookies-from-browser", browser_candidates[0]])
            return args
        if not cookie_path:
            cookie_path = youtube_next_cookie_file() if rotate else youtube_cookie_file()
        if cookie_path:
            args.extend(["--cookies", cookie_path])
        return args

    if not cookie_path:
        cookie_path = youtube_next_cookie_file() if rotate else youtube_cookie_file()
    if auth_mode in {"auto", "cookies"} and cookie_path:
        args.extend(["--cookies", cookie_path])
    elif auth_mode == "auto" and browser_candidates:
        args.extend(["--cookies-from-browser", browser_candidates[0]])
    return args


def yt_dlp_command() -> list[str]:
    """Return an executable command for yt-dlp that works in this repo.

    Prefer an explicit override, then PATH, then the repo venv, and finally the
    current Python module invocation as a last-resort fallback.
    """
    explicit = (os.getenv("YT_DLP_BIN") or "").strip()
    candidates = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.append(Path(sys.executable))
    path_bin = shutil.which("yt-dlp")
    if path_bin:
        candidates.append(Path(path_bin))

    for candidate in candidates:
        try:
            if candidate.exists() and os.access(candidate, os.X_OK):
                if candidate.name == Path(sys.executable).name:
                    return [str(candidate), "-m", "yt_dlp"]
                return [str(candidate)]
        except Exception:
            continue

    return [sys.executable, "-m", "yt_dlp"]


def fetch_youtube_upload_date(video_url: str, timeout_seconds: int = 45) -> str:
    """Fetch upload date from YouTube using yt_dlp.
    
    Args:
        video_url: YouTube video URL or video ID
        timeout_seconds: Request timeout in seconds
    
    Returns:
        Upload date as string in YYYY-MM-DD format
    
    Raises:
        RuntimeError: If unable to fetch upload date
    """
    import yt_dlp
    
    # If video_url is just a video ID, construct the full URL
    video_id = str(video_url).strip()
    if not video_id.startswith("http"):
        video_id = f"https://www.youtube.com/watch?v={video_id}"
    
    cookie_files = youtube_cookie_files()
    attempts = [None]
    if cookie_files:
        start_cookie = youtube_next_cookie_file()
        if start_cookie and start_cookie in cookie_files:
            start_index = cookie_files.index(start_cookie)
            attempts = cookie_files[start_index:] + cookie_files[:start_index]
        else:
            attempts = list(cookie_files)

    last_error: Exception | None = None
    for cookie_file in attempts:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "socket_timeout": timeout_seconds,
            "extractor_args": {
                "youtube": {"skip": ["hls", "dash"]}
            },
        }
        if cookie_file:
            ydl_opts["cookiefile"] = cookie_file

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_id, download=False)
                if not info:
                    raise RuntimeError(f"Unable to extract info from {video_id}")

                # Try to get upload_date in YYYYMMDD format
                upload_date = info.get("upload_date")
                if upload_date:
                    # Convert YYYYMMDD to YYYY-MM-DD
                    date_str = str(upload_date)
                    if len(date_str) == 8:
                        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    return date_str

                # Fallback to timestamp
                timestamp = info.get("timestamp")
                if timestamp:
                    from datetime import datetime, timezone
                    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
                    return dt.strftime("%Y-%m-%d")

                raise RuntimeError(f"No upload date found for {video_id}")
        except Exception as exc:
            last_error = exc
            continue

    if last_error is None:
        raise RuntimeError(f"Failed to fetch upload date: {video_id}")
    raise RuntimeError(f"Failed to fetch upload date: {last_error}") from last_error


def youtube_api_key_pool() -> list[str]:
    """Return list of YouTube Data API v3 keys from env."""
    env_map = _load_dotenv_map()
    keys: list[str] = []
    # Comma-separated YOUTUBE_API_KEYS  (key=label format)
    raw = (env_map.get("YOUTUBE_API_KEYS") or "").strip()
    if raw:
        for entry in raw.split(","):
            entry = entry.strip()
            if "=" in entry:
                keys.append(entry.split("=", 1)[1].strip())
            elif entry:
                keys.append(entry)
    # Individual keys
    for name in ("YOUTUBE_API_KEY", "YOUTUBE_API_KEY_ITUAJA", "YOUTUBE_API_KEY_SILFI", "YOUTUBE_API_KEY_ALBERT"):
        k = (env_map.get(name) or os.getenv(name) or "").strip()
        if k and k not in keys:
            keys.append(k)
    return keys
