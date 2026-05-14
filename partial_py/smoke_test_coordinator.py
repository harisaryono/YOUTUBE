#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
from datetime import datetime, timezone
from typing import Any

from local_services import (
    coordinator_acquire_specific_account,
    coordinator_release_lease,
    coordinator_status_accounts,
)

PRESET_TARGETS: dict[str, dict[str, str]] = {
    "nvidia": {"provider": "nvidia", "model_name": "openai/gpt-oss-120b"},
    "groq": {"provider": "groq", "model_name": "moonshotai/kimi-k2-instruct"},
    "cerebras": {"provider": "cerebras", "model_name": "qwen-3-235b-a22b-instruct-2507"},
}


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] {message}", flush=True)


def is_leaseable_account(item: dict[str, Any]) -> bool:
    if "leaseable" in item:
        return bool(item.get("leaseable"))
    state = str(item.get("state") or "").strip().lower()
    return state in {"idle", "leaseable"}


def select_account(
    accounts: list[dict[str, Any]],
    *,
    provider_account_id: int,
    provider: str,
    model_name: str,
) -> dict[str, Any]:
    provider = provider.strip().lower()
    model_name = model_name.strip()
    for item in accounts:
        item_id = int(item.get("provider_account_id") or 0)
        item_provider = str(item.get("provider") or "").strip().lower()
        item_model = str(item.get("runtime_model_name") or item.get("default_model_name") or "").strip()
        is_active = int(item.get("is_active") or 0) == 1
        if not is_active:
            continue
        if provider_account_id > 0 and item_id != provider_account_id:
            continue
        if provider and item_provider != provider:
            continue
        if model_name and item_model != model_name:
            continue
        if is_leaseable_account(item):
            return item
    raise RuntimeError("Tidak ada account aktif dan leaseable yang cocok untuk smoke test.")


def case_prefix(case_label: str) -> str:
    return f"[{case_label.upper()}]"


def run_smoke_case(
    *,
    case_label: str,
    provider: str,
    model_name: str,
    provider_account_id: int,
    include_inactive: bool,
    lease_ttl_seconds: int,
    require_model_limits: bool,
) -> None:
    prefix = case_prefix(case_label)
    accounts = coordinator_status_accounts(
        provider=provider or None,
        model_name=model_name or None,
        include_inactive=include_inactive,
    )
    if not accounts:
        raise RuntimeError(f"{prefix} status/accounts tidak mengembalikan account apa pun.")

    selected = select_account(
        accounts,
        provider_account_id=int(provider_account_id or 0),
        provider=str(provider or ""),
        model_name=str(model_name or ""),
    )
    selected_provider_account_id = int(selected.get("provider_account_id") or 0)
    selected_provider = str(selected.get("provider") or "").strip()
    selected_model_name = str(
        selected.get("runtime_model_name") or selected.get("default_model_name") or model_name or ""
    ).strip()
    if selected_provider_account_id <= 0 or not selected_provider or not selected_model_name:
        raise RuntimeError(f"{prefix} account terpilih tidak punya provider/model yang valid.")

    log(
        f"{prefix} Status selected: "
        + json.dumps(
            {
                "provider_account_id": selected_provider_account_id,
                "provider": selected_provider,
                "account_name": selected.get("account_name", ""),
                "model_name": selected_model_name,
                "state": selected.get("state", ""),
                "is_active": selected.get("is_active", 0),
            },
            ensure_ascii=False,
        )
    )

    lease_token = ""
    host = socket.gethostname()
    pid = os.getpid()
    try:
        lease = coordinator_acquire_specific_account(
            provider_account_id=selected_provider_account_id,
            model_name=selected_model_name,
            holder=host,
            host=host,
            pid=pid,
            task_type=f"coordinator_smoke_test_{case_label}",
            lease_ttl_seconds=int(lease_ttl_seconds or 300),
        )
        if not lease:
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan data lease.")

        lease_token = str(lease.get("lease_token") or "").strip()
        api_key = str(lease.get("api_key") or "").strip()
        usage_method = str(lease.get("usage_method") or "").strip()
        endpoint_url = str(lease.get("endpoint_url") or "").strip()
        extra_headers = lease.get("extra_headers")
        model_limits = lease.get("model_limits")

        if not lease_token:
            raise RuntimeError(f"{prefix} Lease token kosong.")
        if not api_key or api_key.startswith("ENC:"):
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan api_key plaintext.")
        if not usage_method:
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan usage_method.")
        if not endpoint_url:
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan endpoint_url.")
        if not isinstance(extra_headers, dict):
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan extra_headers dalam bentuk dict.")
        if not isinstance(model_limits, dict):
            raise RuntimeError(f"{prefix} Acquire lease tidak mengembalikan model_limits dalam bentuk dict.")
        if require_model_limits and not model_limits:
            raise RuntimeError(f"{prefix} Acquire lease mengembalikan model_limits kosong.")

        log(
            f"{prefix} Lease acquired: "
            + json.dumps(
                {
                    "provider_account_id": selected_provider_account_id,
                    "provider": selected_provider,
                    "model_name": selected_model_name,
                    "lease_token": lease_token,
                    "usage_method": usage_method,
                    "endpoint_url": endpoint_url,
                    "extra_headers_keys": sorted(extra_headers.keys()),
                    "model_limits": model_limits,
                },
                ensure_ascii=False,
            )
        )
    finally:
        if lease_token:
            try:
                coordinator_release_lease(lease_token, final_state="idle", note="coordinator smoke test")
            except Exception as exc:
                raise SystemExit(f"{prefix} Release lease gagal: {exc}") from exc

    log(f"{prefix} Coordinator smoke test selesai tanpa error.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test untuk coordinator status/accounts dan lease acquire.")
    parser.add_argument(
        "--preset",
        choices=("nvidia", "groq", "cerebras", "all"),
        default="",
        help="Jalankan preset target yang sudah dipilih. 'all' menjalankan NVIDIA, Groq, dan Cerebras berurutan.",
    )
    parser.add_argument("--provider", default="", help="Filter provider, misalnya nvidia atau groq.")
    parser.add_argument("--model-name", default="", help="Filter model tertentu untuk diuji.")
    parser.add_argument("--provider-account-id", type=int, default=0, help="Uji account spesifik jika sudah tahu ID-nya.")
    parser.add_argument("--include-inactive", action="store_true", help="Sertakan account inactive saat membaca status.")
    parser.add_argument("--lease-ttl-seconds", type=int, default=300, help="TTL lease yang dipakai saat smoke test.")
    parser.add_argument(
        "--require-model-limits",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Gagal jika bundle lease tidak membawa model_limits.",
    )
    args = parser.parse_args()

    if args.preset and (args.provider or args.model_name or int(args.provider_account_id or 0)):
        raise SystemExit("--preset tidak boleh digabung dengan --provider, --model-name, atau --provider-account-id.")

    preset_names = [args.preset] if args.preset and args.preset != "all" else []
    if args.preset == "all":
        preset_names = ["nvidia", "groq", "cerebras"]

    failures: list[str] = []
    if preset_names:
        for preset_name in preset_names:
            preset = PRESET_TARGETS[preset_name]
            try:
                run_smoke_case(
                    case_label=preset_name,
                    provider=preset["provider"],
                    model_name=preset["model_name"],
                    provider_account_id=0,
                    include_inactive=args.include_inactive,
                    lease_ttl_seconds=args.lease_ttl_seconds,
                    require_model_limits=args.require_model_limits,
                )
            except Exception as exc:
                failures.append(f"{preset_name}: {exc}")
                log(f"[{preset_name.upper()}] FAIL: {exc}")
        if failures:
            raise SystemExit("; ".join(failures))
        return 0

    run_smoke_case(
        case_label="single",
        provider=str(args.provider or ""),
        model_name=str(args.model_name or ""),
        provider_account_id=int(args.provider_account_id or 0),
        include_inactive=args.include_inactive,
        lease_ttl_seconds=args.lease_ttl_seconds,
        require_model_limits=args.require_model_limits,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
