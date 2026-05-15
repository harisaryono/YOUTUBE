"""
Audio storage guard for ASR download/cache.

This is a hard safety guard: audio_download must not keep downloading while ASR
is stalled and local audio/chunk storage is already too large.

CLI examples:
  python -m orchestrator.audio_guard status --audio-dir uploads/audio --max-gb 5
  python -m orchestrator.audio_guard check --audio-dir uploads/audio --max-gb 5

Exit codes:
  0 = allowed / under limit
  75 = temporary failure / over limit, caller should stop or defer
  2 = invalid arguments
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEMPFAIL_EXIT_CODE = 75


def resolve_audio_dir(value: str | None = None) -> Path:
    raw = str(value or os.getenv("ASR_AUDIO_DIR") or os.getenv("YT_ASR_AUDIO_DIR") or "uploads/audio").strip()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for filename in files:
            file_path = Path(root) / filename
            try:
                if file_path.is_file():
                    total += int(file_path.stat().st_size)
            except OSError:
                continue
    return total


def human_size(value: int) -> str:
    size = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def audio_guard_status(audio_dir: str | Path | None = None, *, max_gb: float = 5.0, warn_gb: float | None = None) -> dict[str, Any]:
    path = resolve_audio_dir(str(audio_dir or ""))
    size = dir_size_bytes(path)
    max_bytes = int(float(max_gb or 0) * 1024 ** 3)
    warn_bytes = int(float(warn_gb if warn_gb is not None else max(float(max_gb or 0) * 0.8, 0)) * 1024 ** 3)
    over_limit = bool(max_bytes > 0 and size >= max_bytes)
    over_warning = bool(warn_bytes > 0 and size >= warn_bytes)
    return {
        "audio_dir": str(path),
        "exists": path.exists(),
        "size_bytes": size,
        "size": human_size(size),
        "max_gb": float(max_gb or 0),
        "max_bytes": max_bytes,
        "max_size": human_size(max_bytes),
        "warn_gb": float(warn_gb if warn_gb is not None else max(float(max_gb or 0) * 0.8, 0)),
        "warn_bytes": warn_bytes,
        "over_warning": over_warning,
        "over_limit": over_limit,
        "allowed": not over_limit,
    }


def _cmd_status(args: argparse.Namespace) -> int:
    payload = audio_guard_status(args.audio_dir, max_gb=float(args.max_gb), warn_gb=args.warn_gb)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    payload = audio_guard_status(args.audio_dir, max_gb=float(args.max_gb), warn_gb=args.warn_gb)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if payload["over_limit"]:
        return TEMPFAIL_EXIT_CODE
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Guard ASR audio cache size")
    sub = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("status", "Print audio cache usage"),
        ("check", "Exit non-zero if audio cache is over limit"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--audio-dir", default="", help="Audio cache directory; defaults to ASR_AUDIO_DIR or uploads/audio")
        p.add_argument("--max-gb", type=float, default=float(os.getenv("ASR_AUDIO_MAX_GB", "5") or 5))
        p.add_argument("--warn-gb", type=float, default=None)
        p.set_defaults(func=_cmd_check if name == "check" else _cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
