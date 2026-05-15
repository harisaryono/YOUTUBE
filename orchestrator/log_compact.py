"""
Fast raw-log compactor for orchestrator run directories.

Purpose:
- Shrink huge runs/orchestrator logs quickly without deleting evidence.
- Compression is reversible and can be done before daily archive markers exist.
- Prefer zstd level 1 for fast compression and fast reading; fallback to gzip.

Typical use:
  python -m orchestrator.log_compact compact --older-than-hours 1 --method auto
  python -m orchestrator.log_compact tail --path runs/orchestrator/.../stdout_stderr.log.zst --lines 120
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config
from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs" / "orchestrator"
LOG_NAMES = ("stdout_stderr.log",)
COMPRESSED_SUFFIXES = (".zst", ".gz")


def _connect(state: OrchestratorState) -> sqlite3.Connection:
    return state._connect()  # noqa: SLF001 - internal orchestration utility


def _zstd_available() -> bool:
    return shutil.which("zstd") is not None


def _choose_method(method: str) -> str:
    method = str(method or "auto").strip().lower()
    if method == "auto":
        return "zstd" if _zstd_available() else "gzip"
    if method == "zstd" and not _zstd_available():
        return "gzip"
    if method not in {"zstd", "gzip"}:
        return "zstd" if _zstd_available() else "gzip"
    return method


def _running_run_dirs(state: OrchestratorState) -> set[str]:
    try:
        rows = state.list_running_jobs()
    except Exception:
        return set()
    result: set[str] = set()
    for row in rows:
        raw = str(row.get("run_dir") or "").strip()
        if not raw:
            continue
        try:
            result.add(str(Path(raw).resolve()))
        except Exception:
            result.add(raw)
    return result


def _find_job_by_log_path(state: OrchestratorState, path: Path) -> dict[str, Any] | None:
    target_variants = {str(path), str(path.resolve())}
    if path.suffix in COMPRESSED_SUFFIXES:
        original = Path(str(path)[: -len(path.suffix)])
        target_variants.add(str(original))
        try:
            target_variants.add(str(original.resolve()))
        except Exception:
            pass
    conn = _connect(state)
    rows = conn.execute(
        """
        SELECT *
        FROM orchestrator_active_jobs
        WHERE log_path != ''
        ORDER BY started_at DESC
        """
    ).fetchall()
    for row in rows:
        item = dict(row)
        raw = str(item.get("log_path") or "").strip()
        variants = {raw}
        try:
            variants.add(str(Path(raw).resolve()))
        except Exception:
            pass
        if variants & target_variants:
            return item
    return None


def _find_job_by_id(state: OrchestratorState, job_id: str) -> dict[str, Any] | None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return None
    try:
        return state.get_job(job_id)
    except Exception:
        return None


def _candidate_log_paths(run_dir: Path) -> list[Path]:
    paths: list[Path] = []
    for name in LOG_NAMES:
        path = run_dir / name
        if path.exists() and path.is_file():
            paths.append(path)
    return paths


def _has_compressed_sibling(path: Path) -> bool:
    return any(path.with_suffix(path.suffix + suffix).exists() for suffix in COMPRESSED_SUFFIXES)


def _age_hours(path: Path) -> float:
    try:
        return (time.time() - path.stat().st_mtime) / 3600
    except FileNotFoundError:
        return 0.0


def _size_bytes(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except FileNotFoundError:
        return 0


def _write_marker(run_dir: Path, payload: dict[str, Any]) -> None:
    marker = run_dir / ".log_compact.json"
    old: dict[str, Any] = {}
    if marker.exists():
        try:
            old = json.loads(marker.read_text(encoding="utf-8"))
            if not isinstance(old, dict):
                old = {}
        except Exception:
            old = {}
    entries = list(old.get("entries") or [])
    entries.append(payload)
    old["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    old["entries"] = entries[-50:]
    try:
        marker.write_text(json.dumps(old, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _compress_zstd(path: Path, *, level: int = 1, delete_original: bool = True) -> tuple[bool, str]:
    dest = path.with_suffix(path.suffix + ".zst")
    if dest.exists():
        if delete_original:
            path.unlink(missing_ok=True)
        return False, "already_compressed"
    cmd = ["zstd", f"-{max(1, min(int(level or 1), 19))}", "--threads=0", "--force", "--quiet", "-o", str(dest), str(path)]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if _size_bytes(dest) <= 0:
            dest.unlink(missing_ok=True)
            return False, "empty_compressed_file"
        shutil.copystat(path, dest)
        if delete_original:
            path.unlink(missing_ok=True)
        return True, "compressed"
    except Exception as exc:
        dest.unlink(missing_ok=True)
        return False, f"zstd_failed:{exc}"


def _compress_gzip(path: Path, *, level: int = 3, delete_original: bool = True) -> tuple[bool, str]:
    dest = path.with_suffix(path.suffix + ".gz")
    if dest.exists():
        if delete_original:
            path.unlink(missing_ok=True)
        return False, "already_compressed"
    try:
        with path.open("rb") as src, gzip.open(dest, "wb", compresslevel=max(1, min(int(level or 3), 9))) as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        if _size_bytes(dest) <= 0:
            dest.unlink(missing_ok=True)
            return False, "empty_compressed_file"
        shutil.copystat(path, dest)
        if delete_original:
            path.unlink(missing_ok=True)
        return True, "compressed"
    except Exception as exc:
        dest.unlink(missing_ok=True)
        return False, f"gzip_failed:{exc}"


def _compress_file(path: Path, *, method: str, level: int, delete_original: bool) -> tuple[bool, str, Path]:
    chosen = _choose_method(method)
    if chosen == "zstd":
        ok, msg = _compress_zstd(path, level=level, delete_original=delete_original)
        return ok, msg, path.with_suffix(path.suffix + ".zst")
    ok, msg = _compress_gzip(path, level=min(max(int(level or 3), 1), 9), delete_original=delete_original)
    return ok, msg, path.with_suffix(path.suffix + ".gz")


def compact_raw_logs(
    config: dict[str, Any],
    state: OrchestratorState,
    *,
    older_than_hours: float = 1.0,
    min_size_kb: int = 64,
    method: str = "auto",
    level: int = 1,
    limit: int = 0,
    include_unarchived: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compress raw stdout_stderr.log files in-place.

    This does not delete run directories and does not require daily archive
    markers. It only replaces large text logs with .zst/.gz after a verified
    compressed file is created.
    """
    chosen_method = _choose_method(method)
    running = _running_run_dirs(state)
    result: dict[str, Any] = {
        "success": True,
        "method": chosen_method,
        "dry_run": dry_run,
        "scanned_dirs": 0,
        "scanned_logs": 0,
        "compressed": 0,
        "already_compressed": 0,
        "skipped_running": 0,
        "skipped_young": 0,
        "skipped_small": 0,
        "skipped_unarchived": 0,
        "errors": [],
        "bytes_before": 0,
        "bytes_after_estimated": 0,
    }
    if not RUNS_DIR.exists():
        return result
    max_items = max(0, int(limit or 0))

    for run_dir in RUNS_DIR.iterdir():
        if max_items and result["compressed"] >= max_items:
            break
        if not run_dir.is_dir() or run_dir.name == "reports":
            continue
        result["scanned_dirs"] += 1
        try:
            resolved_run_dir = str(run_dir.resolve())
        except Exception:
            resolved_run_dir = str(run_dir)
        if resolved_run_dir in running:
            result["skipped_running"] += 1
            continue
        if not include_unarchived and not (run_dir / ".log_archive.json").exists():
            result["skipped_unarchived"] += 1
            continue
        for log_path in _candidate_log_paths(run_dir):
            if max_items and result["compressed"] >= max_items:
                break
            result["scanned_logs"] += 1
            if _has_compressed_sibling(log_path):
                result["already_compressed"] += 1
                continue
            age = _age_hours(log_path)
            if age < float(older_than_hours or 0):
                result["skipped_young"] += 1
                continue
            size = _size_bytes(log_path)
            if size < max(0, int(min_size_kb or 0)) * 1024:
                result["skipped_small"] += 1
                continue
            result["bytes_before"] += size
            if dry_run:
                result["compressed"] += 1
                continue
            ok, msg, dest = _compress_file(log_path, method=chosen_method, level=level, delete_original=True)
            if ok or msg == "already_compressed":
                result["compressed"] += 1 if ok else 0
                result["already_compressed"] += 1 if msg == "already_compressed" else 0
                after = _size_bytes(dest)
                result["bytes_after_estimated"] += after
                _write_marker(
                    run_dir,
                    {
                        "compressed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "source": str(log_path),
                        "compressed": str(dest),
                        "method": chosen_method,
                        "bytes_before": size,
                        "bytes_after": after,
                    },
                )
            else:
                result["errors"].append({"path": str(log_path), "error": msg})
    if not dry_run:
        state.add_event(
            event_type="log_compact",
            message=(
                f"Raw log compact completed: compressed={result['compressed']} "
                f"method={chosen_method} before={result['bytes_before']} after={result['bytes_after_estimated']}"
            ),
            severity="info" if not result["errors"] else "warning",
            payload=result,
        )
    return result


def _open_text_lines(path: Path):
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            yield from fh
        return
    if path.suffix == ".zst":
        if not _zstd_available():
            raise RuntimeError("zstd command not found; install package 'zstd' to read .zst logs")
        proc = subprocess.Popen(["zstd", "-dcq", str(path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="ignore")
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                yield line
        finally:
            proc.stdout.close()
            proc.wait(timeout=30)
        return
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        yield from fh


def _resolve_log_path(state: OrchestratorState, *, path: str = "", job_id: str = "") -> Path:
    if job_id:
        job = _find_job_by_id(state, job_id)
        if not job:
            raise FileNotFoundError(f"job not found: {job_id}")
        raw = str(job.get("log_path") or "").strip()
        if not raw:
            raise FileNotFoundError(f"job has no log_path: {job_id}")
        base = Path(raw)
    else:
        base = Path(str(path or "").strip())
    if not base.is_absolute():
        base = PROJECT_ROOT / base
    if base.exists():
        return base
    for suffix in COMPRESSED_SUFFIXES:
        candidate = Path(str(base) + suffix)
        if candidate.exists():
            return candidate
    raise FileNotFoundError(str(base))


def tail_log(state: OrchestratorState, *, path: str = "", job_id: str = "", lines: int = 120) -> dict[str, Any]:
    log_path = _resolve_log_path(state, path=path, job_id=job_id)
    q: deque[str] = deque(maxlen=max(1, int(lines or 120)))
    for line in _open_text_lines(log_path):
        q.append(line.rstrip("\n"))
    return {"path": str(log_path), "lines": list(q)}


def _cmd_compact(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    state = OrchestratorState()
    try:
        result = compact_raw_logs(
            config,
            state,
            older_than_hours=float(args.older_than_hours),
            min_size_kb=int(args.min_size_kb),
            method=str(args.method),
            level=int(args.level),
            limit=int(args.limit or 0),
            include_unarchived=not bool(args.archived_only),
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0 if result.get("success") else 1
    finally:
        state.close()


def _cmd_tail(args: argparse.Namespace) -> int:
    state = OrchestratorState()
    try:
        result = tail_log(state, path=str(args.path or ""), job_id=str(args.job_id or ""), lines=int(args.lines or 120))
        print(f"==> {result['path']} <==")
        for line in result["lines"]:
            print(line)
        return 0
    finally:
        state.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast raw log compression and reading")
    parser.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("compact", help="Compress raw stdout_stderr.log files in-place")
    p.add_argument("--older-than-hours", type=float, default=1.0)
    p.add_argument("--min-size-kb", type=int, default=64)
    p.add_argument("--method", choices=["auto", "zstd", "gzip"], default="auto")
    p.add_argument("--level", type=int, default=1, help="zstd level 1-19 or gzip level 1-9")
    p.add_argument("--limit", type=int, default=0, help="Max files to compress; 0 = no limit")
    p.add_argument("--archived-only", action="store_true", help="Only compact dirs already marked by daily archive")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_compact)

    p = sub.add_parser("tail", help="Tail a plain/.gz/.zst log by path or job id")
    p.add_argument("--path", default="")
    p.add_argument("--job-id", default="")
    p.add_argument("--lines", type=int, default=120)
    p.set_defaults(func=_cmd_tail)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
