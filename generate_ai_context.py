#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
OUT_DIR = REPO_ROOT / "AI_CONTEXT"

TEXT_EXTS = {
    ".py", ".sh", ".md", ".txt", ".json", ".yml", ".yaml", ".html", ".css",
    ".js", ".toml", ".ini", ".cfg", ".sql", ".csv"
}

EXCLUDE_DIRS = {
    ".git", "__pycache__", "AI_CONTEXT", "runs", "uploads", "pending_updates",
    "logs", "tmp", "snap", "downloads", "artifacts", "backups"
}

ROOT_FILES = [
    "README.md",
    "AGENTS.md",
    "database_optimized.py",
    "database_blobs.py",
    "recover_transcripts.py",
    "recover_transcripts_from_csv.py",
    "recover_asr_transcripts.py",
    "launch_resume_queue.py",
    "fill_missing_resumes_youtube_db.py",
    "format_transcripts_pool.py",
    "update_latest_channel_videos.py",
    "manage_database.py",
    "local_services.py",
    "provider_encryption.py",
    "job_tracker.py",
    "savesubs_playwright.py",
    "wsgi.py",
    "passenger_wsgi.py",
]

IMPORTANT_DIRS = [
    "docs",
    "scripts",
    "flask_app",
    "partial_py",
    "partial_docs",
    "partial_ops",
    "webapp",
]

IMPORTANT_DOCS = [
    "docs/README.md",
    "docs/WORKFLOWS.md",
    "docs/PROGRESS.md",
    "docs/VERIFY.md",
    "docs/DB_DIET_AUDIT.md",
    "docs/FTS_MIGRATION_PLAN.md",
    "docs/STATE_MACHINE.md",
    "docs/LOCAL_SETUP_GUIDE.md",
    "scripts/README.md",
    "README.md",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTS


def iter_repo_files() -> list[Path]:
    files: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if path.is_dir():
            continue
        rel = path.relative_to(REPO_ROOT)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if path.is_symlink():
            files.append(path)
            continue
        if is_text_file(path):
            files.append(path)
    return sorted(files, key=lambda p: str(p.relative_to(REPO_ROOT)))


def read_preview(path: Path, max_lines: int = 12, max_chars: int = 900) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"[unreadable: {exc}]"
    lines = text.splitlines()
    snippet = "\n".join(lines[:max_lines]).strip()
    if len(snippet) > max_chars:
        snippet = snippet[:max_chars].rstrip() + "..."
    return snippet


def summarize_file(path: Path) -> dict:
    rel = str(path.relative_to(REPO_ROOT))
    stat = path.lstat() if path.is_symlink() else path.stat()
    info = {
        "path": rel,
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "is_symlink": path.is_symlink(),
    }
    if path.is_symlink():
        info["target"] = os.readlink(path)
    if path.is_file() and not path.is_symlink():
        info["sha256"] = sha256_file(path)
    return info


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def collect_directory_summary(dir_name: str) -> str:
    path = REPO_ROOT / dir_name
    if not path.exists():
        return f"- `{dir_name}`: missing"
    count = sum(1 for p in path.rglob("*") if p.is_file())
    return f"- `{dir_name}`: {count} files"


def build_file_index(files: list[Path]) -> str:
    grouped: dict[str, list[Path]] = {}
    for f in files:
        rel = f.relative_to(REPO_ROOT)
        top = rel.parts[0] if rel.parts else "."
        grouped.setdefault(top, []).append(f)

    lines = ["# File Index", ""]
    for top in sorted(grouped):
        lines.append(f"## {top}")
        for f in grouped[top]:
            rel = f.relative_to(REPO_ROOT)
            if rel.parts[0] in EXCLUDE_DIRS:
                continue
            if rel.name == "generate_ai_context.py":
                continue
            if rel.parts[0] == "docs" and str(rel) not in IMPORTANT_DOCS:
                continue
            if rel.parts[0] == "scripts" and rel.name.startswith("migrate_") is False and rel.name not in {
                "README.md", "run_pipeline.sh", "run_flask.sh", "discover.sh", "transcript.sh",
                "manual_transcript_then_resume_format.sh", "resume.sh", "format.sh", "audio.sh",
                "asr.sh", "supervisor.sh", "generate_tasks.py", "repair_db_state.py",
                "refresh_stats.py", "search.sh", "monitor_pipeline.sh",
                "migrate_search_cache.py", "migrate_metadata_to_blob.py",
                "backfill_text_blobs.py", "backfill_transcript_files_to_blob.py",
                "cleanup_redundant_transcript_files.py", "cleanup_redundant_summary_files.py",
                "update_db_from_tar.py", "verify_blobs.py", "verify_fix.py",
                "audit_no_subtitle_webshare.py", "sync_buffer_to_main.py",
                "sync_missing_rows_to_server.sh", "archive_channels.sh", "deploy_exclude.txt",
            }:
                continue
            size = f.stat().st_size
            lines.append(f"- `{rel}` ({size} bytes)")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_journal(files: list[Path]) -> str:
    lines = ["# File Journal", ""]
    preview_targets = [
        "README.md",
        "docs/WORKFLOWS.md",
        "docs/PROGRESS.md",
        "docs/FTS_MIGRATION_PLAN.md",
        "scripts/README.md",
        "scripts/manual_transcript_then_resume_format.sh",
        "scripts/migrate_search_cache.py",
        "scripts/generate_tasks.py",
        "scripts/format.sh",
        "flask_app/app.py",
        "flask_app/templates/video_detail.html",
        "database_optimized.py",
    ]
    for rel in preview_targets:
        path = REPO_ROOT / rel
        if not path.exists():
            continue
        lines.append(f"## `{rel}`")
        lines.append(f"- size: {path.stat().st_size} bytes")
        lines.append(f"- sha256: `{sha256_file(path)[:16]}`")
        preview = read_preview(path)
        lines.append("```text")
        lines.append(preview)
        lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_project_state() -> str:
    lines = ["# Project State", ""]
    lines.append(f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append("- repo_type: YouTube transcript / resume / format / ASR pipeline")
    lines.append("- active web app: Flask under `flask_app/app.py`")
    lines.append("- database layout: active DBs are under `db/` with root symlinks for compat")
    lines.append("- search: migrated to `videos_search_cache` + `videos_search_fts`")
    lines.append("- manual transcript chain: `manual transcript -> resume -> format`")
    lines.append("- transcript/summary content: blob-first, file artifacts cleaned where safe")
    lines.append("- discovery: channel/video ingest and repair utilities remain in `scripts/` and `partial_py/`")
    lines.append("")
    lines.append("## Directory Snapshot")
    for d in IMPORTANT_DIRS:
        lines.append(collect_directory_summary(d))
    lines.append("")
    lines.append("## Root Files")
    for rel in ROOT_FILES:
        path = REPO_ROOT / rel
        if path.exists():
            lines.append(f"- `{rel}` ({path.stat().st_size} bytes)")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_changelog() -> str:
    lines = ["# Repo Changelog", ""]
    lines.extend(
        [
            "- DB moved to `db/` with symlink compatibility at the repo root.",
            "- Manual web flow now chains transcript -> resume -> format automatically.",
            "- Search FTS moved to blob-first cache `videos_search_cache` + `videos_search_fts`.",
            "- Transcript/summary files under `uploads/*/text/` were cleaned once mirrored safely into blobs.",
            "- ASR and transcript wrapper logic now use lease coordinator aware fallback and cache reuse.",
        ]
    )
    lines.append("")
    return "\n".join(lines)


def build_handoff() -> str:
    return f"""# AI_CONTEXT Handoff

Generated: `{datetime.now(timezone.utc).isoformat()}`

## What to open first

1. [`docs/WORKFLOWS.md`](../docs/WORKFLOWS.md)
2. [`docs/PROGRESS.md`](../docs/PROGRESS.md)
3. [`scripts/README.md`](../scripts/README.md)
4. [`database_optimized.py`](../database_optimized.py)
5. [`flask_app/app.py`](../flask_app/app.py)

## Current shape

- Web UI is Flask-based.
- DB is blob-first for transcript/summary content.
- Search uses `videos_search_cache` + `videos_search_fts`.
- Manual download flows into resume and formatting automatically.
- Shell entrypoints live under `scripts/`.

## Important constraints

- Do not reintroduce `videos_fts` or search paths that depend on `videos.transcript_text`.
- Keep manual transcript jobs from being double-submitted.
- Treat large data dirs (`runs/`, `uploads/`, `logs/`, `tmp/`) as runtime artifacts, not source of truth.

## Notes for AI readers

- The repo has many legacy compatibility files in `partial_py/`, `partial_docs/`, and `partial_ops/`.
- Use `docs/README.md` as the index, then `docs/WORKFLOWS.md` for operational flow.
"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files = iter_repo_files()
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "file_count": len(files),
        "root_files": [rel for rel in ROOT_FILES if (REPO_ROOT / rel).exists()],
        "important_dirs": IMPORTANT_DIRS,
        "files": [summarize_file(path) for path in files[:4000]],
        "db_files": [
            summarize_file(path)
            for path in [
                REPO_ROOT / "db" / "youtube_transcripts.db",
                REPO_ROOT / "db" / "youtube_transcripts_blobs.db",
                REPO_ROOT / "db" / "youtube_cache.db",
                REPO_ROOT / "db" / "provider_accounts.sqlite3",
                REPO_ROOT / "db" / "youtube.db",
            ]
            if path.exists()
        ],
    }

    write_markdown(OUT_DIR / "LATEST_HANDOFF.md", build_handoff())
    write_markdown(OUT_DIR / "FILE_INDEX.md", build_file_index(files))
    write_markdown(OUT_DIR / "FILE_JOURNAL.md", build_journal(files))
    write_markdown(OUT_DIR / "PROJECT_STATE.md", build_project_state())
    write_markdown(OUT_DIR / "REPO_CHANGELOG.md", build_changelog())
    write_markdown(
        OUT_DIR / "GDRIVE_HANDOFF.md",
        "# Google Drive Hand-off\n\n- AI_CONTEXT generated locally from repo files.\n- Use `LATEST_HANDOFF.md` as the first read.\n- Source-of-truth docs: `docs/README.md`, `docs/WORKFLOWS.md`, `docs/PROGRESS.md`.\n",
    )
    (OUT_DIR / "repo_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"AI_CONTEXT generated in {OUT_DIR}")


if __name__ == "__main__":
    main()
