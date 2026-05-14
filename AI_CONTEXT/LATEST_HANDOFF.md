# AI_CONTEXT Handoff

Generated: `2026-05-14T12:06:40.607209+00:00`

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
- Orchestrator daemon: pipeline controller (discovery, transcript, resume, format, ASR)

## Important constraints

- Do not reintroduce `videos_fts` or search paths that depend on `videos.transcript_text`.
- Keep manual transcript jobs from being double-submitted.
- Treat large data dirs (`runs/`, `uploads/`, `logs/`, `tmp/`) as runtime artifacts, not source of truth.
- Orchestrator state DB: `runs/orchestrator/orchestrator_state.db`

## Notes for AI readers

- The repo has many legacy compatibility files in `partial_py/`, `partial_docs/`, and `partial_ops/`.
- Use `docs/README.md` as the index, then `docs/WORKFLOWS.md` for operational flow.
- Orchestrator: `orchestrator/` directory, config `orchestrator.yaml`, shell wrapper `scripts/orchestrator.sh`
