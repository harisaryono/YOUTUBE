# Repo Map

## Orchestrator

- `orchestrator/config.py`
- `orchestrator/daemon.py`
- `orchestrator/planner.py`
- `orchestrator/dispatcher.py`
- `orchestrator/safety.py`
- `orchestrator/state.py`
- `orchestrator/db_queries.py`
- `orchestrator/error_analyzer.py`
- `orchestrator/reports.py`
- `orchestrator/validate.py`

## Pipeline Entrypoints

- `scripts/run_pipeline.sh`
- `scripts/orchestrator.sh`
- `scripts/discover.sh`
- `scripts/transcript.sh`
- `scripts/resume.sh`
- `scripts/format.sh`
- `scripts/asr.sh`
- `scripts/audio.sh`

## UI

- `flask_app/app.py`
- `flask_app/templates/*.html`

## Data / DB

- `database_optimized.py`
- `local_services.py`
- `job_tracker.py`
- `youtube_transcripts.db` via `db/` symlink layout

## Legacy / Partial

- `partial_py/`
- `partial_docs/`
- `partial_ops/`

