# Project State

- generated_at: `2026-05-15T08:23:57+07:00`
- repo_type: YouTube transcript / resume / format / ASR pipeline
- active web app: Flask under `flask_app/app.py`
- database layout: active DBs are under `db/` with root symlinks for compat
- search: migrated to `videos_search_cache` + `videos_search_fts`
- manual transcript chain: `manual transcript -> resume -> format`
- transcript/summary content: blob-first, file artifacts cleaned where safe
- discovery: channel/video ingest and repair utilities remain in `scripts/` and `partial_py/`
- orchestrator daemon: pipeline controller (discovery, transcript, resume, format, ASR)
- stage 8 hardening: AI_CONTEXT working files, `validate` command, PID cleanup, async report hooks, scope lock
- stage 9 observability: `doctor` command for backlog/cooldown/failure diagnosis
- stage 10 web integration: `/admin/orchestrator` dashboard + actions
- stage 11 safe actions: `orchestrator/actions.py`, pause/resume stage/group, quarantine/unquarantine channel, retry-failed dry-run
- stage 12 policy requeue: retry queue persistence, retry-failed enqueue mode, policy blocker gating
- stage 13 retry executor: `orchestrator/retry_executor.py`, `retry-queue stats/list/drain`, claim-before-launch

## Directory Snapshot
- `docs`: 18 files
- `scripts`: 62 files
- `flask_app`: 20 files
- `partial_py`: 115 files
- `partial_docs`: 14 files
- `partial_ops`: 25 files
- `webapp`: 17 files
- `orchestrator`: 26 files

## Root Files
- `README.md` (11404 bytes)
- `AGENTS.md` (11612 bytes)
- `orchestrator.yaml` (3949 bytes)
- `database_optimized.py` (106459 bytes)
- `database_blobs.py` (4244 bytes)
- `recover_transcripts.py` (59861 bytes)
- `recover_transcripts_from_csv.py` (24603 bytes)
- `recover_asr_transcripts.py` (103682 bytes)
- `launch_resume_queue.py` (25697 bytes)
- `fill_missing_resumes_youtube_db.py` (37628 bytes)
- `format_transcripts_pool.py` (74566 bytes)
- `update_latest_channel_videos.py` (55722 bytes)
- `manage_database.py` (16182 bytes)
- `local_services.py` (53394 bytes)
- `provider_encryption.py` (5831 bytes)
- `job_tracker.py` (5204 bytes)
- `savesubs_playwright.py` (20437 bytes)
- `wsgi.py` (281 bytes)
- `passenger_wsgi.py` (125 bytes)

## Orchestrator
- orchestrator daemon: pipeline controller (discovery, transcript, resume, format, ASR)
- safety gate: disk, memory, cooldown YouTube/provider
- dispatch via subprocess ke script yang sudah ada
- auto cooldown berdasarkan klasifikasi error
- report Markdown + JSON di runs/orchestrator/reports/
- control-plane hardening: `active`, `logs`, `cancel`, `reconcile`, `validate`, timeout, scope lock, async report analysis
- operational observability: `doctor` summary, cycle-failure diagnosis, recommendations
- safe actions: pause/resume stage/group, quarantine/unquarantine channel, retry-failed dry-run
- retry recovery: retry queue summary, retry queue claim/release, retry-queue drain
- web observability: dashboard snapshot for pauses, quarantines, control actions, and cancel/diagnosis commands
