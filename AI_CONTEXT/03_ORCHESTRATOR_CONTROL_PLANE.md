# Orchestrator Control Plane

## Commands

- `./scripts/orchestrator.sh once`
- `./scripts/orchestrator.sh run`
- `./scripts/orchestrator.sh status`
- `./scripts/orchestrator.sh active`
- `./scripts/orchestrator.sh logs --job-id <JOB_ID> --tail 100`
- `./scripts/orchestrator.sh cancel --job-id <JOB_ID>`
- `./scripts/orchestrator.sh cancel-stage <stage>`
- `./scripts/orchestrator.sh cancel-group <group>`
- `./scripts/orchestrator.sh reconcile`
- `./scripts/orchestrator.sh explain`
- `./scripts/orchestrator.sh doctor`
- `./scripts/orchestrator.sh report`
- `./scripts/orchestrator.sh validate`
- `./scripts/orchestrator.sh pause-stage <stage> --minutes N --reason ...`
- `./scripts/orchestrator.sh resume-stage <stage>`
- `./scripts/orchestrator.sh pause-group <group> --minutes N --reason ...`
- `./scripts/orchestrator.sh resume-group <group>`
- `./scripts/orchestrator.sh retry-failed --stage <stage> --limit N --dry-run`
- `./scripts/orchestrator.sh quarantine-channel <CHANNEL_ID> --reason ...`
- `./scripts/orchestrator.sh unquarantine-channel <CHANNEL_ID>`

## Invariants

- PID file harus menunjuk proses daemon yang benar.
- `active_jobs` disimpan di SQLite.
- `reconcile` harus bisa memulihkan job stale setelah restart/crash.
- `timeout` harus menutup job yang terlalu lama.
- `cancel` harus melepaskan lock.
- pause/quarantine harus tercermin di doctor dan dashboard snapshot.

## Scope

- `stage:{stage}:slot:{n}` adalah slot lock stage.
- `scope:<value>` dipakai untuk mencegah stage sensitif pada scope yang sama jalan paralel.
- stage sensitif YouTube awal yang dibatasi scope lock:
  - `transcript`
  - `audio_download`

## Async Job Finish

- Job selesai harus tetap:
  - release lock,
  - mark status terminal,
  - dan bila ada report CSV, dianalisis untuk cooldown / event.
