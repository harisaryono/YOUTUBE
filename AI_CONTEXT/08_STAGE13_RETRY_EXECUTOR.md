# Stage 13 Retry Executor

Stage 13 menambah kontrol operasional untuk retry queue yang sudah dipersist
di Stage 12. Fokusnya bukan menambah policy baru, tetapi memberi cara aman
untuk melihat, menyaring, dan men-drain retry queue tanpa menimbulkan double
launch.

## Target

- `./scripts/orchestrator.sh retry-queue stats`
- `./scripts/orchestrator.sh retry-queue list --status pending --limit 50`
- `./scripts/orchestrator.sh retry-queue drain --limit 3 --dry-run`
- `./scripts/orchestrator.sh retry-queue drain --limit 3`

## Invariant

- Pending retry queue harus di-claim sebelum launch.
- Daemon dan CLI drain memakai state claim yang sama.
- Retry queue blocked oleh pause/quarantine/policy harus tetap tertahan.
- Jika launch gagal sebelum job running, queue item harus dilepas balik.
- `doctor` dan dashboard hanya membaca snapshot retry queue, bukan membuat
  logic retry sendiri.

## Files

- `orchestrator/retry_executor.py`
- `orchestrator/state.py`
- `orchestrator/daemon.py`
- `orchestrator/reports.py`
- `orchestrator/doctor.py`
- `scripts/orchestrator.sh`
