# AI Context Index

Tujuan folder ini:
- memberi working context kecil untuk AI saat mengubah repo ini,
- mengurangi risiko drift,
- dan memisahkan kontrak stage dari source code utama.

## Baca Dulu

1. [01_REPO_MAP.md](01_REPO_MAP.md)
2. [02_STAGE_CONTRACTS.md](02_STAGE_CONTRACTS.md)
3. [03_ORCHESTRATOR_CONTROL_PLANE.md](03_ORCHESTRATOR_CONTROL_PLANE.md)
4. [05_SAFE_PATCH_RULES.md](05_SAFE_PATCH_RULES.md)
5. [06_STAGE11_SAFE_ACTIONS.md](06_STAGE11_SAFE_ACTIONS.md)
6. [07_STAGE12_POLICY_REQUEUE.md](07_STAGE12_POLICY_REQUEUE.md)

## Objective Saat Ini

- Stage 12: policy requeue dan enforcement.
- Fokus:
  - context kecil per tugas,
  - validate command sebelum patch,
  - lifecycle daemon bersih,
  - report hook async,
  - scope lock opsional untuk stage YouTube sensitif,
  - `doctor` command untuk backlog, cooldown, failures, cycle-failure, dan rekomendasi,
  - pause/resume stage/group,
  - quarantine/unquarantine channel,
  - retry-failed dry-run,
  - retry queue persisten untuk safe requeue,
  - policy blocker dicek sebelum launch retry,
  - `doctor`/dashboard menampilkan retry queue ringkas.

## Source of Truth

- [docs/README.md](../docs/README.md)
- [docs/WORKFLOWS.md](../docs/WORKFLOWS.md)
- [docs/PROGRESS.md](../docs/PROGRESS.md)
- [docs/VERIFY.md](../docs/VERIFY.md)
- [scripts/README.md](../scripts/README.md)
