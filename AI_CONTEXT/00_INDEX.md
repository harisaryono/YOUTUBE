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

## Objective Saat Ini

- Stage 9: observability operasional dan recovery dashboard.
- Fokus:
  - context kecil per tugas,
  - validate command sebelum patch,
  - lifecycle daemon bersih,
  - report hook async,
  - scope lock opsional untuk stage YouTube sensitif,
  - `doctor` command untuk backlog, cooldown, failures, dan rekomendasi.

## Source of Truth

- [docs/README.md](../docs/README.md)
- [docs/WORKFLOWS.md](../docs/WORKFLOWS.md)
- [docs/PROGRESS.md](../docs/PROGRESS.md)
- [docs/VERIFY.md](../docs/VERIFY.md)
- [scripts/README.md](../scripts/README.md)
