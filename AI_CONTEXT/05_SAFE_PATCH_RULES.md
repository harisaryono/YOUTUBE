# Safe Patch Rules

## Rules

- Mulai dari file yang paling kecil dan paling langsung terkait.
- Jangan refactor besar saat objective hanya hardening control-plane.
- Jangan ubah pipeline stage jika validator belum ada.
- Jangan menambah provider/model baru dalam patch kontrol-plane.
- Jangan mengandalkan cache startup jika status harus fresh.
- Jika job async selesai, release lock dulu, baru import/analisis tambahan.

## Before Editing

- Baca `docs/WORKFLOWS.md`
- Baca `docs/PROGRESS.md`
- Jalankan `orchestrator/validate.py`

## After Editing

- `python3 -m py_compile orchestrator/*.py`
- `bash -n scripts/orchestrator.sh`
- `./scripts/orchestrator.sh validate`

