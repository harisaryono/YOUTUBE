# Stage 14 Web Admin Control Actions & Retry Queue UI

Stage 14 menambahkan halaman admin web untuk retry queue dan tombol
safe actions (pause/resume stage, pause/resume group, quarantine/
unquarantine channel) yang memanggil modul Python yang sudah ada.

## Target

- Halaman `/admin/orchestrator/retry-queue`
- Tombol dry-run drain dan drain 1 item (wajib konfirmasi)
- Pause/Resume stage/group dari web
- Quarantine/Unquarantine channel dari web
- Semua action via `orchestrator.actions` atau `orchestrator.retry_executor`
- Semua action tercatat di `orchestrator_events`

## Files

- `flask_app/templates/admin_retry_queue.html` — halaman retry queue
- `flask_app/templates/admin_orchestrator.html` — tambah link Retry Queue
- `flask_app/app.py` — route `/admin/orchestrator/retry-queue` dan
  `/admin/orchestrator/retry-queue/action`

## Routes

- `GET /admin/orchestrator/retry-queue` — render retry queue page
- `POST /admin/orchestrator/retry-queue/action` — handle actions

## Invariant

- Web hanya wrapper, tidak membuat logic sendiri
- Drain nyata limit=1, butuh JS confirm
- Dry-run drain tidak perlu konfirmasi
- Semua action audit event
- Retry stats di dashboard tetap via doctor report
