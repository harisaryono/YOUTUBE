# Partial Docs

`partial_docs/` adalah folder arsip dokumentasi non-canonical.

Isi folder ini biasanya:
- hasil eksperimen,
- setup sekali jalan,
- migration note,
- channel/provider specific,
- ringkasan run lama,
- atau panduan yang sudah digantikan dokumen utama.

Dokumen di sini **bukan source of truth utama**.

## Source of Truth Utama

Kalau Anda mencari acuan aktif, buka dokumen root berikut dulu:
- `README.md`
- `docs/README.md`
- `docs/WORKFLOWS.md`
- `docs/PLAN.md`
- `docs/PROGRESS.md`
- `docs/VERIFY.md`
- `docs/DEVELOPER_GUIDE.md`
- `AGENTS.md`

## Kapan Masuk ke Sini

Pindahkan dokumen ke `partial_docs/` kalau:
- isinya hanya relevan untuk satu migration/repair tertentu,
- sudah digantikan workflow baru,
- atau hanya berguna sebagai arsip historis.

## Contoh Isi

- setup lama (`AUTO_*`, `CRON_SETUP.md`)
- hasil run / hasil API lama (`API_METADATA_RESULTS.md`)
- migration/provider notes lama (`PROVIDER_MIGRATION_SUMMARY.md`, `QWEN.md`)
- guide yang terlalu spesifik atau sudah tidak jadi jalur utama (`README_FLASK.md`, `README_COMPLETE.md`, `README_CHANNEL_STRUCTURE.md`, `YOUTUBE_API_GUIDE.md`, `SYNC_ENRICHMENT_GUIDE.md`)

## Aturan Pakai

- Jangan menjadikan file di sini sebagai referensi utama untuk workflow baru.
- Jika ada dokumen baru yang resmi, update dokumen root dan biarkan versi lama tetap di sini hanya sebagai arsip.
