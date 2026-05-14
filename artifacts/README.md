# Artifacts

Folder ini berisi output kerja sekali pakai, ekspor, arsip, dan file lampiran yang bukan source code.

Contoh isi:
- CSV / JSON export
- notebook eksperimen
- cookie session lama
- ZIP / TAR arsip hasil kerja
- HTML/template sementara
- output helper sekali jalan, misalnya `artifacts/missing_transcripts/`

Aturan:
- Jangan taruh source code aktif di sini.
- Jika artifact sudah digantikan workflow baru, pindahkan ke `partial_docs/` atau hapus.
- Manifest/helper lama yang masih menunjuk ke layout root lama sebaiknya diganti ke folder turunan di bawah `artifacts/`, bukan dipertahankan di root repo.
