# Database Layout

Folder ini berisi database aktif untuk repo `YOUTUBE`.

## File Aktif

- `youtube_transcripts.db`
- `youtube_transcripts.db-wal`
- `youtube_transcripts.db-shm`
- `youtube_transcripts_blobs.db`
- `channels.db`
- `youtube_cache.db`
- `provider_accounts.sqlite3`
- `youtube.db`

## Catatan

- Root filename di repo masih ada sebagai symlink kompatibilitas.
- Jika menulis script baru, prefer path eksplisit ke `db/`.
- Jangan commit file database mentah atau sidecar WAL/SHM.

