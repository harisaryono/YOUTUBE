# Stage Contracts

## discovery

Input:
- `channel_id`
- `scan_mode` = `latest_only` | `full_history`

Output:
- video metadata masuk DB
- discovery state channel ter-update
- tidak mengunduh transcript/audio

Group:
- `discovery`

Notes:
- `full_history` hanya untuk bootstrap channel yang belum pernah full scan.
- `latest_only` dipakai setelah full scan pertama atau untuk refresh ringan.

## transcript

Input:
- pending videos without transcript
- optional `channel_id`

Output:
- transcript tersimpan ke DB
- `recover_report.csv`
- `transcript_text` / `transcript_file_path`

Group:
- `youtube`

Notes:
- tetap rate-limit-safe
- hard block harus dicatat sebagai status final, bukan diulang tight-loop

## audio_download

Input:
- `no_subtitle` candidates

Output:
- audio cache lokal
- `video_audio_assets.audio_file_path`

Group:
- `youtube`

Notes:
- jangan dobel unduh jika audio sudah ada

## asr

Input:
- audio lokal yang sudah ada

Output:
- `recover_asr_report.csv`
- `transcript_raw.txt`
- `transcript.txt`
- `videos.transcript_text`

Group:
- `provider`

Notes:
- tidak boleh download YouTube lagi
- local-audio-only adalah rule utama

## resume

Input:
- transcript final tersedia

Output:
- resume tersimpan ke DB / file
- lease provider tercatat

Group:
- `provider`

## format

Input:
- transcript tersedia

Output:
- formatted transcript
- path formatted tersimpan ke SQLite

Group:
- `local`

## import_pending

Input:
- pending CSV/JSON update files

Output:
- row hasil import masuk DB

Group:
- `local`

## safe_control_actions

Input:
- `stage`
- `group`
- `channel_id`
- `reason`
- `minutes`
- `dry_run`

Output:
- pause/resume state tersimpan
- quarantine state tersimpan
- audit event masuk ke `orchestrator_events`
- retry candidate report untuk dry-run

Group:
- `control-plane`

Notes:
- action harus lewat `orchestrator/actions.py`
- retry default harus dry-run
- web dashboard hanya membaca snapshot / memanggil action helper, bukan menyusun policy sendiri
