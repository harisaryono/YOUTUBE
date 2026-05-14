# State Machine

Dokumen ini menjelaskan status operasional utama di repo `YOUTUBE` supaya mudah dipahami oleh manusia dan supervisor.

## Video States

| State | Artinya | Trigger masuk | Next step |
|---|---|---|---|
| `discovered` | Video sudah ada di DB | Discovery menemukan row baru | Transcript / audio / resume / format |
| `transcript_pending` | Belum ada subtitle/transkrip | `transcript_downloaded = 0` dan bukan `no_subtitle` | Jalankan transcript worker |
| `no_subtitle` | Transcript biasa gagal, perlu jalur audio/ASR | `transcript_language = 'no_subtitle'` | Warm audio cache lalu ASR |
| `audio_cached` | Audio hasil `yt-dlp` sudah ada di cache | `scripts/audio.sh` atau ASR download cache berhasil | ASR consumer |
| `asr_done` | Transcript dari audio sudah jadi | `recover_asr_transcripts.py` selesai sukses | Resume / format |
| `resume_pending` | Resume belum dibuat | `summary_file_path` kosong | Resume worker |
| `resume_done` | Resume sudah ada | `summary_file_path` terisi | Format |
| `format_pending` | Transcript belum diformat | `transcript_formatted_path` kosong | Format worker |
| `format_done` | Transcript diformat | `transcript_formatted_path` terisi | Selesai |
| `skip_access_blocked` | Video tidak bisa diambil audionya | Probe access / member-only / private | Retry nanti atau skip permanen |
| `retry_later` | Stage gagal tapi masih layak dicoba lagi | Lease kosong, rate limit, timeout, atau transient error | Supervisor menunggu cooldown |

## Stage Ordering

Urutan yang direkomendasikan:

1. Discovery.
2. Transcript biasa.
3. Audio warmup untuk `no_subtitle`.
4. ASR dengan `--require-cached-audio`.
5. Resume.
6. Format.
7. Repair hanya jika metadata/source salah ingest.

## Backlog Fields

Supervisor membaca backlog dari kolom yang sudah ada:

- `videos.transcript_downloaded`
- `videos.transcript_language`
- `videos.transcript_retry_after`
- `videos.summary_file_path`
- `videos.transcript_formatted_path`
- `videos.is_short`
- `videos.is_member_only`

## Catatan

- Discovery belum punya flag status permanen per video, jadi supervisor menjalankannya sebagai sweep periodik.
- Audio cache tidak menambah kolom baru di DB dulu; cache dianggap tersedia jika file `source/` sudah ada di run root.
- ASR consumer bisa dipaksa hanya memakai cache audio sehingga pemisahan producer/consumer tetap tegas.
