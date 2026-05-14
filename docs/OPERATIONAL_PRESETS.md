# Operational Presets

Set environment once:

```bash
export YT_PROVIDER_COORDINATOR_URL=http://127.0.0.1:8788
```

## 1. Daily Safe

Discovery ringan, transcript terbatas, ASR kecil, resume stabil.

```bash
./scripts/discover.sh --latest-only --rate-limit-safe --channel-limit 5 --run-dir runs/daily_discover
./scripts/transcript.sh --rate-limit-safe --workers 2 --limit 100 --run-dir runs/daily_transcript
./scripts/asr.sh --rate-limit-safe --video-workers 2 --providers groq,nvidia --limit 20 --run-dir runs/daily_asr
./scripts/resume.sh --nvidia-only --max-workers 8 --run-dir runs/daily_resume
```

## 2. Channel Safe

Untuk satu channel tertentu.

```bash
./scripts/discover.sh --latest-only --rate-limit-safe --channel-id UC1234567890 --run-dir runs/channel_discover
./scripts/transcript.sh --channel-id UC1234567890 --limit 50 --rate-limit-safe --workers 1 --run-dir runs/channel_transcript
./scripts/asr.sh --channel-id UC1234567890 --limit 10 --providers groq,nvidia --video-workers 1 --run-dir runs/channel_asr
./scripts/resume.sh --channel-id UC1234567890 --nvidia-only --max-workers 4 --run-dir runs/channel_resume
```

## 3. ASR Safe

Hanya recovery `no_subtitle`, tanpa GPT post-process.

```bash
./scripts/asr.sh --channel-id UC1234567890 --limit 20 --providers groq,nvidia --video-workers 2 --run-dir runs/asr_safe
```

## 4. Resume Safe

Hanya pipeline resume, tidak menyentuh YouTube.

```bash
./scripts/resume.sh --nvidia-only --max-workers 12 --run-dir runs/resume_safe
```

## Notes

- Discovery default aman tetap `--latest-only --rate-limit-safe`.
- Transcript default aman tetap `--rate-limit-safe` dan worker kecil.
- ASR default aman sekarang tanpa post-process.
- Jika muncul hard block, stop batch itu dan lanjutkan dari report/retry later.
