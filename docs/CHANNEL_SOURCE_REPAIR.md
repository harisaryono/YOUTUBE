# Channel Source Repair

Dokumen ini mencatat perbaikan channel yang awalnya diingest dari halaman handle root `https://www.youtube.com/@handle`, lalu ternyata menghasilkan pseudo entry `"<name> - Videos"` / `"<name> - Shorts"`.

## Kapan Dipakai

Gunakan repair ini jika channel di database menunjukkan gejala:

- `video_count` kecil tapi tidak masuk akal
- `db_videos` hanya `1-2`
- ada row video dengan judul seperti `Channel Name - Videos`
- ada row video dengan `video_id` yang sama dengan ID channel/handle

## Script

Script batch repair:

```bash
/media/harry/DATA120B/venv_youtube/bin/python /media/harry/DATA120B/GIT/YOUTUBE/repair_channel_video_sources.py --apply
```

Mode scan-only:

```bash
/media/harry/DATA120B/venv_youtube/bin/python /media/harry/DATA120B/GIT/YOUTUBE/repair_channel_video_sources.py
```

Target satu channel:

```bash
/media/harry/DATA120B/venv_youtube/bin/python /media/harry/DATA120B/GIT/YOUTUBE/repair_channel_video_sources.py --apply --channel-id @FuelofLegends
```

## Cara Kerja

1. Script scan channel yang punya pseudo title atau mismatch handle.
2. Script membackup state channel dan video lama ke `runs/repair_channel_sources_*`.
3. Source channel dinormalisasi ke `https://www.youtube.com/@handle/videos`.
4. Video asli di-fetch ulang dengan `yt-dlp`.
5. Row lama yang tidak ada di source baru dihapus.
6. Metadata channel dan video di-update.

## Hasil Repair yang Sudah Ditemukan

- `Fuel of Legends` -> `74` video
- `Topi Merah` -> `53` video
- `Jeda Jajan` -> `62` video
- `BINCANG FINANSIAL` -> `21` video

## Backup

Backup otomatis disimpan di:

- `runs/rework_fuel_of_legends_*`
- `runs/repair_channel_sources_*`

Gunakan backup tersebut jika ingin membandingkan state sebelum dan sesudah repair.
