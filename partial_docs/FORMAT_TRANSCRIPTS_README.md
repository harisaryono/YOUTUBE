# Format Transcripts

Program untuk memformat transkrip YouTube agar lebih mudah dibaca **TANPA** merangkum atau menerjemahkan.

## Cara Menggunakan

### Smoke Test (1-3 video)
```bash
cd /media/harry/DATA120B/GIT/YOUTUBE
/media/harry/DATA120B/venv_youtube/bin/python format_transcripts.py --limit 3
```

### Batch Penuh (semua video yang belum diformat)
```bash
/media/harry/DATA120B/venv_youtube/bin/python format_transcripts.py
```

### Dengan Limit Tertentu
```bash
/media/harry/DATA120B/venv_youtube/bin/python format_transcripts.py --limit 100
```

## Provider Urutan Prioritas

Berdasarkan `PROGRESS.md`, program menggunakan urutan provider berikut:

1. **nvidia/openai/gpt-oss-120b** - Backbone utama (tercepat, paling akurat)
2. **groq/moonshotai/kimi-k2-instruct** - Fallback utama
3. **cerebras/qwen-3-235b-a22b-instruct-2507** - Fallback berikutnya
4. **z.ai/glm-4.7** - Fallback (cenderung rewrite agresif)
5. **nvidia/mistralai/mistral-small-24b-instruct** - Usable tapi lambat
6. **groq/openai/gpt-oss-20b** - Usable, lebih pendek
7. **cerebras/llama3.1-8b** - Usable, lebih lemah

Program akan otomatis fallback ke provider berikutnya jika provider sebelumnya gagal.

## Output

### File
- Lokasi: `uploads/{channel_slug}/text_formatted/{video_id}.txt`
- Format: Text plain tanpa timestamp

### Database
- Kolom: `transcript_formatted_path` dan `link_file_formatted`
- Path: `uploads/{channel_slug}/text_formatted/{video_id}.txt`

## Web UI

Setelah diformat, tab **"Transcript Formatted"** akan muncul di halaman detail video:
- URL: `http://localhost:5000/video/{video_id}`
- Tab berada di antara "Summary" dan "Transcript Raw"
- Fitur: Copy, Download, Toggle Full View

## API Endpoint

```bash
curl http://localhost:5000/api/formatted/{video_id}
```

Response:
```json
{
  "success": true,
  "formatted": "..."
}
```

## Requirements

- API keys di `.env`:
  - `NVIDIA_API_KEY` (utama)
  - `GROQ_API_KEY` (fallback)
  - `CEREBRAS_API_KEY` (fallback)
  - `ZAI_API_KEY` (fallback)

- Virtual environment dengan `openai` package terinstall

## Error Handling

Program akan skip video jika:
- File transcript tidak ditemukan
- Semua provider gagal memformat

Error dicatat di report akhir.

## Contoh Output

Input (dengan timestamp):
```
[00:00:01.240] Pernahkah kalian memperhatikan
[00:00:03.120] bentuk pesawat tempur siluman
```

Output (tanpa timestamp, dengan paragraf):
```
Pernahkah kalian memperhatikan bentuk pesawat tempur siluman

Modern yang paling mematikan di dunia? Tidak seperti pesawat komersial...
```

## Status

✅ Smoke test passed (1 video)
⏳ Siap untuk batch penuh
