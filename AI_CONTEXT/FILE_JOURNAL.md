# File Journal

## `README.md`
- size: 11404 bytes
- sha256: `822a29b84e41c123`
```text
# YouTube Transcript Framework

Framework Python untuk mengambil transkrip dari video YouTube, memformat hasil transkrip, dan membuat ringkasan otomatis. Framework ini mendukung pengambilan skala besar (channel) dan penyimpanan ke database terpusat.

## Dokumen Acuan

Dokumen acuan utama sekarang dipusatkan di `docs/`:
- [docs/README.md](docs/README.md)
- [docs/WORKFLOWS.md](docs/WORKFLOWS.md)
- [docs/PLAN.md](docs/PLAN.md)
- [docs/PROGRESS.md](docs/PROGRESS.md)
- [docs/VERIFY.md](docs/VERIFY.md)
```

## `docs/WORKFLOWS.md`
- size: 8598 bytes
- sha256: `65bc324a922e020c`
```text
# Workflows

Dokumen ini adalah ringkasan operasional cepat untuk alur kerja repo `YOUTUBE`. Tujuannya supaya pencarian manual lebih cepat saat Anda ingin tahu:

- discovery berjalan lewat apa,
- transcript diambil dari mana,
- resume diproduksi bagaimana,
- format diproses di mana,
- repair channel dipakai kapan,
- dan ASR chunking berjalan bagaimana.

## Peta Cepat
```

## `docs/PROGRESS.md`
- size: 30903 bytes
- sha256: `98b07764731dbb8a`
```text
# RECOVERY PROGRESS

## Overall Backlog: 4,162 videos
- Status: **Phase 2 Completed (50 videos processed)**

## Current Local Run
- `runs/transcript_no_subtitle_webshare_audit_20260507_0355_rlsafe/` sedang berjalan ulang dengan `3200` target, `6` worker, pacing aktif, dan mode `rate-limit-safe`.
- Batch transcript lama yang memakai `20` worker sudah dihentikan supaya tekanan ke YouTube turun.
- Mode aman sekarang mematikan fallback mahal lebih awal untuk mengurangi request tambahan saat recovery transcript.
- Discovery full-history lama sudah dihentikan dan diganti run baru `--latest-only --rate-limit-safe` supaya scan channel besar tidak terus kena throttling.
- Resume summary backlog yang aktif sekarang berjalan di `runs/resume_resume_20260509_070000_nvidia_only/` dengan `318` target dari DB utama, `12` worker, dan `nvidia-only` mode supaya `clod` tidak dipakai.
- Jalur ASR baru untuk...
```

## `docs/FTS_MIGRATION_PLAN.md`
- size: 2724 bytes
- sha256: `7378224c8db03296`
```text
# FTS Migration Plan

Historically this repo kept `videos.transcript_text` and `videos.summary_text` because the active FTS5 setup was still wired to those columns. The live DB has since been cleaned so those shadow columns are empty again, and the search cache now lives in `youtube_transcripts_search.db` without `summary_search` in the indexed corpus.

Current state:
- Search is now stored in `youtube_transcripts_search.db` as `videos_search_cache` + `videos_search_fts`, both blob-first.
- Legacy `videos_fts` / `videos_ai` / `videos_ad` / `videos_au` still exist in old DBs but are no longer the target path.
- Runtime reads are already blob-first for transcript, summary, and formatted content.
- `transcript_text` and `summary_text` are legacy shadow columns during the stabilization window and should stay empty in new writes.
- The formatting pipeline now reads transcript content from blo...
```

## `scripts/README.md`
- size: 5626 bytes
- sha256: `56d3b6f603d9ff28`
```text
# Scripts Index

`scripts/` adalah indeks command resmi repo `YOUTUBE`.

Gunakan direktori ini sebagai pintu masuk utama untuk entrypoint shell dan utilitas operasional yang masih aktif.

## Batas Dengan Root

- `scripts/` adalah sumber kebenaran utama.
- Jika ada wrapper/entrypoint lain di tempat lain, anggap itu kompatibilitas atau arsip, bukan jalur resmi.

## Entry Point Utama
```

## `scripts/manual_transcript_then_resume_format.sh`
- size: 2135 bytes
- sha256: `31424673c8943ea4`
```text
#!/bin/bash
# Manual transcript chain: transcript -> resume -> format

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"

VIDEO_ID=""
RUN_DIR_VALUE=""
```

## `scripts/migrate_search_cache.py`
- size: 4129 bytes
- sha256: `2a5c54627aeed68f`
```text
#!/usr/bin/env python3
"""Rebuild the separate search DB with a slimmer search corpus."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
```

## `scripts/generate_tasks.py`
- size: 5567 bytes
- sha256: `5b66ac6116ef9811`
```text
#!/usr/bin/env python3
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

REPO_ROOT = Path(__file__).resolve().parent.parent
```

## `scripts/format.sh`
- size: 7328 bytes
- sha256: `1e62d4cf954a9056`
```text
#!/bin/bash
# Wrapper script untuk format transcript video YouTube
# Script ini otomatis menggunakan .venv di repo ini

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
TRACKER_PY="$REPO_DIR/job_tracker.py"

# Check if virtualenv exists
```

## `flask_app/app.py`
- size: 88018 bytes
- sha256: `77e242c11cfaf751`
```text
#!/usr/bin/env python3
"""
Flask Application untuk YouTube Transcript Manager
Web interface untuk menampilkan dan mengelola transkrip YouTube
"""

import os
import sys
import re
import json
import gzip
import time
```

## `flask_app/templates/video_detail.html`
- size: 33242 bytes
- sha256: `a3ab91fcf419ff94`
```text
{% extends "base.html" %}

{% block title %}{{ video.title }} - YouTube Transcript Manager{% endblock %}

{% block content %}
<div class="container">
    {% if previous_video %}
    <a href="{{ url_for('video_detail', video_id=previous_video.video_id) }}"
       class="channel-nav-button channel-nav-prev"
       title="Prev: {{ previous_video.title }}">
        <span class="channel-nav-label">Prev</span>
        <span class="channel-nav-arrow"><i class="bi bi-chevron-right"></i></span>
```

## `database_optimized.py`
- size: 100325 bytes
- sha256: `48b84b1fac893eed`
```text
#!/usr/bin/env python3
"""
Optimized Database Module untuk YouTube Transcript Framework
Menangani penyimpanan data video dan references ke file transkrip/resume
"""

import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
```
