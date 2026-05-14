#!/usr/bin/env python3
"""Cron job: refresh stats_cache table periodically"""
import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from database_optimized import OptimizedDatabase

db = OptimizedDatabase(
    os.path.join(ROOT, 'youtube_transcripts.db'),
    os.path.join(ROOT, 'uploads'),
)
db.refresh_stats_cache()
print('Stats refreshed at', __import__('datetime').datetime.now().isoformat())
db.close()
