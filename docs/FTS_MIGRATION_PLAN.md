# FTS Migration Plan

Historically this repo kept `videos.transcript_text` and `videos.summary_text` because the active FTS5 setup was still wired to those columns. The live DB has since been cleaned so those shadow columns are empty again, and the search cache now lives in `youtube_transcripts_search.db` without `summary_search` in the indexed corpus.

Current state:
- Search is now stored in `youtube_transcripts_search.db` as `videos_search_cache` + `videos_search_fts`, both blob-first.
- Legacy `videos_fts` / `videos_ai` / `videos_ad` / `videos_au` still exist in old DBs but are no longer the target path.
- Runtime reads are already blob-first for transcript, summary, and formatted content.
- `transcript_text` and `summary_text` are legacy shadow columns during the stabilization window and should stay empty in new writes.
- The formatting pipeline now reads transcript content from blob-backed helpers in the active wrappers, so future removal of `videos.transcript_text` only needs search/legacy cleanup.

Goal:
- Remove the need for `transcript_text` and `summary_text` from the main `videos` table.
- Keep full-text search working with a new search cache / FTS layer.

Recommended migration path:

1. Create a new search cache table
   - Implemented: `videos_search_cache(video_id, title, description, transcript_search, updated_at)`
   - Populate `transcript_search` from blob-backed readers, not from the legacy file paths.

2. Build a new FTS table on top of that cache
   - Implemented: `videos_search_fts`
   - Index only normalized search text from the cache table.
   - Keep triggers attached to the cache table, not the raw `videos` table.

3. Backfill and verify
   - Backfill all existing rows from blob storage via `scripts/migrate_search_cache.py`.
   - Compare search results between old and new search paths for a sample set.
   - Verify `read_transcript()` and `read_summary()` remain blob-first.

4. Switch application reads
   - Implemented in `database_optimized.search_videos()` and `count_search_videos()`.
   - Search now queries the separate search DB while main content stays in the main DB and blobs.

5. Keep legacy shadow columns empty, then drop them only after the new path is stable
   - `videos.transcript_text`
   - `videos.summary_text`
   - old FTS triggers and any stale maintenance code

Rollback plan:
- Keep the old FTS table until the new one is validated.
- If search quality regresses, revert the search endpoint to the old FTS table without touching the blob-backed content.

Why this matters:
- It preserves the current blob-first runtime.
- It removes the last big duplicated text columns from `videos`.
- It lets the database stay smaller without losing search.
