# Repo Changelog

- DB moved to `db/` with symlink compatibility at the repo root.
- Manual web flow now chains transcript -> resume -> format automatically.
- Search FTS moved to blob-first cache `videos_search_cache` + `videos_search_fts`.
- Transcript/summary files under `uploads/*/text/` were cleaned once mirrored safely into blobs.
- ASR and transcript wrapper logic now use lease coordinator aware fallback and cache reuse.
- Orchestrator daemon added: pipeline controller with safety gate, auto cooldown, and reports.
