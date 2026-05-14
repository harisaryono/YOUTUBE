#!/usr/bin/env python3
"""Simple shard storage module stub for local development."""
from __future__ import annotations

import hashlib
import json
import os
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


INDEX_FILENAME = ".shard_index.json"


def normalize_rel_path(rel_path: Optional[str]) -> str:
    """Normalize relative path."""
    if not rel_path:
        return ""
    rel = rel_path.replace("\\", "/").strip("/")
    parts = [p for p in rel.split("/") if p and p != "."]
    if not parts:
        return ""
    return "/".join(parts)


def safe_resolve(channel_root: Path, rel_path: str) -> Optional[Path]:
    """Safely resolve relative path within channel root."""
    if not rel_path:
        return None
    
    # Normalize and check for path traversal
    normalized = normalize_rel_path(rel_path)
    if not normalized or ".." in normalized:
        return None
    
    try:
        full_path = (channel_root / normalized).resolve()
        # Ensure the resolved path is still within channel_root
        try:
            full_path.relative_to(channel_root.resolve())
            return full_path
        except ValueError:
            return None
    except Exception:
        return None


def load_index(channel_root: Path) -> Dict[str, Any]:
    """Load shard index from channel root."""
    index_path = channel_root / INDEX_FILENAME
    if not index_path.exists():
        return {"version": 1, "entries": {}}
    
    try:
        with index_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    
    return {"version": 1, "entries": {}}


def save_index(channel_root: Path, index: Dict[str, Any]) -> None:
    """Save shard index to channel root."""
    index_path = channel_root / INDEX_FILENAME
    temp_path = index_path.with_suffix(".tmp")
    
    try:
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=True, sort_keys=True, indent=2)
        temp_path.replace(index_path)
    except Exception:
        raise


def link_exists(channel_root: Path, rel_path: Optional[str]) -> bool:
    """Check if a link/file exists (either plain or in shard)."""
    if not rel_path:
        return False
    
    # Check plain file first
    plain_path = safe_resolve(channel_root, rel_path)
    if plain_path and plain_path.is_file():
        return True
    
    # Check shard index
    index = load_index(channel_root)
    entries = index.get("entries", {})
    normalized = normalize_rel_path(rel_path)
    return normalized in entries


def link_mtime(channel_root: Path, rel_path: Optional[str]) -> Optional[float]:
    """Get modification time of a link/file."""
    if not rel_path:
        return None
    
    # Check plain file first
    plain_path = safe_resolve(channel_root, rel_path)
    if plain_path and plain_path.is_file():
        return float(plain_path.stat().st_mtime)
    
    # Check shard index
    index = load_index(channel_root)
    entries = index.get("entries", {})
    normalized = normalize_rel_path(rel_path)
    entry = entries.get(normalized)
    
    if entry and isinstance(entry, dict):
        return float(entry.get("mtime", 0))
    
    return None


def link_source_label(channel_root: Path, rel_path: Optional[str]) -> str:
    """Get source label for a link (plain file or shard)."""
    if not rel_path:
        return "none"
    
    # Check plain file first
    plain_path = safe_resolve(channel_root, rel_path)
    if plain_path and plain_path.is_file():
        return "plain"
    
    # Check shard index
    index = load_index(channel_root)
    entries = index.get("entries", {})
    normalized = normalize_rel_path(rel_path)
    entry = entries.get(normalized)
    
    if entry and isinstance(entry, dict):
        shard = entry.get("shard", "")
        codec = entry.get("codec", "")
        return f"{codec}:{shard}"
    
    return "none"


def read_link_bytes(channel_root: Path, rel_path: Optional[str]) -> Optional[bytes]:
    """Read bytes from a link (plain file or from shard)."""
    if not rel_path:
        return None
    
    # Try plain file first
    plain_path = safe_resolve(channel_root, rel_path)
    if plain_path and plain_path.is_file():
        try:
            return plain_path.read_bytes()
        except Exception:
            return None
    
    # Try shard
    index = load_index(channel_root)
    entries = index.get("entries", {})
    normalized = normalize_rel_path(rel_path)
    entry = entries.get(normalized)
    
    if not entry or not isinstance(entry, dict):
        return None
    
    shard_name = entry.get("shard")
    offset = entry.get("offset")
    length = entry.get("length")
    codec = entry.get("codec")
    
    if not all([shard_name, offset is not None, length is not None, codec]):
        return None
    
    shard_path = channel_root / shard_name
    if not shard_path.is_file():
        return None
    
    try:
        with shard_path.open("rb") as f:
            f.seek(int(offset))
            data = f.read(int(length))
        
        # Decompress if needed
        if codec == "zstd":
            try:
                import zstandard as zstd
                d = zstd.ZstdDecompressor()
                return d.decompress(data)
            except Exception:
                return None
        
        return data
    except Exception:
        return None


def choose_append_shard(
    channel_root: Path,
    kind: str,
    incoming_bytes: int,
    max_shard_bytes: int,
) -> Path:
    """Choose or create a shard for appending data."""
    shard_dir = channel_root / ".shards" / kind
    shard_dir.mkdir(parents=True, exist_ok=True)
    
    # Try to find existing shard with space
    for shard_path in sorted(shard_dir.glob("*.zst"), reverse=True):
        try:
            if shard_path.stat().st_size + incoming_bytes <= max_shard_bytes:
                return shard_path
        except Exception:
            continue
    
    # Create new shard
    import time
    timestamp = int(time.time())
    new_shard = shard_dir / f"shard_{timestamp}.zst"
    return new_shard


def append_blob(shard_path: Path, data: bytes) -> Tuple[int, int]:
    """Append blob to shard and return (offset, length)."""
    try:
        with shard_path.open("ab") as f:
            offset = f.tell()
            f.write(data)
            length = len(data)
        return offset, length
    except Exception:
        raise