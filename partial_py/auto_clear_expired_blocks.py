#!/usr/bin/env python3
"""
Auto-clear expired model blocks for provider coordinator.
Run this every 5-15 minutes via cron.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Configuration
SERVICES_DIR = Path("/root/services")
DB_PATH = SERVICES_DIR / "provider_accounts.sqlite3"
LOG_FILE = SERVICES_DIR / "auto_clear_blocks.log"
BLOCKS_TABLE = "provider_account_model_blocks"


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def main() -> int:
    log("=" * 60)
    log("AUTO-CLEAR EXPIRED BLOCKS")
    log("=" * 60)
    
    # Check if database exists
    if not DB_PATH.exists():
        log(f"ERROR: Database not found: {DB_PATH}")
        return 1
    
    con = sqlite3.connect(str(DB_PATH))
    
    try:
        # Count current blocks
        before = con.execute(f"SELECT COUNT(*) FROM {BLOCKS_TABLE}").fetchone()[0]
        log(f"Blocks before: {before}")
        
        if before == 0:
            log("✓ No active blocks")
            log("=== Done ===")
            return 0
        
        # Get current UTC time for comparison
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        log(f"Current UTC: {now_utc}")
        
        # Get expired blocks info before deleting
        # Compare ISO format strings (works because ISO8601 is lexicographically sortable)
        expired = con.execute(f"""
            SELECT provider_account_id, provider, model_name, blocked_until
            FROM {BLOCKS_TABLE}
            WHERE blocked_until < ?
        """, (now_utc,)).fetchall()
        
        if not expired:
            log("✓ No expired blocks to clear")
            log("=== Done ===")
            return 0
        
        log(f"Found {len(expired)} expired block(s):")
        for row in expired:
            log(f"  - {row[0]}: {row[1]}/{row[2]} (expired: {row[3]})")
        
        # Clear expired blocks (blocked_until < now UTC)
        con.execute(f"""
            DELETE FROM {BLOCKS_TABLE}
            WHERE blocked_until < ?
        """, (now_utc,))
        con.commit()
        
        # Count remaining blocks
        after = con.execute(f"SELECT COUNT(*) FROM {BLOCKS_TABLE}").fetchone()[0]
        cleared = before - after
        
        log(f"Blocks cleared: {cleared}")
        log(f"Blocks remaining: {after}")
        log(f"✅ Cleared {cleared} expired block(s)")
        
    except Exception as e:
        log(f"ERROR: {e}")
        return 1
    finally:
        con.close()
    
    log("=== Done ===")
    return 0


if __name__ == "__main__":
    exit(main())
