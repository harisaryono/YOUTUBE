#!/usr/bin/env python3
import os
"""
Sync expired blocks from coordinator server to local database.
Clear blocks that have passed their blocked_until time.
"""

import json
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# Configuration
COORDINATOR_URL = "http://localhost:8788"
LOCAL_DB = Path(os.environ.get("YT_PROVIDERS_DB", str(Path(__file__).resolve().parent.parent.parent / "services/provider_accounts.sqlite3")))
BLOCKS_TABLE = "provider_account_model_blocks"


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def fetch_accounts() -> list[dict]:
    """Fetch all accounts from coordinator."""
    url = f"{COORDINATOR_URL}/v1/status/accounts"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("accounts", [])
    except Exception as e:
        log(f"ERROR: Failed to fetch accounts: {e}")
        return []


def clear_expired_blocks(con: sqlite3.Connection) -> int:
    """Clear blocks that have expired."""
    now = datetime.now(timezone.utc).isoformat()
    
    # Find expired blocks
    expired = con.execute(f"""
        SELECT provider_account_id, provider, model_name, blocked_until
        FROM {BLOCKS_TABLE}
        WHERE blocked_until < ?
    """, (now,)).fetchall()
    
    if not expired:
        log("No expired blocks found")
        return 0
    
    log(f"Found {len(expired)} expired blocks to clear:")
    for row in expired:
        log(f"  - {row[0]}: {row[1]}/{row[2]} (expired: {row[3]})")
    
    # Delete expired blocks
    con.execute(f"""
        DELETE FROM {BLOCKS_TABLE}
        WHERE blocked_until < ?
    """, (now,))
    
    con.commit()
    return len(expired)


def sync_runtime_state(con: sqlite3.Connection, accounts: list[dict]) -> int:
    """Sync runtime state from coordinator to local DB."""
    updated = 0
    
    for acct in accounts:
        provider_account_id = acct.get("provider_account_id")
        state = acct.get("state", "idle")
        lease_token = acct.get("lease_token", "")
        
        if not provider_account_id:
            continue
        
        # Update runtime state
        con.execute("""
            INSERT OR REPLACE INTO provider_account_runtime_state
            (provider_account_id, provider, model_name, state, lease_token, holder, host, pid, task_type, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            provider_account_id,
            acct.get("provider", ""),
            acct.get("runtime_model_name", "") or acct.get("default_model_name", ""),
            state,
            lease_token,
            acct.get("holder", "") or "",
            acct.get("host", "") or "",
            acct.get("pid", 0),
            acct.get("task_type", "") or ""
        ))
        updated += 1
    
    con.commit()
    return updated


def check_stale_leases(con: sqlite3.Connection, max_idle_minutes: int = 60) -> list[dict]:
    """Find stale leases that should be released."""
    # Find leases that haven't had heartbeat in a while
    stale = con.execute("""
        SELECT provider_account_id, provider, model_name, state, last_heartbeat_at, lease_expires_at
        FROM provider_account_runtime_state
        WHERE state = 'active'
        AND (
            last_heartbeat_at < datetime('now', '-60 minutes')
            OR lease_expires_at < datetime('now')
        )
    """).fetchall()
    
    return [
        {
            "provider_account_id": row[0],
            "provider": row[1],
            "model_name": row[2],
            "state": row[3],
            "last_heartbeat": row[4],
            "expires_at": row[5]
        }
        for row in stale
    ]


def main():
    log("=" * 60)
    log("SYNC COORDINATOR EXPIRED BLOCKS")
    log("=" * 60)
    
    # Fetch accounts from coordinator
    log(f"Fetching accounts from {COORDINATOR_URL}...")
    accounts = fetch_accounts()
    log(f"Found {len(accounts)} accounts")
    
    # Connect to local DB
    if not LOCAL_DB.exists():
        log(f"ERROR: Local DB not found: {LOCAL_DB}")
        return 1
    
    con = sqlite3.connect(str(LOCAL_DB))
    con.row_factory = sqlite3.Row
    
    try:
        # Clear expired blocks
        log("\n--- Clearing Expired Blocks ---")
        cleared = clear_expired_blocks(con)
        log(f"Cleared {cleared} expired blocks")
        
        # Sync runtime state
        log("\n--- Syncing Runtime State ---")
        synced = sync_runtime_state(con, accounts)
        log(f"Synced {synced} account states")
        
        # Check for stale leases
        log("\n--- Checking Stale Leases ---")
        stale = check_stale_leases(con)
        if stale:
            log(f"Found {len(stale)} stale leases:")
            for s in stale:
                log(f"  - {s['provider_account_id']}: {s['provider']}/{s['model_name']} "
                    f"(last heartbeat: {s['last_heartbeat']})")
        else:
            log("No stale leases found")
        
        # Summary
        log("\n" + "=" * 60)
        log("SUMMARY")
        log("=" * 60)
        
        # Count remaining blocks
        remaining = con.execute(f"""
            SELECT provider, COUNT(*) as count
            FROM {BLOCKS_TABLE}
            GROUP BY provider
        """).fetchall()
        
        log("Remaining blocks by provider:")
        for row in remaining:
            log(f"  - {row[0]}: {row[1]}")
        
        if not remaining:
            log("  (no active blocks)")
        
        log("\n✅ Sync completed!")
        
    finally:
        con.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
