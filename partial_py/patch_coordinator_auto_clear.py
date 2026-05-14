#!/usr/bin/env python3
"""
Patch coordinator server to auto-clear expired blocks.
This script adds cleanup_expired_blocks function to provider_coordinator_server.py
"""

import re
from pathlib import Path

COORDINATOR_SCRIPT = Path("/root/services/provider_coordinator_server.py")

def add_cleanup_blocks_function(content: str) -> str:
    """Add cleanup_expired_blocks function after cleanup_expired_leases."""
    
    new_function = '''
def cleanup_expired_blocks(con: sqlite3.Connection) -> int:
    """Clear expired model blocks (blocked_until < now)."""
    now = now_iso()
    rows = con.execute(
        f"""
        SELECT provider_account_id, provider, model_name, blocked_until
        FROM {PROVIDER_MODEL_BLOCKS_TABLE}
        WHERE blocked_until < ?
        """,
        (now,),
    ).fetchall()
    if not rows:
        return 0
    
    # Delete expired blocks
    con.execute(
        f"DELETE FROM {PROVIDER_MODEL_BLOCKS_TABLE} WHERE blocked_until < ?",
        (now,),
    )
    
    # Log event for each cleared block
    for r in rows:
        append_event(
            con,
            provider_account_id=int(r[0]),
            provider=str(r[1]),
            model_name=str(r[2]),
            event_type="block_cleared",
            lease_token='',
            holder='',
            host='',
            pid=0,
            task_type='',
            payload={"blocked_until": str(r[3]), "cleared_at": now},
        )
    
    return len(rows)


'''
    
    # Find the end of cleanup_expired_leases function
    pattern = r'(def cleanup_expired_leases\(con: sqlite3\.Connection\) -> int:.*?return len\(rows\))'
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        insert_pos = match.end()
        # Find the end of the function (next def or end of line)
        return content[:insert_pos] + new_function + content[insert_pos:]
    
    return content


def add_cleanup_to_handle_acquire(content: str) -> str:
    """Add cleanup_expired_blocks call in handle_acquire."""
    
    # Find where cleanup_expired_leases is called in handle_acquire
    old_pattern = r'(con\.execute\("BEGIN IMMEDIATE"\)\s+cleanup_expired_leases\(con\))'
    new_code = '''con.execute("BEGIN IMMEDIATE")
            cleanup_expired_leases(con)
            cleared = cleanup_expired_blocks(con)'''
    
    return re.sub(old_pattern, new_code, content)


def add_cleanup_to_handle_status(content: str) -> str:
    """Add cleanup_expired_blocks call in handle_status_accounts."""
    
    old_pattern = r'(con\.row_factory = sqlite3\.Row\s+try:\s+cleanup_expired_leases\(con\))'
    new_code = '''con.row_factory = sqlite3.Row
        try:
            cleanup_expired_leases(con)
            cleanup_expired_blocks(con)'''
    
    return re.sub(old_pattern, new_code, content)


def main():
    if not COORDINATOR_SCRIPT.exists():
        print(f"ERROR: {COORDINATOR_SCRIPT} not found")
        return 1
    
    content = COORDINATOR_SCRIPT.read_text()
    
    # Add cleanup_expired_blocks function
    if 'def cleanup_expired_blocks' not in content:
        content = add_cleanup_blocks_function(content)
        print("✓ Added cleanup_expired_blocks function")
    else:
        print("✓ cleanup_expired_blocks already exists")
    
    # Add cleanup call to handle_acquire
    if 'cleanup_expired_blocks(con)' not in content:
        content = add_cleanup_to_handle_acquire(content)
        print("✓ Added cleanup call to handle_acquire")
    else:
        print("✓ cleanup call already in handle_acquire")
    
    # Write back
    COORDINATOR_SCRIPT.write_text(content)
    print(f"✅ Patched {COORDINATOR_SCRIPT}")
    
    return 0


if __name__ == "__main__":
    exit(main())
