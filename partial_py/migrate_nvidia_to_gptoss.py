#!/usr/bin/env python3
import os
"""
Migrate NVIDIA accounts from mistral-small to gpt-oss-120b.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("YT_PROVIDERS_DB", str(Path(__file__).resolve().parent.parent.parent / "services/provider_accounts.sqlite3")))

def migrate_nvidia_models():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    
    try:
        # Check current state
        print("=== Current NVIDIA Models ===")
        rows = con.execute("""
            SELECT id, account_name, model_name, is_active 
            FROM provider_accounts 
            WHERE provider = 'nvidia'
            ORDER BY id
        """).fetchall()
        
        mistral_count = 0
        gpt_count = 0
        
        for row in rows:
            model = row['model_name']
            if 'mistral-small' in model:
                mistral_count += 1
            elif 'gpt-oss' in model:
                gpt_count += 1
            print(f"  {row['id']}: {row['account_name']} -> {model} (active={row['is_active']})")
        
        print(f"\nSummary:")
        print(f"  mistral-small: {mistral_count}")
        print(f"  gpt-oss-120b: {gpt_count}")
        
        # Confirm migration
        if mistral_count == 0:
            print("\n✓ No accounts to migrate!")
            return 0
        
        print(f"\n=== Migrating {mistral_count} accounts to openai/gpt-oss-120b ===")
        
        # Update provider_accounts
        con.execute("""
            UPDATE provider_accounts 
            SET model_name = 'openai/gpt-oss-120b',
                updated_at = CURRENT_TIMESTAMP
            WHERE provider = 'nvidia' 
              AND model_name = 'mistralai/mistral-small-24b-instruct'
        """)
        
        # Update provider_account_models (set old as deprecated, add new)
        con.execute("""
            UPDATE provider_account_models 
            SET is_deprecated = 1,
                notes = 'Migrated to gpt-oss-120b'
            WHERE provider_account_id IN (
                SELECT id FROM provider_accounts 
                WHERE provider = 'nvidia' AND model_name = 'mistralai/mistral-small-24b-instruct'
            )
            AND model_name = 'mistralai/mistral-small-24b-instruct'
        """)
        
        # Insert new default model for migrated accounts
        con.execute("""
            INSERT OR IGNORE INTO provider_account_models 
            (provider_account_id, model_name, is_default, capability, notes)
            SELECT id, 'openai/gpt-oss-120b', 1, 'chat', 'Migrated from mistral-small'
            FROM provider_accounts
            WHERE provider = 'nvidia' 
              AND model_name = 'openai/gpt-oss-120b'
        """)
        
        con.commit()
        
        # Verify migration
        print("\n=== After Migration ===")
        rows = con.execute("""
            SELECT id, account_name, model_name 
            FROM provider_accounts 
            WHERE provider = 'nvidia'
            ORDER BY id
        """).fetchall()
        
        new_mistral = 0
        new_gpt = 0
        
        for row in rows:
            model = row['model_name']
            if 'mistral-small' in model:
                new_mistral += 1
            elif 'gpt-oss' in model:
                new_gpt += 1
            print(f"  {row['id']}: {row['account_name']} -> {model}")
        
        print(f"\nNew Summary:")
        print(f"  mistral-small: {new_mistral}")
        print(f"  gpt-oss-120b: {new_gpt}")
        
        migrated = mistral_count - new_mistral
        print(f"\n✅ Migrated {migrated} accounts successfully!")
        
        return migrated
        
    except Exception as e:
        print(f"ERROR: {e}")
        con.rollback()
        return -1
    finally:
        con.close()


if __name__ == "__main__":
    exit(migrate_nvidia_models())
