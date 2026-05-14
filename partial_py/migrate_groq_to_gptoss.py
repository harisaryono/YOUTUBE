#!/usr/bin/env python3
import os
"""
Migrate Groq accounts from llama-3.3-70b to gpt-oss-120b.
Also update Cerebras if needed.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("YT_PROVIDERS_DB", str(Path(__file__).resolve().parent.parent.parent / "services/provider_accounts.sqlite3")))

def migrate_groq_models():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    
    try:
        # Check current Groq state
        print("=== Current Groq Models ===")
        rows = con.execute("""
            SELECT id, account_name, model_name, is_active 
            FROM provider_accounts 
            WHERE provider = 'groq'
            ORDER BY id
        """).fetchall()
        
        llama_count = 0
        gpt_count = 0
        
        for row in rows:
            model = row['model_name']
            if 'llama' in model:
                llama_count += 1
            elif 'gpt-oss' in model:
                gpt_count += 1
            print(f"  {row['id']}: {row['account_name']} -> {model}")
        
        print(f"\nSummary:")
        print(f"  llama-3.3-70b: {llama_count}")
        print(f"  gpt-oss-120b: {gpt_count}")
        
        # Confirm migration
        if llama_count == 0:
            print("\n✓ No Groq accounts to migrate!")
        else:
            print(f"\n=== Migrating {llama_count} Groq accounts to openai/gpt-oss-120b ===")
            
            # Update provider_accounts
            con.execute("""
                UPDATE provider_accounts 
                SET model_name = 'openai/gpt-oss-120b',
                    updated_at = CURRENT_TIMESTAMP
                WHERE provider = 'groq' 
                  AND model_name LIKE '%llama%'
            """)
            
            # Update provider_account_models (set old as deprecated)
            con.execute("""
                UPDATE provider_account_models 
                SET is_deprecated = 1,
                    notes = 'Migrated to gpt-oss-120b'
                WHERE provider_account_id IN (
                    SELECT id FROM provider_accounts 
                    WHERE provider = 'groq'
                )
                AND model_name LIKE '%llama%'
            """)
            
            # Insert new default model for migrated accounts
            con.execute("""
                INSERT OR IGNORE INTO provider_account_models 
                (provider_account_id, model_name, is_default, capability, notes)
                SELECT id, 'openai/gpt-oss-120b', 1, 'chat', 'Migrated from llama-3.3-70b'
                FROM provider_accounts
                WHERE provider = 'groq'
                  AND model_name = 'openai/gpt-oss-120b'
            """)
            
            con.commit()
            
            # Verify migration
            print("\n=== After Groq Migration ===")
            rows = con.execute("""
                SELECT id, account_name, model_name 
                FROM provider_accounts 
                WHERE provider = 'groq'
                ORDER BY id
            """).fetchall()
            
            new_llama = 0
            new_gpt = 0
            
            for row in rows:
                model = row['model_name']
                if 'llama' in model:
                    new_llama += 1
                elif 'gpt-oss' in model:
                    new_gpt += 1
                print(f"  {row['id']}: {row['account_name']} -> {model}")
            
            print(f"\nNew Summary:")
            print(f"  llama-3.3-70b: {new_llama}")
            print(f"  gpt-oss-120b: {new_gpt}")
            
            migrated = llama_count - new_llama
            print(f"\n✅ Migrated {migrated} Groq accounts successfully!")
        
        # Check Cerebras
        print("\n=== Cerebras Models ===")
        rows = con.execute("""
            SELECT id, account_name, model_name, is_active 
            FROM provider_accounts 
            WHERE provider = 'cerebras'
            ORDER BY id
        """).fetchall()
        
        for row in rows:
            print(f"  {row['id']}: {row['account_name']} -> {row['model_name']}")
        
        # Check for any existing blocks
        print("\n=== Active Model Blocks ===")
        blocks = con.execute("""
            SELECT provider, model_name, blocked_until, reason
            FROM provider_account_model_blocks
            WHERE blocked_until > datetime('now')
        """).fetchall()
        
        if blocks:
            print("WARNING: Active blocks found:")
            for block in blocks:
                print(f"  {block[0]}/{block[1]} until {block[2]}")
                print(f"    Reason: {block[3][:100]}...")
        else:
            print("✓ No active model blocks")
        
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        con.rollback()
        return -1
    finally:
        con.close()


if __name__ == "__main__":
    exit(migrate_groq_models())
