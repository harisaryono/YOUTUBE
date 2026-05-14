#!/usr/bin/env python3
import sqlite3
import os

def migrate():
    db_path = "youtube_transcripts.db"
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        return

    print(f"🔄 Migrating database: {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing columns in videos table
    cursor.execute("PRAGMA table_info(videos)")
    existing_columns = [row[1] for row in cursor.fetchall()]

    missing_columns = [
        ("metadata", "TEXT"),
        ("transcript_formatted_path", "TEXT"),
    ]

    for col_name, col_type in missing_columns:
        if col_name not in existing_columns:
            print(f"➕ Adding column {col_name} to videos table...")
            try:
                cursor.execute(f"ALTER TABLE videos ADD COLUMN {col_name} {col_type}")
            except Exception as e:
                print(f"⚠️ Error adding {col_name}: {e}")
        else:
            print(f"✅ Column {col_name} already exists.")

    # Check for cached_stats table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cached_stats'")
    if not cursor.fetchone():
        print("➕ Creating cached_stats table...")
        cursor.execute("""
            CREATE TABLE cached_stats (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    else:
        print("✅ cached_stats table already exists.")

    conn.commit()
    conn.close()
    print("✅ Migration completed successfully!")

if __name__ == "__main__":
    migrate()
