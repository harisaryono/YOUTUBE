import sqlite3
import os

db_path = "youtube_transcripts.db"

def migrate():
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    print("Fixing NULL values in videos table...")
    cursor.execute("UPDATE videos SET is_short = 0 WHERE is_short IS NULL")
    cursor.execute("UPDATE videos SET is_member_only = 0 WHERE is_member_only IS NULL")
    
    print("Creating FTS5 table and triggers...")
    # FTS5 for fast search
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS videos_fts USING fts5(
            title, 
            description,
            content='videos',
            content_rowid='id'
        )
    """)

    # Triggers to keep FTS in sync
    cursor.execute("DROP TRIGGER IF EXISTS videos_ai")
    cursor.execute("DROP TRIGGER IF EXISTS videos_ad")
    cursor.execute("DROP TRIGGER IF EXISTS videos_au")

    cursor.execute("""
        CREATE TRIGGER videos_ai AFTER INSERT ON videos BEGIN
            INSERT INTO videos_fts(rowid, title, description) VALUES (new.id, new.title, new.description);
        END
    """)
    cursor.execute("""
        CREATE TRIGGER videos_ad AFTER DELETE ON videos BEGIN
            INSERT INTO videos_fts(videos_fts, rowid, title, description) VALUES('delete', old.id, old.title, old.description);
        END
    """)
    cursor.execute("""
        CREATE TRIGGER videos_au AFTER UPDATE ON videos BEGIN
            INSERT INTO videos_fts(videos_fts, rowid, title, description) VALUES('delete', old.id, old.title, old.description);
            INSERT INTO videos_fts(rowid, title, description) VALUES (new.id, new.title, new.description);
        END
    """)

    print("Rebuilding FTS index from existing data...")
    cursor.execute("DELETE FROM videos_fts")
    cursor.execute("INSERT INTO videos_fts(rowid, title, description) SELECT id, title, description FROM videos")

    print("Optimizing database...")
    cursor.execute("ANALYZE")
    
    conn.commit()
    conn.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate()
