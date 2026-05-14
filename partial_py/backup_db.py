#!/usr/bin/env python3
"""
Database Backup Script untuk YouTube Transcript Framework
Membuat backup otomatis sebelum migrasi data
"""

import shutil
import sqlite3
from pathlib import Path
from datetime import datetime


def backup_database(db_path: str, backup_dir: str = "backups") -> str:
    """
    Membuat backup database dengan timestamp
    
    Args:
        db_path: Path ke file database yang akan dibackup
        backup_dir: Direktori untuk menyimpan backup
        
    Returns:
        Path ke file backup yang dibuat
    """
    db_file = Path(db_path)
    
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    # Create backup directory
    backup_path = Path(backup_dir)
    backup_path.mkdir(exist_ok=True)
    
    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{db_file.stem}_{timestamp}.db"
    backup_file = backup_path / backup_filename
    
    # Copy database file
    shutil.copy2(db_file, backup_file)
    
    # Also backup WAL and SHM files if they exist
    for ext in ['-wal', '-shm']:
        wal_file = Path(str(db_file) + ext)
        if wal_file.exists():
            shutil.copy2(wal_file, Path(str(backup_file) + ext))
    
    print(f"✅ Backup created: {backup_file}")
    print(f"   Size: {backup_file.stat().st_size:,} bytes")
    
    return str(backup_file)


def verify_backup(backup_path: str) -> bool:
    """
    Verifikasi bahwa backup dapat dibuka dan valid
    
    Args:
        backup_path: Path ke file backup
        
    Returns:
        True jika backup valid, False jika tidak
    """
    try:
        conn = sqlite3.connect(backup_path)
        cursor = conn.cursor()
        
        # Check if we can query the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Check integrity
        cursor.execute("PRAGMA integrity_check")
        integrity = cursor.fetchone()[0]
        
        conn.close()
        
        if integrity == 'ok':
            print(f"✅ Backup verified: {len(tables)} tables found, integrity OK")
            return True
        else:
            print(f"❌ Backup integrity check failed: {integrity}")
            return False
            
    except Exception as e:
        print(f"❌ Backup verification failed: {str(e)}")
        return False


def cleanup_old_backups(backup_dir: str = "backups", keep_count: int = 5):
    """
    Hapus backup lama, simpan hanya N backup terakhir
    
    Args:
        backup_dir: Direktori backup
        keep_count: Jumlah backup yang disimpan
    """
    backup_path = Path(backup_dir)
    
    if not backup_path.exists():
        return
    
    # Get all backup files
    backups = sorted(backup_path.glob("*.db"), key=lambda x: x.stat().st_mtime)
    
    # Remove old backups
    while len(backups) > keep_count:
        old_backup = backups.pop(0)
        old_backup.unlink()
        print(f"🗑️  Removed old backup: {old_backup}")


def main():
    """Main function untuk backup database"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Backup YouTube Transcripts Database")
    parser.add_argument(
        "--db", 
        default="youtube_transcripts.db",
        help="Path ke database file (default: youtube_transcripts.db)"
    )
    parser.add_argument(
        "--backup-dir",
        default="backups",
        help="Direktori untuk menyimpan backup (default: backups)"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verification setelah backup"
    )
    parser.add_argument(
        "--cleanup",
        type=int,
        default=5,
        help="Jumlah backup yang disimpan (default: 5)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔄 YouTube Transcripts Database Backup")
    print("=" * 60)
    print(f"📁 Source: {args.db}")
    print(f"💾 Backup dir: {args.backup_dir}")
    print()
    
    try:
        # Create backup
        backup_file = backup_database(args.db, args.backup_dir)
        
        # Verify backup
        if not args.no_verify:
            print()
            verify_backup(backup_file)
        
        # Cleanup old backups
        print()
        cleanup_old_backups(args.backup_dir, args.cleanup)
        
        print()
        print("=" * 60)
        print("✅ Backup completed successfully!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Backup failed: {str(e)}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    exit(main())
