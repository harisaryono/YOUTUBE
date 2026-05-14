#!/usr/bin/env python3
"""
Module untuk enkripsi API key di provider_accounts database.

Penggunaan:
- Key enkripsi harus disimpan di environment variable: PROVIDER_ENCRYPTION_KEY
- Key ini hanya ada di server coordinator, tidak disebarkan ke client
- Client hanya bisa mendapatkan API key yang sudah di-decrypt melalui API coordinator
"""
from typing import Optional, Dict
from base64 import b64decode, b64encode

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:
    Fernet = None  # type: ignore[assignment]
    InvalidToken = Exception

from local_services import DEFAULT_PROVIDERS_DB, service_env

ENCRYPTION_KEY_ENV = "PROVIDER_ENCRYPTION_KEY"
_DEFAULT_CIPHERTEXT_PREFIX = "ENC:"


def get_encryption_key() -> bytes:
    """Dapatkan encryption key dari environment variable."""
    key = service_env(ENCRYPTION_KEY_ENV, "")
    if not key:
        # Jika tidak ada di env, kembalikan None untuk fallback tanpa enkripsi
        # Ini untuk backward compatibility
        return None
    
    # Key harus 32 bytes untuk Fernet
    key_bytes = key.encode("utf-8")
    if len(key_bytes) != 32:
        # Pad atau trunc ke 32 bytes
        key_bytes = (key_bytes + b"0" * 32)[:32]
    
    # Generate Fernet key dari 32 bytes key
    # Fernet mengharapkan base64-encoded 32-byte key
    return b64encode(key_bytes)


def get_fernet() -> Optional[Fernet]:
    """Dapatkan Fernet cipher instance."""
    if Fernet is None:
        return None
    
    key = get_encryption_key()
    if key is None:
        return None
    
    try:
        return Fernet(key)
    except Exception:
        return None


def encrypt_api_key(api_key: str) -> str:
    """Enkripsi API key."""
    if not api_key:
        return ""
    
    cipher = get_fernet()
    if cipher is None:
        # Fallback: tidak enkripsi (backward compatibility)
        return api_key
    
    try:
        encrypted = cipher.encrypt(api_key.encode("utf-8"))
        return _DEFAULT_CIPHERTEXT_PREFIX + b64encode(encrypted).decode("utf-8")
    except Exception:
        # Jika gagal enkripsi, kembalikan original
        return api_key


def decrypt_api_key(encrypted: str) -> str:
    """Dekripsi API key."""
    if not encrypted:
        return ""
    
    # Cek apakah ini ciphertext
    if not encrypted.startswith(_DEFAULT_CIPHERTEXT_PREFIX):
        return encrypted
    
    cipher = get_fernet()
    if cipher is None:
        # Jika cipher tidak ada (misal key salah/tidak ada), 
        # jangan kembalikan string terenkripsi karena akan menyebabkan 401
        return ""
    
    try:
        # Strip 'ENC:' prefix
        ciphertext_b64 = encrypted[len(_DEFAULT_CIPHERTEXT_PREFIX):]
        # Decrypt
        encrypted_bytes = b64decode(ciphertext_b64.encode("utf-8"))
        decrypted = cipher.decrypt(encrypted_bytes)
        return decrypted.decode("utf-8")
    except (InvalidToken, Exception) as e:
        # Jika gagal dekripsi, kembalikan kosong agar caller tahu auth gagal
        return ""


def migrate_api_keys_to_encryption(db_path: str = str(DEFAULT_PROVIDERS_DB)) -> Dict[str, int]:
    """
    Migrasi semua API key di database ke format terenkripsi.
    
    Returns:
        Dictionary dengan statistik: {
            "total": jumlah total akun,
            "already_encrypted": yang sudah terenkripsi,
            "encrypted": yang berhasil dienkripsi,
            "skipped": yang dilewati (tanpa enkripsi key)
        }
    """
    import sqlite3
    
    stats = {
        "total": 0,
        "already_encrypted": 0,
        "encrypted": 0,
        "skipped": 0
    }
    
    cipher = get_fernet()
    if cipher is None:
        stats["skipped"] = 0
        return stats
    
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        # Cek semua akun
        rows = con.execute(
            "SELECT id, provider, account_name, api_key FROM provider_accounts"
        ).fetchall()
        
        stats["total"] = len(rows)
        
        for row in rows:
            api_key = str(row["api_key"] or "")
            
            # Skip jika tidak ada api key
            if not api_key:
                continue
            
            # Skip jika sudah terenkripsi
            if api_key.startswith(_DEFAULT_CIPHERTEXT_PREFIX):
                stats["already_encrypted"] += 1
                continue
            
            # Enkripsi dan update
            encrypted = encrypt_api_key(api_key)
            if encrypted != api_key:
                con.execute(
                    "UPDATE provider_accounts SET api_key=? WHERE id=?",
                    (encrypted, row["id"])
                )
                stats["encrypted"] += 1
        
        con.commit()
    finally:
        con.close()
    
    return stats


def verify_encryption(db_path: str = str(DEFAULT_PROVIDERS_DB)) -> Dict[str, int]:
    """
    Verifikasi status enkripsi API key di database.
    
    Returns:
        Dictionary dengan statistik: {
            "total": jumlah total akun,
            "encrypted": yang sudah terenkripsi,
            "plaintext": yang masih plaintext
        }
    """
    import sqlite3
    
    stats = {
        "total": 0,
        "encrypted": 0,
        "plaintext": 0
    }
    
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT id, provider, account_name, api_key FROM provider_accounts"
        ).fetchall()
        
        stats["total"] = len(rows)
        
        for row in rows:
            api_key = str(row["api_key"] or "")
            
            if api_key.startswith(_DEFAULT_CIPHERTEXT_PREFIX):
                stats["encrypted"] += 1
            elif api_key:
                stats["plaintext"] += 1
        
    finally:
        con.close()
    
    return stats
