#!/usr/bin/env python3
from pathlib import Path
"""
Test Groq Llama-3.3-70b langsung (bypass coordinator).

Gunakan API key langsung untuk test model llama-3.3-70b-versatile
yang tidak terblok, meskipun gpt-oss-120b sedang blocked.
"""
import json
import os
import sqlite3
from urllib import request as urllib_request
from urllib import error as urllib_error

# Groq API endpoints
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

# Test prompt
FORMAT_PROMPT = """Anda bertugas memformat transcript YouTube mentah agar jauh lebih mudah dibaca dan dipahami, TANPA merangkum isi.

Aturan ketat:
- jangan merangkum
- pertahankan isi pembahasan selengkap mungkin
- rapikan tanda baca, kapitalisasi, paragraf
- output hanya hasil akhir dalam Markdown
"""

def get_groq_api_keys():
    """Get Groq API keys from database."""
    db_path = os.environ.get("YT_PROVIDERS_DB", str(Path(__file__).resolve().parent.parent.parent / "services/provider_accounts.sqlite3"))
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return []
    
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT pa.id, pa.account_name, pa.api_key 
        FROM provider_accounts pa 
        WHERE pa.provider = 'groq' 
          AND pa.is_active = 1
        ORDER BY pa.id
    """).fetchall()
    con.close()
    
    return [dict(row) for row in rows]


def test_groq_model(api_key: str, model_name: str, prompt: str) -> dict:
    """Test Groq model directly."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": "Format transcript berikut agar mudah dibaca:"},
            {"role": "user", "content": prompt[:5000]},  # Test dengan prompt pendek
        ],
        "max_tokens": 1000,
        "temperature": 0.4,
        "stream": False,
    }
    
    req = urllib_request.Request(
        GROQ_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    
    try:
        with urllib_request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw)
            
            # Extract response
            choices = result.get("choices") or []
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                return {
                    "ok": True,
                    "content": content[:500] + "..." if len(content) > 500 else content,
                    "usage": usage,
                    "model": result.get("model", model_name),
                }
            return {"ok": False, "error": "No choices in response"}
            
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "http_status": exc.code,
            "error": raw[:300] if raw else str(exc.reason),
        }
    except urllib_error.URLError as exc:
        return {"ok": False, "error": str(exc.reason)}


def main():
    print("=== Test Groq Llama-3.3-70b Direct API ===\n")
    
    # Get Groq accounts
    accounts = get_groq_api_keys()
    print(f"Found {len(accounts)} Groq accounts\n")
    
    # Test models
    models_to_test = [
        "llama-3.3-70b-versatile",
        "openai/gpt-oss-120b",  # This should be blocked
    ]
    
    test_prompt = """
Berikut transcript contoh:

"assalamualaikum warahmatullahi wabarakatuh baik pada kesempatan kali ini 
kita akan membahas tentang pentingnya menjaga lingkungan hidup lingkungan 
itu sangat penting bagi kehidupan kita semua oleh karena itu mari kita 
jaga lingkungan kita dengan baik terima kasih wassalamualaikum 
warahmatullahi wabarakatuh"
    """
    
    for model in models_to_test:
        print(f"\n{'='*50}")
        print(f"Testing model: {model}")
        print(f"{'='*50}")
        
        for i, acc in enumerate(accounts[:3]):  # Test first 3 accounts
            account_name = acc.get("account_name", f"Account {acc['id']}")
            api_key = acc.get("api_key", "")
            
            if not api_key:
                print(f"\n  Account {account_name}: No API key")
                continue
            
            result = test_groq_model(api_key, model, test_prompt)
            
            if result.get("ok"):
                print(f"\n  ✅ Account {account_name}: SUCCESS")
                print(f"     Model returned: {result.get('model')}")
                print(f"     Usage: {result.get('usage')}")
                print(f"     Preview: {result.get('content', '')[:100]}...")
            else:
                status = result.get("http_status", "ERROR")
                error = result.get("error", "Unknown")[:100]
                print(f"\n  ❌ Account {account_name}: {status}")
                print(f"     Error: {error}")
            
            if i < 2:
                import time
                time.sleep(0.5)  # Small delay between tests
        
        print()
    
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    main()
