import requests
import time
import sys

BASE_URL = "http://localhost:5000"
ADMIN_TOKEN = "admin123"

# Sample IDs
VIDEO_ID = "fO6Qj_PfoSU"
CHANNEL_ID = "@AncestralYields"

ROUTES = [
    ("/", 200),
    ("/channels", 200),
    (f"/channel/{CHANNEL_ID}", 200),
    ("/videos", 200),
    (f"/video/{VIDEO_ID}", 200),
    ("/search?q=test", 200),
    ("/api/statistics", 200),
    ("/api/videos", 200),
    (f"/api/video/{VIDEO_ID}", 200),
    (f"/api/transcript/{VIDEO_ID}", 200),
    (f"/api/summary/{VIDEO_ID}", 200),
    (f"/api/formatted/{VIDEO_ID}", 200),
    ("/robots.txt", 200),
    # Admin routes (Protected)
    ("/admin/data", 200, True),
    ("/admin/data/jobs", 200, True),
    ("/admin/data/metrics", 200, True),
    # Admin routes (Unauthenticated should redirect/fail)
    ("/admin/data", 302, False), 
]

def test_routes():
    session = requests.Session()
    session.cookies.set("admin_data_token", ADMIN_TOKEN)
    
    no_auth_session = requests.Session()

    results = []
    print(f"{'Route':<40} | {'Expected':<10} | {'Actual':<10} | {'Status'}")
    print("-" * 75)
    
    all_ok = True
    for route_info in ROUTES:
        path = route_info[0]
        expected = route_info[1]
        use_auth = route_info[2] if len(route_info) > 2 else False
        
        url = f"{BASE_URL}{path}"
        s = session if use_auth else no_auth_session
        
        try:
            # allow_redirects=False to check for 302
            resp = s.get(url, allow_redirects=False, timeout=5)
            actual = resp.status_code
            status = "✅ PASS" if actual == expected else "❌ FAIL"
            if actual != expected:
                all_ok = False
            
            print(f"{path:<40} | {expected:<10} | {actual:<10} | {status}")
            results.append((path, expected, actual, status))
        except Exception as e:
            print(f"{path:<40} | {expected:<10} | ERROR      | ❌ FAIL ({e})")
            all_ok = False

    return all_ok

if __name__ == "__main__":
    if not test_routes():
        sys.exit(1)
