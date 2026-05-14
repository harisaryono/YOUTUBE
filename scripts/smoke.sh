#!/bin/bash
# ============================================
# Smoke Test untuk YouTube Transcript Framework
# ============================================
# Mengecek:
#   - Syntax Python file utama
#   - Help message wrapper scripts
#   - Koneksi database
#   - Path uploads
#   - Dependency Playwright
#   - Coordinator health check (jika env tersedia)
#
# Usage:
#   ./scripts/smoke.sh              # semua test
#   ./scripts/smoke.sh --quick      # hanya syntax + help
#   ./scripts/smoke.sh --db-only    # hanya database
# ============================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh" 2>/dev/null || echo "")"

PASS=0
FAIL=0
SKIP=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { PASS=$((PASS+1)); echo -e "  ${GREEN}✓${NC} $1"; }
fail() { FAIL=$((FAIL+1)); echo -e "  ${RED}✗${NC} $1"; }
skip() { SKIP=$((SKIP+1)); echo -e "  ${YELLOW}−${NC} $1"; }

echo "============================================="
echo "🔍 Smoke Test: YouTube Transcript Framework"
echo "============================================="
echo "Repo: $REPO_DIR"
echo "Date: $(date)"
echo "============================================="
echo ""

# --- 1. Python syntax check ---
echo "--- Python Syntax Check ---"
PY_FILES=(
    "$REPO_DIR/recover_transcripts.py"
    "$REPO_DIR/recover_transcripts_from_csv.py"
    "$REPO_DIR/fill_missing_resumes_youtube_db.py"
    "$REPO_DIR/launch_resume_queue.py"
    "$REPO_DIR/local_services.py"
    "$REPO_DIR/manage_database.py"
    "$REPO_DIR/database_optimized.py"
    "$REPO_DIR/format_transcripts_pool.py"
    "$REPO_DIR/job_tracker.py"
    "$REPO_DIR/update_latest_channel_videos.py"
    "$REPO_DIR/savesubs_playwright.py"
    "$REPO_DIR/shard_storage.py"
)

for f in "${PY_FILES[@]}"; do
    if [ -f "$f" ]; then
        if python3 -m py_compile "$f" 2>/dev/null; then
            pass "Syntax OK: $(basename "$f")"
        else
            fail "Syntax ERROR: $(basename "$f")"
        fi
    else
        skip "File not found: $(basename "$f")"
    fi
done
echo ""

# --- 2. Wrapper script help ---
echo "--- Wrapper Script Help ---"
WRAPPER_SCRIPTS=(
    "$REPO_DIR/scripts/transcript.sh"
    "$REPO_DIR/scripts/discover.sh"
    "$REPO_DIR/scripts/resume.sh"
    "$REPO_DIR/scripts/format.sh"
    "$REPO_DIR/scripts/run_pipeline.sh"
)

for s in "${WRAPPER_SCRIPTS[@]}"; do
    if [ -f "$s" ]; then
        if [ -x "$s" ]; then
            # Just check if --help exits with 0
            if bash "$s" --help >/dev/null 2>&1; then
                pass "Help OK: $(basename "$s")"
            else
                fail "Help FAILED: $(basename "$s")"
            fi
        else
            fail "Not executable: $(basename "$s")"
        fi
    else
        skip "Script not found: $(basename "$s")"
    fi
done
echo ""

# --- 3. Virtual environment ---
echo "--- Virtual Environment ---"
if [ -n "$VENV_PYTHON" ] && [ -x "$VENV_PYTHON" ]; then
    PY_VER=$("$VENV_PYTHON" --version 2>&1)
    pass "Python: $PY_VER"
else
    fail "Virtualenv not found or not executable"
fi
echo ""

# --- 4. Database ---
echo "--- Database ---"
DB_PATH="$REPO_DIR/youtube_transcripts.db"
if [ -f "$DB_PATH" ]; then
    if command -v sqlite3 &>/dev/null; then
        DB_SIZE=$(stat --format=%s "$DB_PATH" 2>/dev/null || echo "0")
        DB_SIZE_HUMAN=$(numfmt --to=iec "$DB_SIZE" 2>/dev/null || echo "${DB_SIZE}B")
        TABLE_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sqlite_master WHERE type='table';" 2>/dev/null || echo "?")
        pass "DB found: $(basename "$DB_PATH") (${DB_SIZE_HUMAN}, ${TABLE_COUNT} tables)"
    else
        pass "DB found: $(basename "$DB_PATH") (sqlite3 CLI not available)"
    fi
else
    skip "DB not found: $(basename "$DB_PATH")"
fi
echo ""

# --- 5. Uploads directory ---
echo "--- Uploads Directory ---"
UPLOADS_DIR="$REPO_DIR/uploads"
if [ -d "$UPLOADS_DIR" ]; then
    CHANNEL_COUNT=$(find "$UPLOADS_DIR" -maxdepth 1 -type d | wc -l)
    CHANNEL_COUNT=$((CHANNEL_COUNT - 1))  # subtract uploads itself
    pass "Uploads dir exists: ${CHANNEL_COUNT} channel directories"
else
    skip "Uploads dir not found"
fi
echo ""

# --- 6. Playwright dependency ---
echo "--- Playwright Dependency ---"
if [ -f "$REPO_DIR/savesubs_playwright.py" ]; then
    if grep -q "playwright" "$REPO_DIR/requirements.txt" 2>/dev/null; then
        pass "Playwright listed in requirements.txt"
    else
        fail "Playwright NOT in requirements.txt"
    fi
    # Check if actually installed
    if [ -n "$VENV_PYTHON" ] && [ -x "$VENV_PYTHON" ]; then
        if "$VENV_PYTHON" -c "import playwright" 2>/dev/null; then
            pass "Playwright package installed"
        else
            skip "Playwright package not installed (run: pip install playwright)"
        fi
    fi
else
    skip "savesubs_playwright.py not found"
fi
echo ""

# --- 7. Coordinator health check (optional) ---
echo "--- Coordinator Health Check ---"
COORD_URL="${YT_PROVIDER_COORDINATOR_URL:-}"
if [ -n "$COORD_URL" ]; then
    if command -v curl &>/dev/null; then
        if curl -sf --max-time 5 "$COORD_URL/health" >/dev/null 2>&1; then
            pass "Coordinator reachable at $COORD_URL"
        else
            skip "Coordinator not reachable at $COORD_URL"
        fi
    else
        skip "curl not available for coordinator check"
    fi
else
    skip "YT_PROVIDER_COORDINATOR_URL not set"
fi
echo ""

# --- 8. .env file ---
echo "--- Environment File ---"
if [ -f "$REPO_DIR/.env" ]; then
    pass ".env file exists"
else
    skip ".env file not found (use .env.example as template)"
fi
echo ""

# --- Summary ---
echo "============================================="
echo "📊 Smoke Test Results"
echo "============================================="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "  Skipped: $SKIP"
echo "  Total: $((PASS + FAIL + SKIP))"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    echo -e "${RED}❌ Some tests FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All tests passed${NC}"
    exit 0
fi
