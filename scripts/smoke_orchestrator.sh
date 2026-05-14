#!/usr/bin/env bash
# Orchestrator Smoke Test
# Validates: dry-run, real run, status, report, and SQLite state.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"

cd "$REPO_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    echo -n "  [ ] $name ... "
    if eval "$cmd" 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "============================================="
echo "🧪 Orchestrator Smoke Test"
echo "============================================="
echo "Repo: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "============================================="
echo ""

# 1. Dry-run
echo "--- 1. Dry-run ---"
check "Dry-run (once --dry-run --max-jobs 1)" \
    "'$VENV_PYTHON' -m orchestrator.daemon once --dry-run --max-jobs 1 2>&1 | grep -q 'DRY-RUN\|No jobs\|Cycle complete'"

# 2. Real run (1 job max)
echo "--- 2. Real run ---"
check "Run once (--max-jobs 1)" \
    "'$VENV_PYTHON' -m orchestrator.daemon once --max-jobs 1 2>&1 | grep -q 'Cycle complete'"

# 3. Status
echo "--- 3. Status ---"
check "Status command" \
    "'$VENV_PYTHON' -m orchestrator.daemon status 2>&1 | grep -q 'Last cycle\|No report'"

# 4. Report JSON
echo "--- 4. Report ---"
check "Report JSON" \
    "'$VENV_PYTHON' -m orchestrator.daemon report 2>&1 | grep -q 'generated_at\|No report'"

# 5. SQLite state check
echo "--- 5. SQLite state ---"
DB_PATH="$REPO_DIR/db/orchestrator.db"
if [ -f "$DB_PATH" ]; then
    check "SQLite events table" \
        "sqlite3 '$DB_PATH' 'SELECT COUNT(*) FROM orchestrator_events;' 2>&1 | grep -q '[0-9]'"
    check "SQLite cooldowns table" \
        "sqlite3 '$DB_PATH' 'SELECT COUNT(*) FROM orchestrator_cooldowns;' 2>&1 | grep -q '[0-9]'"
    check "SQLite locks table" \
        "sqlite3 '$DB_PATH' 'SELECT COUNT(*) FROM orchestrator_locks;' 2>&1 | grep -q '[0-9]'"
else
    echo -e "  ${YELLOW}⚠️  DB not found at $DB_PATH — skipping SQLite checks${NC}"
fi

# 6. Report file exists
echo "--- 6. Report files ---"
REPORT_DIR="$REPO_DIR/runs/orchestrator/reports"
if [ -d "$REPORT_DIR" ]; then
    check "latest.json exists" \
        "test -f '$REPORT_DIR/latest.json'"
    check "latest.md exists" \
        "test -f '$REPORT_DIR/latest.md'"
else
    echo -e "  ${YELLOW}⚠️  Report dir not found — skipping report file checks${NC}"
fi

# Summary
echo ""
echo "============================================="
echo -e "Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
