#!/bin/bash
# Assert no mock/demo data generators in collectors or services
# Used in pre-commit hooks and CI to prevent mock data from entering production

set -e

echo "Checking for mock/demo/sample data in codebase..."

# Define forbidden patterns (case-insensitive)
PATTERNS=(
    "mock"
    "demo"
    "sample.*data"
    "fake.*data"
    "test.*data"
    "dummy"
    "Team[0-9]"
    "Home[0-9]"
    "Away[0-9]"
)

# Directories to check
DIRS=(
    "collectors"
    "services"
)

# Files to exclude from check
EXCLUDE_PATTERNS=(
    "__pycache__"
    ".pyc"
    "venv/"
    ".git/"
    "docs/"
    "README"
    "INFRASTRUCTURE_ANALYSIS"
    "test_"
    "_test"
)

FOUND_VIOLATIONS=0

for dir in "${DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        continue
    fi

    echo "Scanning $dir/..."

    for pattern in "${PATTERNS[@]}"; do
        # Build grep command with exclusions
        GREP_CMD="grep -r -i '$pattern' $dir --include='*.py' --include='*.js'"

        for exclude in "${EXCLUDE_PATTERNS[@]}"; do
            GREP_CMD="$GREP_CMD --exclude-dir='$exclude' --exclude='*$exclude*'"
        done

        # Execute grep and capture results
        if eval "$GREP_CMD" 2>/dev/null; then
            echo "❌ Found forbidden pattern: $pattern in $dir"
            FOUND_VIOLATIONS=$((FOUND_VIOLATIONS + 1))
        fi
    done
done

if [ $FOUND_VIOLATIONS -gt 0 ]; then
    echo ""
    echo "❌ FAIL: Found $FOUND_VIOLATIONS mock/demo violations"
    echo "Mock data generators are not allowed in production code."
    echo "Please remove all mock/demo/sample data and use real API endpoints only."
    exit 1
else
    echo "✅ PASS: No mock/demo data found"
    exit 0
fi