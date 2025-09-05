#!/bin/bash
# Verify no secrets are committed to the repository
# Exit with error if any potential secrets are found

set -e

echo "🔍 Scanning for potential secrets in repository..."

FOUND_SECRETS=0

# Patterns to search for (case-insensitive)
# Focus on actual secrets, not documentation/examples
SECRET_PATTERNS=(
    "postgresql://[^<]+:npg_[A-Za-z0-9]+@[^<]+neon\.tech/"  # Real Neon URLs with npg_ passwords
    "AKIA[A-Z0-9]{16}"  # AWS access keys always start with AKIA
    "BEGIN PRIVATE KEY"
    "BEGIN RSA PRIVATE KEY"
)

# Files to exclude from scanning
EXCLUDE_PATTERNS=(
    "*.log"
    "*.pyc"
    ".git"
    "node_modules"
    "__pycache__"
    "*.min.js"
    "*.map"
    ".env"
    ".env.*"
)

# Build grep exclude arguments
GREP_EXCLUDES=""
for pattern in "${EXCLUDE_PATTERNS[@]}"; do
    GREP_EXCLUDES="$GREP_EXCLUDES --exclude=$pattern"
done

# Check each secret pattern
for pattern in "${SECRET_PATTERNS[@]}"; do
    echo -n "Checking for: $pattern ... "

    # Special handling for .env.example - allow only if it contains placeholders
    if [[ "$pattern" == "postgresql://"* ]]; then
        # Check if .env.example has real URLs (not placeholders)
        if grep -E "postgresql://[^<]+@[^<]+neon\.tech/" .env.example 2>/dev/null | grep -v "<user>\|<pass>\|<host>\|<db>" > /dev/null; then
            echo "❌ FOUND in .env.example (not a placeholder!)"
            grep -n -E "postgresql://[^<]+@[^<]+neon\.tech/" .env.example
            FOUND_SECRETS=1
        else
            echo "✅ OK (placeholders only in .env.example)"
        fi
    fi

    # Check all other files
    MATCHES=$(grep -r -i -E "$pattern" . \
        --exclude-dir=.git \
        --exclude-dir=node_modules \
        --exclude-dir=__pycache__ \
        --exclude-dir=venv \
        --exclude-dir=.venv \
        --exclude-dir=docs \
        --exclude-dir=backups \
        --exclude-dir=archive \
        --exclude="*.log" \
        --exclude="*.pyc" \
        --exclude="*.gz" \
        --exclude="*.sql" \
        --exclude=".env" \
        --exclude=".env.*" \
        --exclude="verify_no_secrets.sh" \
        2>/dev/null | head -5 || true)

    if [ -n "$MATCHES" ]; then
        echo "❌ POTENTIAL SECRET FOUND!"
        echo "$MATCHES"
        FOUND_SECRETS=1
    else
        echo "✅ OK"
    fi
done

# Additional check for common secret file names
echo -n "Checking for secret files... "
SECRET_FILES=(
    "id_rsa"
    "id_dsa"
    "*.key"
    "*.p12"
    "*.pfx"
    "credentials.json"
    "service-account.json"
    "firebase-admin.json"
)

for file_pattern in "${SECRET_FILES[@]}"; do
    # Exclude vendor/venv directories and known safe files
    if find . -name "$file_pattern" -not -path "./.git/*" -not -path "./node_modules/*" -not -path "./venv/*" -not -path "*/site-packages/*" -not -path "./tools/venv/*" 2>/dev/null | head -1 | grep .; then
        echo "❌ Found secret file: $file_pattern"
        FOUND_SECRETS=1
    fi
done

if [ $FOUND_SECRETS -eq 0 ]; then
    echo "✅ No secret files found"
fi

# Check git history for recently added secrets (last 5 commits)
echo -n "Checking recent commits for secrets... "
# Exclude placeholders like <user>, <password>, <host>, <database>
if git log --diff-filter=A -p -5 2>/dev/null | grep -E "password=|api_key=|secret_key=|npg_" | grep -v "<user>\|<password>\|<host>\|<database>\|<pass>" | head -3 | grep .; then
    echo "⚠️  WARNING: Potential secrets in recent commits (consider rewriting history)"
    # Don't fail on git history, just warn
else
    echo "✅ OK"
fi

# Final result
echo ""
if [ $FOUND_SECRETS -eq 1 ]; then
    echo "❌ FAILED: Potential secrets detected in repository!"
    echo "Action required:"
    echo "1. Remove or replace secrets with placeholders"
    echo "2. Rotate any exposed credentials immediately"
    echo "3. Consider rewriting git history if secrets were committed"
    exit 1
else
    echo "✅ PASSED: No secrets detected"
    exit 0
fi
