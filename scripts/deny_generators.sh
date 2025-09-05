#!/bin/bash
# Deny list for fake/test/generator services
# Exit with error if any are found in production configs

set -euo pipefail

# Parse arguments
DRY_RUN=false
CUSTOM_PATHS=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --paths)
      CUSTOM_PATHS="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--dry-run] [--paths 'path1 path2 ...']"
      exit 1
      ;;
  esac
done

# Portable word boundary emulation for grep -E
# Pattern matches if preceded/followed by non-word chars or line boundaries
# Note: Excludes legitimate browser args like --no-sandbox
PATTERN="(remaining_books|universal_collector|dkfd_real|multi_collector|sandbox|shim|cdp|test_data|fake_data|mock_|generator|universal_scraper|barstool_sandbox|draftkings_sandbox|fanduel_sandbox|betmgm_sandbox)"
WRAPPED="(^|[^A-Za-z0-9_-])(${PATTERN})([^A-Za-z0-9_]|$)"

# Default to production-relevant paths
if [ -z "$CUSTOM_PATHS" ]; then
  PATHS="docker-compose.prod.yml render.yaml collectors/ normalizer/ infra/ .github/workflows/"
else
  PATHS="$CUSTOM_PATHS"
fi

echo "🔍 Checking for banned generator/fake services..."
echo "Paths: $PATHS"
echo "Mode: $([ "$DRY_RUN" = true ] && echo "dry-run" || echo "enforce")"
echo ""

FOUND_VIOLATIONS=0
for path in $PATHS; do
  if [ ! -e "$path" ]; then
    continue
  fi

  # Find files to check
  if [ -f "$path" ]; then
    FILES="$path"
  else
    FILES=$(find "$path" -type f \( -name "*.yml" -o -name "*.yaml" -o -name "*.py" -o -name "*.sh" \) 2>/dev/null | grep -v "__pycache__" | grep -v ".pyc" | grep -v "/venv/" | grep -v "/.venv/" | grep -v "/site-packages/" || true)
  fi

  for file in $FILES; do
    # Skip archive and docs directories
    if echo "$file" | grep -E "(archive/|docs/)" > /dev/null; then
      continue
    fi

    # Check for violations (exclude comments)
    if grep -E "$WRAPPED" "$file" 2>/dev/null | grep -v "^[[:space:]]*#" | head -5 > /tmp/violations.txt; then
      if [ -s /tmp/violations.txt ]; then
        echo "❌ Found banned patterns in: $file"
        cat /tmp/violations.txt | sed 's/^/    /'
        FOUND_VIOLATIONS=1
      fi
    fi
  done
done

# Clean up temp file
rm -f /tmp/violations.txt

# Check if any are running (not in dry-run mode)
if [ "$DRY_RUN" = false ]; then
  RUNNING=$(docker ps --format "{{.Names}}" | grep -E "$PATTERN" || true)
  if [ ! -z "$RUNNING" ]; then
    echo "❌ ERROR: Banned services are running:"
    echo "$RUNNING"
    echo "Stop these immediately: docker stop $RUNNING"
    FOUND_VIOLATIONS=1
  fi
fi

# Final result
echo ""
if [ $FOUND_VIOLATIONS -eq 1 ]; then
  echo "❌ FAILED: Found banned patterns in production paths"
  exit 1
else
  echo "✅ PASSED: No banned patterns found in production paths"
  exit 0
fi
