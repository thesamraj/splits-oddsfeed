#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create audit directory
mkdir -p .audit
AUDIT_LOG=".audit/audit_$(date +%Y%m%d_%H%M%S).log"

# Load environment if exists
if [ -f .env ]; then
    set -a
    source .env >/dev/null 2>&1 || true
    set +a
fi

echo "========================================" | tee -a "$AUDIT_LOG"
echo "  SPLITS ODDSFEED REPO AUDIT" | tee -a "$AUDIT_LOG"
echo "  $(date)" | tee -a "$AUDIT_LOG"
echo "========================================" | tee -a "$AUDIT_LOG"
echo "" | tee -a "$AUDIT_LOG"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# Function to check result
check_result() {
    local name="$1"
    local result="$2"
    local status="$3"
    
    if [ "$status" = "PASS" ]; then
        echo -e "[${GREEN}PASS${NC}] $name: $result" | tee -a "$AUDIT_LOG"
        ((PASS_COUNT++))
    elif [ "$status" = "WARN" ]; then
        echo -e "[${YELLOW}WARN${NC}] $name: $result" | tee -a "$AUDIT_LOG"
        ((WARN_COUNT++))
    else
        echo -e "[${RED}FAIL${NC}] $name: $result" | tee -a "$AUDIT_LOG"
        ((FAIL_COUNT++))
    fi
}

echo "1. CODE INVENTORY" | tee -a "$AUDIT_LOG"
echo "-----------------" | tee -a "$AUDIT_LOG"

# Count collectors
COLLECTOR_COUNT=$(find collectors -name "Dockerfile" -type f 2>/dev/null | wc -l | tr -d ' ')
if [ "$COLLECTOR_COUNT" -gt 0 ]; then
    check_result "Collectors found" "$COLLECTOR_COUNT collectors with Dockerfiles" "PASS"
else
    check_result "Collectors found" "No collectors found" "FAIL"
fi

# Check for key collectors
for collector in bovada_real kambi_browser kambi_unified draftkings_ws fanduel_ws; do
    if [ -f "collectors/$collector/Dockerfile" ]; then
        check_result "Collector $collector" "Present" "PASS"
    else
        check_result "Collector $collector" "Missing" "WARN"
    fi
done

echo "" | tee -a "$AUDIT_LOG"
echo "2. ENVIRONMENT CHECK" | tee -a "$AUDIT_LOG"
echo "--------------------" | tee -a "$AUDIT_LOG"

# Check critical env vars
for var in DATABASE_URL REDIS_URL; do
    if [ -n "${!var:-}" ]; then
        check_result "Env $var" "Set" "PASS"
    else
        check_result "Env $var" "Not set" "WARN"
    fi
done

# Check proxy config
if [ -n "${PROXY_HOST:-}" ] || [ -n "${ZENROWS_API_KEY:-}" ] || [ -n "${BRIGHTDATA_API_KEY:-}" ]; then
    check_result "Proxy config" "Configured" "PASS"
else
    check_result "Proxy config" "No proxy configured" "WARN"
fi

echo "" | tee -a "$AUDIT_LOG"
echo "3. DOCKER COMPOSE VALIDATION" | tee -a "$AUDIT_LOG"
echo "----------------------------" | tee -a "$AUDIT_LOG"

# Validate compose file
if docker compose -f docker-compose.local.yml config >/dev/null 2>&1; then
    check_result "Docker compose config" "Valid" "PASS"
else
    check_result "Docker compose config" "Invalid" "FAIL"
fi

echo "" | tee -a "$AUDIT_LOG"
echo "4. SERVICE HEALTH CHECKS" | tee -a "$AUDIT_LOG"
echo "------------------------" | tee -a "$AUDIT_LOG"

# Check each service endpoint
for service in "metrics-proxy:8000" "bovada:19081" "normalizer:19082"; do
    name="${service%%:*}"
    port="${service##*:}"
    
    if curl -s -m 2 "http://localhost:$port/health" >/dev/null 2>&1; then
        check_result "Service $name" "Responding on :$port" "PASS"
    else
        check_result "Service $name" "Not responding on :$port" "WARN"
    fi
done

echo "" | tee -a "$AUDIT_LOG"
echo "5. METRICS CHECK" | tee -a "$AUDIT_LOG"
echo "----------------" | tee -a "$AUDIT_LOG"

# Check Bovada metrics if available
METRICS=$(curl -s -m 2 http://localhost:8000/metrics 2>/dev/null || echo "")
if [ -n "$METRICS" ]; then
    COLLECTOR_UP=$(echo "$METRICS" | grep -E "collector_up.*bovada" | grep -oE "[0-9]+\.[0-9]+" | head -1 || echo "0")
    TICKS=$(echo "$METRICS" | grep -E "ticks_total.*bovada" | grep -oE "[0-9]+" | tail -1 || echo "0")
    
    if [ "${COLLECTOR_UP%.*}" = "1" ]; then
        check_result "Bovada collector" "UP (ticks: $TICKS)" "PASS"
    else
        check_result "Bovada collector" "DOWN" "FAIL"
    fi
else
    check_result "Metrics endpoint" "Not available" "WARN"
fi

echo "" | tee -a "$AUDIT_LOG"
echo "6. DATA CONNECTIVITY" | tee -a "$AUDIT_LOG"
echo "--------------------" | tee -a "$AUDIT_LOG"

# Check database
if [ -n "${DATABASE_URL:-}" ]; then
    if python3 -c "import psycopg; psycopg.connect('$DATABASE_URL').close()" 2>/dev/null; then
        check_result "PostgreSQL" "Connected" "PASS"
    else
        check_result "PostgreSQL" "Connection failed" "FAIL"
    fi
else
    check_result "PostgreSQL" "No DATABASE_URL" "WARN"
fi

# Check Redis
if [ -n "${REDIS_URL:-}" ]; then
    if python3 -c "import redis; redis.from_url('$REDIS_URL').ping()" 2>/dev/null; then
        check_result "Redis" "Connected" "PASS"
    else
        check_result "Redis" "Connection failed" "FAIL"
    fi
else
    check_result "Redis" "No REDIS_URL" "WARN"
fi

echo "" | tee -a "$AUDIT_LOG"
echo "========================================" | tee -a "$AUDIT_LOG"
echo "SUMMARY" | tee -a "$AUDIT_LOG"
echo "========================================" | tee -a "$AUDIT_LOG"
echo -e "${GREEN}PASS: $PASS_COUNT${NC}" | tee -a "$AUDIT_LOG"
echo -e "${YELLOW}WARN: $WARN_COUNT${NC}" | tee -a "$AUDIT_LOG"
echo -e "${RED}FAIL: $FAIL_COUNT${NC}" | tee -a "$AUDIT_LOG"
echo "" | tee -a "$AUDIT_LOG"

# Overall status
if [ "$FAIL_COUNT" -eq 0 ]; then
    if [ "$WARN_COUNT" -eq 0 ]; then
        echo -e "${GREEN}✓ SYSTEM HEALTHY${NC}" | tee -a "$AUDIT_LOG"
        exit 0
    else
        echo -e "${YELLOW}⚠ SYSTEM OPERATIONAL WITH WARNINGS${NC}" | tee -a "$AUDIT_LOG"
        exit 0
    fi
else
    echo -e "${RED}✗ SYSTEM HAS FAILURES${NC}" | tee -a "$AUDIT_LOG"
    echo "See $AUDIT_LOG for details" | tee -a "$AUDIT_LOG"
    exit 1
fi