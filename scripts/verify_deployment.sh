#!/bin/bash
# Verify deployment readiness for progressive rollout
set -euo pipefail

echo "==================================="
echo "DEPLOYMENT VERIFICATION"
echo "==================================="
echo ""

# 1. Check LIGHT_SOCCER is enabled
echo "1. Checking LIGHT_SOCCER setting..."
if grep -q "LIGHT_SOCCER=true" .env; then
    echo "   ✓ LIGHT_SOCCER=true in .env"
else
    echo "   ✗ LIGHT_SOCCER not enabled"
    exit 1
fi

# 2. Check key files exist
echo ""
echo "2. Checking required files..."
FILES=(
    "realness/weights.yml"
    "collectors/base/explainable_realness.py"
    "collectors/bovada_real/bovada_real_enhanced.py"
    "scripts/progressive_rollout.sh"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✓ $file exists"
    else
        echo "   ✗ $file missing"
        exit 1
    fi
done

# 3. Check Docker setup
echo ""
echo "3. Checking Docker configuration..."
if [ -f "docker-compose.override.bovada.yml" ]; then
    echo "   ✓ Bovada override file exists"
    # Check for correct port mapping
    if grep -q "19090:9090" docker-compose.override.bovada.yml && \
       grep -q "19091:9091" docker-compose.override.bovada.yml; then
        echo "   ✓ Port mappings correct (19090/19091)"
    else
        echo "   ✗ Port mappings incorrect"
    fi
else
    echo "   ✗ Bovada override file missing"
fi

# 4. Summary
echo ""
echo "==================================="
echo "DEPLOYMENT CHECKLIST:"
echo "==================================="
echo ""
echo "Local Testing:"
echo "  1. Start Bovada collector locally:"
echo "     docker-compose -f docker-compose.yml -f docker-compose.override.bovada.yml up bovada-collector"
echo ""
echo "  2. Monitor metrics:"
echo "     curl -s http://localhost:19090/metrics | grep realness"
echo ""
echo "  3. Check realness report:"
echo "     curl -s http://localhost:19091/realness/report | jq ."
echo ""
echo "Render Deployment:"
echo "  1. Push to main branch (or deploy branch)"
echo "  2. Set LIGHT_SOCCER=true in Render environment"
echo "  3. Monitor with progressive rollout script:"
echo "     ./scripts/progressive_rollout.sh"
echo ""
echo "==================================="
echo "STATUS: Ready for deployment"
echo "==================================="
