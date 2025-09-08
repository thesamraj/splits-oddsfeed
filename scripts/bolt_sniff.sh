#!/usr/bin/env bash
set -euo pipefail

export BOLT_API_TOKEN='ba19414d-a166-4760-bd1b-51019c7b0cd1'
export BOLT_WS_URL="wss://spro.agency/api?key=${BOLT_API_TOKEN}"
export BOLT_INFO_URL="https://spro.agency/api/get_info?key=${BOLT_API_TOKEN}"
export BOLT_SPORTS="NFL,NBA,NHL,MLB,NCAAF,SOC"
export BOLT_BOOKS="draftkings,betmgm,espnbet,thescore,bet365,pointsbet,betway,superbook,wynnbet,bookmaker,betonline,circa,pinnacle,betrivers"
export BOLT_SNIFF_SECS="${BOLT_SNIFF_SECS:-600}"

echo "BoltOdds Protocol Analysis"
echo "=========================="
echo "Duration: ${BOLT_SNIFF_SECS}s"
echo ""

# Run sniffer
python3 docs/bolt_teardown/bolt_sniff.py

# Analyze captured frames
echo ""
echo "Analyzing captured frames..."
python3 docs/bolt_teardown/analyze_frames.py | tee docs/bolt_teardown/ANALYSIS.json

# Generate report
echo ""
echo "Generating report..."