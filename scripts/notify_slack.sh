#!/bin/bash
# Send notification to Slack webhook
# Usage: notify_slack.sh "<message>"

set -euo pipefail

# Check arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 \"<message>\""
    exit 1
fi

MESSAGE="$1"

# Check for webhook URL
if [ -z "${SLACK_WEBHOOK:-}" ]; then
    echo "Warning: SLACK_WEBHOOK not set, skipping notification"
    exit 0
fi

# Send to Slack
PAYLOAD=$(cat <<EOF
{
    "text": "$MESSAGE"
}
EOF
)

curl -X POST \
    -H 'Content-Type: application/json' \
    -d "$PAYLOAD" \
    "$SLACK_WEBHOOK" \
    -s -o /dev/null -w "Slack notification sent (HTTP %{http_code})\n"
