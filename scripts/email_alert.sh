#!/bin/bash

# Email Alert Script
# Sends email notifications for critical alerts

ALERT_TYPE=$1
ALERT_MESSAGE=$2
EMAIL_TO=${EMAIL_TO:-"admin@example.com"}
EMAIL_FROM="oddsfeed-alerts@localhost"

# Check if mail command exists
if ! command -v mail &> /dev/null; then
    echo "Mail command not found. Installing..."
    apt-get update && apt-get install -y mailutils
fi

# Send email alert
echo "$ALERT_MESSAGE" | mail -s "[ODDS ALERT] $ALERT_TYPE" -r "$EMAIL_FROM" "$EMAIL_TO"

# Log alert
echo "[$(date)] Alert sent: $ALERT_TYPE - $ALERT_MESSAGE" >> /var/log/oddsfeed_alerts.log

# Also send to webhook if configured
if [ -n "$SLACK_WEBHOOK_URL" ]; then
    curl -X POST -H 'Content-type: application/json' \
        --data "{\"text\":\"🚨 *$ALERT_TYPE*\n$ALERT_MESSAGE\"}" \
        "$SLACK_WEBHOOK_URL"
fi

# PagerDuty integration if configured
if [ -n "$PAGERDUTY_KEY" ]; then
    curl -X POST https://events.pagerduty.com/v2/enqueue \
        -H 'Content-Type: application/json' \
        -d "{
            \"routing_key\": \"$PAGERDUTY_KEY\",
            \"event_action\": \"trigger\",
            \"payload\": {
                \"summary\": \"$ALERT_TYPE\",
                \"severity\": \"error\",
                \"source\": \"oddsfeed\",
                \"custom_details\": {
                    \"message\": \"$ALERT_MESSAGE\"
                }
            }
        }"
fi
