#!/bin/bash

# Setup cron job for data retention
# Run every hour to maintain 24-hour retention policy

# Add to crontab
(crontab -l 2>/dev/null; echo "0 * * * * /Users/sam/Desktop/splits-oddsfeed/scripts/data_retention.sh >> /var/log/data_retention.log 2>&1") | crontab -

echo "Data retention cron job installed - runs hourly"
echo "Log file: /var/log/data_retention.log"
