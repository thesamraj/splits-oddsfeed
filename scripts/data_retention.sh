#!/bin/bash

# Data Retention Policy Script
# Maintains 24 hours of data in main tables, archives older data

# Configuration
RETENTION_HOURS=${RETENTION_HOURS:-24}
ARCHIVE_DIR="/var/oddsfeed/archive"
DB_NAME="oddsfeed"
DB_USER="odds"

# Create archive directory if it doesn't exist
mkdir -p $ARCHIVE_DIR

# Function to archive old data
archive_old_data() {
    local table=$1
    local timestamp_col=$2
    local archive_file="$ARCHIVE_DIR/${table}_$(date +%Y%m%d_%H%M%S).csv"

    echo "Archiving old data from $table..."

    # Export old data to CSV
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "\
        COPY (SELECT * FROM $table WHERE $timestamp_col < NOW() - INTERVAL '$RETENTION_HOURS hours') \
        TO STDOUT WITH CSV HEADER;" > "$archive_file"

    # Compress the archive
    gzip "$archive_file"

    # Delete old data from table
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "\
        DELETE FROM $table WHERE $timestamp_col < NOW() - INTERVAL '$RETENTION_HOURS hours';"

    # Get row count deleted
    local deleted=$(docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -t -c "\
        SELECT COUNT(*) FROM $table WHERE $timestamp_col < NOW() - INTERVAL '$RETENTION_HOURS hours';")

    echo "Archived and deleted $deleted rows from $table"
}

# Function to vacuum tables
vacuum_tables() {
    echo "Vacuuming tables for performance..."
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "VACUUM ANALYZE odds;"
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "VACUUM ANALYZE events;"
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "VACUUM ANALYZE odds_ticks;"
}

# Function to check disk usage
check_disk_usage() {
    local usage=$(docker exec splits-oddsfeed-store-1 df -h /var/lib/postgresql/data | tail -1 | awk '{print $5}' | sed 's/%//')

    if [ "$usage" -gt 80 ]; then
        echo "WARNING: Disk usage is at ${usage}%"
        # Send alert
        if [ -n "$EMAIL_TO" ]; then
            echo "Disk usage critical: ${usage}%" | mail -s "[ODDS ALERT] High Disk Usage" "$EMAIL_TO"
        fi
    else
        echo "Disk usage is at ${usage}%"
    fi
}

# Function to report statistics
report_stats() {
    echo "=== Data Retention Report ==="
    echo "Timestamp: $(date)"

    # Get table sizes
    docker exec splits-oddsfeed-store-1 psql -U $DB_USER -d $DB_NAME -c "\
        SELECT
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

    # Archive directory size
    echo "Archive size: $(du -sh $ARCHIVE_DIR 2>/dev/null | cut -f1)"

    echo "=============================="
}

# Main execution
echo "Starting data retention cleanup..."

# Archive old data from each table
archive_old_data "odds" "ts"
archive_old_data "events" "created_at"
archive_old_data "odds_ticks" "timestamp"

# Vacuum tables
vacuum_tables

# Check disk usage
check_disk_usage

# Report statistics
report_stats

echo "Data retention cleanup completed"
