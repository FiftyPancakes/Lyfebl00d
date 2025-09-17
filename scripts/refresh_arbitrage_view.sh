#!/bin/bash

# Script to refresh triangle arbitrage materialized view
# Run this daily via cron: 0 2 * * * /path/to/scripts/refresh_arbitrage_view.sh

# Database connection parameters
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="lyfeblood"
DB_USER="ichevako"
DB_PASS="maddie"

# Log file
LOG_FILE="/tmp/arbitrage_refresh.log"

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_message "Starting triangle arbitrage view refresh..."

# Connect to database and refresh the view
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << EOF
\set ON_ERROR_STOP on

-- Refresh the materialized view
SELECT refresh_triangle_arbitrage_view();

-- Log the refresh
INSERT INTO arbitrage_refresh_log (refresh_time, opportunities_count) 
SELECT NOW(), COUNT(*) 
FROM triangle_arbitrage_opportunities;

-- Show summary
SELECT 
    COUNT(*) as total_opportunities,
    COUNT(CASE WHEN arbitrage_type = 'Cross DEX' THEN 1 END) as cross_dex_opportunities,
    COUNT(CASE WHEN arbitrage_type = 'Same DEX' THEN 1 END) as same_dex_opportunities,
    COUNT(CASE WHEN arbitrage_type = 'Mixed' THEN 1 END) as mixed_opportunities,
    AVG(total_liquidity) as avg_total_liquidity,
    MAX(total_liquidity) as max_total_liquidity,
    MIN(total_liquidity) as min_total_liquidity
FROM triangle_arbitrage_opportunities;

EOF

if [ $? -eq 0 ]; then
    log_message "Triangle arbitrage view refresh completed successfully"
else
    log_message "ERROR: Triangle arbitrage view refresh failed"
    exit 1
fi 