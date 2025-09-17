#!/bin/bash

# Gas Price Outlier Cleanup Script
# Run this after each new data fetch to remove problematic gas prices

set -e

# Database connection parameters
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="ichevako"
DB_PASSWORD="maddie"
DB_NAME="lyfeblood"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Gas Price Outlier Cleanup Script ===${NC}"
echo "Timestamp: $(date)"
echo ""

# Function to run SQL query and capture output
run_sql() {
    local query="$1"
    local description="$2"
    
    echo -e "${YELLOW}$description${NC}"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "$query"
    echo ""
}

# Check current state before cleanup
echo -e "${GREEN}=== Current State Analysis ===${NC}"

run_sql "SELECT COUNT(*) as total_swaps FROM swaps;" "Total swaps in database:"

run_sql "SELECT 
    'Zero gas prices' as issue_type,
    COUNT(*) as count
FROM swaps 
WHERE CAST(tx_gas_price AS NUMERIC) = 0;" "Zero gas prices:"

run_sql "SELECT 
    'Extremely high gas prices (>10,000 gwei)' as issue_type,
    COUNT(*) as count
FROM swaps 
WHERE CAST(tx_gas_price AS NUMERIC) > 10000000000000;" "Extremely high gas prices:"

run_sql "SELECT 
    'Very high gas prices (>1,000 gwei)' as issue_type,
    COUNT(*) as count
FROM swaps 
WHERE CAST(tx_gas_price AS NUMERIC) > 1000000000000;" "Very high gas prices:"

# Show current gas price ranges
run_sql "SELECT 
    network_id,
    COUNT(*) as swap_count,
    ROUND(MIN(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as min_gas_gwei,
    ROUND(MAX(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as max_gas_gwei,
    ROUND(AVG(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as avg_gas_gwei
FROM swaps 
WHERE tx_gas_price ~ '^[0-9]+$'
GROUP BY network_id
ORDER BY swap_count DESC;" "Current gas price ranges:"

# Perform cleanup
echo -e "${GREEN}=== Performing Cleanup ===${NC}"

echo -e "${YELLOW}Removing zero gas prices...${NC}"
ZERO_DELETED=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "DELETE FROM swaps WHERE CAST(tx_gas_price AS NUMERIC) = 0;" | grep -o '[0-9]*')
echo "Deleted $ZERO_DELETED records with zero gas prices"

echo -e "${YELLOW}Removing extremely high gas prices (>10,000 gwei)...${NC}"
HIGH_DELETED=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "DELETE FROM swaps WHERE CAST(tx_gas_price AS NUMERIC) > 10000000000000;" | grep -o '[0-9]*')
echo "Deleted $HIGH_DELETED records with extremely high gas prices"

echo -e "${YELLOW}Removing very high gas prices (>1,000 gwei)...${NC}"
VERY_HIGH_DELETED=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "DELETE FROM swaps WHERE CAST(tx_gas_price AS NUMERIC) > 1000000000000;" | grep -o '[0-9]*')
echo "Deleted $VERY_HIGH_DELETED records with very high gas prices"

TOTAL_DELETED=$((ZERO_DELETED + HIGH_DELETED + VERY_HIGH_DELETED))
echo -e "${GREEN}Total records deleted: $TOTAL_DELETED${NC}"

# Verify cleanup results
echo -e "${GREEN}=== Cleanup Verification ===${NC}"

run_sql "SELECT COUNT(*) as total_swaps_after_cleanup FROM swaps;" "Total swaps after cleanup:"

run_sql "SELECT 
    network_id,
    COUNT(*) as swap_count,
    ROUND(MIN(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as min_gas_gwei,
    ROUND(MAX(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as max_gas_gwei,
    ROUND(AVG(CAST(tx_gas_price AS NUMERIC)) / 1000000000.0, 2) as avg_gas_gwei
FROM swaps 
WHERE tx_gas_price ~ '^[0-9]+$'
GROUP BY network_id
ORDER BY swap_count DESC;" "Gas price ranges after cleanup:"

# Check for any remaining outliers
run_sql "SELECT 
    'Remaining outliers' as check_type,
    COUNT(*) as count
FROM swaps 
WHERE CAST(tx_gas_price AS NUMERIC) = 0 
   OR CAST(tx_gas_price AS NUMERIC) > 1000000000000;" "Remaining outliers:"

echo -e "${GREEN}=== Cleanup Complete ===${NC}"
echo "Timestamp: $(date)"

if [ "$TOTAL_DELETED" -gt 0 ]; then
    echo -e "${GREEN}Successfully cleaned $TOTAL_DELETED problematic records${NC}"
else
    echo -e "${GREEN}No problematic records found - database is clean${NC}"
fi 