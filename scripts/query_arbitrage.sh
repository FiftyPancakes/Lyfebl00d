#!/bin/bash

# Helper script to query triangle arbitrage opportunities

# Database connection parameters
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="lyfeblood"
DB_USER="ichevako"
DB_PASS="maddie"

# Default parameters
LIMIT=50
MIN_LIQUIDITY=50000
CHAIN_NAME=""
ARBITRAGE_TYPE=""

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -l, --limit NUM          Number of results to return (default: 50)"
    echo "  -m, --min-liquidity NUM  Minimum total liquidity (default: 50000)"
    echo "  -c, --chain NAME         Filter by chain name (e.g., ethereum, polygon)"
    echo "  -t, --type TYPE          Filter by arbitrage type (Cross DEX, Same DEX, Mixed)"
    echo "  -s, --summary            Show summary statistics only"
    echo "  -r, --refresh            Refresh the view before querying"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Show top 50 opportunities"
    echo "  $0 -l 10 -m 100000                   # Show top 10 with min $100k liquidity"
    echo "  $0 -c ethereum -t 'Cross DEX'        # Ethereum cross-DEX opportunities"
    echo "  $0 -s                                 # Show summary only"
    echo "  $0 -r                                 # Refresh view then query"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--limit)
            LIMIT="$2"
            shift 2
            ;;
        -m|--min-liquidity)
            MIN_LIQUIDITY="$2"
            shift 2
            ;;
        -c|--chain)
            CHAIN_NAME="$2"
            shift 2
            ;;
        -t|--type)
            ARBITRAGE_TYPE="$2"
            shift 2
            ;;
        -s|--summary)
            SUMMARY_ONLY=true
            shift
            ;;
        -r|--refresh)
            REFRESH=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Function to run SQL query
run_query() {
    local query="$1"
    PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$query"
}

# Refresh view if requested
if [ "$REFRESH" = true ]; then
    echo "Refreshing arbitrage view..."
    run_query "SELECT refresh_triangle_arbitrage_view_with_logging();"
    echo ""
fi

# Build the query
if [ "$SUMMARY_ONLY" = true ]; then
    QUERY="SELECT 
        COUNT(*) as total_opportunities,
        COUNT(CASE WHEN arbitrage_type = 'Cross DEX' THEN 1 END) as cross_dex_opportunities,
        COUNT(CASE WHEN arbitrage_type = 'Same DEX' THEN 1 END) as same_dex_opportunities,
        COUNT(CASE WHEN arbitrage_type = 'Mixed' THEN 1 END) as mixed_opportunities,
        AVG(total_liquidity) as avg_total_liquidity,
        MAX(total_liquidity) as max_total_liquidity,
        MIN(total_liquidity) as min_total_liquidity,
        COUNT(DISTINCT chain_name) as unique_chains,
        STRING_AGG(DISTINCT chain_name, ', ' ORDER BY chain_name) as chains
    FROM triangle_arbitrage_opportunities
    WHERE total_liquidity >= $MIN_LIQUIDITY"
    
    if [ -n "$CHAIN_NAME" ]; then
        QUERY="$QUERY AND chain_name = '$CHAIN_NAME'"
    fi
    
    if [ -n "$ARBITRAGE_TYPE" ]; then
        QUERY="$QUERY AND arbitrage_type = '$ARBITRAGE_TYPE'"
    fi
else
    QUERY="SELECT 
        token_a,
        token_b,
        token_c,
        symbol_a,
        symbol_b,
        symbol_c,
        chain_name,
        dex_ab,
        dex_bc,
        dex_ca,
        liquidity_ab,
        liquidity_bc,
        liquidity_ca,
        total_liquidity,
        avg_liquidity,
        arbitrage_type,
        last_updated
    FROM get_top_arbitrage_opportunities($LIMIT, $MIN_LIQUIDITY"
    
    if [ -n "$CHAIN_NAME" ]; then
        QUERY="$QUERY, '$CHAIN_NAME'"
    else
        QUERY="$QUERY, NULL"
    fi
    
    if [ -n "$ARBITRAGE_TYPE" ]; then
        QUERY="$QUERY, '$ARBITRAGE_TYPE'"
    else
        QUERY="$QUERY, NULL"
    fi
    
    QUERY="$QUERY)"
fi

# Run the query
echo "Querying arbitrage opportunities..."
echo "Parameters: Limit=$LIMIT, Min Liquidity=$MIN_LIQUIDITY"
if [ -n "$CHAIN_NAME" ]; then
    echo "Chain: $CHAIN_NAME"
fi
if [ -n "$ARBITRAGE_TYPE" ]; then
    echo "Type: $ARBITRAGE_TYPE"
fi
echo ""

run_query "$QUERY" 