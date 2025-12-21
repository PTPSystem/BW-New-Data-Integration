#!/bin/bash
# Backfill Sales Channel data for FY2023, 2024, and 2025
# Processes one period at a time to avoid MDX query size limits
# Sales Channel has 5 dimensions which makes the result set very large

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "Sales Channel Multi-Year Backfill by Period"
echo "========================================================================"
echo ""
echo "This will populate the Sales Channel table with historical data"
echo "by processing one 13-4 fiscal period at a time."
echo ""
echo "Scope:"
echo "  - Fiscal Years: 2023, 2024, 2025"
echo "  - Periods per year: 13 (28-day periods)"
echo "  - Total iterations: 39 (3 years × 13 periods)"
echo ""
echo "Estimated time: 1-3 minutes per period (40-120 minutes total)"
echo ""

# Default to all years, or accept specific years as arguments
if [ $# -eq 0 ]; then
    YEARS=(2023 2024 2025)
else
    YEARS=("$@")
fi

echo "Processing fiscal years: ${YEARS[*]}"
echo ""

# Confirm before proceeding
read -p "Continue? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Ensure virtual environment is activated
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Track progress
TOTAL_PERIODS=$((${#YEARS[@]} * 13))
CURRENT=0
FAILED=0
FAILED_PERIODS=()

# Function to run a single period sync
run_period() {
    local fy=$1
    local period=$2
    CURRENT=$((CURRENT + 1))
    
    echo ""
    echo "========================================================================"
    echo "[$CURRENT/$TOTAL_PERIODS] Processing FY$fy Period $period"
    echo "========================================================================"
    echo ""
    
    python olap_to_dataverse.py --query sales_channel --fy "$fy" --period "$period"
    
    if [ $? -eq 0 ]; then
        echo "✓ FY$fy Period $period completed successfully"
    else
        echo "✗ FY$fy Period $period failed"
        FAILED=$((FAILED + 1))
        FAILED_PERIODS+=("FY$fy-P$period")
        
        # Ask if should continue
        read -p "Continue to next period? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# Process each fiscal year and period
for year in "${YEARS[@]}"; do
    echo ""
    echo "========================================================================"
    echo "Starting Fiscal Year $year (13 periods)"
    echo "========================================================================"
    
    for period in {1..13}; do
        run_period "$year" "$period"
    done
    
    echo ""
    echo "✓ Completed all 13 periods for FY$year"
done

echo ""
echo "========================================================================"
echo "Multi-Year Backfill Complete!"
echo "========================================================================"
echo ""
echo "Summary:"
echo "  - Total periods processed: $CURRENT"
echo "  - Successful: $((CURRENT - FAILED))"
echo "  - Failed: $FAILED"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "Failed periods:"
    for fp in "${FAILED_PERIODS[@]}"; do
        echo "  - $fp"
    done
    echo ""
    echo "To retry failed periods, run:"
    for fp in "${FAILED_PERIODS[@]}"; do
        IFS='-P' read -r fy period <<< "$fp"
        echo "  python olap_to_dataverse.py --query sales_channel --fy ${fy#FY} --period $period"
    done
fi

echo ""
echo "Next steps:"
echo "  - Verify data in Dataverse"
echo "  - Set up daily incremental updates:"
echo "    python olap_to_dataverse.py --query sales_channel --length 1wk"
echo ""
