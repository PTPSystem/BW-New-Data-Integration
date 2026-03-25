#!/bin/bash
# Backfill Sales Channel data for FY2023, 2024, and 2025
# This script breaks the large query into manageable fiscal year chunks

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "Sales Channel Multi-Year Backfill (FY2023, 2024, 2025)"
echo "========================================================================"
echo ""
echo "This will populate the Sales Channel table with 3 years of historical data."
echo "Each fiscal year will be processed separately to avoid MDX query size limits."
echo ""
echo "Estimated time: 5-15 minutes per fiscal year (15-45 minutes total)"
echo ""

# Confirm before proceeding
read -p "Continue? (y/n): " -n 1 -r
echo
if [[ ! $REPL =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Ensure virtual environment is activated
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Function to run a fiscal year sync
run_fiscal_year() {
    local fy=$1
    echo ""
    echo "========================================================================"
    echo "Processing Fiscal Year $fy"
    echo "========================================================================"
    echo ""
    
    python olap_to_dataverse.py --query sales_channel --fy "$fy"
    
    if [ $? -eq 0 ]; then
        echo "✓ FY$fy completed successfully"
    else
        echo "✗ FY$fy failed"
        exit 1
    fi
}

# Process each fiscal year
run_fiscal_year 2023
run_fiscal_year 2024
run_fiscal_year 2025

echo ""
echo "========================================================================"
echo "Multi-Year Backfill Complete!"
echo "========================================================================"
echo ""
echo "All three fiscal years have been loaded to Dataverse."
echo ""
echo "Next steps:"
echo "  - Verify data in Dataverse"
echo "  - Set up daily incremental updates using: python olap_to_dataverse.py --query sales_channel --length 1wk"
echo ""
