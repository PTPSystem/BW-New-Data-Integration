#!/usr/bin/env python3
"""
Fix Calendar Date Timezone Issue - Update all dates to 12PM UTC

This script updates all existing records in the OARS BI Data table (crf63_oarsbidatas)
to use 12PM UTC timestamp instead of midnight, preventing timezone shift issues.

Problem:
- Dates stored as 2026-01-26T00:00:00Z (midnight UTC)
- Displayed as 2026-01-25 in negative UTC offset timezones (EST, PST, etc.)

Solution:
- Update to 2026-01-26T12:00:00Z (noon UTC)
- Displays correctly as 2026-01-26 in all US timezones

Usage:
    python3 fix_calendar_dates_to_noon.py [--dry-run]
"""

import sys
import os
import json
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

# Add modules to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.utils.keyvault import get_dataverse_credentials

# Configuration
TABLE_NAME = "crf63_oarsbidatas"
BATCH_SIZE = 100

def fetch_all_records(environment_url, access_token, logger):
    """Fetch all records from OARS BI Data table"""
    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    table_url = f"{api_url}/{TABLE_NAME}"
    
    # Select only needed columns
    select_columns = "crf63_oarsbidataid,crf63_calendardate,crf63_businesskey,crf63_storenumber"
    url = f"{table_url}?$select={select_columns}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    all_records = []
    
    logger(f"Fetching records from {TABLE_NAME}...")
    
    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logger(f"ERROR: Failed to fetch records: {response.status_code}")
            logger(response.text)
            return []
        
        data = response.json()
        records = data.get('value', [])
        all_records.extend(records)
        
        logger(f"  Fetched {len(all_records)} records so far...")
        
        # Check for next page
        url = data.get('@odata.nextLink')
    
    logger(f"✓ Total records fetched: {len(all_records)}")
    return all_records

def fix_date_to_noon(date_string):
    """Convert date from midnight to noon UTC"""
    if not date_string:
        return None
    
    try:
        # Parse the date (handles various formats)
        if 'T' in date_string:
            # Already has time component
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        else:
            # Date only, add midnight
            dt = datetime.fromisoformat(date_string)
        
        # Set to noon UTC
        dt_noon = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        
        # Return in ISO format with Z suffix
        return dt_noon.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"ERROR parsing date '{date_string}': {e}")
        return None

def update_records(environment_url, access_token, records, dry_run=False, logger=None):
    """Update records to use noon timestamp"""
    
    # Use print if no logger provided
    def log_msg(msg):
        if logger:
            logger(msg)
        else:
            print(msg)
    
    if not records:
        log_msg("No records to update")
        return
    
    log_msg(f"\n{'DRY RUN - ' if dry_run else ''}Preparing to update {len(records)} records...")
    
    # Prepare update records
    update_records = []
    skipped = 0
    
    for record in records:
        original_date = record.get('crf63_calendardate')
        
        if not original_date:
            skipped += 1
            continue
        
        # Check if already at noon (hour 12)
        if 'T12:' in original_date:
            skipped += 1
            continue
        
        # Fix the date to noon
        fixed_date = fix_date_to_noon(original_date)
        
        if not fixed_date:
            skipped += 1
            continue
        
        # Create update record with business key
        update_record = {
            'crf63_businesskey': record['crf63_businesskey'],
            'crf63_calendardate': fixed_date
        }
        
        update_records.append(update_record)
    
    log_msg(f"  Records to update: {len(update_records)}")
    log_msg(f"  Records skipped (already correct or invalid): {skipped}")
    
    if dry_run:
        log_msg("\n✓ DRY RUN COMPLETE - No changes made")
        log_msg(f"\nSample updates (first 5):")
        for i, rec in enumerate(update_records[:5], 1):
            original = next((r for r in records if r['crf63_businesskey'] == rec['crf63_businesskey']), None)
            if original:
                log_msg(f"  {i}. Store {original.get('crf63_storenumber')}")
                log_msg(f"     Before: {original.get('crf63_calendardate')}")
                log_msg(f"     After:  {rec['crf63_calendardate']}")
        return
    
    # Perform actual update
    log_msg(f"\nUpdating records in batches of {BATCH_SIZE}...")
    
    total_batches = (len(update_records) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(update_records), BATCH_SIZE):
        batch = update_records[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        log_msg(f"\n  Batch {batch_num}/{total_batches} ({len(batch)} records)...")
        
        result = upsert_to_dataverse(
            environment_url=environment_url,
            access_token=access_token,
            table_name=TABLE_NAME,
            records=batch,
            alternate_key="crf63_businesskey",
            logger=None
        )
    
    log_msg(f"\n✓ UPDATE COMPLETE - {len(update_records)} records updated to 12PM UTC")

def main():
    """Main execution"""
    dry_run = "--dry-run" in sys.argv
    
    def log(msg):
        print(msg)
    
    log("=" * 70)
    log("Fix Calendar Date Timezone Issue - Update to 12PM UTC")
    log("=" * 70)
    
    if dry_run:
        log("\n*** DRY RUN MODE - No changes will be made ***\n")
    
    # Get credentials (same way as olap_to_dataverse.py)
    log("\n1. Loading credentials from Azure Key Vault...")
    try:
        dv_creds = get_dataverse_credentials()
        environment_url = dv_creds['environment_url']
        client_id = dv_creds['client_id']
        client_secret = dv_creds['client_secret']
        tenant_id = dv_creds['tenant_id']
        
        if not all([client_id, client_secret, tenant_id]):
            log("ERROR: Failed to retrieve credentials from Key Vault")
            log("  Missing one or more: client_id, client_secret, tenant_id")
            log("\nPlease ensure:")
            log("  1. You're authenticated: az login")
            log("  2. Your account has 'Key Vault Secrets User' role")
            return 1
            
    except Exception as e:
        log(f"ERROR: Failed to get credentials: {e}")
        return 1
    
    log(f"   Environment: {environment_url}")
    log(f"   Table: {TABLE_NAME}")
    
    # Get access token
    log("\n2. Authenticating to Dataverse...")
    access_token = get_dataverse_access_token(
        environment_url, client_id, client_secret, tenant_id, logger=None
    )
    if not access_token:
        log("ERROR: Failed to authenticate")
        return 1
    
    # Fetch all records
    log("\n3. Fetching all records from Dataverse...")
    records = fetch_all_records(environment_url, access_token, log)
    
    if not records:
        log("ERROR: No records found or failed to fetch")
        return 1
    
    # Update records
    log("\n4. Updating calendar dates to 12PM UTC...")
    update_records(environment_url, access_token, records, dry_run=dry_run, logger=None)
    
    log("\n" + "=" * 70)
    log("COMPLETE!")
    log("=" * 70)
    
    if dry_run:
        log("\nTo apply changes, run without --dry-run flag:")
        log("  python3 fix_calendar_dates_to_noon.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
