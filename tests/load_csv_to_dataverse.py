#!/usr/bin/env python3
"""
Load CSV data into Dataverse Sales Channel Daily table.

This script reads the CSV file and upserts data into crf63_saleschanneldaily table.
Uses Azure Key Vault for credentials (same as olap_to_dataverse.py).

CSV Columns:
  - Store Number Label -> crf63_storenumber
  - Calendar Date -> crf63_calendardate
  - Source Actor -> crf63_sourceactor
  - Source Channel -> crf63_sourcechannel
  - Day Part -> crf63_daypart
  - TY Net Sales USD -> crf63_tynetsalesusd
  - TY Orders -> crf63_tyorders
  - Discounts USD -> crf63_discountsusd
  - LY Net Sales USD -> crf63_lynetsalesusd
  - LY Orders -> crf63_lyorders

Business Key Format: {StoreNumber}_{YYYYMMDD}_{SourceActor}_{SourceChannel}_{DayPart}
"""

import os
import sys
import json
import uuid
import csv
import argparse
from datetime import datetime
from dotenv import load_dotenv
import requests
import pandas as pd
import msal

# Import Key Vault utility
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from modules.utils.keyvault import get_dataverse_credentials, get_secret

load_dotenv()

# Default CSV path
DEFAULT_CSV_PATH = os.path.join(
    os.path.dirname(__file__), 
    'Old Excels', 
    'BI Sales Channel - Daily.csv'
)

# Dataverse table name (plural form for API)
TABLE_NAME = "crf63_saleschanneldailies"


def get_dataverse_access_token(environment_url, client_id, client_secret, tenant_id):
    """Obtain an access token for Dataverse."""
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        scope = [f"{environment_url}/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            print("✓ Dataverse access token obtained")
            return result["access_token"]
        else:
            print(f"✗ Failed to obtain Dataverse access token: {result.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"✗ Error obtaining Dataverse access token: {e}")
        return None


def generate_business_key(store_number, calendar_date, source_actor, source_channel, day_part):
    """
    Generate business key for upsert operations.
    
    Format: {StoreNumber}_{YYYYMMDD}_{SourceActor}_{SourceChannel}_{DayPart}
    Example: 125_20250209_Android_App_Dinner
    """
    # Parse date if it's a string
    if isinstance(calendar_date, str):
        # Handle YYYY-MM-DD format
        dt = datetime.strptime(calendar_date, '%Y-%m-%d')
        date_str = dt.strftime('%Y%m%d')
    else:
        date_str = calendar_date.strftime('%Y%m%d')
    
    # Clean values for business key (replace spaces with underscores, remove special chars)
    actor_clean = source_actor.replace(' ', '_').replace('-', '_') if source_actor else 'Unknown'
    channel_clean = source_channel.replace(' ', '_').replace('-', '_') if source_channel else 'Unknown'
    daypart_clean = day_part.replace(' ', '_') if day_part else 'Unknown'
    
    return f"{store_number}_{date_str}_{actor_clean}_{channel_clean}_{daypart_clean}"


def transform_csv_row_to_record(row):
    """
    Transform a CSV row to a Dataverse record.
    
    Args:
        row: Dictionary from CSV reader
    
    Returns:
        Dictionary ready for Dataverse upsert
    """
    store_number = str(row.get('Store Number Label', '')).strip()
    calendar_date = row.get('Calendar Date', '').strip()
    source_actor = row.get('Source Actor', '').strip()
    source_channel = row.get('Source Channel', '').strip()
    day_part = row.get('Day Part', '').strip()
    
    # Skip rows with missing key fields
    if not store_number or not calendar_date:
        return None
    
    # Generate business key
    business_key = generate_business_key(
        store_number, calendar_date, source_actor, source_channel, day_part
    )
    
    # Helper to safely get numeric value
    def get_num(value):
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def get_int(value):
        if value is None or value == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    # Parse date for display name
    dt = datetime.strptime(calendar_date, '%Y-%m-%d')
    date_str = dt.strftime('%Y%m%d')
    
    # Build Dataverse record
    record = {
        # Key fields
        "crf63_businesskey": business_key,
        "crf63_storenumber": store_number,
        "crf63_calendardate": calendar_date,
        "crf63_sourceactor": source_actor,
        "crf63_sourcechannel": source_channel,
        "crf63_daypart": day_part,
        
        # Display name
        "crf63_name": f"{store_number} - {date_str} - {source_channel} - {day_part}",
        
        # Measures
        "crf63_tynetsalesusd": get_num(row.get('TY Net Sales USD')),
        "crf63_tyorders": get_int(row.get('TY Orders')),
        "crf63_discountsusd": get_num(row.get('Discounts USD')),
        "crf63_lynetsalesusd": get_num(row.get('LY Net Sales USD')),
        "crf63_lyorders": get_int(row.get('LY Orders')),
        
        # Metadata
        "crf63_lastrefreshed": datetime.now().isoformat(),
    }
    
    return record


def upsert_batch_to_dataverse(environment_url, access_token, table_name, records, use_post=False):
    """
    Upsert a batch of records to Dataverse using $batch API.
    
    Args:
        environment_url: Dataverse environment URL
        access_token: OAuth access token
        table_name: Plural table name for API
        records: List of record dictionaries
        use_post: If True, use POST (create) instead of PATCH (upsert).
                  Use POST for initial bulk load when alternate key isn't ready.
    
    Returns:
        Tuple of (created_count, updated_count, error_count)
    """
    if not records:
        return 0, 0, 0
    
    # Build multipart/mixed batch request
    batch_id = f"batch_{uuid.uuid4()}"
    changeset_id = f"changeset_{uuid.uuid4()}"
    
    # Build batch body with CRLF line endings (required by OData spec)
    lines = []
    lines.append(f"--{batch_id}\r\n")
    lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}\r\n")
    lines.append("\r\n")
    
    # Add each request to the changeset
    for i, record in enumerate(records, 1):
        if use_post:
            # POST for create - use collection URL
            request_url = f"{environment_url}/api/data/v9.2/{table_name}"
            http_method = "POST"
        else:
            # PATCH for upsert - use alternate key URL
            business_key = record['crf63_businesskey']
            request_url = f"{environment_url}/api/data/v9.2/{table_name}(crf63_businesskey='{business_key}')"
            http_method = "PATCH"
        
        lines.append(f"--{changeset_id}\r\n")
        lines.append("Content-Type: application/http\r\n")
        lines.append("Content-Transfer-Encoding: binary\r\n")
        lines.append(f"Content-ID: {i}\r\n")
        lines.append("\r\n")
        lines.append(f"{http_method} {request_url} HTTP/1.1\r\n")
        lines.append("Content-Type: application/json\r\n")
        lines.append("\r\n")
        lines.append(json.dumps(record) + "\r\n")
    
    lines.append(f"--{changeset_id}--\r\n")
    lines.append(f"--{batch_id}--\r\n")
    
    batch_body = "".join(lines)
    
    # Send batch request
    batch_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": f"multipart/mixed; boundary={batch_id}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "MSCRM.BypassCustomPluginExecution": "true",  # Skip custom plugins for faster inserts
        "Prefer": "odata.continue-on-error"  # Continue processing even if some records fail
    }
    
    try:
        batch_url = f"{environment_url}/api/data/v9.2/$batch"
        response = requests.post(batch_url, headers=batch_headers, data=batch_body.encode('utf-8'), timeout=120)
        
        if response.status_code == 200:
            # Parse response to count creates vs updates
            import re
            response_text = response.text
            
            created = 0
            updated = 0
            errors = 0
            
            # Find all HTTP responses in the batch
            response_pattern = r'HTTP/\d\.\d\s+(\d{3})\s+[^\r\n]*'
            matches = re.findall(response_pattern, response_text)
            
            for status in matches:
                if status == '201':
                    created += 1
                elif status == '204':
                    # 204 No Content = successful update/create
                    created += 1
                elif status.startswith('2'):
                    updated += 1
                else:
                    errors += 1
            
            return created, updated, errors
        else:
            print(f"    ✗ Batch failed: HTTP {response.status_code}")
            print(f"    Response: {response.text[:500]}")
            return 0, 0, len(records)
            
    except Exception as e:
        print(f"    ✗ Batch error: {e}")
        return 0, 0, len(records)


def load_csv_to_dataverse(csv_path, batch_size=100, limit=None, use_post=False, skip=0):
    """
    Load CSV data into Dataverse.
    
    Args:
        csv_path: Path to CSV file
        batch_size: Number of records per batch (default 100)
        limit: Optional limit on number of rows to process
        use_post: If True, use POST (create) instead of PATCH (upsert)
        skip: Number of rows to skip from the beginning (for resuming)
    """
    print("="*80)
    print("Load CSV to Dataverse - Sales Channel Daily")
    print("="*80)
    print(f"\nCSV File: {csv_path}")
    print(f"Table: {TABLE_NAME}")
    print(f"Batch Size: {batch_size}")
    print(f"Mode: {'POST (create)' if use_post else 'PATCH (upsert)'}")
    if skip:
        print(f"Skip first: {skip:,} rows")
    if limit:
        print(f"Row Limit: {limit}")
    print()
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"✗ CSV file not found: {csv_path}")
        return {"success": False, "error": "File not found"}
    
    # Get Dataverse credentials from Key Vault
    print("1. Getting Dataverse credentials from Key Vault...")
    dv_creds = get_dataverse_credentials()
    dataverse_url = dv_creds['environment_url']
    client_id = dv_creds['client_id']
    tenant_id = dv_creds['tenant_id']
    client_secret = dv_creds['client_secret']
    
    print(f"   Dataverse URL: {dataverse_url}")
    
    # Get access token
    print("\n2. Getting Dataverse access token...")
    access_token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
    
    if not access_token:
        return {"success": False, "error": "Failed to get Dataverse token"}
    
    # Count total rows
    print("\n3. Counting CSV rows...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Subtract header row
    
    if limit:
        total_rows = min(total_rows, limit)
    
    # Adjust for skipped rows
    if skip:
        total_rows = max(0, total_rows - skip)
    
    print(f"   Total rows to process: {total_rows:,}")
    
    # Process CSV in batches
    print("\n4. Processing CSV and upserting to Dataverse...")
    
    total_created = 0
    total_updated = 0
    total_errors = 0
    total_skipped = 0
    processed = 0
    
    batch = []
    start_time = datetime.now()
    token_refresh_time = datetime.now()
    TOKEN_REFRESH_INTERVAL = 45 * 60  # Refresh token every 45 minutes (tokens expire in 60 min)
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, 1):
            # Skip rows if resuming
            if row_num <= skip:
                continue
            
            if limit and (row_num - skip) > limit:
                break
            
            # Transform row to Dataverse record
            record = transform_csv_row_to_record(row)
            
            if record is None:
                total_skipped += 1
                continue
            
            batch.append(record)
            
            # When batch is full, upsert to Dataverse
            if len(batch) >= batch_size:
                # Check if we need to refresh the token (every 45 minutes)
                if (datetime.now() - token_refresh_time).total_seconds() > TOKEN_REFRESH_INTERVAL:
                    print("   🔄 Refreshing access token...")
                    access_token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
                    token_refresh_time = datetime.now()
                    if not access_token:
                        print("   ✗ Failed to refresh token!")
                        return {"success": False, "error": "Token refresh failed"}
                    print("   ✓ Token refreshed")
                
                created, updated, errors = upsert_batch_to_dataverse(
                    dataverse_url, access_token, TABLE_NAME, batch, use_post=use_post
                )
                
                total_created += created
                total_updated += updated
                total_errors += errors
                processed += len(batch)
                
                # Progress update
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = processed / elapsed if elapsed > 0 else 0
                eta_seconds = (total_rows - processed) / rate if rate > 0 else 0
                
                print(f"   [{processed:,}/{total_rows:,}] {processed*100/total_rows:.1f}% | "
                      f"+{created} created, ~{updated} updated, {errors} errors | "
                      f"{rate:.0f} rows/sec | ETA: {eta_seconds/60:.1f} min")
                
                batch = []
        
        # Process remaining batch
        if batch:
            created, updated, errors = upsert_batch_to_dataverse(
                dataverse_url, access_token, TABLE_NAME, batch, use_post=use_post
            )
            
            total_created += created
            total_updated += updated
            total_errors += errors
            processed += len(batch)
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*80)
    print("✅ CSV Load Complete!")
    print("="*80)
    print(f"  Rows processed: {processed:,}")
    print(f"  Records created: {total_created:,}")
    print(f"  Records updated: {total_updated:,}")
    print(f"  Errors: {total_errors:,}")
    print(f"  Skipped (missing data): {total_skipped:,}")
    print(f"  Time elapsed: {elapsed/60:.1f} minutes")
    print(f"  Average rate: {processed/elapsed:.0f} rows/second")
    print("="*80)
    
    return {
        "success": True,
        "records_processed": processed,
        "records_created": total_created,
        "records_updated": total_updated,
        "errors": total_errors,
        "skipped": total_skipped
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Load CSV data into Dataverse Sales Channel Daily table')
    parser.add_argument(
        '--csv',
        default=DEFAULT_CSV_PATH,
        help=f'Path to CSV file (default: {DEFAULT_CSV_PATH})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of records per batch (default: 100)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of rows to process (for testing)'
    )
    parser.add_argument(
        '--use-post',
        action='store_true',
        help='Use POST (create) instead of PATCH (upsert). Use for initial bulk load.'
    )
    parser.add_argument(
        '--skip',
        type=int,
        default=0,
        help='Number of rows to skip (for resuming interrupted loads)'
    )
    args = parser.parse_args()
    
    result = load_csv_to_dataverse(
        csv_path=args.csv,
        batch_size=args.batch_size,
        limit=args.limit,
        use_post=args.use_post,
        skip=args.skip
    )
    
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
