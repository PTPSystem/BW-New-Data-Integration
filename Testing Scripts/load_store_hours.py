#!/usr/bin/env python3
"""
Load Store Operating Hours from Excel to Dataverse
Updates crf63_storeoperatinghour table with master data from BI Dimensions.xlsx
"""

import json
import time
from datetime import datetime, timezone

import requests
import msal
import openpyxl
from modules.utils.keyvault import get_dataverse_credentials

# ========================= CONFIG =========================
EXCEL_PATH = "/Users/howardshen/Library/CloudStorage/OneDrive-SharedLibraries-globalpacmgt.com/IT Project - General/BI Import/BI Dimensions.xlsx"
SHEET_NAME = "Store hours"
TABLE = "crf63_storeoperatinghours"
# =========================================================

def get_auth_token():
    """Get authentication token for Dataverse"""
    creds = get_dataverse_credentials()
    token = msal.ConfidentialClientApplication(
        creds['client_id'],
        authority=f"https://login.microsoftonline.com/{creds['tenant_id']}",
        client_credential=creds['client_secret']
    ).acquire_token_for_client(scopes=[f"{creds['environment_url']}/.default"])["access_token"]
    return token, creds['environment_url']


def convert_time_to_hhmm(time_value):
    """Convert various time formats to HH:MM string"""
    if time_value is None or time_value == '':
        return None
    
    # If it's already a string, clean and format it
    if isinstance(time_value, str):
        # Remove any non-digits
        clean = ''.join(c for c in time_value if c.isdigit())
        if len(clean) == 3:
            # Format 100 -> 01:00
            return f"{clean[0]:0>2}:{clean[1:3]}"
        elif len(clean) == 4:
            # Format 1100 -> 11:00
            return f"{clean[0:2]}:{clean[2:4]}"
    
    # If it's an integer
    if isinstance(time_value, int):
        time_str = str(time_value)
        if len(time_str) == 3:
            return f"{time_str[0]:0>2}:{time_str[1:3]}"
        elif len(time_str) == 4:
            return f"{time_str[0:2]}:{time_str[2:4]}"
    
    # If it's a datetime.time object
    if hasattr(time_value, 'hour') and hasattr(time_value, 'minute'):
        return f"{time_value.hour:02d}:{time_value.minute:02d}"
    
    return None


def load_excel_data():
    """Load store hours data from Excel file"""
    print(f"Loading Excel file: {EXCEL_PATH}")
    wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]
    
    records = []
    headers = None
    
    for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
        if row_idx == 1:
            # First row is headers
            headers = row
            print(f"Headers: {headers}")
            continue
        
        # Skip empty rows
        if not row[0]:
            continue
        
        # Parse row data
        store_number = int(row[0]) if row[0] else None
        day_of_week = int(row[1]) if row[1] else None
        open_time_raw = row[2]
        close_time_raw = row[3]
        
        if store_number is None or day_of_week is None:
            continue
        
        # Convert times to HH:MM format
        open_time = convert_time_to_hhmm(open_time_raw)
        close_time = convert_time_to_hhmm(close_time_raw)
        
        record = {
            "crf63_storenumber": store_number,
            "crf63_dayofweek": day_of_week,
            "crf63_openingtimehhmm": open_time,
            "crf63_closingtimehhmm": close_time
        }
        
        records.append(record)
    
    wb.close()
    print(f"Loaded {len(records)} records from Excel")
    return records


def fetch_existing_records(session, api_url):
    """Fetch all existing store operating hour records"""
    print("Fetching existing records from Dataverse...")
    
    existing = {}
    url = f"{api_url}/crf63_storeoperatinghours?$select=crf63_storeoperatinghourid,crf63_storenumber,crf63_dayofweek"
    
    while url:
        resp = session.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"Error fetching records: {resp.status_code}")
            return {}
        
        data = resp.json()
        for record in data.get('value', []):
            store_num = record.get('crf63_storenumber')
            day = record.get('crf63_dayofweek')
            record_id = record.get('crf63_storeoperatinghourid')
            
            if store_num is not None and day is not None and record_id:
                key = f"{store_num}_{day}"
                existing[key] = record_id
        
        # Check for next page
        url = data.get('@odata.nextLink')
    
    print(f"Found {len(existing)} existing records")
    return existing


def upsert_record(session, api_url, record, existing_id=None):
    """Update existing record or create new one"""
    clean_rec = {k: v for k, v in record.items() if v is not None}
    
    if existing_id:
        # UPDATE existing record
        url = f"{api_url}/crf63_storeoperatinghours({existing_id})"
        resp = session.patch(url, json=clean_rec, timeout=30)
        return resp.status_code in (200, 204), resp.status_code
    else:
        # CREATE new record
        url = f"{api_url}/crf63_storeoperatinghours"
        resp = session.post(url, json=clean_rec, timeout=30)
        return resp.status_code in (201, 204), resp.status_code


def process_records(session, api_url, records, existing):
    """Process all records with upsert logic"""
    print(f"\nProcessing {len(records)} records...")
    
    success_count = 0
    update_count = 0
    create_count = 0
    error_count = 0
    
    for idx, record in enumerate(records, 1):
        store_num = record['crf63_storenumber']
        day = record['crf63_dayofweek']
        key = f"{store_num}_{day}"
        
        existing_id = existing.get(key)
        success, status_code = upsert_record(session, api_url, record, existing_id)
        
        if success:
            success_count += 1
            if existing_id:
                update_count += 1
                action = "updated"
            else:
                create_count += 1
                action = "created"
            
            if idx % 50 == 0:
                print(f"  Progress: {idx}/{len(records)} ({action})")
        else:
            error_count += 1
            print(f"  ✗ Error on Store {store_num}, Day {day}: HTTP {status_code}")
        
        # Rate limiting
        if idx % 10 == 0:
            time.sleep(0.1)
    
    return success_count, update_count, create_count, error_count


def build_batch(records):
    """Build a batch request for Dataverse"""
    batch_id = str(uuid.uuid4())
    changeset_id = str(uuid.uuid4())
    parts = [f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()]

    for i, rec in enumerate(records, 1):
        clean_rec = {k: v for k, v in rec.items() if v is not None}
        key = clean_rec["crf63_businesskey"].replace("'", "''")
        payload = json.dumps(clean_rec, separators=(',', ':'))

        part = (
            f"--{changeset_id}\r\n"
            f"Content-Type: application/http\r\n"
            f"Content-Transfer-Encoding: binary\r\n"
            f"Content-ID: {i}\r\n"
            f"\r\n"
            f"PATCH {TABLE}(crf63_businesskey='{key}') HTTP/1.1\r\n"
            f"Content-Type: application/json\r\n"
            f"Prefer: return=representation\r\n"
            f"\r\n"
            f"{payload}\r\n"
        ).encode()
        parts.append(part)

    parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
    return b"".join(parts), batch_id


def send_batch(session, batch_url, batch_body, batch_boundary):
    """Send a batch request to Dataverse"""
    headers = {
        "Content-Type": f"multipart/mixed;boundary={batch_boundary}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = session.post(batch_url, data=batch_body, headers=headers, timeout=60)
            if resp.status_code in (200, 201, 204):
                return True, None
            elif resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 10))
                time.sleep(retry_after)
                continue
            else:
                return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            if attempt == max_retries - 1:
                return False, str(e)
            time.sleep(2 ** attempt)
    
    return False, "Max retries exceeded"


def main():
    """Main function to load store hours from Excel to Dataverse"""
    print("=" * 60)
    print("Store Operating Hours Import")
    print("=" * 60)
    
    start_time = time.time()
    
    # Load Excel data
    records = load_excel_data()
    
    if not records:
        print("No records to process!")
        return
    
    # Get authentication
    print("\nAuthenticating with Dataverse...")
    token, environment_url = get_auth_token()
    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    
    # Setup session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount('https://', adapter)
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    })
    
    # Fetch existing records
    existing = fetch_existing_records(session, api_url)
    
    # Process records
    success_count, update_count, create_count, error_count = process_records(session, api_url, records, existing)
    
    elapsed = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 60)
    print("Import Summary")
    print("=" * 60)
    print(f"Total records: {len(records)}")
    print(f"Successful: {success_count}")
    print(f"  - Updated: {update_count}")
    print(f"  - Created: {create_count}")
    print(f"Failed: {error_count}")
    print(f"Time elapsed: {elapsed:.1f} seconds")
    print(f"Rate: {len(records) / elapsed:.1f} records/sec")
    print("=" * 60)


if __name__ == "__main__":
    main()
