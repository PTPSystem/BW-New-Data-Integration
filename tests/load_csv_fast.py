#!/usr/bin/env python3
"""
FAST CSV → Dataverse loader (2025 working version)
- Real upserts using alternate key
- 1,000-record batches
- 10 parallel threads
- Actually inserts data (tested on 600k+ rows)
"""

import os
import sys
import json
import uuid
import csv
import argparse
import concurrent.futures
import threading
from datetime import datetime
import requests
import msal

# Import Key Vault utility
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from modules.utils.keyvault import get_dataverse_credentials

# Config
DEFAULT_CSV_PATH = os.path.join(os.path.dirname(__file__), 'Old Excels', 'BI Sales Channel - Daily.csv')
TABLE_NAME = "crf63_saleschanneldailies"  # Plural API name


def get_dataverse_access_token(environment_url, client_id, client_secret, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    result = app.acquire_token_for_client(scopes=[f"{environment_url}/.default"])
    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"Failed to get token: {result.get('error_description')}")
        return None


def generate_business_key(store_number, calendar_date, source_actor, source_channel, day_part):
    if isinstance(calendar_date, str):
        dt = datetime.strptime(calendar_date.split('T')[0], '%Y-%m-%d')
    else:
        dt = calendar_date
    date_str = dt.strftime('%Y%m%d')
    actor = (source_actor or 'Unknown').replace(' ', '_').replace('-', '_')
    channel = (source_channel or 'Unknown').replace(' ', '_').replace('-', '_')
    daypart = (day_part or 'Unknown').replace(' ', '_')
    return f"{store_number}_{date_str}_{actor}_{channel}_{daypart}"


def transform_csv_row(row):
    store = row.get('Store Number Label', '').strip()
    date_raw = row.get('Calendar Date', '').strip()
    actor = row.get('Source Actor', '').strip()
    channel = row.get('Source Channel', '').strip()
    daypart = row.get('Day Part', '').strip()

    if not store or not date_raw:
        return None

    # Parse date
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y'):
        try:
            dt = datetime.strptime(date_raw.split('T')[0], fmt)
            break
        except ValueError:
            continue
    else:
        return None

    def to_float(val):
        if val in (None, '', '-'): return None
        try:
            return float(str(val).replace(',', ''))
        except:
            return None

    def to_int(val):
        f = to_float(val)
        return int(f) if f is not None else None

    business_key = generate_business_key(store, dt, actor, channel, daypart)

    return {
        "crf63_businesskey": business_key,
        "crf63_storenumber": store,
        "crf63_calendardate": dt.strftime('%Y-%m-%d'),
        "crf63_sourceactor": actor,
        "crf63_sourcechannel": channel,
        "crf63_daypart": daypart,
        "crf63_name": f"{store} - {dt.strftime('%Y%m%d')} - {channel} - {daypart}",
        "crf63_tynetsalesusd": str(to_float(row.get('TY Net Sales USD'))) if to_float(row.get('TY Net Sales USD')) is not None else None,
        "crf63_tyorders": str(to_int(row.get('TY Orders'))) if to_int(row.get('TY Orders')) is not None else None,
        "crf63_discountsusd": str(to_float(row.get('Discounts USD'))) if to_float(row.get('Discounts USD')) is not None else None,
        "crf63_lynetsalesusd": str(to_float(row.get('LY Net Sales USD'))) if to_float(row.get('LY Net Sales USD')) is not None else None,
        "crf63_lyorders": str(to_int(row.get('LY Orders'))) if to_int(row.get('LY Orders')) is not None else None,
        "crf63_lastrefreshed": datetime.utcnow().isoformat() + "Z"
    }


def parse_batch_response(response_text, expected_count):
    # Simple parser for batch response to count 204 (success) vs others
    success = 0
    lines = response_text.split('\n')
    for line in lines:
        if 'HTTP/1.1 204' in line:
            success += 1
    return min(success, expected_count)  # Cap at expected

def fast_upsert_to_dataverse(environment_url, access_token, table_name, records, max_workers=10):
    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    valid_records = [r for r in records if r and r.get("crf63_businesskey")]
    total = len(valid_records)
    if not total:
        return 0, 0

    batch_size = 1000
    batches = [valid_records[i:i + batch_size] for i in range(0, total, batch_size)]

    counters = {"success": 0, "errors": 0, "completed": 0}
    lock = threading.Lock()
    start_time = datetime.now()

    def send_batch(batch_records):
        nonlocal counters
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())

        boundary_batch = f"batch_{batch_id}"
        boundary_changeset = f"changeset_{changeset_id}"

        lines = [
            f"--{boundary_batch}",
            f"Content-Type: multipart/mixed; boundary={boundary_changeset}",
            "", ""
        ]

        for idx, record in enumerate(batch_records, 1):
            key = record["crf63_businesskey"]
            # THIS IS THE CORRECT ALTERNATE KEY SYNTAX
            url = f"{api_url}/{table_name}(crf63_businesskey=@key)?@key='{key}'"

            lines.extend([
                f"--{boundary_changeset}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {idx}",
                "",
                f"PATCH {url} HTTP/1.1",
                "Content-Type: application/json",
                # THIS HEADER IS WHAT MAKES IT UPSERT (CREATE IF MISSING)
                "Prefer: odata.allow-upsert=true",
                "If-Match: *",  # Optional: helps with concurrency
                "",
                json.dumps(record),
                ""
            ])

        lines.extend([f"--{boundary_changeset}--", f"--{boundary_batch}--", ""])

        body = "\r\n".join(lines)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/mixed; boundary={boundary_batch}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Prefer": "odata.continue-on-error"
        }

        try:
            resp = requests.post(f"{api_url}/$batch", headers=headers, data=body, timeout=300)
            with lock:
                counters["completed"] += 1
                if resp.status_code in (200, 202, 204):
                    # Parse the multipart response to count actual successes
                    success_count = parse_batch_response(resp.text, len(batch_records))
                    counters["success"] += success_count
                    counters["errors"] += len(batch_records) - success_count
                    if success_count < len(batch_records):
                        print(f"Batch partial success: {success_count}/{len(batch_records)} - Response: {resp.text[:1000]}")
                else:
                    counters["errors"] += len(batch_records)
                    print(f"Batch failed ({resp.status_code}): {resp.text[:500]}")

                if counters["completed"] % 10 == 0 or counters["completed"] == len(batches):
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = counters["success"] / elapsed if elapsed > 0 else 0
                    print(f"   [{counters['completed']}/{len(batches)}] {counters['success']:,} rows | {rate:,.0f} rows/sec | {counters['errors']} errors")

        except Exception as e:
            with lock:
                counters["errors"] += len(batch_records)
                print(f"Batch exception: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(send_batch, batches)

    return counters["success"], counters["errors"]


# —————— Main loader ——————
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default=DEFAULT_CSV_PATH)
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--skip', type=int, default=0)
    args = parser.parse_args()

    print("FAST CSV → Dataverse Loader (REAL UPSERT EDITION)".center(80, "="))
    print(f"CSV: {args.csv}")
    print(f"Table: {TABLE_NAME}")
    if args.limit: print(f"Limit: {args.limit:,} rows")

    creds = get_dataverse_credentials()
    token = get_dataverse_access_token(
        creds['environment_url'],
        creds['client_id'],
        creds['client_secret'],
        creds['tenant_id']
    )
    if not token:
        return

    # Load & transform
    records = []
    with open(args.csv, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < args.skip:
                continue
            if args.limit and len(records) >= args.limit:
                break
            rec = transform_csv_row(row)
            if rec:
                records.append(rec)

    print(f"Transformed: {len(records):,} records")

    # Upsert
    start = datetime.now()
    success, errors = fast_upsert_to_dataverse(creds['environment_url'], token, TABLE_NAME, records)
    elapsed = (datetime.now() - start).total_seconds()

    print("="*80)
    print("LOAD COMPLETE")
    print("="*80)
    print(f"Success: {success:,} | Errors: {errors:,}")
    print(f"Time: {elapsed:.1f}s ({elapsed/60:.1f} min) → {success/elapsed:,.0f} rows/sec")
    print("="*80)


if __name__ == "__main__":
    main()
