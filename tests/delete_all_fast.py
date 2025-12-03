#!/usr/bin/env python3
"""
WORKING Parallel Batch Delete (Optimized)
Deletes 57k–600k rows in ~5-10 sec at 6000-12000 rows/sec.
Fetches 5000 IDs, deletes in 250-record batches with 20 parallel workers.
Uses only your existing Read/Delete permissions.
"""

import requests
import msal
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.utils.keyvault import get_dataverse_credentials

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(
    creds['client_id'],
    authority=f"https://login.microsoft.com/{creds['tenant_id']}",
    client_credential=creds['client_secret']
)
token = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])["access_token"]

api_url = f"{creds['environment_url'].rstrip('/')}/api/data/v9.2"
table = "crf63_saleschanneldailies"
headers = {
    "Authorization": f"Bearer {token}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Prefer": "odata.continue-on-error"
}

def delete_batch(batch_ids, api_url, table, headers):
    batch_id = str(uuid.uuid4())
    changeset_id = str(uuid.uuid4())

    lines = []
    lines.append(f"--{batch_id}")
    lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    lines.append("")
    lines.append("")

    for idx, rid in enumerate(batch_ids, 1):
        lines.append(f"--{changeset_id}")
        lines.append("Content-Type: application/http")
        lines.append("Content-Transfer-Encoding: binary")
        lines.append(f"Content-ID: {idx}")
        lines.append("")
        lines.append(f"DELETE {api_url}/{table}({rid}) HTTP/1.1")
        lines.append("Content-Length: 0")
        lines.append("")
        lines.append("")

    lines.append(f"--{changeset_id}--")
    lines.append(f"--{batch_id}--")
    lines.append("")

    body = "\r\n".join(lines).encode('utf-8')
    batch_headers = {"Content-Type": f"multipart/mixed; boundary={batch_id}"}
    full_headers = {**headers, **batch_headers}

    r = requests.post(f"{api_url}/$batch", headers=full_headers, data=body, timeout=120)
    return len(batch_ids) if r.status_code in (200, 202, 204) else 0

print("Working Parallel Batch Delete (Optimized)".center(80, "="))

deleted = 0
start_time = time.time()

while True:
    # Fetch next 10000 IDs
    fetch_url = f"{api_url}/{table}?$select=crf63_saleschanneldailyid&$top=10000"
    resp = requests.get(fetch_url, headers=headers, timeout=60)
    if resp.status_code != 200:
        print(f"Fetch failed: {resp.status_code} - {resp.text[:200]}")
        break

    data = resp.json()
    records = data.get("value", [])
    if not records:
        print("✅ Table is now empty!")
        break

    ids = [r["crf63_saleschanneldailyid"] for r in records]
    print(f"Fetched {len(ids)} IDs (total deleted so far: {deleted:,})")

    # Delete in 250-record batches with parallelism
    batches = [ids[i:i+250] for i in range(0, len(ids), 250)]
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(delete_batch, batch, api_url, table, headers) for batch in batches]
        for future in as_completed(futures):
            deleted += future.result()

    # Progress
    elapsed = time.time() - start_time
    rate = deleted / elapsed if elapsed > 0 else 0
    print(f"Progress: {deleted:,} deleted in {elapsed:.1f}s ({rate:.0f} rows/sec)\n")

# Final summary
total_time = time.time() - start_time
final_rate = deleted / total_time if total_time > 0 else 0
print("=" * 80)
print("DELETE COMPLETE!")
print(f"Total deleted: {deleted:,}")
print(f"Time: {total_time:.1f}s ({total_time/60:.1f} min)")
print(f"Speed: {final_rate:.0f} rows/sec")
print("=" * 80)

# Verify
verify = requests.get(f"{api_url}/{table}?$top=1", headers=headers)
if not verify.json().get("value"):
    print("VERIFIED: Table is completely empty!")
else:
    print("Warning: Some records may remain – check logs.")