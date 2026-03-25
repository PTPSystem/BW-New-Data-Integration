#!/usr/bin/env python3
"""
Check uniqueness of store + txn_no + account as an alternate key
for crf63_gltransactions in Dataverse.

Fetches all records (paginated) and reports any duplicate combinations.

Usage:
    python tests/check_gl_key_uniqueness.py
    python tests/check_gl_key_uniqueness.py --env-url https://orgbf93e3c3.crm.dynamics.com

Requires: az login (uses Azure CLI credentials)
"""

import sys
import os
import argparse
import requests
from azure.identity import AzureCliCredential
from collections import Counter

DEFAULT_ENV_URL = "https://orgbf93e3c3.crm.dynamics.com"

parser = argparse.ArgumentParser()
parser.add_argument("--env-url", default=DEFAULT_ENV_URL, help="Dataverse environment URL")
args = parser.parse_args()

env_url = args.env_url.rstrip("/")

# --- Auth via Azure CLI (az login) ---
print("Authenticating via Azure CLI...")
credential = AzureCliCredential()
token_obj = credential.get_token(f"{env_url}/.default")
token = token_obj.token

api_url = f"{env_url}/api/data/v9.2"
table = "crf63_gltransactions"
headers = {
    "Authorization": f"Bearer {token}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Prefer": 'odata.maxpagesize=5000'
}

# --- Fetch all records (paginated) ---
print(f"Fetching all records from {table}...")
url = (
    f"{api_url}/{table}"
    f"?$select=crf63_storenumber,crf63_transactionnumber,crf63_accountnumber"
    f"&$orderby=crf63_storenumber asc,crf63_transactionnumber asc,crf63_accountnumber asc"
)

records = []
page = 1
while url:
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        print(f"ERROR: HTTP {resp.status_code} - {resp.text[:300]}")
        sys.exit(1)

    data = resp.json()
    batch = data.get("value", [])
    records.extend(batch)
    print(f"  Page {page}: fetched {len(batch)} records (total so far: {len(records):,})")
    page += 1

    # Follow @odata.nextLink for next page
    url = data.get("@odata.nextLink")

print(f"\nTotal records fetched: {len(records):,}")

# --- Check uniqueness ---
keys = [
    (
        r.get("crf63_storenumber"),
        r.get("crf63_transactionnumber"),
        r.get("crf63_accountnumber"),
    )
    for r in records
]

total = len(keys)
unique = len(set(keys))
dupes = total - unique

print(f"\n--- Uniqueness check: store + txn_no + account ---")
print(f"  Total rows   : {total:,}")
print(f"  Unique keys  : {unique:,}")
print(f"  Duplicates   : {dupes:,}")

if dupes == 0:
    print("\n✅ SAFE — store + txn_no + account is unique across all records.")
    print("   You can use this as an alternate key.")
else:
    print(f"\n❌ NOT SAFE — {dupes:,} duplicate key combinations found.")
    print("\nTop duplicate combos:")
    counts = Counter(keys)
    for combo, count in counts.most_common(10):
        store, txn, acct = combo
        print(f"  store={store}, txn_no={txn}, account={acct}  -> {count}x")
        # Show full details of the dupes
        matching = [
            r for r in records
            if r.get("crf63_storenumber") == store
            and r.get("crf63_transactionnumber") == txn
            and r.get("crf63_accountnumber") == acct
        ]
        for m in matching[:3]:
            print(f"    id={m.get('crf63_gltransactionid')}")
