#!/usr/bin/env python3
"""
Test expanded composite key combinations for crf63_gltransactions.
Checks if adding more fields eliminates duplicates from store+txn+account.
"""

import sys, os, argparse, requests
from azure.identity import AzureCliCredential
from collections import Counter

DEFAULT_ENV_URL = "https://orgbf93e3c3.crm.dynamics.com"
parser = argparse.ArgumentParser()
parser.add_argument("--env-url", default=DEFAULT_ENV_URL)
args = parser.parse_args()
env_url = args.env_url.rstrip("/")

print("Authenticating via Azure CLI...")
credential = AzureCliCredential()
token = credential.get_token(f"{env_url}/.default").token

api_url = f"{env_url}/api/data/v9.2"
table = "crf63_gltransactions"
headers = {
    "Authorization": f"Bearer {token}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Prefer": "odata.maxpagesize=5000"
}

print(f"Fetching all records from {table}...")
url = (
    f"{api_url}/{table}"
    f"?$select=crf63_storenumber,crf63_transactionnumber,crf63_accountnumber,"
    f"crf63_transactionamount,crf63_posteddate,crf63_transactiondescription,crf63_journalcode"
)

records = []
page = 1
while url:
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=120)
            break
        except requests.exceptions.ReadTimeout:
            print(f"  Page {page}: timeout, retrying ({attempt+1}/3)...")
            import time; time.sleep(5)
    else:
        print(f"  Page {page}: failed after 3 attempts, stopping with {len(records):,} records")
        url = None
        break
    if resp.status_code != 200:
        print(f"ERROR: HTTP {resp.status_code} - {resp.text[:300]}")
        sys.exit(1)
    data = resp.json()
    records.extend(data.get("value", []))
    print(f"  Page {page}: {len(records):,} total")
    page += 1
    url = data.get("@odata.nextLink")

total = len(records)
print(f"\nTotal records: {total:,}\n")

def check(label, keyfn):
    keys = [keyfn(r) for r in records]
    dupes = total - len(set(keys))
    status = "✅ SAFE" if dupes == 0 else f"❌ {dupes:,} duplicates"
    print(f"  {status}  —  {label}")
    return dupes == 0

print("=== Key uniqueness results ===")
check("store + txn + account (baseline)",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber")))

check("store + txn + account + date",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_posteddate")))

check("store + journal + txn + account + date",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_journalcode"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_posteddate")))

check("store + txn + account + amount",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_transactionamount")))

check("store + txn + account + amount + date",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_transactionamount"), r.get("crf63_posteddate")))

check("store + journal + txn + account + amount + date",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_journalcode"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_transactionamount"), r.get("crf63_posteddate")))

check("store + txn + account + amount + date + description",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_transactionamount"), r.get("crf63_posteddate"), r.get("crf63_transactiondescription")))

check("store + journal + txn + account + amount + date + description",
      lambda r: (r.get("crf63_storenumber"), r.get("crf63_journalcode"), r.get("crf63_transactionnumber"), r.get("crf63_accountnumber"), r.get("crf63_transactionamount"), r.get("crf63_posteddate"), r.get("crf63_transactiondescription")))
