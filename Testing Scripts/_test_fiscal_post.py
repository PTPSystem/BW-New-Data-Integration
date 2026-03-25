#!/usr/bin/env python3
"""Test a single POST to crf63_bwfiscalcalendars to diagnose create failures."""
from modules.utils.keyvault import get_dataverse_credentials
from modules.dataverse import get_dataverse_access_token
import requests
import json

creds = get_dataverse_credentials()
url = creds['environment_url']
token = get_dataverse_access_token(url, creds['client_id'], creds['client_secret'], creds['tenant_id'])
headers = {
    "Authorization": "Bearer " + token,
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}
api = url.rstrip('/') + '/api/data/v9.2'

# Attempt 1: all fields
rec_full = {
    "crf63_date":                    "2019-12-30T06:00:00Z",
    "crf63_dayofyear":               1,
    "crf63_calendardayofyear":       364,
    "crf63_fiscalyearnumber":        2020,
    "crf63_fiscalyearlabel":         "Y2020",
    "crf63_fiscalperiodnumber":      1,
    "crf63_fiscalperiodlabel":       "P01",
    "crf63_fiscalweeknumber":        1,
    "crf63_fiscalweeklabel":         "W01",
    "crf63_fiscalquarternumber":     1,
    "crf63_fiscalquarterlabel":      "Q1",
    "crf63__445fiscalperiodnumber":  1,
    "crf63__445fiscalperiodlabel":   "P01",
    "crf63__445fiscalquarternumber": 1,
    "crf63__445fiscalquarterlabel":  "Q01",
}

print("=== Attempt 1: all fields ===")
r = requests.post(api + '/crf63_bwfiscalcalendars', json=rec_full, headers=headers, timeout=30)
print("HTTP", r.status_code)
if r.status_code in (200, 201, 204):
    print("SUCCESS")
    created = r.json()
    print("Created ID:", created.get('crf63_bwfiscalcalendarid'))
    # Delete it to keep things clean
    rid = created.get('crf63_bwfiscalcalendarid')
    if rid:
        rd = requests.delete(api + '/crf63_bwfiscalcalendars(' + rid + ')', headers=headers, timeout=30)
        print("Cleanup delete:", rd.status_code)
else:
    print("FAILED:", r.text[:1000])

# Attempt 2: only 13-4 fields (no 445)
rec_no445 = {k: v for k, v in rec_full.items() if '445' not in k}
print()
print("=== Attempt 2: no 445 fields ===")
r2 = requests.post(api + '/crf63_bwfiscalcalendars', json=rec_no445, headers=headers, timeout=30)
print("HTTP", r2.status_code)
if r2.status_code in (200, 201, 204):
    print("SUCCESS")
    created2 = r2.json()
    rid2 = created2.get('crf63_bwfiscalcalendarid')
    if rid2:
        rd2 = requests.delete(api + '/crf63_bwfiscalcalendars(' + rid2 + ')', headers=headers, timeout=30)
        print("Cleanup delete:", rd2.status_code)
else:
    print("FAILED:", r2.text[:1000])
