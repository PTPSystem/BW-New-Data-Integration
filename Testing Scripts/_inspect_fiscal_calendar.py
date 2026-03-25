#!/usr/bin/env python3
"""Check what date range exists in crf63_bwfiscalcalendars."""
from modules.utils.keyvault import get_dataverse_credentials
from modules.dataverse import get_dataverse_access_token
import requests

creds = get_dataverse_credentials()
url = creds['environment_url']
token = get_dataverse_access_token(url, creds['client_id'], creds['client_secret'], creds['tenant_id'])
headers = {"Authorization": "Bearer " + token, "OData-MaxVersion": "4.0", "OData-Version": "4.0", "Prefer": "odata.include-annotations=*"}
api = url.rstrip('/') + '/api/data/v9.2'

# Get the absolute first and last dates in the table
r_first = requests.get(api + '/crf63_bwfiscalcalendars?$select=crf63_date,crf63_fiscalyearnumber&$orderby=crf63_date asc&$top=3', headers=headers, timeout=30)
r_last  = requests.get(api + '/crf63_bwfiscalcalendars?$select=crf63_date,crf63_fiscalyearnumber&$orderby=crf63_date desc&$top=3', headers=headers, timeout=30)

print('Earliest rows:')
for row in r_first.json().get('value', []):
    print(' ', row.get('crf63_date','')[:10], 'FY', row.get('crf63_fiscalyearnumber'))

print('Latest rows:')
for row in r_last.json().get('value', []):
    print(' ', row.get('crf63_date','')[:10], 'FY', row.get('crf63_fiscalyearnumber'))

# Count rows for FY2020 by date range
for fy_start, fy_end, fy_label in [
    ('2019-12-30T00:00:00Z', '2020-12-28T00:00:00Z', 'FY2020'),
    ('2020-12-28T00:00:00Z', '2021-12-27T00:00:00Z', 'FY2021'),
    ('2021-12-27T00:00:00Z', '2022-12-26T00:00:00Z', 'FY2022'),
    ('2022-12-26T00:00:00Z', '2024-01-01T00:00:00Z', 'FY2023'),
]:
    r = requests.get(
        api + '/crf63_bwfiscalcalendars?$select=crf63_date'
            + '&$filter=crf63_date ge ' + fy_start + ' and crf63_date lt ' + fy_end
            + '&$top=1',
        headers=headers, timeout=30
    )
    rows = r.json().get('value', [])
    found = rows[0]['crf63_date'][:10] if rows else 'NO ROWS FOUND'
    print(fy_label + ': first row =', found)
from modules.utils.keyvault import get_dataverse_credentials
from modules.dataverse import get_dataverse_access_token
import requests

creds = get_dataverse_credentials()
url = creds['environment_url']
token = get_dataverse_access_token(url, creds['client_id'], creds['client_secret'], creds['tenant_id'])
headers = {"Authorization": "Bearer " + token, "OData-MaxVersion": "4.0", "OData-Version": "4.0"}
api = url.rstrip('/') + '/api/data/v9.2'

for fy in [2020, 2021, 2022, 2023, 2024]:
    r = requests.get(
        api + "/crf63_bwfiscalcalendars?$select=crf63_date,crf63_fiscalyearnumber"
            + "&$filter=crf63_fiscalyearnumber eq " + str(fy)
            + "&$orderby=crf63_date asc&$top=1",
        headers=headers, timeout=30
    )
    r2 = requests.get(
        api + "/crf63_bwfiscalcalendars?$select=crf63_date"
            + "&$filter=crf63_fiscalyearnumber eq " + str(fy)
            + "&$orderby=crf63_date desc&$top=1",
        headers=headers, timeout=30
    )
    rc = requests.get(
        api + "/crf63_bwfiscalcalendars?$select=crf63_bwfiscalcalendarid"
            + "&$filter=crf63_fiscalyearnumber eq " + str(fy)
            + "&$count=true&$top=0",
        headers={**headers, "Prefer": "odata.include-annotations=*"},
        timeout=30
    )
    first_rows = r.json().get('value', [])
    last_rows  = r2.json().get('value', [])
    count = rc.json().get('@odata.count', '?')
    first_date = first_rows[0]['crf63_date'][:10] if first_rows else 'N/A'
    last_date  = last_rows[0]['crf63_date'][:10]  if last_rows  else 'N/A'
    print(f"FY{fy}: {count:>4} rows   {first_date} → {last_date}")
from modules.utils.keyvault import get_dataverse_credentials
from modules.dataverse import get_dataverse_access_token
import requests
import json

creds = get_dataverse_credentials()
url = creds['environment_url']
token = get_dataverse_access_token(url, creds['client_id'], creds['client_secret'], creds['tenant_id'])
headers = {"Authorization": "Bearer " + token, "OData-MaxVersion": "4.0", "OData-Version": "4.0"}
api = url.rstrip('/') + '/api/data/v9.2'

FIELDS = (
    "crf63_date,crf63_dayofyear,crf63_calendardayofyear,"
    "crf63_fiscalyearnumber,crf63_fiscalperiodnumber,crf63_fiscalperiodlabel,"
    "crf63_fiscalweeknumber,crf63_fiscalweeklabel,"
    "crf63_fiscalquarternumber,crf63_fiscalquarterlabel,"
    "crf63__445fiscalperiodnumber,crf63__445fiscalperiodlabel,"
    "crf63__445fiscalquarternumber,crf63__445fiscalquarterlabel"
)

def fetch_around_day(day_of_year):
    """Fetch rows at a specific fiscal day-of-year in FY2023."""
    lo = day_of_year - 1
    hi = day_of_year + 1
    r = requests.get(
        api + ("/crf63_bwfiscalcalendars"
               "?$select=" + FIELDS +
               "&$filter=crf63_fiscalyearnumber eq 2023"
               " and crf63_dayofyear ge " + str(lo) +
               " and crf63_dayofyear le " + str(hi) +
               "&$orderby=crf63_dayofyear asc"),
        headers=headers, timeout=60
    )
    return r.json().get('value', [])

# Check all 13 period boundaries + quarter boundaries
boundaries = [
    ("P01 start", 1), ("P01->P02", 28), ("P02->P03", 56), ("P03->P04", 84),
    ("P04->P05", 112), ("P05->P06", 140), ("P06->P07", 168),
    ("P07->P08", 196), ("P08->P09", 224), ("P09->P10", 252),
    ("P10->P11", 280), ("P11->P12", 308), ("P12->P13", 336), ("P13 end", 364),
]

for label, day in boundaries:
    rows = fetch_around_day(day)
    if rows:
        print(f"--- {label} (around day {day}) ---")
        for row in rows:
            print(f"  day={row.get('crf63_dayofyear'):3d}"
                  f"  date={row.get('crf63_date','')[:10]}"
                  f"  13-4: FY={row.get('crf63_fiscalyearnumber')} P={row.get('crf63_fiscalperiodnumber'):2d} W={row.get('crf63_fiscalweeknumber'):2d} Q={row.get('crf63_fiscalquarternumber')}"
                  f"  445: P={row.get('crf63__445fiscalperiodnumber')} Q={row.get('crf63__445fiscalquarternumber')}")
        print()
