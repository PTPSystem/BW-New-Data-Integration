"""Delete all records from crf63_saleschanneldaily table"""
from modules.utils.keyvault import get_dataverse_credentials
import msal
import requests
import time

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(
    creds['client_id'], 
    authority=f'https://login.microsoftonline.com/{creds["tenant_id"]}', 
    client_credential=creds['client_secret']
)
token = app.acquire_token_for_client([f'{creds["environment_url"]}/.default'])['access_token']
headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'Prefer': 'odata.maxpagesize=5000'}

api_url = f'{creds["environment_url"]}/api/data/v9.2'
table = 'crf63_saleschanneldailies'

print('Deleting all records from crf63_saleschanneldaily...')
print('This may take a while for ~188,000 records...')
deleted = 0
start = time.time()

while True:
    # Get batch of record IDs
    r = requests.get(f'{api_url}/{table}?$select=crf63_saleschanneldailyid&$top=1000', headers=headers)
    records = r.json().get('value', [])
    if not records:
        break
    
    # Delete each record
    for rec in records:
        rid = rec['crf63_saleschanneldailyid']
        del_headers = {'Authorization': f'Bearer {token}'}
        requests.delete(f'{api_url}/{table}({rid})', headers=del_headers)
        deleted += 1
    
    elapsed = time.time() - start
    rate = deleted / elapsed if elapsed > 0 else 0
    print(f'  Deleted {deleted:,} records... ({rate:.0f}/sec)')

print(f'\n=== DONE: Deleted {deleted:,} total records in {time.time()-start:.1f}s ===')
