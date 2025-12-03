import requests
import json
from modules.utils.keyvault import get_dataverse_credentials
import msal

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(creds['client_id'], authority=f'https://login.microsoftonline.com/{creds["tenant_id"]}', client_credential=creds['client_secret'])
token = app.acquire_token_for_client([f'{creds["environment_url"]}/.default'])['access_token']

api_url = f'{creds["environment_url"]}/api/data/v9.2'
table_name = 'crf63_saleschanneldailies'

test_record = {
    'crf63_businesskey': 'TEST_20251130_Test_Test_Test',
    'crf63_storenumber': 'TEST',
    'crf63_calendardate': '2025-11-30',
    'crf63_sourceactor': 'Test',
    'crf63_sourcechannel': 'Test',
    'crf63_daypart': 'Test',
    'crf63_name': 'TEST Record',
    'crf63_tynetsalesusd': 123.45,
    'crf63_tyorders': 5
}

business_key = test_record['crf63_businesskey']

# For true upsert: NO If-Match header
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0'
}

print('=== Testing PATCH upsert (no If-Match header) ===')
url = f"{api_url}/{table_name}(crf63_businesskey='{business_key}')"
print(f'URL: {url}')
resp = requests.patch(url, headers=headers, json=test_record, timeout=30)
print(f'Status: {resp.status_code}')
print(f'Response: {resp.text[:500] if resp.text else "(empty - success!)"}')
