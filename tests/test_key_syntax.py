import requests
from modules.utils.keyvault import get_dataverse_credentials
import msal
import json

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(creds['client_id'], authority=f'https://login.microsoft.com/{creds["tenant_id"]}', client_credential=creds['client_secret'])
token = app.acquire_token_for_client([f'{creds["environment_url"]}/.default'])['access_token']

api_url = f'{creds["environment_url"]}/api/data/v9.2'
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
    'Prefer': 'odata.allow-upsert=true'
}

table = 'crf63_saleschanneldailies'
key_value = 'TEST_KEY_001'
record = {
    'crf63_businesskey': key_value,
    'crf63_name': 'Test Record 001'
}

# Test 1: Attribute Name
print("Test 1: Attribute Name")
url = f"{api_url}/{table}(crf63_businesskey='{key_value}')"
resp = requests.patch(url, headers=headers, json=record)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text}")

# Test 2: Key Schema Name (unlikely but checking)
print("\nTest 2: Key Schema Name")
url = f"{api_url}/{table}(crf63_saleschanneldaily_businesskey_key='{key_value}')"
resp = requests.patch(url, headers=headers, json=record)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text}")
