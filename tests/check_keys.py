import requests
from modules.utils.keyvault import get_dataverse_credentials
import msal
import json

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(creds['client_id'], authority=f"https://login.microsoftonline.com/{creds['tenant_id']}", client_credential=creds['client_secret'])
token = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])['access_token']

api_url = f"{creds['environment_url']}/api/data/v9.2"
headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

# Get Entity Definition to find keys
entity_name = "crf63_saleschanneldaily"
url = f"{api_url}/EntityDefinitions(LogicalName='{entity_name}')/Keys"
print(f"Querying: {url}")

r = requests.get(url, headers=headers)
if r.status_code == 200:
    keys = r.json().get('value', [])
    print(f"Found {len(keys)} keys:")
    for k in keys:
        print(f"Key Name: {k['SchemaName']}")
        print(f"Key Attributes: {k['KeyAttributes']}")
else:
    print(f"Error: {r.status_code} - {r.text}")

# Also check the attributes to be sure about the column name
url_attr = f"{api_url}/EntityDefinitions(LogicalName='{entity_name}')/Attributes(LogicalName='crf63_businesskey')"
r_attr = requests.get(url_attr, headers=headers)
if r_attr.status_code == 200:
    print("\nAttribute 'crf63_businesskey' exists.")
else:
    print("\nAttribute 'crf63_businesskey' NOT found.")
