import requests
from modules.utils.keyvault import get_dataverse_credentials
import msal

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(creds['client_id'], authority=f'https://login.microsoft.com/{creds["tenant_id"]}', client_credential=creds['client_secret'])
token = app.acquire_token_for_client([f'{creds["environment_url"]}/.default'])['access_token']

api_url = f'{creds["environment_url"]}/api/data/v9.2'
headers = {'Authorization': f'Bearer {token}'}

print('Querying EntityDefinitions...')
# Fetch all entities and filter in python to avoid OData syntax issues in shell
resp = requests.get(f'{api_url}/EntityDefinitions?$select=SchemaName,EntitySetName,LogicalName,PrimaryIdAttribute', headers=headers)

if resp.status_code == 200:
    entities = resp.json().get('value', [])
    found = False
    for e in entities:
        if 'saleschannel' in e['SchemaName'].lower():
            found = True
            print(f"Entity: {e['LogicalName']}")
            print(f"  Set Name: {e['EntitySetName']}")
            print(f"  Primary ID: {e['PrimaryIdAttribute']}")
            
            # Get Keys
            keys_resp = requests.get(f'{api_url}/EntityDefinitions(LogicalName=\'{e["LogicalName"]}\')/Keys', headers=headers)
            if keys_resp.status_code == 200:
                keys = keys_resp.json().get('value', [])
                for k in keys:
                    print(f"  Key: {k['SchemaName']}")
                    print(f"    Attributes: {k['KeyAttributes']}")
            print("-" * 30)
    if not found:
        print("No entity found with 'saleschannel' in SchemaName.")
else:
    print(f'Error: {resp.status_code} - {resp.text[:500]}')
