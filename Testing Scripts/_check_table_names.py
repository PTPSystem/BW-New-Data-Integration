import requests
from msal import PublicClientApplication

env = 'https://orgbf93e3c3.crm.dynamics.com'
tenant = 'c8b6ba98-3fc0-4153-83a9-01374492c0f5'
client = '51f81489-12ee-4a9e-aaae-a2591f45987d'

app = PublicClientApplication(client_id=client, authority=f'https://login.microsoftonline.com/{tenant}')
accts = app.get_accounts()
r = app.acquire_token_silent([f'{env}/.default'], account=accts[0]) if accts else None
if not r or 'access_token' not in r:
    r = app.acquire_token_interactive(scopes=[f'{env}/.default'], prompt='select_account')

tok = r['access_token']
h = {
    'Authorization': f'Bearer {tok}',
    'Accept': 'application/json',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0'
}

url = f"{env}/api/data/v9.2/EntityDefinitions?$select=LogicalName,EntitySetName,PrimaryIdAttribute&$filter=startswith(LogicalName,'crf63_bw_labor')"
resp = requests.get(url, headers=h)
print(f"Status: {resp.status_code}")
data = resp.json()
for e in data.get('value', []):
    print(f"  {e['LogicalName']}")
    print(f"    EntitySetName: {e['EntitySetName']}")
    print(f"    PrimaryIdAttribute: {e['PrimaryIdAttribute']}")
