#!/usr/bin/env python3
"""Retry: add alternate key on crf63_record_key for crf63_bw_labor_forecast_records."""
import requests, time, sys
from msal import PublicClientApplication

DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID  = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID  = "51f81489-12ee-4a9e-aaae-a2591f45987d"
AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES     = [f"{DATAVERSE_ENVIRONMENT}/.default"]
TABLE      = "crf63_bw_labor_forecast_records"

app = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)
accounts = app.get_accounts()
result = app.acquire_token_silent(SCOPES, account=accounts[0]) if accounts else None
if not result or "access_token" not in result:
    result = app.acquire_token_interactive(scopes=SCOPES, prompt="select_account")
token = result["access_token"]
print(f"✓ Token acquired")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
}

print("Waiting 3 s before fetching metadata...")
time.sleep(3)

# Fetch table + attribute metadata
url = (
    f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/"
    f"EntityDefinitions(LogicalName='{TABLE}')"
    f"?$select=MetadataId&$expand=Attributes($select=LogicalName,MetadataId)"
)
resp = requests.get(url, headers=headers)
if resp.status_code != 200:
    print(f"❌ Failed to fetch metadata: {resp.status_code} {resp.text}")
    sys.exit(1)

data = resp.json()
table_meta_id = data["MetadataId"]
rk_found = any(a["LogicalName"] == "crf63_record_key" for a in data.get("Attributes", []))
print(f"Table MetadataId       : {table_meta_id}")
print(f"crf63_record_key found : {rk_found}")

if not rk_found:
    print("❌ crf63_record_key not found in attributes yet")
    sys.exit(1)

alt_key = {
    "SchemaName": "crf63_record_key_key",
    "DisplayName": {
        "@odata.type": "Microsoft.Dynamics.CRM.Label",
        "LocalizedLabels": [
            {
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": "Record Key",
                "LanguageCode": 1033,
            }
        ],
    },
    # Use logical name strings, NOT MetadataId GUIDs
    "KeyAttributes": ["crf63_record_key"],
    "EntityLogicalName": TABLE,
}

url2 = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({table_meta_id})/Keys"
resp2 = requests.post(url2, headers=headers, json=alt_key)
if resp2.status_code in [200, 201, 204]:
    print("✓ Alternate key created successfully! (Activates asynchronously ~5 min)")
else:
    print(f"❌ {resp2.status_code}: {resp2.text}")
    sys.exit(1)
