"""Test batch upsert with correct syntax"""
import requests
import json
import uuid
from modules.utils.keyvault import get_dataverse_credentials
import msal

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(
    creds['client_id'], 
    authority=f'https://login.microsoftonline.com/{creds["tenant_id"]}', 
    client_credential=creds['client_secret']
)
token = app.acquire_token_for_client([f'{creds["environment_url"]}/.default'])['access_token']

api_url = f'{creds["environment_url"]}/api/data/v9.2'
table_name = 'crf63_saleschanneldailies'

# Create 3 test records
test_records = [
    {
        'crf63_businesskey': 'BATCH_TEST_1',
        'crf63_storenumber': 'BATCH1',
        'crf63_calendardate': '2025-11-30',
        'crf63_sourceactor': 'Test',
        'crf63_sourcechannel': 'Test',
        'crf63_daypart': 'Test',
        'crf63_name': 'BATCH Test 1',
        'crf63_tynetsalesusd': 100.00,
        'crf63_tyorders': 1
    },
    {
        'crf63_businesskey': 'BATCH_TEST_2',
        'crf63_storenumber': 'BATCH2',
        'crf63_calendardate': '2025-11-30',
        'crf63_sourceactor': 'Test',
        'crf63_sourcechannel': 'Test',
        'crf63_daypart': 'Test',
        'crf63_name': 'BATCH Test 2',
        'crf63_tynetsalesusd': 200.00,
        'crf63_tyorders': 2
    },
    {
        'crf63_businesskey': 'BATCH_TEST_3',
        'crf63_storenumber': 'BATCH3',
        'crf63_calendardate': '2025-11-30',
        'crf63_sourceactor': 'Test',
        'crf63_sourcechannel': 'Test',
        'crf63_daypart': 'Test',
        'crf63_name': 'BATCH Test 3',
        'crf63_tynetsalesusd': 300.00,
        'crf63_tyorders': 3
    }
]

batch_id = f"batch_{uuid.uuid4()}"
changeset_id = f"changeset_{uuid.uuid4()}"

# Build multipart/mixed batch request - ORIGINAL syntax (not @key)
lines = []
lines.append(f"--{batch_id}\r\n")
lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}\r\n")
lines.append("\r\n")

for i, record in enumerate(test_records, 1):
    business_key = record['crf63_businesskey']
    # Use the ORIGINAL syntax that works for direct PATCH
    upsert_url = f"{api_url}/{table_name}(crf63_businesskey='{business_key}')"
    
    lines.append(f"--{changeset_id}\r\n")
    lines.append("Content-Type: application/http\r\n")
    lines.append("Content-Transfer-Encoding: binary\r\n")
    lines.append(f"Content-ID: {i}\r\n")
    lines.append("\r\n")
    lines.append(f"PATCH {upsert_url} HTTP/1.1\r\n")
    lines.append("Content-Type: application/json\r\n")
    lines.append("\r\n")
    lines.append(json.dumps(record) + "\r\n")

lines.append(f"--{changeset_id}--\r\n")
lines.append(f"--{batch_id}--\r\n")

batch_body = "".join(lines)

print("=== BATCH REQUEST ===")
print(batch_body[:1000])
print("...")

batch_headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": f"multipart/mixed; boundary={batch_id}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0"
}

resp = requests.post(f"{api_url}/$batch", headers=batch_headers, data=batch_body.encode('utf-8'), timeout=30)
print(f"\n=== RESPONSE {resp.status_code} ===")
print(resp.text[:2000])
