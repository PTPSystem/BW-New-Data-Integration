#!/usr/bin/env python3
"""
Bulk Delete using Dataverse BulkDelete Action
Deletes all records asynchronously via server-side job.
"""

import requests
import msal
import time
from modules.utils.keyvault import get_dataverse_credentials

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(
    creds['client_id'],
    authority=f"https://login.microsoft.com/{creds['tenant_id']}",
    client_credential=creds['client_secret']
)
token = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])["access_token"]

api_url = f"{creds['environment_url'].rstrip('/')}/api/data/v9.2"
table = "crf63_saleschanneldailies"
headers = {
    "Authorization": f"Bearer {token}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Content-Type": "application/json"
}

print("Starting Bulk Delete Job...")

# BulkDelete request body
body = {
    "QuerySet": [
        {
            "EntityName": table,
            "Criteria": {
                "Conditions": [],  # Empty means all records
                "FilterOperator": "And"
            }
        }
    ],
    "JobName": "Delete all sales channel dailies",
    "SendEmailNotification": False,
    "RecurrencePattern": "",
    "ToRecipients": [],
    "CCRecipients": []
}

response = requests.post(f"{api_url}/BulkDelete", headers=headers, json=body)
if response.status_code == 200:
    job_id = response.json().get("JobId")
    print(f"Bulk Delete Job Started: {job_id}")
    
    # Poll for completion
    while True:
        status_resp = requests.get(f"{api_url}/bulkdeleteoperations({job_id})", headers=headers)
        if status_resp.status_code == 200:
            status_data = status_resp.json()
            status_code = status_data.get("statuscode")
            success_count = status_data.get("successcount", 0)
            failure_count = status_data.get("failurecount", 0)
            print(f"Status: {status_code} | Success: {success_count} | Failures: {failure_count}")
            
            if status_code == 30:  # Succeeded
                print("✅ Bulk Delete Completed Successfully!")
                break
            elif status_code == 31:  # Failed
                print("❌ Bulk Delete Failed!")
                break
            elif status_code == 32:  # Canceled
                print("⚠️ Bulk Delete Canceled!")
                break
        else:
            print(f"Error checking status: {status_resp.status_code}")
        
        time.sleep(10)  # Check every 10 seconds
else:
    print(f"Failed to start Bulk Delete: {response.status_code} - {response.text}")