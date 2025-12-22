#!/usr/bin/env python3
"""
Publish Dataverse table customizations
Makes the table available via Web API
"""

import requests
import json
from msal import PublicClientApplication
import sys

# Dataverse Configuration
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

def publish_table(table_name):
    """Publish customizations for a table"""
    
    # Interactive authentication
    app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
    result = app.acquire_token_interactive(scopes=SCOPES)
    
    if "access_token" not in result:
        print(f"❌ Authentication failed: {result.get('error_description', result)}")
        return False
    
    access_token = result["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # PublishXml request to publish table
    publish_xml = f"""
    <importexportxml>
        <entities>
            <entity>{table_name}</entity>
        </entities>
        <nodes/>
        <securityroles/>
        <settings/>
        <workflows/>
    </importexportxml>
    """
    
    publish_request = {
        "ParameterXml": publish_xml.strip()
    }
    
    print(f"📤 Publishing table: {table_name}")
    
    # Execute PublishXml action
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/PublishXml"
    
    try:
        response = requests.post(url, headers=headers, json=publish_request)
        
        if response.status_code in [200, 204]:
            print(f"✅ Table '{table_name}' published successfully")
            return True
        else:
            print(f"❌ Publish failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error publishing table: {e}")
        return False

if __name__ == "__main__":
    table_name = sys.argv[1] if len(sys.argv) > 1 else "crf63_inventories"
    publish_table(table_name)
