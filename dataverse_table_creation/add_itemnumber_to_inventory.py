#!/usr/bin/env python3
"""
Add ItemNumber column to the Inventory table in Dataverse.
"""

import requests
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.dataverse import get_dataverse_access_token
from modules.utils.config import load_config
from modules.utils.keyvault import get_dataverse_credentials

def add_itemnumber_column(token, dataverse_url):
    """Add ItemNumber column to inventory table."""
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Column definition
    column_data = {
        "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
        "SchemaName": "crf63_itemnumber",
        "DisplayName": {
            "LocalizedLabels": [{"Label": "Item Number", "LanguageCode": 1033}]
        },
        "Description": {
            "LocalizedLabels": [{"Label": "Inventory item number/code", "LanguageCode": 1033}]
        },
        "RequiredLevel": {
            "Value": "None",
            "CanBeChanged": True
        },
        "MaxLength": 100,
        "FormatName": {
            "Value": "Text"
        }
    }
    
    print("\n📝 Adding ItemNumber column...")
    print(f"   Schema Name: crf63_itemnumber")
    print(f"   Display Name: Item Number")
    
    url = f"{dataverse_url}/api/data/v9.2/EntityDefinitions(LogicalName='crf63_inventories')/Attributes"
    
    response = requests.post(url, headers=headers, json=column_data)
    
    if response.status_code in [200, 201, 204]:
        print("✓ ItemNumber column added successfully!")
        return True
    else:
        print(f"❌ Failed to add column")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def main():
    print("=" * 80)
    print("Add ItemNumber Column to Inventory Table")
    print("=" * 80)
    
    try:
        # Get config and token
        print("\n🔐 Authenticating...")
        config = load_config()
        creds = get_dataverse_credentials()
        token = get_dataverse_access_token(
            creds['environment_url'],
            creds['client_id'],
            creds['client_secret'],
            creds['tenant_id']
        )
        dataverse_url = creds['environment_url']
        print("✓ Authentication successful")
        
        # Add column
        success = add_itemnumber_column(token, dataverse_url)
        
        if success:
            print("\n" + "=" * 80)
            print("✓ Column added successfully!")
            print("=" * 80)
            print("\nNext steps:")
            print("1. The column is now available in the Inventory table")
            print("2. Run the inventory sync to populate data:")
            print("   python olap_to_dataverse.py --query inventory --length 1wk")
        else:
            print("\n❌ Failed to add column")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
