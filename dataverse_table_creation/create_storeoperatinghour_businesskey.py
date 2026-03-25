#!/usr/bin/env python3
"""
Add business key column and create alternate key for crf63_storeoperatinghour table

This script:
1. Adds crf63_businesskey column to the Store Operating Hour table
2. Creates an alternate key on the business key column for efficient upsert operations

Business Key Format: {StoreNumber}_{DayOfWeek}
Example: 101_1 (Store 101, Monday)

The alternate key enables upsert operations using the business key instead of GUID,
which is essential for efficient data synchronization from external sources.

Prerequisites:
- Table crf63_storeoperatinghour must exist in Dataverse
- User must have appropriate permissions to modify table schema

Usage:
  python create_storeoperatinghour_businesskey.py
"""

import requests
import json
import sys
import time
from pathlib import Path
from msal import PublicClientApplication

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

# Configuration
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell
DATAVERSE_URL = "https://orgbf93e3c3.crm.dynamics.com"
TABLE_LOGICAL_NAME = "crf63_storeoperatinghour"
TABLE_PLURAL_NAME = "crf63_storeoperatinghours"


def get_access_token():
    """Get access token using MSAL interactive authentication."""
    print("\n🔐 Authenticating with Microsoft...")
    
    app = PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    
    scopes = [f"{DATAVERSE_URL}/.default"]
    result = app.acquire_token_interactive(scopes=scopes)
    
    if "access_token" in result:
        print("✓ Authentication successful")
        return result["access_token"]
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description', result)}")


def check_column_exists(token, column_name):
    """Check if a column already exists in the table."""
    print(f"\n🔍 Checking if column '{column_name}' exists...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Attributes?$filter=LogicalName eq '{column_name}'"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            exists = len(data.get('value', [])) > 0
            if exists:
                print(f"✓ Column '{column_name}' already exists")
            else:
                print(f"  Column '{column_name}' does not exist")
            return exists
        else:
            print(f"⚠️  Could not check column existence: HTTP {response.status_code}")
            return False
    
    except Exception as e:
        print(f"⚠️  Error checking column existence: {e}")
        return False


def add_business_key_column(token):
    """Add crf63_businesskey column to the table."""
    print("\n📝 Adding crf63_businesskey column...")
    
    # Check if column already exists
    if check_column_exists(token, "crf63_businesskey"):
        print("  Skipping column creation (already exists)")
        return True
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "MSCRM.SolutionUniqueName": "Default"
    }
    
    # String column definition for business key
    column_definition = {
        "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
        "AttributeType": "String",
        "AttributeTypeName": {
            "Value": "StringType"
        },
        "SchemaName": "crf63_businesskey",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Business Key",
                    "LanguageCode": 1033
                }
            ]
        },
        "Description": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Composite business key: {StoreNumber}_{DayOfWeek}",
                    "LanguageCode": 1033
                }
            ]
        },
        "RequiredLevel": {
            "Value": "ApplicationRequired",
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
        },
        "MaxLength": 100,
        "FormatName": {
            "Value": "Text"
        }
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Attributes"
    
    try:
        response = requests.post(url, headers=headers, json=column_definition, timeout=60)
        
        if response.status_code in [200, 201, 204]:
            print("✓ Business key column created successfully")
            return True
        else:
            print(f"❌ Failed to create business key column: HTTP {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return False
    
    except Exception as e:
        print(f"❌ Exception creating business key column: {e}")
        return False


def check_alternate_key_exists(token):
    """Check if alternate key already exists."""
    print("\n🔍 Checking if alternate key exists...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Keys"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            keys = data.get('value', [])
            
            # Check for our specific key
            for key in keys:
                if key.get('SchemaName') == 'crf63_businesskey_key':
                    print("✓ Alternate key 'crf63_businesskey_key' already exists")
                    return True
            
            print("  Alternate key does not exist")
            return False
        else:
            print(f"⚠️  Could not check alternate key: HTTP {response.status_code}")
            return False
    
    except Exception as e:
        print(f"⚠️  Error checking alternate key: {e}")
        return False


def create_alternate_key(token):
    """Create alternate key on crf63_businesskey column."""
    print("\n🔑 Creating alternate key on business key column...")
    
    # Check if alternate key already exists
    if check_alternate_key_exists(token):
        print("  Skipping alternate key creation (already exists)")
        return True
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Alternate key definition
    alternate_key_definition = {
        "SchemaName": "crf63_businesskey_key",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Business Key",
                    "LanguageCode": 1033
                }
            ]
        },
        "KeyAttributes": [
            "crf63_businesskey"
        ],
        "EntityLogicalName": TABLE_LOGICAL_NAME
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Keys"
    
    try:
        response = requests.post(url, headers=headers, json=alternate_key_definition, timeout=60)
        
        if response.status_code in [200, 201, 204]:
            print("✓ Alternate key creation initiated successfully")
            print("\n⏳ Note: Alternate key activation is an asynchronous process")
            print("   It may take 5-10 minutes for the key to become fully active")
            return True
        else:
            print(f"❌ Failed to create alternate key: HTTP {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return False
    
    except Exception as e:
        print(f"❌ Exception creating alternate key: {e}")
        return False


def verify_table_exists(token):
    """Verify that the table exists in Dataverse."""
    print(f"\n🔍 Verifying table '{TABLE_LOGICAL_NAME}' exists...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')?$select=LogicalName,DisplayName"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            display_name = data.get('DisplayName', {}).get('UserLocalizedLabel', {}).get('Label', 'Unknown')
            print(f"✓ Table found: {display_name} ({TABLE_LOGICAL_NAME})")
            return True
        elif response.status_code == 404:
            print(f"❌ Table '{TABLE_LOGICAL_NAME}' not found in Dataverse")
            print("\nPlease create the table first or verify the table name is correct.")
            return False
        else:
            print(f"⚠️  Could not verify table: HTTP {response.status_code}")
            return False
    
    except Exception as e:
        print(f"⚠️  Error verifying table: {e}")
        return False


def main():
    """Main function to add business key and create alternate key."""
    print("=" * 80)
    print("Add Business Key to Store Operating Hour Table")
    print("=" * 80)
    print(f"Table: {TABLE_LOGICAL_NAME}")
    print(f"Environment: {DATAVERSE_URL}")
    print()
    print("This script will:")
    print("  1. Add crf63_businesskey column (String, max 100 chars)")
    print("  2. Create alternate key on the business key column")
    print("  3. Enable efficient upsert operations using business key")
    print()
    print("Business Key Format: {StoreNumber}_{DayOfWeek}")
    print("Example: 101_1 (Store 101, Monday)")
    print("=" * 80)
    
    try:
        # Step 1: Authenticate
        token = get_access_token()
        
        # Step 2: Verify table exists
        if not verify_table_exists(token):
            print("\n❌ Cannot proceed without valid table")
            return 1
        
        # Step 3: Add business key column
        print("\n" + "=" * 80)
        print("Step 1: Add Business Key Column")
        print("=" * 80)
        
        if not add_business_key_column(token):
            print("\n❌ Failed to add business key column")
            print("   Please check the error messages above")
            return 1
        
        # Wait a moment for the column to be created
        time.sleep(2)
        
        # Step 4: Create alternate key
        print("\n" + "=" * 80)
        print("Step 2: Create Alternate Key")
        print("=" * 80)
        
        if not create_alternate_key(token):
            print("\n❌ Failed to create alternate key")
            print("   Please check the error messages above")
            return 1
        
        # Final summary
        print("\n" + "=" * 80)
        print("✓ Setup Complete!")
        print("=" * 80)
        print("\nNext Steps:")
        print("1. Wait 5-10 minutes for the alternate key to activate")
        print("2. Verify key status in Power Apps:")
        print("   - Go to https://make.powerapps.com")
        print("   - Navigate to Tables > BW Store Operating Hour")
        print("   - Go to Keys tab")
        print("   - Check that 'Business Key' shows as Active")
        print("\n3. Once active, you can use populate_store_hours.py to import data")
        print("   The script will use the business key for efficient upserts")
        print("\n4. To populate existing records with business key values, run:")
        print("   UPDATE query in Dataverse or use a data update script")
        print("=" * 80)
        
        return 0
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
