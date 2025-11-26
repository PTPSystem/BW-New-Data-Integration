#!/usr/bin/env python3
"""
Add business key column to existing crf63_oarsbidata table.

Business Key Format: {StoreNumber}_{YYYYMMDD}
Example: 4280_20250115

This column will be used for efficient upsert operations.
"""

import msal
import requests

# Configuration
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
# Use well-known Azure PowerShell client ID for interactive auth (has redirect URIs configured)
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TABLE_LOGICAL_NAME = "crf63_oarsbidata"

def get_access_token():
    """Get access token using interactive browser authentication."""
    print("🔐 Authenticating to Dataverse...")
    print("   A browser window will open for authentication")
    
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.PublicClientApplication(CLIENT_ID, authority=authority)
    
    scopes = [f"{DATAVERSE_ENVIRONMENT}/.default"]
    
    # Try to get token from cache first
    accounts = app.get_accounts()
    if accounts:
        print(f"   Found cached account: {accounts[0]['username']}")
        result = app.acquire_token_silent(scopes, account=accounts[0])
        if result:
            print("✅ Using cached authentication")
            return result["access_token"]
    
    # Try to get token interactively
    result = app.acquire_token_interactive(scopes=scopes, prompt="select_account")
    
    if "access_token" in result:
        print("✅ Authentication successful")
        return result["access_token"]
    else:
        print(f"❌ Authentication failed: {result.get('error_description', 'Unknown error')}")
        return None

def add_business_key_column(access_token):
    """Add crf63_businesskey column to the table."""
    print(f"\n📊 Adding business key column to {TABLE_LOGICAL_NAME}...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Get table metadata to find the table ID
    print("   Getting table metadata...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to get table metadata: {response.status_code}")
        print(f"   Response: {response.text}")
        return False
    
    table_data = response.json()
    metadata_id = table_data.get("MetadataId")
    print(f"   ✓ Table found: {metadata_id}")
    
    # Define the business key column
    column_definition = {
        "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
        "AttributeType": "String",
        "AttributeTypeName": {
            "Value": "StringType"
        },
        "Description": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [{
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": "Composite key for upsert operations (StoreNumber_YYYYMMDD)",
                "LanguageCode": 1033
            }]
        },
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [{
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": "Business Key",
                "LanguageCode": 1033
            }]
        },
        "RequiredLevel": {
            "Value": "None",
            "CanBeChanged": True
        },
        "SchemaName": "crf63_businesskey",
        "MaxLength": 50,
        "FormatName": {
            "Value": "Text"
        }
    }
    
    # Create the column
    print("   Creating business key column...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({metadata_id})/Attributes"
    response = requests.post(url, headers=headers, json=column_definition)
    
    if response.status_code in [200, 204]:
        print("✅ Business key column created successfully")
        print(f"   Column: crf63_businesskey (String, 50 chars)")
        print(f"   Format: {{StoreNumber}}_{{YYYYMMDD}}")
        print(f"   Example: 4280_20250115")
        return True
    else:
        print(f"❌ Failed to create column: {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def create_alternate_key(access_token):
    """Create alternate key on business key column for faster lookups."""
    print(f"\n🔑 Creating alternate key on business key column...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Get table metadata
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to get table metadata: {response.status_code}")
        return False
    
    table_data = response.json()
    metadata_id = table_data.get("MetadataId")
    
    # Define alternate key
    key_definition = {
        "SchemaName": "crf63_businesskey_key",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [{
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": "Business Key",
                "LanguageCode": 1033
            }]
        },
        "KeyAttributes": ["crf63_businesskey"]
    }
    
    # Create alternate key
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({metadata_id})/Keys"
    response = requests.post(url, headers=headers, json=key_definition)
    
    if response.status_code in [200, 204]:
        print("✅ Alternate key created successfully")
        print("   ⚠️  Note: Key activation may take several minutes")
        print("   Monitor status in Power Apps maker portal")
        return True
    else:
        print(f"⚠️  Could not create alternate key: {response.status_code}")
        print(f"   Response: {response.text}")
        print("   This is optional - you can still use filter queries")
        return False

def main():
    """Main execution."""
    print("=" * 70)
    print("Add Business Key Column to Dataverse Table")
    print("=" * 70)
    print()
    
    # Get access token
    token = get_access_token()
    if not token:
        return 1
    
    # Add business key column
    if not add_business_key_column(token):
        return 1
    
    # Optional: Create alternate key for performance
    print()
    response = input("Create alternate key for faster lookups? (y/n): ")
    if response.lower() == 'y':
        create_alternate_key(token)
    
    print()
    print("=" * 70)
    print("✅ Complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Test the column exists:")
    print("   - Open Power Apps maker portal")
    print("   - Navigate to Tables → crf63_oarsbidata → Columns")
    print("   - Verify 'Business Key' column appears")
    print()
    print("2. Run OLAP sync with business key:")
    print("   cd NewIntegration")
    print("   python olap_to_dataverse.py --query-type last_2_weeks")
    print()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
