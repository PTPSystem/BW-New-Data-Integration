"""
Create alternate key on crf63_businesskey for the inventory table.
"""
import requests
import json
from msal import PublicClientApplication

TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell
DATAVERSE_URL = "https://orgbf93e3c3.crm.dynamics.com"

def get_access_token():
    """Get access token using MSAL interactive authentication."""
    app = PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    
    scopes = [f"{DATAVERSE_URL}/.default"]
    result = app.acquire_token_interactive(scopes=scopes)
    
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description', result)}")

def create_alternate_key(token):
    """Create alternate key on crf63_businesskey."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Create alternate key definition
    alternate_key_data = {
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
        "EntityLogicalName": "crf63_inventory"  # Singular - will reference crf63_inventories
    }
    
    print("\n🔑 Creating alternate key...")
    print(f"   Schema Name: crf63_businesskey_key")
    print(f"   Key Attribute: crf63_businesskey")
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='crf63_inventory')/Keys"
    
    response = requests.post(url, headers=headers, json=alternate_key_data)
    
    if response.status_code in [200, 201, 204]:
        print("✓ Alternate key created successfully!")
        print("\n⏳ Note: The alternate key activation is an asynchronous process.")
        print("   It may take 5-10 minutes before the key is fully active.")
        print("   You can check the status in Power Apps > Tables > Inventory > Keys")
        return True
    else:
        print(f"❌ Failed to create alternate key")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def main():
    print("=" * 80)
    print("Create Alternate Key for Inventory Table")
    print("=" * 80)
    
    try:
        # Get access token
        print("\n🔐 Authenticating...")
        token = get_access_token()
        print("✓ Authentication successful")
        
        # Create alternate key
        success = create_alternate_key(token)
        
        if success:
            print("\n" + "=" * 80)
            print("✓ Alternate key creation initiated successfully!")
            print("=" * 80)
            print("\nNext steps:")
            print("1. Wait 5-10 minutes for the key to activate")
            print("2. Verify key status in Power Apps:")
            print("   - Go to Power Apps (make.powerapps.com)")
            print("   - Navigate to Tables > Inventory")
            print("   - Go to Keys tab")
            print("   - Check that 'Business Key' shows as Active")
            print("3. Once active, run the inventory sync again:")
            print("   python olap_to_dataverse.py --query inventory --length 1wk")
        else:
            print("\n❌ Failed to create alternate key")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
