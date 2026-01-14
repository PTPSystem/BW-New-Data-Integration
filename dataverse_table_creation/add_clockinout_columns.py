#!/usr/bin/env python3
"""
Add new columns to crf63_bw_clockinout table:
- crf63_systemuserid (System User ID) - string
- crf63_regularpay (Regular Pay) - decimal
- crf63_overtimepay (Overtime Pay) - decimal
"""

import requests
import time
from msal import PublicClientApplication
import sys

# Dataverse Configuration
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

TABLE_SCHEMA_NAME = "crf63_bw_clockinout"


def get_access_token():
    """Get access token using interactive browser flow"""
    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY
    )
    
    # Try to get token from cache first
    accounts = app.get_accounts()
    if accounts:
        print(f"Found cached account: {accounts[0]['username']}")
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result:
            return result['access_token']
    
    # Use interactive browser flow
    print("\nAuthentication required...")
    result = app.acquire_token_interactive(
        scopes=SCOPES,
        prompt="select_account"
    )
    
    if "access_token" in result:
        print(f"✓ Authenticated as: {result.get('id_token_claims', {}).get('preferred_username', 'Unknown')}")
        return result["access_token"]
    else:
        raise Exception(f"Authentication failed: {result.get('error_description', result)}")


def create_column(token, entity_logical_name, column_definition):
    """Create a single column in the table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }
    
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{entity_logical_name}')/Attributes"
    
    response = requests.post(url, headers=headers, json=column_definition)
    
    if response.status_code in [200, 201, 204]:
        return True
    else:
        print(f"✗ Failed to create column {column_definition['SchemaName']}: {response.status_code}")
        print(f"Response: {response.text}")
        return False


def main():
    print("="*80)
    print("Add New Columns to BW Clock In Out Table")
    print("="*80)
    print(f"Table: {TABLE_SCHEMA_NAME}")
    print("\nNew columns to add:")
    print("  1. crf63_systemuserid - System User ID (string)")
    print("  2. crf63_regularpay - Regular Pay (decimal)")
    print("  3. crf63_overtimepay - Overtime Pay (decimal)")
    print("="*80)
    
    try:
        token = get_access_token()
        
        # Define new columns
        columns = [
            {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": "crf63_systemuserid",
                "DisplayName": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [{
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": "System User ID",
                        "LanguageCode": 1033
                    }]
                },
                "AttributeType": "String",
                "AttributeTypeName": {"Value": "StringType"},
                "MaxLength": 50,
                "RequiredLevel": {
                    "Value": "None",
                    "CanBeChanged": True,
                    "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
                }
            },
            {
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "SchemaName": "crf63_regularpay",
                "DisplayName": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [{
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": "Regular Pay",
                        "LanguageCode": 1033
                    }]
                },
                "AttributeType": "Decimal",
                "AttributeTypeName": {"Value": "DecimalType"},
                "Precision": 2,
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "RequiredLevel": {
                    "Value": "None",
                    "CanBeChanged": True,
                    "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
                }
            },
            {
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "SchemaName": "crf63_overtimepay",
                "DisplayName": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [{
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": "Overtime Pay",
                        "LanguageCode": 1033
                    }]
                },
                "AttributeType": "Decimal",
                "AttributeTypeName": {"Value": "DecimalType"},
                "Precision": 2,
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "RequiredLevel": {
                    "Value": "None",
                    "CanBeChanged": True,
                    "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
                }
            }
        ]
        
        print("\nCreating columns...")
        success_count = 0
        for i, column in enumerate(columns, 1):
            col_name = column['SchemaName']
            display_name = column['DisplayName']['LocalizedLabels'][0]['Label']
            print(f"  [{i}/{len(columns)}] Creating {col_name} ({display_name})...")
            
            if create_column(token, TABLE_SCHEMA_NAME, column):
                success_count += 1
                time.sleep(0.5)
            else:
                print(f"    ⚠️  Failed to create {col_name}")
        
        print(f"\n✓ Created {success_count}/{len(columns)} columns successfully")
        
        if success_count == len(columns):
            print("\n" + "="*80)
            print("✅ All columns added successfully!")
            print("="*80)
            print("\nNext steps:")
            print("  1. Test with: python olap_to_dataverse.py --query clock_in_out --length 1wk")
            return 0
        else:
            print("\n⚠️  Some columns failed to create")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
