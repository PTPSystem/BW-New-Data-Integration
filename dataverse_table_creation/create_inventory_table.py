#!/usr/bin/env python3
"""
Create crf63_inventory table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: Inventory
Schema Name: crf63_inventory (pluralizes to crf63_inventories)
Description: Daily inventory quantities by store and item from OARS Franchise cube

All data stored as flat table columns:

Columns (7 total):
  1. crf63_itemnumber - Item Number (string)
  2. crf63_storenumber - Store Number (string)
  3. crf63_calendardate - Calendar Date (date)
  4. crf63_itemdescription - Item Description (string)
  5. crf63_qtyonhand - Qty On Hand (decimal)
  6. crf63_businesskey - Business Key (string) - {Store}_{YYYYMMDD}_{ItemNumber}
  7. crf63_lastrefreshed - Last Refreshed (datetime)

MDX Query:
SELECT NON EMPTY CrossJoin(CrossJoin(
    Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers}), 
    Hierarchize({[Franchise].[Store Number Label].[Store Number Label].AllMembers})), 
    Hierarchize({[Inventory - Description].[Item_Description].[Item_Description].AllMembers})) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS  
FROM [OARS Franchise] 
WHERE ([MyView].[My View].[My View].&[81],[Measures].[Qty On Hand]) 
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

import requests
import json
from msal import PublicClientApplication
import sys
import time

# Dataverse Configuration
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"

# Public client ID for interactive auth (no secret needed)
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

# Table Configuration
TABLE_SCHEMA_NAME = "crf63_inventory"  # Will pluralize to crf63_inventories
TABLE_DISPLAY_NAME = "Inventory"
TABLE_DESCRIPTION = "Daily inventory quantities by store and item from OARS Franchise cube"


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
    print("A browser window will open for you to sign in...")
    
    result = app.acquire_token_interactive(
        scopes=SCOPES,
        prompt="select_account"
    )
    
    if "access_token" in result:
        print(f"✓ Authenticated as: {result.get('id_token_claims', {}).get('preferred_username', 'Unknown')}")
        return result["access_token"]
    else:
        raise Exception(f"Authentication failed: {result.get('error_description', result)}")


def create_table(token):
    """Create the crf63_inventories table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }
    
    # Table definition with primary attribute
    table_definition = {
        "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
        "Attributes": [
            {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": "crf63_name",
                "IsPrimaryName": True,
                "RequiredLevel": {
                    "Value": "None",
                    "CanBeChanged": True,
                    "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
                },
                "MaxLength": 200,
                "FormatName": {
                    "Value": "Text"
                },
                "DisplayName": {
                    "LocalizedLabels": [{"Label": "Name", "LanguageCode": 1033}]
                },
                "Description": {
                    "LocalizedLabels": [{"Label": "Primary name field", "LanguageCode": 1033}]
                }
            }
        ],
        "SchemaName": TABLE_SCHEMA_NAME,
        "DisplayName": {
            "LocalizedLabels": [{"Label": TABLE_DISPLAY_NAME, "LanguageCode": 1033}]
        },
        "DisplayCollectionName": {
            "LocalizedLabels": [{"Label": TABLE_DISPLAY_NAME, "LanguageCode": 1033}]
        },
        "Description": {
            "LocalizedLabels": [{"Label": TABLE_DESCRIPTION, "LanguageCode": 1033}]
        },
        "OwnershipType": "UserOwned",
        "IsActivity": False,
        "HasNotes": False,
        "HasActivities": False
    }
    
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions"
    
    print(f"\n📋 Creating table: {TABLE_DISPLAY_NAME}")
    print(f"   Schema name: {TABLE_SCHEMA_NAME}")
    
    response = requests.post(url, headers=headers, json=table_definition)
    
    if response.status_code in [200, 204]:
        print(f"✓ Table created successfully")
        # Extract the entity ID from the response header
        entity_id = response.headers.get('OData-EntityId', '').split('(')[-1].split(')')[0]
        return entity_id
    else:
        print(f"✗ Failed to create table")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        sys.exit(1)


def create_column(token, column_def):
    """Create a column in the table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }
    
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')/Attributes"
    
    print(f"   Creating column: {column_def['SchemaName']}")
    
    response = requests.post(url, headers=headers, json=column_def)
    
    if response.status_code in [200, 204]:
        print(f"   ✓ Column created")
        return True
    else:
        print(f"   ✗ Failed to create column")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text}")
        return False


def main():
    """Main execution"""
    print("="*80)
    print("Dataverse Table Creation Script")
    print(f"Table: {TABLE_DISPLAY_NAME} ({TABLE_SCHEMA_NAME})")
    print("="*80)
    
    # Get authentication token
    token = get_access_token()
    
    # Create the table
    entity_id = create_table(token)
    
    # Wait a moment for table creation to complete
    print("\n⏳ Waiting for table creation to complete...")
    time.sleep(3)
    
    # Define columns to create
    columns = [
        {
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
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": "crf63_storenumber",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Store Number", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Store number from OARS", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "MaxLength": 100,
            "FormatName": {
                "Value": "Text"
            }
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": "crf63_calendardate",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Calendar Date", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Date from OARS cube", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "Format": "DateOnly"
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": "crf63_itemdescription",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Item Description", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Inventory item description", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "MaxLength": 500,
            "FormatName": {
                "Value": "Text"
            }
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": "crf63_qtyonhand",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Qty On Hand", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Quantity on hand", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "Precision": 2,
            "MinValue": -100000000000.0,
            "MaxValue": 100000000000.0
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": "crf63_businesskey",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Business Key", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Unique business key: {Store}_{YYYYMMDD}_{ItemNumber}", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "MaxLength": 600,
            "FormatName": {
                "Value": "Text"
            }
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": "crf63_lastrefreshed",
            "DisplayName": {
                "LocalizedLabels": [{"Label": "Last Refreshed", "LanguageCode": 1033}]
            },
            "Description": {
                "LocalizedLabels": [{"Label": "Timestamp of last data refresh from OARS", "LanguageCode": 1033}]
            },
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True
            },
            "Format": "DateAndTime"
        }
    ]
    
    # Create each column
    print(f"\n📊 Creating {len(columns)} columns...")
    success_count = 0
    for col in columns:
        if create_column(token, col):
            success_count += 1
        time.sleep(0.5)  # Small delay between column creations
    
    print(f"\n{'='*80}")
    print(f"✓ Table creation complete!")
    print(f"  Created {success_count}/{len(columns)} columns successfully")
    print(f"\nNext steps:")
    print(f"  1. Run add_business_key_column.py to create alternate key on crf63_businesskey")
    print(f"  2. Test with: python olap_to_dataverse.py --query inventory --length 1wk")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
