#!/usr/bin/env python3
"""
Create crf63_offers table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: Offers
Schema Name: crf63_offers
Description: Daily offers data by store and offer code from OARS Franchise cube

Columns:
  1. crf63_storenumber - Store Number (string)
  2. crf63_calendardate - Calendar Date (date)
  3. crf63_offercode - Offer Code (string)
  4. crf63_offerposdescription - Offer POS Description (string)
  5. crf63_redeemedcount - Redeemed Count (integer)
  6. crf63_discountamountusd - Discount Amount USD (decimal)
  7. crf63_grossmarginusd - Gross Margin USD (decimal)
  8. crf63_ordermixpercent - Order Mix % (decimal)
  9. crf63_salesmixusdpercent - Sales Mix USD % (decimal)
  10. crf63_netsalesusd - Net Sales USD (decimal)
  11. crf63_ordercount - Order Count (integer)
  12. crf63_targetfoodcostusd - Target Food Cost USD (decimal)
  13. crf63_businesskey - Business Key (string) - {Store}_{YYYYMMDD}_{OfferCode}
  14. crf63_lastrefreshed - Last Refreshed (datetime)

MDX Query:
SELECT {[Measures].[Redeemed Count],[Measures].[Discount Amount USD],[Measures].[Gross Margin USD],
        [Measures].[Order Mix %],[Measures].[Sales Mix USD %],[Measures].[Net Sales USD],
        [Measures].[Order Count],[Measures].[Target Food Cost USD]} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS, 
NON EMPTY CrossJoin(CrossJoin(CrossJoin(
    Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers}), 
    Hierarchize({[Stores].[Store Number].[Store Number].AllMembers})), 
    Hierarchize({[Offer Code].[Offer Code Hierarchy].[Offer Code Level].AllMembers})), 
    Hierarchize({[Offer Code].[Offer POS Description].[Offer POS Description].AllMembers})) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  
FROM [Offers] 
WHERE ([MyView].[My View].[My View].&[81],[13-4 Calendar].[Alternate Calendar Hierarchy].[All]) 
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
TABLE_SCHEMA_NAME = "crf63_offers"
TABLE_DISPLAY_NAME = "Offers"
TABLE_DESCRIPTION = "Daily offers data by store and offer code from OARS Franchise cube"


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
    """Create the crf63_offers table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # 1. Check if table exists
    print(f"Checking if table {TABLE_SCHEMA_NAME} exists...")
    check_url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Table {TABLE_SCHEMA_NAME} already exists.")
        return True
    
    # 2. Create table
    print(f"Creating table {TABLE_SCHEMA_NAME}...")
    create_url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions"
    
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
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [
                        {
                            "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                            "Label": "Name",
                            "LanguageCode": 1033
                        }
                    ]
                },
                "Description": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [
                        {
                            "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                            "Label": "Primary name column combining store, date, and offer info",
                            "LanguageCode": 1033
                        }
                    ]
                }
            }
        ],
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": TABLE_DISPLAY_NAME,
                    "LanguageCode": 1033
                }
            ]
        },
        "DisplayCollectionName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Offers",
                    "LanguageCode": 1033
                }
            ]
        },
        "Description": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": TABLE_DESCRIPTION,
                    "LanguageCode": 1033
                }
            ]
        },
        "SchemaName": TABLE_SCHEMA_NAME,
        "HasActivities": False,
        "HasNotes": False,
        "IsActivity": False,
        "OwnershipType": "UserOwned"
    }
    
    response = requests.post(create_url, headers=headers, json=table_definition)
    
    if response.status_code == 204:
        print("✓ Table created successfully")
        return True
    else:
        print(f"✗ Failed to create table: {response.text}")
        return False


def create_column(token, schema_name, display_name, data_type, format_type=None, max_length=None, required=False):
    """Create a column in the table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    print(f"Creating column {schema_name} ({display_name})...")
    
    # Check if column exists
    check_url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')/Attributes(LogicalName='{schema_name}')"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"  Column {schema_name} already exists.")
        return True
    
    # Create column
    create_url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')/Attributes"
    
    attribute_type = data_type
    
    attribute_def = {
        "SchemaName": schema_name,
        "DisplayName": {"LocalizedLabels": [{"Label": display_name, "LanguageCode": 1033}]},
        "RequiredLevel": {"Value": "ApplicationRequired" if required else "None", "CanBeChanged": True}
    }
    
    if data_type == "String":
        attribute_def["@odata.type"] = "Microsoft.Dynamics.CRM.StringAttributeMetadata"
        attribute_def["FormatName"] = {"Value": format_type if format_type else "Text"}
        attribute_def["MaxLength"] = max_length if max_length else 100
        
    elif data_type == "Integer":
        attribute_def["@odata.type"] = "Microsoft.Dynamics.CRM.IntegerAttributeMetadata"
        attribute_def["Format"] = format_type if format_type else "None"
        
    elif data_type == "Decimal":
        attribute_def["@odata.type"] = "Microsoft.Dynamics.CRM.DecimalAttributeMetadata"
        attribute_def["Precision"] = 2
        
    elif data_type == "DateTime":
        attribute_def["@odata.type"] = "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata"
        attribute_def["Format"] = format_type if format_type else "DateOnly"
        
    elif data_type == "Memo":
        attribute_def["@odata.type"] = "Microsoft.Dynamics.CRM.MemoAttributeMetadata"
        attribute_def["MaxLength"] = max_length if max_length else 2000
        
    response = requests.post(create_url, headers=headers, json=attribute_def)
    
    if response.status_code == 204:
        print(f"  ✓ Column {schema_name} created")
        return True
    else:
        print(f"  ✗ Failed to create column {schema_name}: {response.text}")
        return False


def create_key(token, key_name, key_columns):
    """Create an alternate key for the table"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    print(f"Creating key {key_name} on columns {key_columns}...")
    
    create_url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')/Keys"
    
    key_def = {
        "SchemaName": key_name,
        "DisplayName": {"LocalizedLabels": [{"Label": key_name, "LanguageCode": 1033}]},
        "KeyAttributes": key_columns
    }
    
    response = requests.post(create_url, headers=headers, json=key_def)
    
    if response.status_code == 204:
        print(f"  ✓ Key {key_name} created")
        return True
    elif response.status_code == 409: # Conflict, key might already exist
        print(f"  Key {key_name} might already exist (Conflict)")
        return True
    else:
        print(f"  ✗ Failed to create key {key_name}: {response.text}")
        return False


def main():
    try:
        token = get_access_token()
        
        if create_table(token):
            # Wait for table creation to propagate
            print("Waiting 10 seconds for table creation to propagate...")
            time.sleep(10)
            
            # Create Columns
            
            # Dimensions
            create_column(token, "crf63_storenumber", "Store Number", "String", max_length=50)
            create_column(token, "crf63_calendardate", "Calendar Date", "DateTime", format_type="DateOnly")
            create_column(token, "crf63_offercode", "Offer Code", "String", max_length=100)
            create_column(token, "crf63_offerposdescription", "Offer POS Description", "String", max_length=200)
            
            # Measures
            create_column(token, "crf63_redeemedcount", "Redeemed Count", "Integer")
            create_column(token, "crf63_discountamountusd", "Discount Amount USD", "Decimal")
            create_column(token, "crf63_grossmarginusd", "Gross Margin USD", "Decimal")
            create_column(token, "crf63_ordermixpercent", "Order Mix %", "Decimal")
            create_column(token, "crf63_salesmixusdpercent", "Sales Mix USD %", "Decimal")
            create_column(token, "crf63_netsalesusd", "Net Sales USD", "Decimal")
            create_column(token, "crf63_ordercount", "Order Count", "Integer")
            create_column(token, "crf63_targetfoodcostusd", "Target Food Cost USD", "Decimal")
            
            # Metadata
            create_column(token, "crf63_businesskey", "Business Key", "String", max_length=200, required=True)
            create_column(token, "crf63_lastrefreshed", "Last Refreshed", "DateTime", format_type="DateAndTime")
            
            # Create Key
            print("Waiting 5 seconds before creating key...")
            time.sleep(5)
            create_key(token, "crf63_offers_key", ["crf63_businesskey"])
            
            print("\n✓ Table setup complete!")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
