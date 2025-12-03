#!/usr/bin/env python3
"""
Create crf63_saleschanneldaily table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: Sales Channel Daily
Schema Name: crf63_saleschanneldaily
Description: Daily sales data by store, source channel, source actor, and day part from OARS Franchise cube

All data stored as flat table columns:

Columns (12 total):
  1. crf63_storenumber - Store Number (string)
  2. crf63_calendardate - Calendar Date (date)
  3. crf63_sourceactor - Source Actor (string) - Android, iOS, Desktop Web, DoorDash, etc.
  4. crf63_sourcechannel - Source Channel (string) - App, Web, Aggregator, Phone, Store, etc.
  5. crf63_daypart - Day Part (string) - Lunch, Dinner, Afternoon, Evening
  6. crf63_tynetsalesusd - TY Net Sales USD (decimal)
  7. crf63_tyorders - TY Orders (integer)
  8. crf63_discountsusd - Discounts USD (decimal)
  9. crf63_lynetsalesusd - LY Net Sales USD (decimal)
  10. crf63_lyorders - LY Orders (integer)
  11. crf63_businesskey - Business Key (string) - {Store}_{YYYYMMDD}_{Actor}_{Channel}_{DayPart}
  12. crf63_lastrefreshed - Last Refreshed (datetime)

MDX Query (uses MyView 81 for 2 weeks):
SELECT {[Measures].[TY Net Sales USD],[Measures].[TY Orders],[Measures].[Discounts USD],
        [Measures].[LY Net Sales USD],[Measures].[LY Orders]} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(CrossJoin(CrossJoin(CrossJoin(
    Hierarchize({[Franchise].[Store Number Label].[Store Number Label].AllMembers}),
    Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers})),
    Hierarchize({[Source Channel].[Source Actor].[Source Actor].AllMembers})),
    Hierarchize({[Source Channel].[Source Channel].[Source Channel].AllMembers})),
    Hierarchize({[Day Part Dimension].[Day Part].[Day Part].AllMembers}))
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE ([MyView].[My View].[My View].&[81])
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
TABLE_SCHEMA_NAME = "crf63_saleschanneldaily"
TABLE_DISPLAY_NAME = "Sales Channel Daily"
TABLE_DESCRIPTION = "Daily sales data by store, source channel, source actor, and day part from OARS Franchise cube"


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
    """Create the crf63_saleschanneldaily table"""
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
                            "Label": "Primary name column combining store, date, and channel info",
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
                    "Label": "Sales Channel Daily",
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
    
    # Create the table
    print(f"\nCreating table {TABLE_SCHEMA_NAME}...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions"
    
    response = requests.post(url, headers=headers, json=table_definition)
    
    if response.status_code in [200, 201, 204]:
        print("✓ Table created successfully!")
        return True
    else:
        print(f"✗ Failed to create table: {response.status_code}")
        print(f"Response: {response.text}")
        return False


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


def create_columns(token):
    """Create all columns for the table"""
    print("\nCreating columns...")
    
    entity_logical_name = TABLE_SCHEMA_NAME
    columns = []
    
    # Helper function to create string column
    def string_col(schema_name, display_name, max_length=100, required=False):
        req_value = "ApplicationRequired" if required else "None"
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": display_name, "LanguageCode": 1033}]
            },
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "MaxLength": max_length,
            "RequiredLevel": {"Value": req_value, "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
        }
    
    # Helper function to create decimal column
    def decimal_col(schema_name, display_name, precision=2):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": display_name, "LanguageCode": 1033}]
            },
            "AttributeType": "Decimal",
            "AttributeTypeName": {"Value": "DecimalType"},
            "Precision": precision,
            "MinValue": -100000000000.0,
            "MaxValue": 100000000000.0,
            "RequiredLevel": {"Value": "None", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
        }
    
    # Helper function to create integer column
    def integer_col(schema_name, display_name):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": display_name, "LanguageCode": 1033}]
            },
            "AttributeType": "Integer",
            "AttributeTypeName": {"Value": "IntegerType"},
            "MinValue": -2147483648,
            "MaxValue": 2147483647,
            "RequiredLevel": {"Value": "None", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
        }
    
    # Helper function to create datetime column
    def datetime_col(schema_name, display_name, date_only=True, required=False):
        req_value = "ApplicationRequired" if required else "None"
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": display_name, "LanguageCode": 1033}]
            },
            "AttributeType": "DateTime",
            "AttributeTypeName": {"Value": "DateTimeType"},
            "Format": "DateOnly" if date_only else "DateAndTime",
            "RequiredLevel": {"Value": req_value, "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
        }
    
    # ============================================
    # Dimension Columns (5)
    # ============================================
    columns.append(string_col("crf63_storenumber", "Store Number", 20))
    columns.append(datetime_col("crf63_calendardate", "Calendar Date", date_only=True, required=True))
    columns.append(string_col("crf63_sourceactor", "Source Actor", 100))  # Android, iOS, Desktop Web, DoorDash, etc.
    columns.append(string_col("crf63_sourcechannel", "Source Channel", 100))  # App, Web, Aggregator, Phone, Store, etc.
    columns.append(string_col("crf63_daypart", "Day Part", 50))  # Lunch, Dinner, Afternoon, Evening
    
    # ============================================
    # Measure Columns (5)
    # ============================================
    columns.append(decimal_col("crf63_tynetsalesusd", "TY Net Sales USD"))
    columns.append(integer_col("crf63_tyorders", "TY Orders"))
    columns.append(decimal_col("crf63_discountsusd", "Discounts USD"))
    columns.append(decimal_col("crf63_lynetsalesusd", "LY Net Sales USD"))
    columns.append(integer_col("crf63_lyorders", "LY Orders"))
    
    # ============================================
    # System Columns (2)
    # ============================================
    # Business Key: {Store}_{YYYYMMDD}_{Actor}_{Channel}_{DayPart}
    # Example: 125_20250209_Android_App_Dinner
    columns.append(string_col("crf63_businesskey", "Business Key", 250))
    columns.append(datetime_col("crf63_lastrefreshed", "Last Refreshed", date_only=False))
    
    # Create each column
    success_count = 0
    for i, column in enumerate(columns, 1):
        col_name = column['SchemaName']
        display_name = column['DisplayName']['LocalizedLabels'][0]['Label']
        print(f"  [{i}/{len(columns)}] Creating {col_name} ({display_name})...")
        
        if create_column(token, entity_logical_name, column):
            success_count += 1
        else:
            print(f"    ⚠️  Failed to create {col_name}")
    
    print(f"\n✓ Created {success_count}/{len(columns)} columns successfully")
    return success_count == len(columns)


def create_alternate_key(token):
    """Create alternate key on business key column for faster upsert operations."""
    print("\n🔑 Creating alternate key on business key column...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Get table metadata
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to get table metadata: {response.status_code}")
        return False
    
    table_data = response.json()
    metadata_id = table_data.get("MetadataId")
    
    # Define alternate key
    key_definition = {
        "SchemaName": "crf63_saleschanneldaily_businesskey_key",
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
        return True
    else:
        print(f"⚠️  Could not create alternate key: {response.status_code}")
        print(f"   Response: {response.text}")
        return False


def verify_table(token):
    """Verify the table was created"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }
    
    print("\nVerifying table creation...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')?$select=SchemaName,LogicalName,DisplayName&$expand=Attributes($select=SchemaName,LogicalName,DisplayName)"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Table found: {data['SchemaName']}")
        print(f"  Display Name: {data['DisplayName']['LocalizedLabels'][0]['Label']}")
        print(f"  Total Columns: {len(data['Attributes'])}")
        return True
    else:
        print(f"✗ Table not found: {response.status_code}")
        return False


def main():
    """Main execution"""
    print("=" * 70)
    print("Dataverse Table Creation Script")
    print(f"Table: {TABLE_SCHEMA_NAME} ({TABLE_DISPLAY_NAME})")
    print("=" * 70)
    print()
    print("Table Structure (Flat Table - All Columns):")
    print("  Dimension Columns:")
    print("    1. crf63_storenumber    - Store Number")
    print("    2. crf63_calendardate   - Calendar Date")
    print("    3. crf63_sourceactor    - Source Actor (Android, iOS, etc.)")
    print("    4. crf63_sourcechannel  - Source Channel (App, Web, etc.)")
    print("    5. crf63_daypart        - Day Part (Lunch, Dinner, etc.)")
    print("  Measure Columns:")
    print("    6. crf63_tynetsalesusd  - TY Net Sales USD")
    print("    7. crf63_tyorders       - TY Orders")
    print("    8. crf63_discountsusd   - Discounts USD")
    print("    9. crf63_lynetsalesusd  - LY Net Sales USD")
    print("   10. crf63_lyorders       - LY Orders")
    print("  System Columns:")
    print("   11. crf63_businesskey    - Business Key")
    print("   12. crf63_lastrefreshed  - Last Refreshed")
    print()
    
    try:
        # Get access token using interactive auth
        token = get_access_token()
        
        # Check if table exists first
        print("\nChecking if table already exists...")
        if verify_table(token):
            print("\n✓ Table already exists!")
            print("\nWould you like to add columns anyway? (y/n): ", end="")
            response = input()
            if response.lower() != 'y':
                print("Exiting...")
                return 0
        else:
            # Create table
            if not create_table(token):
                print("\n✗ Table creation failed. Exiting.")
                sys.exit(1)
            
            # Wait a moment for table to be ready
            print("\nWaiting for table to be ready...")
            time.sleep(3)
        
        # Create columns
        if not create_columns(token):
            print("\n⚠️  Some columns failed to create")
        
        # Ask about creating alternate key
        print("\nCreate alternate key for faster upsert operations? (y/n): ", end="")
        response = input()
        if response.lower() == 'y':
            create_alternate_key(token)
        
        # Verify
        print("\n" + "=" * 70)
        print("Final Verification:")
        print("=" * 70)
        verify_table(token)
        
        print("\n" + "=" * 70)
        print("✓ Table creation complete!")
        print("=" * 70)
        print("\nBusiness Key Format:")
        print("  {StoreNumber}_{YYYYMMDD}_{SourceActor}_{SourceChannel}_{DayPart}")
        print("  Example: 125_20250209_Android_App_Dinner")
        print()
        print("Next steps:")
        print("1. Verify table in Power Apps: https://make.powerapps.com")
        print("2. Add OLAP query to olap_to_dataverse.py")
        print("3. Run sync: python olap_to_dataverse.py --query-type sales_channel_daily")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
