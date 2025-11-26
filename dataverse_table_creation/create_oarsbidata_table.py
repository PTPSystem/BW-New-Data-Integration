#!/usr/bin/env python3
"""
Create crf63_oarsbidata table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: OARS BI Data
Schema Name: crf63_oarsbidata
Total Columns: 36 (1 primary name + 2 keys + 33 measures + metadata)
"""

import requests
import json
from msal import PublicClientApplication
import sys

# Dataverse Configuration
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"

# Public client ID for interactive auth (no secret needed)
# This is a well-known client ID for PowerShell/CLI tools
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

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
    """Create the crf63_oarsbidata table"""
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
                "MaxLength": 100,
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
                            "Label": "The primary name column for the OARS BI Data table",
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
                    "Label": "OARS BI Data",
                    "LanguageCode": 1033
                }
            ]
        },
        "DisplayCollectionName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "OARS BI Data",
                    "LanguageCode": 1033
                }
            ]
        },
        "Description": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Table to store OLAP cube data from OARS Franchise and OARS Offers",
                    "LanguageCode": 1033
                }
            ]
        },
        "SchemaName": "crf63_oarsbidata",
        "HasActivities": False,
        "HasNotes": False,
        "IsActivity": False,
        "OwnershipType": "UserOwned"
    }
    
    # Create the table
    print("\nCreating table crf63_oarsbidata...")
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
    """Create all 35 columns"""
    print("\nCreating columns...")
    
    entity_logical_name = "crf63_oarsbidata"
    columns = []
    
    # Helper function to create string column
    def string_col(schema_name, display_name, max_length=100):
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
            "RequiredLevel": {"Value": "None", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
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
    def datetime_col(schema_name, display_name):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": display_name, "LanguageCode": 1033}]
            },
            "AttributeType": "DateTime",
            "AttributeTypeName": {"Value": "DateTimeType"},
            "Format": "DateOnly",
            "RequiredLevel": {"Value": "ApplicationRequired", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
        }
    
    # Key Fields (Required)
    columns.append(string_col("crf63_storenumber", "Store Number", 20))
    columns.append(datetime_col("crf63_calendardate", "Calendar Date"))
    columns.append(string_col("crf63_datasource", "Data Source", 50))
    
    # Sales Metrics (4)
    columns.append(decimal_col("crf63_tynetsales", "TY Net Sales"))
    columns.append(decimal_col("crf63_l2ycompsales", "L2Y Comp Sales"))
    columns.append(decimal_col("crf63_l3ycompsales", "L3Y Comp Sales"))
    columns.append(decimal_col("crf63_lycompsales", "LY Comp Sales"))
    
    # Cost Metrics (6)
    columns.append(decimal_col("crf63_targetfoodcost", "Target Food Cost"))
    columns.append(decimal_col("crf63_actualfoodcost", "Actual Food Cost"))
    columns.append(decimal_col("crf63_flmd", "FLMD"))
    columns.append(decimal_col("crf63_actuallabor", "Actual Labor"))
    columns.append(decimal_col("crf63_mileagecost", "Mileage Cost"))
    columns.append(decimal_col("crf63_discounts", "Discounts"))
    
    # Operations Metrics (6)
    columns.append(decimal_col("crf63_totalhours", "Total Hours"))
    columns.append(integer_col("crf63_storedays", "Store Days"))
    columns.append(decimal_col("crf63_maketime", "Make Time"))
    columns.append(decimal_col("crf63_racktime", "Rack Time"))
    columns.append(decimal_col("crf63_otdtime", "OTD Time"))
    columns.append(decimal_col("crf63_avgttdt", "Avg TTDT"))
    
    # Order Metrics (6)
    columns.append(integer_col("crf63_tyorders", "TY Orders"))
    columns.append(integer_col("crf63_lyorders", "LY Orders"))
    columns.append(integer_col("crf63_deliveries", "Deliveries"))
    columns.append(integer_col("crf63_bozocoroorders", "BOZOCORO Orders"))
    columns.append(integer_col("crf63_otdordercount", "OTD Order Count"))
    columns.append(integer_col("crf63_dispatchedorders", "Dispatched Orders"))
    
    # Financial Metrics (5)
    columns.append(decimal_col("crf63_targetprofit", "Target Profit"))
    columns.append(decimal_col("crf63_actualflm", "Actual FLM"))
    columns.append(decimal_col("crf63_flmdpc", "FLMDPC"))
    columns.append(decimal_col("crf63_commission", "Commission"))
    columns.append(decimal_col("crf63_cashovershort", "Cash Over/Short"))
    
    # Customer Satisfaction Metrics (6)
    columns.append(integer_col("crf63_osatsurveycount", "OSAT Survey Count"))
    columns.append(integer_col("crf63_osatsatisfied", "OSAT Satisfied"))
    columns.append(integer_col("crf63_accuracysurveycount", "Accuracy Survey Count"))
    columns.append(decimal_col("crf63_orderaccuracypct", "Order Accuracy %"))
    columns.append(integer_col("crf63_totalcalls", "Total Calls"))
    columns.append(integer_col("crf63_answeredcalls", "Answered Calls"))
    
    # Metadata
    columns.append({
        "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
        "SchemaName": "crf63_lastrefreshed",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": "Last Refreshed", "LanguageCode": 1033}]
        },
        "AttributeType": "DateTime",
        "AttributeTypeName": {"Value": "DateTimeType"},
        "Format": "DateAndTime",
        "RequiredLevel": {"Value": "None", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"}
    })
    
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
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='crf63_oarsbidata')?$select=SchemaName,LogicalName,DisplayName&$expand=Attributes($select=SchemaName,LogicalName,DisplayName)"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Table found: {data['SchemaName']}")
        print(f"  Display Name: {data['DisplayName']['LocalizedLabels'][0]['Label']}")
        print(f"  Total Columns: {len(data['Attributes'])}")
        return True
    else:
        print(f"✗ Failed to verify table: {response.status_code}")
        return False


def main():
    """Main execution"""
    print("=" * 70)
    print("Dataverse Table Creation Script")
    print("Table: crf63_oarsbidata (OARS BI Data)")
    print("=" * 70)
    
    try:
        # Get access token using interactive auth
        token = get_access_token()
        
        # Check if table exists first
        print("\nChecking if table already exists...")
        if verify_table(token):
            print("\n✓ Table already exists!")
            print("\nSkipping table creation. Proceeding to add columns...")
        else:
            # Create table
            if not create_table(token):
                print("\n✗ Table creation failed. Exiting.")
                sys.exit(1)
            
            # Wait a moment for table to be ready
            import time
            print("\nWaiting for table to be ready...")
            time.sleep(3)
        
        # Create columns
        if not create_columns(token):
            print("\n⚠️  Some columns failed to create")
        
        # Verify
        print("\n" + "=" * 70)
        print("Final Verification:")
        print("=" * 70)
        verify_table(token)
        
        print("\n" + "=" * 70)
        print("✓ Table creation complete!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Verify table in Power Apps: https://make.powerapps.com")
        print("2. Proceed to Step 2: OLAP to Dataverse Migration")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
