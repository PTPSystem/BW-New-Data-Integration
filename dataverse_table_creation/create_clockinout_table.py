#!/usr/bin/env python3
"""
Create crf63_bw_clockinout table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: BW Clock In Out
Schema Name: crf63_bw_clockinout
Description: Employee clock in/out data from VBO cube

All data stored as flat table columns:

Columns (14 total):
  1. crf63_storenumber - Store Number (string)
  2. crf63_calendardate - Calendar Date (date) - The date the shift is counted towards
  3. crf63_employeename - Employee Name (string)
  4. crf63_clockintime - Clock In Time (datetime)
  5. crf63_clockouttime - Clock Out Time (datetime)
  6. crf63_regularhours - Regular Hours (decimal)
  7. crf63_overtimehours - Overtime Hours (decimal)
  8. crf63_totalhours - Total Hours (decimal)
  9. crf63_totalpay - Total Pay (decimal)
  10. crf63_businesskey - Business Key (string) - {Store}_{YYYYMMDD}_{EmployeeName}
  11. crf63_name - Name (string) - Primary name field
  12. crf63_datasource - Data Source (string)
  13. crf63_lastrefreshed - Last Refreshed (datetime)

MDX Query (uses VBO cube with Last 7 Days filter):
SELECT {
    [Measures].[Actual Clock In Ts],
    [Measures].[Actual Clock Out Ts],
    [Measures].[m_reg_hours_worked_sum],
    [Measures].[m_ovt_hours_worked_sum],
    [Measures].[m_total_hours_worked_sum],
    [Measures].[m_total_pay_usd_sum]
} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS, 
NON EMPTY CrossJoin(CrossJoin(
    Hierarchize({DrilldownLevel({[Stores].[Store Number].[All]},,,INCLUDE_CALC_MEMBERS)}), 
    Hierarchize({DrilldownLevel({[Calendar].[Calendar Date].[All]},,,INCLUDE_CALC_MEMBERS)})), 
    Hierarchize({DrilldownLevel({[Employee Name].[Employee_Name Hierarchy].[All]},,,INCLUDE_CALC_MEMBERS)})) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [VBO] 
WHERE ([MyView].[My View].[My View].&[Last 7 Days])
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS

Note: Clock out times may be after midnight but are counted towards the Calendar Date of clock in.
Example: Clock in 6/9/2025 17:43, Clock out 6/10/2025 2:25 → counts as 6/9/2025
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
TABLE_SCHEMA_NAME = "crf63_bw_clockinout"
TABLE_DISPLAY_NAME = "BW Clock In Out"
TABLE_DESCRIPTION = "Employee clock in/out data from VBO cube with hours worked and pay information"


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
    """Create the crf63_bw_clockinout table"""
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
                "MaxLength": 300,
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
                            "Label": "Primary name column combining store, date, and employee",
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
                    "Label": "BW Clock In Out",
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
    # Dimension Columns (3)
    # ============================================
    columns.append(string_col("crf63_storenumber", "Store Number", 20))
    columns.append(datetime_col("crf63_calendardate", "Calendar Date", date_only=True, required=True))
    columns.append(string_col("crf63_employeename", "Employee Name", 200))
    
    # ============================================
    # Time Columns (2)
    # ============================================
    columns.append(datetime_col("crf63_clockintime", "Clock In Time", date_only=False))
    columns.append(datetime_col("crf63_clockouttime", "Clock Out Time", date_only=False))
    
    # ============================================
    # Measure Columns (4)
    # ============================================
    columns.append(decimal_col("crf63_regularhours", "Regular Hours", precision=2))
    columns.append(decimal_col("crf63_overtimehours", "Overtime Hours", precision=2))
    columns.append(decimal_col("crf63_totalhours", "Total Hours", precision=2))
    columns.append(decimal_col("crf63_totalpay", "Total Pay", precision=2))
    
    # ============================================
    # System Columns (3)
    # ============================================
    # Business Key: {Store}_{YYYYMMDD}_{EmployeeName}
    # Example: 1334_20250609_IMMANUEL CAMPBELL
    columns.append(string_col("crf63_businesskey", "Business Key", 300))
    columns.append(string_col("crf63_datasource", "Data Source", 100))
    columns.append(datetime_col("crf63_lastrefreshed", "Last Refreshed", date_only=False))
    
    # Create each column
    success_count = 0
    for i, column in enumerate(columns, 1):
        col_name = column['SchemaName']
        display_name = column['DisplayName']['LocalizedLabels'][0]['Label']
        print(f"  [{i}/{len(columns)}] Creating {col_name} ({display_name})...")
        
        if create_column(token, entity_logical_name, column):
            success_count += 1
            time.sleep(0.5)  # Small delay between column creations
        else:
            print(f"    ⚠️  Failed to create {col_name}")
    
    print(f"\n✓ Created {success_count}/{len(columns)} columns successfully")
    return success_count == len(columns)


def create_alternate_key(token):
    """Create alternate key on business key column for faster upsert operations."""
    print("\n🔑 Creating alternate key on business key column...")
    print("   This will take a few minutes as Dataverse validates data uniqueness...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Get table metadata to find the business key attribute
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')?$select=MetadataId&$expand=Attributes($select=LogicalName,MetadataId)"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to get table metadata: {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    table_data = response.json()
    table_metadata_id = table_data.get('MetadataId')
    
    # Find the business key attribute metadata ID
    business_key_metadata_id = None
    for attr in table_data.get('Attributes', []):
        if attr.get('LogicalName') == 'crf63_businesskey':
            business_key_metadata_id = attr.get('MetadataId')
            break
    
    if not business_key_metadata_id:
        print("❌ Could not find crf63_businesskey attribute")
        return False
    
    # Create the alternate key
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
        "KeyAttributes": [business_key_metadata_id],
        "EntityLogicalName": TABLE_SCHEMA_NAME
    }
    
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({table_metadata_id})/Keys"
    response = requests.post(url, headers=headers, json=alternate_key_definition)
    
    if response.status_code in [200, 201, 204]:
        print("✓ Alternate key creation initiated")
        print("  Note: Key activation happens asynchronously. Check status in ~5 minutes.")
        return True
    else:
        print(f"❌ Failed to create alternate key: {response.status_code}")
        print(f"Response: {response.text}")
        return False


def main():
    """Main execution"""
    print("="*80)
    print("BW Clock In Out Table Creation")
    print("="*80)
    print(f"\nTable: {TABLE_SCHEMA_NAME}")
    print(f"Display Name: {TABLE_DISPLAY_NAME}")
    print(f"Environment: {DATAVERSE_ENVIRONMENT}")
    print("\nThis script will:")
    print("  1. Create the table structure")
    print("  2. Create all columns (dimensions, times, measures, system)")
    print("  3. Create alternate key on business key column")
    print("\n" + "="*80)
    
    try:
        # Get authentication token
        token = get_access_token()
        
        # Create table
        if not create_table(token):
            print("\n❌ Table creation failed. Exiting.")
            return 1
        
        # Wait a bit for table to be ready
        print("\nWaiting 5 seconds for table to be ready...")
        time.sleep(5)
        
        # Create columns
        if not create_columns(token):
            print("\n⚠️  Some columns failed to create.")
            response = input("Continue with alternate key creation? (y/n): ")
            if response.lower() != 'y':
                return 1
        
        # Wait for columns to be ready
        print("\nWaiting 3 seconds for columns to be ready...")
        time.sleep(3)
        
        # Create alternate key
        if not create_alternate_key(token):
            print("\n⚠️  Alternate key creation failed.")
            print("   You can manually create it later using add_business_key_column.py")
        
        print("\n" + "="*80)
        print("✅ Table creation complete!")
        print("="*80)
        print(f"\nTable: {TABLE_SCHEMA_NAME}")
        print("Next steps:")
        print("  1. Wait ~5 minutes for alternate key to activate")
        print("  2. Test with: python olap_to_dataverse.py --query clock_in_out --length 1wk")
        print("  3. Monitor in Dataverse: Advanced Settings > Customizations > Customize the System")
        
        return 0
        
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
