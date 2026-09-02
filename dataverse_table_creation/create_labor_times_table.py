#!/usr/bin/env python3
"""
Create crf63_labortime table in Dataverse using Web API.
Uses interactive user authentication (not app registration).

Table: BW Labor Time
Schema Name: crf63_labortime
EntitySet: crf63_labortimes
Description: Employee labor hours by store, date, and day part from files.papajohns.com

Columns:
  1. crf63_storenumber - Store Number (string)
  2. crf63_employeeid - Employee ID (string)
  3. crf63_employeename - Employee Name (string)
  4. crf63_daypart - Day Part (string)
  5. crf63_totalhours - Total Hours (decimal)
  6. crf63_workdate - Work Date (date)
  7. crf63_businesskey - Business Key (string) - {Store}_{EmployeeID}_{YYYYMMDD}_{DayPart}
  8. crf63_name - Name (string) - Primary name field
  9. crf63_datasource - Data Source (string)
  10. crf63_lastrefreshed - Last Refreshed (datetime)
"""

import sys
import time

import requests
from msal import PublicClientApplication

DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

TABLE_SCHEMA_NAME = "crf63_labortime"
TABLE_DISPLAY_NAME = "BW Labor Time"
TABLE_DESCRIPTION = "Employee labor hours by store, date, and day part from Papa John's labor files"


def get_access_token():
    app = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)
    accounts = app.get_accounts()
    if accounts:
        print(f"Found cached account: {accounts[0]['username']}")
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result:
            return result["access_token"]

    print("\nAuthentication required...")
    print("A browser window will open for you to sign in...")
    result = app.acquire_token_interactive(scopes=SCOPES, prompt="select_account")
    if "access_token" in result:
        print(f"✓ Authenticated as: {result.get('id_token_claims', {}).get('preferred_username', 'Unknown')}")
        return result["access_token"]
    raise Exception(f"Authentication failed: {result.get('error_description', result)}")


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }


def create_table(token):
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
                    "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                },
                "MaxLength": 300,
                "FormatName": {"Value": "Text"},
                "DisplayName": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [
                        {
                            "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                            "Label": "Name",
                            "LanguageCode": 1033,
                        }
                    ],
                },
            }
        ],
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": TABLE_DISPLAY_NAME,
                    "LanguageCode": 1033,
                }
            ],
        },
        "DisplayCollectionName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "BW Labor Times",
                    "LanguageCode": 1033,
                }
            ],
        },
        "Description": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": TABLE_DESCRIPTION,
                    "LanguageCode": 1033,
                }
            ],
        },
        "SchemaName": TABLE_SCHEMA_NAME,
        "HasActivities": False,
        "HasNotes": False,
        "IsActivity": False,
        "OwnershipType": "UserOwned",
    }

    print(f"\nCreating table {TABLE_SCHEMA_NAME}...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions"
    response = requests.post(url, headers=_headers(token), json=table_definition)
    if response.status_code in [200, 201, 204]:
        print("✓ Table created successfully!")
        return True
    print(f"✗ Failed to create table: {response.status_code}")
    print(f"Response: {response.text}")
    return False


def create_column(token, entity_logical_name, column_definition):
    url = (
        f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/"
        f"EntityDefinitions(LogicalName='{entity_logical_name}')/Attributes"
    )
    response = requests.post(url, headers=_headers(token), json=column_definition)
    if response.status_code in [200, 201, 204]:
        return True
    print(f"✗ Failed to create column {column_definition['SchemaName']}: {response.status_code}")
    print(f"Response: {response.text}")
    return False


def create_columns(token):
    print("\nCreating columns...")
    columns = []

    def string_col(schema_name, display_name, max_length=100, required=False):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": display_name,
                        "LanguageCode": 1033,
                    }
                ],
            },
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "MaxLength": max_length,
            "RequiredLevel": {
                "Value": "ApplicationRequired" if required else "None",
                "CanBeChanged": True,
                "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
            },
        }

    def decimal_col(schema_name, display_name, precision=2):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": display_name,
                        "LanguageCode": 1033,
                    }
                ],
            },
            "AttributeType": "Decimal",
            "AttributeTypeName": {"Value": "DecimalType"},
            "Precision": precision,
            "MinValue": -100000000000.0,
            "MaxValue": 100000000000.0,
            "RequiredLevel": {
                "Value": "None",
                "CanBeChanged": True,
                "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
            },
        }

    def datetime_col(schema_name, display_name, date_only=True, required=False):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": display_name,
                        "LanguageCode": 1033,
                    }
                ],
            },
            "AttributeType": "DateTime",
            "AttributeTypeName": {"Value": "DateTimeType"},
            "Format": "DateOnly" if date_only else "DateAndTime",
            "RequiredLevel": {
                "Value": "ApplicationRequired" if required else "None",
                "CanBeChanged": True,
                "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
            },
        }

    columns.append(string_col("crf63_storenumber", "Store Number", 20))
    columns.append(string_col("crf63_employeeid", "Employee ID", 50))
    columns.append(string_col("crf63_employeename", "Employee Name", 200))
    columns.append(string_col("crf63_daypart", "Day Part", 50))
    columns.append(decimal_col("crf63_totalhours", "Total Hours", precision=2))
    columns.append(datetime_col("crf63_workdate", "Work Date", date_only=True, required=True))
    columns.append(string_col("crf63_businesskey", "Business Key", 300))
    columns.append(string_col("crf63_datasource", "Data Source", 100))
    columns.append(datetime_col("crf63_lastrefreshed", "Last Refreshed", date_only=False))

    success_count = 0
    for i, column in enumerate(columns, 1):
        col_name = column["SchemaName"]
        display_name = column["DisplayName"]["LocalizedLabels"][0]["Label"]
        print(f"  [{i}/{len(columns)}] Creating {col_name} ({display_name})...")
        if create_column(token, TABLE_SCHEMA_NAME, column):
            success_count += 1
            time.sleep(0.5)
        else:
            print(f"    ⚠️  Failed to create {col_name}")

    print(f"\n✓ Created {success_count}/{len(columns)} columns successfully")
    return success_count == len(columns)


def create_alternate_key(token):
    print("\n🔑 Creating alternate key on business key column...")
    url = (
        f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/"
        f"EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')"
        f"?$select=MetadataId&$expand=Attributes($select=LogicalName,MetadataId)"
    )
    response = requests.get(url, headers=_headers(token))
    if response.status_code != 200:
        print(f"❌ Failed to get table metadata: {response.status_code}")
        print(f"Response: {response.text}")
        return False

    table_data = response.json()
    table_metadata_id = table_data.get("MetadataId")
    business_key_metadata_id = None
    for attr in table_data.get("Attributes", []):
        if attr.get("LogicalName") == "crf63_businesskey":
            business_key_metadata_id = attr.get("MetadataId")
            break
    if not business_key_metadata_id:
        print("❌ Could not find crf63_businesskey attribute")
        return False

    alternate_key_definition = {
        "SchemaName": "crf63_businesskey_key",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Business Key",
                    "LanguageCode": 1033,
                }
            ],
        },
        "KeyAttributes": [business_key_metadata_id],
        "EntityLogicalName": TABLE_SCHEMA_NAME,
    }
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({table_metadata_id})/Keys"
    response = requests.post(url, headers=_headers(token), json=alternate_key_definition)
    if response.status_code in [200, 201, 204]:
        print("✓ Alternate key creation initiated")
        print("  Note: Key activation happens asynchronously. Check status in ~5 minutes.")
        return True
    print(f"❌ Failed to create alternate key: {response.status_code}")
    print(f"Response: {response.text}")
    return False


def main():
    print("=" * 80)
    print("BW Labor Time Table Creation")
    print("=" * 80)
    print(f"\nTable: {TABLE_SCHEMA_NAME}")
    print(f"Display Name: {TABLE_DISPLAY_NAME}")
    print(f"Environment: {DATAVERSE_ENVIRONMENT}")
    try:
        token = get_access_token()
        if not create_table(token):
            print("\n❌ Table creation failed. Exiting.")
            return 1
        print("\nWaiting 5 seconds for table to be ready...")
        time.sleep(5)
        if not create_columns(token):
            print("\n⚠️  Some columns failed to create.")
        print("\nWaiting 3 seconds for columns to be ready...")
        time.sleep(3)
        if not create_alternate_key(token):
            print("\n⚠️  Alternate key creation failed.")
        print("\n✅ Table creation complete!")
        print("Next steps:")
        print("  1. Wait ~5 minutes for the alternate key to activate")
        print("  2. Run: python labor_processing.py --date YYYYMMDD")
        return 0
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        return 1
    except Exception as exc:
        print(f"\n❌ Error: {exc}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
