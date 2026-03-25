#!/usr/bin/env python3
"""
Create crf63_bw_labor_forecast_records table in Dataverse using Web API
Uses interactive user authentication (not app registration)

Table: BW Labor Forecast Record
Schema Name: crf63_bw_labor_forecast_records
Description: Summary audit record written each time a labor forecast is saved.
             Tracks the person, store, week, total sales, labor costs, DDD costs,
             and total labor % = (labor_costs + ddd_costs) / total_sales * 100.

Columns (13 total):
  1.  crf63_name               - Name (string, PK label)  →  {store}_{fiscalWeek}
  2.  crf63_store               - Store Number (string)
  3.  crf63_fyw                 - Fiscal Year Week label (string)  e.g. Y2026W08
  4.  crf63_fiscalyear          - Fiscal Year (integer)
  5.  crf63_fiscalweek          - Fiscal Week Number (integer)
  6.  crf63_saved_on            - Date/Time Saved (datetime)
  7.  crf63_saved_by_email      - Saved By Email (string)
  8.  crf63_saved_by_name       - Saved By Name (string)
  9.  crf63_total_sales         - Total Forecasted Sales (decimal)
  10. crf63_labor_costs         - Total Labor Wages (decimal)
  11. crf63_ddd_costs           - Average Historical DDD / Dispatch Costs (decimal)
  12. crf63_total_labor_percent - (Labor + DDD) / Sales × 100 (decimal)
  13. crf63_record_key          - Alternate key field: {store}_{fiscalWeek} (string, unique)

One record per store + fiscal week; upserted on every save click.
"""

import requests
import json
from msal import PublicClientApplication
import sys
import time

# ── Dataverse Configuration ────────────────────────────────────────────────────
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID             = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID             = "51f81489-12ee-4a9e-aaae-a2591f45987d"   # Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES    = [f"{DATAVERSE_ENVIRONMENT}/.default"]

# ── Table Configuration ────────────────────────────────────────────────────────
TABLE_SCHEMA_NAME       = "crf63_bw_labor_forecast_records"
TABLE_DISPLAY_NAME      = "BW Labor Forecast Record"
TABLE_DISPLAY_PLURAL    = "BW Labor Forecast Records"
TABLE_DESCRIPTION       = (
    "Audit record written each time a labor forecast is saved. "
    "Tracks person, store, fiscal week, total sales, labor costs, DDD costs, "
    "and total labor % = (labor + DDD) / sales."
)


# ── Auth ───────────────────────────────────────────────────────────────────────
def get_access_token():
    """Get access token using interactive browser flow."""
    app = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)

    accounts = app.get_accounts()
    if accounts:
        print(f"Found cached account: {accounts[0]['username']}")
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    print("\nAuthentication required...")
    print("A browser window will open for you to sign in...")
    result = app.acquire_token_interactive(scopes=SCOPES, prompt="select_account")

    if "access_token" in result:
        username = result.get("id_token_claims", {}).get("preferred_username", "Unknown")
        print(f"✓ Authenticated as: {username}")
        return result["access_token"]
    else:
        raise Exception(f"Authentication failed: {result.get('error_description', result)}")


# ── Table creation ─────────────────────────────────────────────────────────────
def create_table(token):
    """Create the crf63_bw_labor_forecast_records table."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }

    def label(text):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": text,
                    "LanguageCode": 1033,
                }
            ],
        }

    table_definition = {
        "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
        "SchemaName": TABLE_SCHEMA_NAME,
        "DisplayName": label(TABLE_DISPLAY_NAME),
        "DisplayCollectionName": label(TABLE_DISPLAY_PLURAL),
        "Description": label(TABLE_DESCRIPTION),
        "HasActivities": False,
        "HasNotes": False,
        "IsActivity": False,
        "OwnershipType": "UserOwned",
        # Primary name attribute (required by Dataverse)
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
                "MaxLength": 200,
                "FormatName": {"Value": "Text"},
                "DisplayName": label("Name"),
                "Description": label(
                    "Primary name field: {store}_{fiscalWeek}  e.g.  000126_Y2026W08"
                ),
            }
        ],
    }

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


# ── Column helpers ─────────────────────────────────────────────────────────────
def _label(text):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.Label",
        "LocalizedLabels": [
            {
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": text,
                "LanguageCode": 1033,
            }
        ],
    }


def string_col(schema_name, display_name, max_length=100, required=False):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
        "SchemaName": schema_name,
        "DisplayName": _label(display_name),
        "AttributeType": "String",
        "AttributeTypeName": {"Value": "StringType"},
        "MaxLength": max_length,
        "RequiredLevel": {
            "Value": "ApplicationRequired" if required else "None",
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        },
    }


def integer_col(schema_name, display_name):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
        "SchemaName": schema_name,
        "DisplayName": _label(display_name),
        "AttributeType": "Integer",
        "AttributeTypeName": {"Value": "IntegerType"},
        "Format": "None",
        "MinValue": 0,
        "MaxValue": 9999,
        "RequiredLevel": {
            "Value": "None",
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        },
    }


def decimal_col(schema_name, display_name, precision=2,
                min_val=-1_000_000_000.0, max_val=1_000_000_000.0):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
        "SchemaName": schema_name,
        "DisplayName": _label(display_name),
        "AttributeType": "Decimal",
        "AttributeTypeName": {"Value": "DecimalType"},
        "Precision": precision,
        "MinValue": min_val,
        "MaxValue": max_val,
        "RequiredLevel": {
            "Value": "None",
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        },
    }


def datetime_col(schema_name, display_name, date_only=False):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
        "SchemaName": schema_name,
        "DisplayName": _label(display_name),
        "AttributeType": "DateTime",
        "AttributeTypeName": {"Value": "DateTimeType"},
        "Format": "DateOnly" if date_only else "DateAndTime",
        "RequiredLevel": {
            "Value": "None",
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        },
    }


# ── Column creation ────────────────────────────────────────────────────────────
def create_column(token, entity_logical_name, column_definition):
    """POST a single column definition to the entity."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }
    url = (
        f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/"
        f"EntityDefinitions(LogicalName='{entity_logical_name}')/Attributes"
    )
    response = requests.post(url, headers=headers, json=column_definition)
    if response.status_code in [200, 201, 204]:
        return True
    print(
        f"  ✗ Failed to create {column_definition['SchemaName']}: "
        f"{response.status_code}  {response.text}"
    )
    return False


def create_columns(token):
    """Create all columns for BW Labor Forecast Record."""
    print("\nCreating columns...")
    columns = [
        # ── Identifiers ────────────────────────────────────────────────────
        string_col("crf63_store",           "Store Number",         max_length=20,  required=True),
        string_col("crf63_fyw",             "Fiscal Year Week",     max_length=20,  required=True),

        # ── Fiscal period numbers ──────────────────────────────────────────
        integer_col("crf63_fiscalyear",     "Fiscal Year"),
        integer_col("crf63_fiscalweek",     "Fiscal Week Number"),

        # ── Who and when ──────────────────────────────────────────────────-
        datetime_col("crf63_saved_on",      "Date Saved",           date_only=False),
        string_col("crf63_saved_by_email",  "Saved By Email",       max_length=255),
        string_col("crf63_saved_by_name",   "Saved By Name",        max_length=255),

        # ── Financial summary ──────────────────────────────────────────────
        decimal_col("crf63_total_sales",    "Total Forecasted Sales"),
        decimal_col("crf63_labor_costs",    "Labor Costs"),
        decimal_col("crf63_ddd_costs",      "DDD / Dispatch Costs"),
        decimal_col("crf63_total_labor_percent",
                    "Total Labor %",        precision=4,
                    min_val=0.0,            max_val=100.0),

        # ── Alternate key field ────────────────────────────────────────────
        # Value = {store}_{fiscalWeek}  e.g.  000126_Y2026W08
        string_col("crf63_record_key",      "Record Key",           max_length=100),
    ]

    success = 0
    for i, col in enumerate(columns, 1):
        display = col["DisplayName"]["LocalizedLabels"][0]["Label"]
        print(f"  [{i:02d}/{len(columns)}] {col['SchemaName']} ({display})...", end=" ")
        if create_column(token, TABLE_SCHEMA_NAME, col):
            print("✓")
            success += 1
        else:
            print("✗")
        time.sleep(0.5)

    print(f"\n  → {success}/{len(columns)} columns created successfully")
    return success == len(columns)


# ── Alternate key ──────────────────────────────────────────────────────────────
def create_alternate_key(token):
    """Create an alternate key on crf63_record_key for fast upsert."""
    print("\n🔑 Creating alternate key on crf63_record_key...")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }

    # Fetch table + attributes metadata
    url = (
        f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/"
        f"EntityDefinitions(LogicalName='{TABLE_SCHEMA_NAME}')"
        f"?$select=MetadataId&$expand=Attributes($select=LogicalName,MetadataId)"
    )
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"  ❌ Could not retrieve table metadata: {resp.status_code}  {resp.text}")
        return False

    data = resp.json()
    table_meta_id = data.get("MetadataId")
    rk_exists = any(
        a["LogicalName"] == "crf63_record_key"
        for a in data.get("Attributes", [])
    )

    if not rk_exists:
        print("  ❌ Could not find crf63_record_key attribute")
        return False

    alt_key_def = {
        "SchemaName": "crf63_record_key_key",
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": "Record Key",
                    "LanguageCode": 1033,
                }
            ],
        },
        # Use logical name string, NOT MetadataId GUID
        "KeyAttributes": ["crf63_record_key"],
        "EntityLogicalName": TABLE_SCHEMA_NAME,
    }

    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions({table_meta_id})/Keys"
    resp = requests.post(url, headers=headers, json=alt_key_def)

    if resp.status_code in [200, 201, 204]:
        print("  ✓ Alternate key creation initiated (activates asynchronously ~5 min).")
        return True
    else:
        print(f"  ❌ Failed to create alternate key: {resp.status_code}  {resp.text}")
        return False


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("BW Labor Forecast Record — Dataverse Table Creation")
    print("=" * 70)
    print(f"\nTable schema name : {TABLE_SCHEMA_NAME}")
    print(f"Display name      : {TABLE_DISPLAY_NAME}")
    print(f"Environment       : {DATAVERSE_ENVIRONMENT}")
    print("\nThis script will:")
    print("  1. Create the table")
    print("  2. Add 12 columns (identifiers, date/person, financials, key)")
    print("  3. Create an alternate key on crf63_record_key")
    print("\n" + "=" * 70)

    try:
        token = get_access_token()

        # Step 1 — table
        if not create_table(token):
            print("\n❌ Table creation failed. Exiting.")
            return 1
        print("\nWaiting 5 s for table to be ready...")
        time.sleep(5)

        # Step 2 — columns
        cols_ok = create_columns(token)
        if not cols_ok:
            print("\n⚠  Some columns failed to create.")
            ans = input("Continue with alternate key creation? (y/n): ")
            if ans.strip().lower() != "y":
                return 1

        print("\nWaiting 3 s for columns to be ready...")
        time.sleep(3)

        # Step 3 — alternate key
        create_alternate_key(token)

        print("\n" + "=" * 70)
        print("✅  Table creation complete!")
        print("=" * 70)
        print(f"\nTable  : {TABLE_SCHEMA_NAME}")
        print("Fields : crf63_store, crf63_fyw, crf63_fiscalyear, crf63_fiscalweek,")
        print("         crf63_saved_on, crf63_saved_by_email, crf63_saved_by_name,")
        print("         crf63_total_sales, crf63_labor_costs, crf63_ddd_costs,")
        print("         crf63_total_labor_percent, crf63_record_key")
        print("\nNext steps:")
        print("  1. Wait ~5 min for alternate key to activate")
        print("  2. Verify table in Power Apps: https://make.powerapps.com")
        print("  3. Click 'Save Labor Forecast' in the app to write the first record")
        return 0

    except KeyboardInterrupt:
        print("\n\n⚠  Operation cancelled by user")
        return 1
    except Exception as exc:
        print(f"\n❌ Error: {exc}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
