"""
Creates the crf63_bw_labor_forecast_submission Dataverse table.
This table tracks labor forecast submission logs (summary record per store/week save).
"""
import requests
import json
from msal import PublicClientApplication

# --- Config ---
DATAVERSE_URL = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"
API_VERSION = "v9.2"
PUBLISHER_PREFIX = "crf63"
TABLE_LOGICAL_NAME = f"{PUBLISHER_PREFIX}_bw_labor_forecast_submission"

def get_access_token():
    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    accounts = app.get_accounts()
    result = app.acquire_token_silent([f"{DATAVERSE_URL}/.default"], account=accounts[0]) if accounts else None
    if not result or "access_token" not in result:
        result = app.acquire_token_interactive(scopes=[f"{DATAVERSE_URL}/.default"], prompt="select_account")
    return result["access_token"]

def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }

def create_table(token):
    url = f"{DATAVERSE_URL}/api/data/{API_VERSION}/EntityDefinitions"
    def label(text):
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": text, "LanguageCode": 1033}]
        }

    payload = {
        "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
        "SchemaName": f"{PUBLISHER_PREFIX}_bw_labor_forecast_submission",
        "DisplayName": label("BW Labor Forecast Submission"),
        "DisplayCollectionName": label("BW Labor Forecast Submissions"),
        "Description": label("Summary log of each labor forecast save per store per fiscal week"),
        "HasActivities": False,
        "HasNotes": False,
        "IsActivity": False,
        "OwnershipType": "UserOwned",
        "Attributes": [
            {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": f"{PUBLISHER_PREFIX}_name",
                "IsPrimaryName": True,
                "RequiredLevel": {"Value": "None", "CanBeChanged": True, "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"},
                "MaxLength": 200,
                "FormatName": {"Value": "Text"},
                "DisplayName": label("Name"),
            }
        ]
    }
    resp = requests.post(url, headers=get_headers(token), data=json.dumps(payload))
    if resp.status_code in (200, 201, 204):
        print(f"Table '{TABLE_LOGICAL_NAME}' created successfully.")
    else:
        print(f"Failed to create table: {resp.status_code} - {resp.text[:500]}")
        raise Exception("Table creation failed")

def _label(text):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.Label",
        "LocalizedLabels": [{"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": text, "LanguageCode": 1033}]
    }

def create_columns(token):
    columns = [
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_store",
            "DisplayName": _label("Store"),
            "MaxLength": 50,
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_fyw",
            "DisplayName": _label("Fiscal Year Week"),
            "MaxLength": 20,
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_fiscalyear",
            "DisplayName": _label("Fiscal Year"),
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_fiscalweek",
            "DisplayName": _label("Fiscal Week"),
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_submitted_by_email",
            "DisplayName": _label("Submitted By Email"),
            "MaxLength": 200,
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_submitted_by_name",
            "DisplayName": _label("Submitted By Name"),
            "MaxLength": 200,
            "RequiredLevel": {"Value": "None"},
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_submitted_on",
            "DisplayName": _label("Submitted On"),
            "RequiredLevel": {"Value": "None"},
            "Format": "DateAndTime",
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_total_labor_hours",
            "DisplayName": _label("Total Labor Hours"),
            "RequiredLevel": {"Value": "None"},
            "Precision": 2,
            "MinValue": 0.0,
            "MaxValue": 99999.0,
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_total_labor_wages",
            "DisplayName": _label("Total Labor Wages"),
            "RequiredLevel": {"Value": "None"},
            "Precision": 2,
            "MinValue": 0.0,
            "MaxValue": 9999999.0,
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_total_forecasted_sales",
            "DisplayName": _label("Total Forecasted Sales"),
            "RequiredLevel": {"Value": "None"},
            "Precision": 2,
            "MinValue": 0.0,
            "MaxValue": 9999999.0,
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_labor_percent",
            "DisplayName": _label("Labor Percent"),
            "RequiredLevel": {"Value": "None"},
            "Precision": 2,
            "MinValue": 0.0,
            "MaxValue": 100.0,
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_avg_productivity",
            "DisplayName": _label("Avg Productivity"),
            "RequiredLevel": {"Value": "None"},
            "Precision": 2,
            "MinValue": 0.0,
            "MaxValue": 9999.0,
        },
        {
            "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
            "SchemaName": f"{PUBLISHER_PREFIX}_record_count",
            "DisplayName": _label("Record Count"),
            "RequiredLevel": {"Value": "None"},
        },
    ]

    url = f"{DATAVERSE_URL}/api/data/{API_VERSION}/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Attributes"
    for col in columns:
        schema_name = col["SchemaName"]
        resp = requests.post(url, headers=get_headers(token), data=json.dumps(col))
        if resp.status_code in (200, 201, 204):
            print(f"  Column '{schema_name}' created.")
        else:
            print(f"  Column '{schema_name}' FAILED: {resp.status_code} - {resp.text[:200]}")

def verify_table(token):
    url = f"{DATAVERSE_URL}/api/data/{API_VERSION}/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')?$select=LogicalName,EntitySetName,PrimaryIdAttribute"
    resp = requests.get(url, headers=get_headers(token))
    if resp.status_code == 200:
        d = resp.json()
        print(f"\nVerification:")
        print(f"  LogicalName:        {d['LogicalName']}")
        print(f"  EntitySetName:      {d['EntitySetName']}")
        print(f"  PrimaryIdAttribute: {d['PrimaryIdAttribute']}")
        return d['EntitySetName'], d['PrimaryIdAttribute']
    else:
        print(f"Verification failed: {resp.status_code}")
        return None, None

if __name__ == "__main__":
    print("Getting access token...")
    token = get_access_token()

    print(f"\nCreating table '{TABLE_LOGICAL_NAME}'...")
    create_table(token)

    print("\nAdding columns...")
    create_columns(token)

    verify_table(token)
    print("\nDone.")
