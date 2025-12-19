#!/usr/bin/env python3
"""
Add 29 new service columns to crf63_oarsbidata table in Dataverse
Uses interactive user authentication (not app registration)

New Columns: Service metrics from the "Services" MDX query
- Quality metrics (SMG, Taste)
- Delivery metrics (Singles, Doubles, Triples, Runs, TTDT)
- Additional operational metrics
"""

import requests
import json
from msal import PublicClientApplication
import sys

# Dataverse Configuration
DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

def get_access_token():
    """Get access token using interactive browser flow"""
    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY
    )
    
    accounts = app.get_accounts()
    if accounts:
        print(f"Found cached account: {accounts[0]['username']}")
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result:
            return result['access_token']
    
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


def add_service_columns(token):
    """Add 29 new service columns to crf63_oarsbidata"""
    print("\nAdding 29 new service columns...")
    
    entity_logical_name = "crf63_oarsbidata"
    columns = []
    
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
    
    # 29 New Service Columns (from your Services MDX query)
    
    # Additional operational metrics (note: some overlaps removed - maketime, racktime, otdtime already exist)
    # These 3 are already in the table, so we skip them:
    # - Make Time Minutes -> crf63_maketime (exists)
    # - Rack Time Minutes -> crf63_racktime (exists)
    # - Total OTD Time (Hours) -> crf63_otdtime (exists)
    
    # Additional delivery/order metrics (note: deliveries, bozocoroorders, otdordercount, dispatchedorders already exist)
    # These 4 are already in the table, so we skip them:
    # - Deliveries -> crf63_deliveries (exists)
    # - BOZOCORO Orders -> crf63_bozocoroorders (exists)
    # - OTD Order Count -> crf63_otdordercount (exists)
    # - TY Dispatched Delivery Orders -> crf63_dispatchedorders (exists)
    
    # Additional satisfaction metrics (note: some already exist)
    # These are already in the table:
    # - TY Total OSAT Survey Count -> crf63_osatsurveycount (exists)
    # - TY OSAT Satisfied Survey Count -> crf63_osatsatisfied (exists)
    # - Total Calls -> crf63_totalcalls (exists)
    # - Answered Calls -> crf63_answeredcalls (exists)
    # - TY Total Order Accuracy Survey Count -> crf63_accuracysurveycount (exists)
    # - Order Accuracy % -> crf63_orderaccuracypct (exists)
    
    # Additional cost metrics (note: mileagecost already exists)
    # - Mileage Cost Local -> crf63_mileagecost (exists)
    
    # Additional financial metrics (note: cashovershort already exists)
    # - Total Cash Over/Short USD -> crf63_cashovershort (exists)
    
    # NEW columns that don't exist yet (14 total):
    
    # SMG Service Quality Metrics (4)
    columns.append(decimal_col("crf63_smgavgclosure", "SMG Avg Closure"))
    columns.append(integer_col("crf63_smgcasesopened", "SMG Cases Opened"))
    columns.append(integer_col("crf63_smgcasesresolved", "SMG Cases Resolved"))
    columns.append(decimal_col("crf63_smgvaluepct", "SMG Value %"))
    
    # Delivery Performance Metrics (4)
    columns.append(integer_col("crf63_singles", "Singles"))
    columns.append(integer_col("crf63_doubles", "Doubles"))
    columns.append(integer_col("crf63_triplesplus", "Triples Plus"))
    columns.append(integer_col("crf63_runs", "Runs"))
    
    # Time-To-Door Metrics (3)
    columns.append(integer_col("crf63_ttdtorders", "TTDT Orders"))
    columns.append(decimal_col("crf63_todoortimedispatch", "To Door Time Dispatch Orders"))
    columns.append(decimal_col("crf63_todoortimeminutes", "To Door Time Minutes"))
    
    # Taste/Quality Survey Metrics (3)
    columns.append(integer_col("crf63_tasteoffoodgood", "Taste Of Food Good Count"))
    columns.append(integer_col("crf63_tasteoffoodtotal", "Taste Of Food Total Count"))
    columns.append(integer_col("crf63_orderaccuracygood", "Order Accuracy Good Count"))
    
    print(f"\nWill add {len(columns)} new columns (15 from Services query were already present)")
    
    # Create each column
    success_count = 0
    failed_columns = []
    
    for i, column in enumerate(columns, 1):
        col_name = column['SchemaName']
        display_name = column['DisplayName']['LocalizedLabels'][0]['Label']
        print(f"  [{i}/{len(columns)}] Creating {col_name} ({display_name})...")
        
        if create_column(token, entity_logical_name, column):
            success_count += 1
            print(f"    ✓ Success")
        else:
            failed_columns.append(col_name)
            print(f"    ✗ Failed")
    
    print(f"\n✓ Created {success_count}/{len(columns)} columns successfully")
    
    if failed_columns:
        print(f"\n⚠️  Failed columns: {', '.join(failed_columns)}")
    
    return success_count == len(columns)


def verify_columns(token):
    """Verify the new columns were added"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }
    
    print("\nVerifying new columns...")
    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/EntityDefinitions(LogicalName='crf63_oarsbidata')?$select=SchemaName&$expand=Attributes($select=SchemaName,DisplayName)"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Table found: {data['SchemaName']}")
        print(f"  Total Columns: {len(data['Attributes'])}")
        
        # Check for new service columns
        new_columns = [
            'crf63_smgavgclosure', 'crf63_smgcasesopened', 'crf63_smgcasesresolved', 'crf63_smgvaluepct',
            'crf63_singles', 'crf63_doubles', 'crf63_triplesplus', 'crf63_runs',
            'crf63_ttdtorders', 'crf63_todoortimedispatch', 'crf63_todoortimeminutes',
            'crf63_tasteoffoodgood', 'crf63_tasteoffoodtotal', 'crf63_orderaccuracygood'
        ]
        
        existing_attrs = [attr['SchemaName'].lower() for attr in data['Attributes']]
        found_new = [col for col in new_columns if col in existing_attrs]
        
        print(f"\n  New service columns found: {len(found_new)}/{len(new_columns)}")
        
        return True
    else:
        print(f"✗ Failed to verify columns: {response.status_code}")
        return False


def main():
    """Main execution"""
    print("=" * 70)
    print("Add Service Columns to crf63_oarsbidata")
    print("Adding 14 new columns from Services MDX query")
    print("=" * 70)
    
    try:
        token = get_access_token()
        
        if not add_service_columns(token):
            print("\n⚠️  Some columns failed to create")
        
        print("\n" + "=" * 70)
        print("Final Verification:")
        print("=" * 70)
        verify_columns(token)
        
        print("\n" + "=" * 70)
        print("✓ Column addition complete!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Run: python populate_service_columns.py")
        print("2. Update olap_to_dataverse.py MDX query")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
