#!/usr/bin/env python3
"""
Delete datetime columns from crf63_storeoperatinghour table

This script removes:
- crf63_openingtimedatetime (Open Time Time)
- crf63_closingtimedatetime (Close Time Time)

These datetime fields are unnecessary since we have the string time fields
(crf63_openingtimehhmm and crf63_closingtimehhmm) which store times correctly.

Usage:
  python delete_storeoperatinghour_datetime_columns.py
"""

import requests
import json
import sys
from pathlib import Path
from msal import PublicClientApplication

# Configuration
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell
DATAVERSE_URL = "https://orgbf93e3c3.crm.dynamics.com"
TABLE_LOGICAL_NAME = "crf63_storeoperatinghour"

COLUMNS_TO_DELETE = [
    "crf63_openingtimedatetime",
    "crf63_closingtimedatetime"
]


def get_access_token():
    """Get access token using MSAL interactive authentication."""
    print("\n🔐 Authenticating with Microsoft...")
    
    app = PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    
    scopes = [f"{DATAVERSE_URL}/.default"]
    result = app.acquire_token_interactive(scopes=scopes)
    
    if "access_token" in result:
        print("✓ Authentication successful")
        return result["access_token"]
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description', result)}")


def get_column_metadata(token, column_name):
    """Get metadata for a specific column."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Attributes?$filter=LogicalName eq '{column_name}'"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            attributes = data.get('value', [])
            if attributes:
                return attributes[0]
        return None
    
    except Exception as e:
        print(f"⚠️  Error checking column: {e}")
        return None


def delete_column(token, column_name):
    """Delete a column from the table."""
    print(f"\n🗑️  Deleting column: {column_name}")
    
    # First check if column exists
    metadata = get_column_metadata(token, column_name)
    
    if not metadata:
        print(f"  ℹ️  Column '{column_name}' does not exist (already deleted or never created)")
        return True
    
    # Get the MetadataId
    metadata_id = metadata.get('MetadataId')
    if not metadata_id:
        print(f"  ⚠️  Could not get MetadataId for column '{column_name}'")
        return False
    
    print(f"  Found column (MetadataId: {metadata_id})")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    # Delete the column using MetadataId
    url = f"{DATAVERSE_URL}/api/data/v9.2/EntityDefinitions(LogicalName='{TABLE_LOGICAL_NAME}')/Attributes({metadata_id})"
    
    try:
        response = requests.delete(url, headers=headers, timeout=60)
        
        if response.status_code in [200, 204]:
            print(f"  ✓ Column '{column_name}' deleted successfully")
            return True
        else:
            print(f"  ❌ Failed to delete column: HTTP {response.status_code}")
            print(f"     Response: {response.text[:500]}")
            return False
    
    except Exception as e:
        print(f"  ❌ Exception deleting column: {e}")
        return False


def main():
    """Main function to delete datetime columns."""
    print("=" * 80)
    print("Delete DateTime Columns from Store Operating Hour Table")
    print("=" * 80)
    print(f"Table: {TABLE_LOGICAL_NAME}")
    print(f"Environment: {DATAVERSE_URL}")
    print()
    print("Columns to delete:")
    for col in COLUMNS_TO_DELETE:
        print(f"  - {col}")
    print()
    print("⚠️  WARNING: This will permanently delete these columns and their data!")
    print("=" * 80)
    
    # Confirm deletion
    response = input("\nAre you sure you want to delete these columns? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ Deletion cancelled by user")
        return 1
    
    try:
        # Authenticate
        token = get_access_token()
        
        # Delete each column
        print("\n" + "=" * 80)
        print("Deleting Columns")
        print("=" * 80)
        
        success_count = 0
        for column_name in COLUMNS_TO_DELETE:
            if delete_column(token, column_name):
                success_count += 1
        
        # Summary
        print("\n" + "=" * 80)
        print("Deletion Summary")
        print("=" * 80)
        print(f"Total columns: {len(COLUMNS_TO_DELETE)}")
        print(f"Successfully deleted: {success_count}")
        print(f"Failed: {len(COLUMNS_TO_DELETE) - success_count}")
        
        if success_count == len(COLUMNS_TO_DELETE):
            print("\n✓ All datetime columns deleted successfully!")
            print("\nThe table now only has the string time fields:")
            print("  - crf63_openingtimehhmm (Open Time)")
            print("  - crf63_closingtimehhmm (Close Time)")
        elif success_count > 0:
            print("\n⚠️  Some columns were deleted, but some failed")
        else:
            print("\n❌ All deletions failed")
        
        print("=" * 80)
        
        return 0 if success_count == len(COLUMNS_TO_DELETE) else 1
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
