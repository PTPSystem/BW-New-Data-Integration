#!/usr/bin/env python3
"""Update Offers decimal column ranges to allow negative values.

Fixes Dataverse validation errors like:
  A validation error occurred for crf63_offers.crf63_grossmarginusd.
  The value -0.88 ... is outside the valid range(0 to 1000000000).

Dataverse does not support PATCH directly on the attribute metadata resource.
This script uses the supported `UpdateAttribute` action.

Usage:
  source .venv/bin/activate && python dataverse_table_creation/update_offers_decimal_ranges.py
"""

import time
import requests
from msal import PublicClientApplication

DATAVERSE_ENVIRONMENT = "https://orgbf93e3c3.crm.dynamics.com"
TENANT_ID = "c8b6ba98-3fc0-4153-83a9-01374492c0f5"
CLIENT_ID = "51f81489-12ee-4a9e-aaae-a2591f45987d"  # Microsoft Azure PowerShell

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [f"{DATAVERSE_ENVIRONMENT}/.default"]

TABLE_SCHEMA_NAME = "crf63_offers"

# Allow negative values for financial measures
MIN_DECIMAL = -1000000000.0
MAX_DECIMAL = 1000000000.0

DECIMAL_COLUMNS_TO_PATCH = [
    # The one we observed failing
    "crf63_grossmarginusd",
    # Likely can go negative too
    "crf63_discountamountusd",
    "crf63_netsalesusd",
    "crf63_targetfoodcostusd",
    # Percentages should generally be >= 0 but keep safe range
    "crf63_ordermixpercent",
    "crf63_salesmixusdpercent",
]


def get_access_token() -> str:
    app = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)

    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    print("\nAuthentication required...\nA browser window will open for you to sign in...")
    result = app.acquire_token_interactive(scopes=SCOPES, prompt="select_account")
    if "access_token" not in result:
        raise RuntimeError(f"Authentication failed: {result.get('error_description', result)}")
    print(f"✓ Authenticated as: {result.get('id_token_claims', {}).get('preferred_username', 'Unknown')}")
    return result["access_token"]


def patch_decimal_range(token: str, logical_name: str, min_value: float, max_value: float) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }

    url = f"{DATAVERSE_ENVIRONMENT}/api/data/v9.2/UpdateAttribute"

    # Using the metadata action avoids 405s on the Attributes resource.
    payload = {
        "EntityLogicalName": TABLE_SCHEMA_NAME,
        "MergeLabels": True,
        "HasChanged": False,
        "Attribute": {
            "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            "LogicalName": logical_name,
            "MinValue": float(min_value),
            "MaxValue": float(max_value),
        },
    }

    r = requests.post(url, headers=headers, json=payload)
    if r.status_code in (200, 204):
        print(f"✓ Updated {logical_name}: MinValue={min_value}, MaxValue={max_value}")
        return

    raise RuntimeError(f"Failed to update {logical_name}: HTTP {r.status_code} {r.text}")


def main() -> int:
    token = get_access_token()

    print(f"\nUpdating decimal ranges on {TABLE_SCHEMA_NAME}...")
    for col in DECIMAL_COLUMNS_TO_PATCH:
        patch_decimal_range(token, col, MIN_DECIMAL, MAX_DECIMAL)
        # small delay to reduce likelihood of transient gateway failures
        time.sleep(0.2)

    print("\n✓ Done. Decimal ranges updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
