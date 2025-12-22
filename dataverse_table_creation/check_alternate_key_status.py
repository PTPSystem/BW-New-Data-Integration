#!/usr/bin/env python3
"""
Check the status of alternate keys on Dataverse tables.
"""

import requests
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.dataverse import get_dataverse_access_token
from modules.utils.config import load_config
from modules.utils.keyvault import get_dataverse_credentials

def check_keys_for_table(token, dataverse_url, table_logical_name, table_display_name):
    """Check all alternate keys for a given table."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }
    
    print(f"\n{'='*80}")
    print(f"Table: {table_display_name} ({table_logical_name})")
    print(f"{'='*80}")
    
    # Get all keys for the table
    url = f"{dataverse_url}/api/data/v9.2/EntityDefinitions(LogicalName='{table_logical_name}')/Keys"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to retrieve keys")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text}")
        return
    
    data = response.json()
    keys = data.get('value', [])
    
    if not keys:
        print("⚠️  No alternate keys found")
        return
    
    print(f"\nFound {len(keys)} key(s):\n")
    
    for key in keys:
        schema_name = key.get('SchemaName', 'N/A')
        logical_name = key.get('LogicalName', 'N/A')
        display_name = key.get('DisplayName', {}).get('UserLocalizedLabel', {}).get('Label', 'N/A')
        entity_key_index_status = key.get('EntityKeyIndexStatus', 'N/A')
        key_attributes = key.get('KeyAttributes', [])
        
        # Map status codes to readable names
        status_map = {
            0: 'Pending',
            1: 'Active',
            2: 'Failed'
        }
        status = status_map.get(entity_key_index_status, f'Unknown ({entity_key_index_status})')
        
        print(f"  🔑 {display_name}")
        print(f"     Schema Name: {schema_name}")
        print(f"     Logical Name: {logical_name}")
        print(f"     Status: {status}")
        print(f"     Attributes: {', '.join(key_attributes)}")
        print()

def main():
    print("=" * 80)
    print("Check Alternate Key Status")
    print("=" * 80)
    
    try:
        # Get config and token
        print("\n🔐 Authenticating...")
        config = load_config()
        client_id, client_secret = get_dataverse_credentials()
        token = get_dataverse_access_token(
            config['dataverse']['environment_url'],
            client_id,
            client_secret,
            config['dataverse']['tenant_id']
        )
        dataverse_url = config['dataverse']['environment_url']
        print("✓ Authentication successful\n")
        
        # Check keys for each table
        tables = [
            ('crf63_oarsbidatas', 'OARS BI Data (Daily Sales)'),
            ('crf63_saleschanneldailies', 'Sales Channel Daily'),
            ('crf63_inventories', 'Inventory'),
            ('crf63_offers', 'Offers')
        ]
        
        for logical_name, display_name in tables:
            check_keys_for_table(token, dataverse_url, logical_name, display_name)
        
        print("\n" + "=" * 80)
        print("✓ Key status check complete")
        print("=" * 80)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
