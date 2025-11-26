#!/usr/bin/env python3
"""
Test OLAP server access using credentials from Key Vault.
This validates that we can connect to the OLAP server and execute queries.
"""

import sys
import os
from pathlib import Path

# Adjust path to find modules from root directory (2 levels up from Testing Scripts)
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.utils.keyvault import get_secret
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import xml.etree.ElementTree as ET

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_olap_credentials():
    """Get OLAP credentials from Key Vault."""
    print("🔐 Retrieving OLAP credentials from Key Vault...")
    
    try:
        username = get_secret('olap-username')
        password = get_secret('olap-password')
        
        print(f"   ✓ Username: {username}")
        print(f"   ✓ Password: {'*' * len(password)}")
        
        return username, password
        
    except Exception as e:
        print(f"   ✗ Failed to retrieve credentials: {e}")
        return None, None


def test_olap_connection(username, password):
    """Test OLAP server connection with a simple MDX query."""
    print("\n🔍 Testing OLAP Server Connection...")
    
    # OLAP server details
    server_url = "https://ednacubes.papajohns.com:10502"
    catalog = "OARS Franchise"
    
    print(f"   • Server: {server_url}")
    print(f"   • Catalog: {catalog}")
    
    # Simple MDX query to test connection (just get one measure aggregated)
    # Note: Catalog = "OARS Franchise", Cube = "OARS Franchise" (same name)
    mdx_query = """
    SELECT 
      [Measures].[TY Net Sales USD] ON COLUMNS
    FROM [OARS Franchise]
    """
    
    # XMLA Execute request (use CDATA to avoid XML escaping issues)
    execute_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement><![CDATA[{mdx_query}]]></Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
          <Format>Multidimensional</Format>
        </PropertyList>
      </Properties>
    </Execute>
  </soap:Body>
</soap:Envelope>"""
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'urn:schemas-microsoft-com:xml-analysis:Execute'
    }
    
    try:
        xmla_endpoint = f"{server_url}/xmla/default"
        print(f"\n   📡 POST {xmla_endpoint}")
        print(f"   • Query: SELECT [Measures].[Net Sales] FROM [Franchise OARS]")
        
        response = requests.post(
            xmla_endpoint,
            data=execute_xml.encode('utf-8'),
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=False,  # Disable SSL verification for self-signed cert
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"   ✓ Connection successful! (HTTP {response.status_code})")
            
            # Check if response contains data
            if b'Cell' in response.content or b'Value' in response.content or b'Axis' in response.content:
                print(f"   ✓ Query returned data successfully")
            else:
                print(f"   ⚠️  Query executed but response format unexpected")
                print(f"   Response preview: {response.text[:200]}")
            
            return True
        else:
            print(f"   ✗ Connection failed: HTTP {response.status_code}")
            print(f"   Error: {response.text[:500]}")
            return False
            
    except Exception as e:
        print(f"   ✗ Error connecting to OLAP server: {e}")
        return False





def main():
    """Main test function."""
    print("=" * 70)
    print("OLAP Server Access Test".center(70))
    print("=" * 70)
    
    # Step 1: Get credentials
    username, password = get_olap_credentials()
    if not username or not password:
        print("\n❌ FAILED: Could not retrieve OLAP credentials")
        return False
    
    # Step 2: Test connection and query execution
    connection_ok = test_olap_connection(username, password)
    if not connection_ok:
        print("\n❌ FAILED: Could not connect to OLAP server or execute query")
        return False
    
    # All tests passed
    print("\n" + "=" * 70)
    print("✅ SUCCESS: All OLAP access tests passed!".center(70))
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
