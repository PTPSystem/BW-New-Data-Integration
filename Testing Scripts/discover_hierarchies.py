#!/usr/bin/env python3
"""
Discover hierarchies in OLAP cube to help fix pipeline configurations.
This script queries the OLAP metadata to find available hierarchies in a dimension.
"""

import sys
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import urllib3
from modules.utils.keyvault import get_secret

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def discover_hierarchies(server, catalog, cube, username, password, dimension_unique_name=None):
    """
    Discover hierarchies in the OLAP cube using XMLA Discover.
    
    Args:
        server: OLAP server URL
        catalog: Catalog name
        cube: Cube name
        username: OLAP username
        password: OLAP password
        dimension_unique_name: Optional dimension filter (e.g., '[Franchise]')
    """
    xmla_url = f"{server}/xmla/default"
    
    # Build restrictions
    restrictions = f"<CATALOG_NAME>{catalog}</CATALOG_NAME><CUBE_NAME>{cube}</CUBE_NAME>"
    if dimension_unique_name:
        restrictions += f"<DIMENSION_UNIQUE_NAME>{dimension_unique_name}</DIMENSION_UNIQUE_NAME>"
    
    discover_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Discover xmlns="urn:schemas-microsoft-com:xml-analysis">
      <RequestType>MDSCHEMA_HIERARCHIES</RequestType>
      <Restrictions>
        <RestrictionList>
          {restrictions}
        </RestrictionList>
      </Restrictions>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
        </PropertyList>
      </Properties>
    </Discover>
  </soap:Body>
</soap:Envelope>"""
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'urn:schemas-microsoft-com:xml-analysis:Discover'
    }
    
    try:
        response = requests.post(
            xmla_url,
            data=discover_xml.encode('utf-8'),
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=False,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"✗ Discovery failed: HTTP {response.status_code}")
            print(response.text[:500])
            return []
        
        # Parse the response
        root = ET.fromstring(response.text)
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'row': 'urn:schemas-microsoft-com:xml-analysis:rowset'
        }
        
        hierarchies = []
        for row in root.findall('.//row:row', ns):
            dimension = row.find('row:DIMENSION_UNIQUE_NAME', ns)
            hierarchy = row.find('row:HIERARCHY_UNIQUE_NAME', ns)
            hierarchy_caption = row.find('row:HIERARCHY_CAPTION', ns)
            
            if dimension is not None and hierarchy is not None:
                hierarchies.append({
                    'dimension': dimension.text,
                    'hierarchy': hierarchy.text,
                    'caption': hierarchy_caption.text if hierarchy_caption is not None else ''
                })
        
        return hierarchies
        
    except Exception as e:
        print(f"✗ Error during discovery: {e}")
        return []


def main():
    print("=" * 80)
    print("OLAP Hierarchy Discovery Tool")
    print("=" * 80)
    
    # Get credentials
    print("\n📦 Loading credentials from Azure Key Vault...")
    username = get_secret('olap-username')
    password = get_secret('olap-password')
    
    server = "https://ednacubes.papajohns.com:10502"
    catalog = "OARS Franchise"
    cube = "OARS Franchise"  # Usually same as catalog
    
    print(f"   Server: {server}")
    print(f"   Catalog: {catalog}")
    print(f"   Cube: {cube}")
    print(f"   Username: {username}\n")
    
    # Discover all hierarchies in Franchise dimension
    print("🔍 Discovering [Franchise] dimension hierarchies...")
    hierarchies = discover_hierarchies(server, catalog, cube, username, password, dimension_unique_name='[Franchise]')
    
    if hierarchies:
        print(f"\n✓ Found {len(hierarchies)} hierarchies:\n")
        for h in hierarchies:
            print(f"   Dimension: {h['dimension']}")
            print(f"   Hierarchy: {h['hierarchy']}")
            print(f"   Caption: {h['caption']}")
            print()
    else:
        print("\n✗ No hierarchies found or discovery failed")
        print("\nTrying to discover ALL hierarchies (no filter)...")
        all_hierarchies = discover_hierarchies(server, catalog, cube, username, password)
        
        if all_hierarchies:
            print(f"\n✓ Found {len(all_hierarchies)} total hierarchies\n")
            
            # Group by dimension
            from collections import defaultdict
            by_dimension = defaultdict(list)
            for h in all_hierarchies:
                by_dimension[h['dimension']].append(h)
            
            # Show dimensions that might be relevant
            print("Relevant dimensions:")
            for dim in sorted(by_dimension.keys()):
                if 'Franchise' in dim or 'Store' in dim:
                    print(f"\n  {dim}:")
                    for h in by_dimension[dim]:
                        print(f"    • {h['hierarchy']}")
        else:
            print("\n✗ Failed to discover hierarchies")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
