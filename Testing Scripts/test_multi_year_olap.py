#!/usr/bin/env python3
"""
Test OLAP data retrieval for multiple fiscal years (2023, 2024, 2025).
This test validates that we can pull larger datasets without timeout.
"""

import sys
import os
from pathlib import Path
import time

# Adjust path to find modules from root directory
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


def test_multi_year_query(username, password, fiscal_years):
    """Test retrieving data for multiple fiscal years with all 33 measures."""
    print(f"\n📊 Testing Multi-Year Data Retrieval...")
    print(f"   • Fiscal Years: {', '.join(map(str, fiscal_years))}")
    print(f"   • Expected: ~77,220 cells (33 measures × 45 stores × 52 weeks)")
    
    server_url = "https://ednacubes.papajohns.com:10502"
    catalog = "OARS Franchise"
    
    # Build fiscal year filter for MDX
    fy_members = ', '.join([f"[Calendar].[Fiscal Year].&[{fy}]" for fy in fiscal_years])
    
    # All 33 measures from the actual query
    mdx_query = f"""
SELECT 
{{
    [Measures].[TY Net Sales USD],
    [Measures].[L2Y Comp Net Sales USD],
    [Measures].[L3Y Comp Net Sales USD],
    [Measures].[LY Comp Net Sales USD],
    [Measures].[TY Target Food Cost USD],
    [Measures].[Actual Food Cost USD],
    [Measures].[FLMD USD],
    [Measures].[Target Profit after FLM Local (Fran)],
    [Measures].[Actual FLM w/o Vacation Accrual Local],
    [Measures].[Actual Labor $ USD],
    [Measures].[HS Total Actual Hours],
    [Measures].[Store Days],
    [Measures].[Make Time Minutes],
    [Measures].[TY Orders],
    [Measures].[Rack Time Minutes],
    [Measures].[Total OTD Time (Hours)],
    [Measures].[Deliveries],
    [Measures].[BOZOCORO Orders],
    [Measures].[OTD Order Count],
    [Measures].[Total Cash Over/Short USD],
    [Measures].[LY Orders],
    [Measures].[TY Total OSAT Survey Count],
    [Measures].[TY OSAT Satisfied Survey Count],
    [Measures].[Total Calls],
    [Measures].[Answered Calls],
    [Measures].[FLMDPC USD (Fran)],
    [Measures].[m_ty_agg_commission_local_sum],
    [Measures].[TY Dispatched Delivery Orders],
    [Measures].[Avg TTDT],
    [Measures].[Mileage Cost Local],
    [Measures].[Discounts USD],
    [Measures].[TY Total Order Accuracy Survey Count],
    [Measures].[Order Accuracy %]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({{[Franchise].[Store Number Label].[Store Number Label].AllMembers}}),
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}})
) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE {{
    {fy_members}
}}
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    
    # XMLA Execute request
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
        print(f"   • Catalog: {catalog}")
        print(f"   • Measures: TY Net Sales USD, TY Orders, Store Days")
        print(f"   • Dimensions: Calendar Date x Fiscal Year")
        print(f"   • Timeout: 5 minutes")
        
        start_time = time.time()
        
        response = requests.post(
            xmla_endpoint,
            data=execute_xml.encode('utf-8'),
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=False,
            timeout=300  # 5 minutes
        )
        
        elapsed_time = time.time() - start_time
        
        if response.status_code == 200:
            print(f"   ✓ Query executed successfully! (HTTP {response.status_code})")
            print(f"   ✓ Response time: {elapsed_time:.2f} seconds")
            print(f"   ✓ Response size: {len(response.content) / 1024:.2f} KB")
            
            # Parse response to count data points
            try:
                root = ET.fromstring(response.content)
                
                # Count cells in response
                cells = root.findall('.//{urn:schemas-microsoft-com:xml-analysis:mddataset}Cell')
                axes = root.findall('.//{urn:schemas-microsoft-com:xml-analysis:mddataset}Axis')
                
                print(f"   ✓ Data cells returned: {len(cells)}")
                
                # Count tuples on each axis
                for idx, axis in enumerate(axes):
                    tuples = axis.findall('.//{urn:schemas-microsoft-com:xml-analysis:mddataset}Tuple')
                    print(f"   ✓ Axis {idx} tuples: {len(tuples)}")
                
                # Show sample cell values
                if cells:
                    print(f"\n   📈 Sample Values (first 5 cells):")
                    for i, cell in enumerate(cells[:5]):
                        cell_ordinal = cell.get('CellOrdinal', '?')
                        value_elem = cell.find('{urn:schemas-microsoft-com:xml-analysis:mddataset}Value')
                        if value_elem is not None:
                            value = value_elem.text
                            print(f"      Cell[{cell_ordinal}]: {value}")
                
            except Exception as e:
                print(f"   ⚠️  Could not parse response details: {e}")
            
            return True
        else:
            print(f"   ✗ Query failed: HTTP {response.status_code}")
            print(f"   Error: {response.text[:500]}")
            return False
            
    except requests.exceptions.Timeout:
        elapsed_time = time.time() - start_time
        print(f"   ✗ Query TIMEOUT after {elapsed_time:.2f} seconds")
        print(f"   ⚠️  Similar to Excel timeout issue")
        return False
    except Exception as e:
        print(f"   ✗ Error executing query: {e}")
        return False


def test_single_year_query(username, password, fiscal_year):
    """Test retrieving data for a single fiscal year with all measures."""
    print(f"\n📊 Testing Single Year Data Retrieval...")
    print(f"   • Fiscal Year: {fiscal_year}")
    print(f"   • Expected: ~25,740 cells (33 measures × 45 stores × ~17 weeks)")
    
    server_url = "https://ednacubes.papajohns.com:10502"
    catalog = "OARS Franchise"
    
    # MDX query for single year with all 33 measures
    mdx_query = f"""
SELECT 
{{
    [Measures].[TY Net Sales USD],
    [Measures].[L2Y Comp Net Sales USD],
    [Measures].[L3Y Comp Net Sales USD],
    [Measures].[LY Comp Net Sales USD],
    [Measures].[TY Target Food Cost USD],
    [Measures].[Actual Food Cost USD],
    [Measures].[FLMD USD],
    [Measures].[Target Profit after FLM Local (Fran)],
    [Measures].[Actual FLM w/o Vacation Accrual Local],
    [Measures].[Actual Labor $ USD],
    [Measures].[HS Total Actual Hours],
    [Measures].[Store Days],
    [Measures].[Make Time Minutes],
    [Measures].[TY Orders],
    [Measures].[Rack Time Minutes],
    [Measures].[Total OTD Time (Hours)],
    [Measures].[Deliveries],
    [Measures].[BOZOCORO Orders],
    [Measures].[OTD Order Count],
    [Measures].[Total Cash Over/Short USD],
    [Measures].[LY Orders],
    [Measures].[TY Total OSAT Survey Count],
    [Measures].[TY OSAT Satisfied Survey Count],
    [Measures].[Total Calls],
    [Measures].[Answered Calls],
    [Measures].[FLMDPC USD (Fran)],
    [Measures].[m_ty_agg_commission_local_sum],
    [Measures].[TY Dispatched Delivery Orders],
    [Measures].[Avg TTDT],
    [Measures].[Mileage Cost Local],
    [Measures].[Discounts USD],
    [Measures].[TY Total Order Accuracy Survey Count],
    [Measures].[Order Accuracy %]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({{[Franchise].[Store Number Label].[Store Number Label].AllMembers}}),
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}})
) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE ([Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{fiscal_year}])
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    
    # XMLA Execute request
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
        
        start_time = time.time()
        
        response = requests.post(
            xmla_endpoint,
            data=execute_xml.encode('utf-8'),
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=False,
            timeout=300
        )
        
        elapsed_time = time.time() - start_time
        
        if response.status_code == 200:
            print(f"   ✓ Query executed successfully!")
            print(f"   ✓ Response time: {elapsed_time:.2f} seconds")
            print(f"   ✓ Response size: {len(response.content) / 1024:.2f} KB")
            
            # Parse to count cells
            try:
                root = ET.fromstring(response.content)
                cells = root.findall('.//{urn:schemas-microsoft-com:xml-analysis:mddataset}Cell')
                print(f"   ✓ Data cells: {len(cells)}")
            except:
                pass
            
            return True, elapsed_time
        else:
            print(f"   ✗ Query failed: HTTP {response.status_code}")
            return False, 0
            
    except requests.exceptions.Timeout:
        elapsed_time = time.time() - start_time
        print(f"   ✗ Query TIMEOUT after {elapsed_time:.2f} seconds")
        return False, elapsed_time
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False, 0


def main():
    """Main test function."""
    print("=" * 70)
    print("OLAP Multi-Year Data Retrieval Test".center(70))
    print("=" * 70)
    
    # Get credentials
    username, password = get_olap_credentials()
    if not username or not password:
        print("\n❌ FAILED: Could not retrieve OLAP credentials")
        return False
    
    # Test 1: Try multi-year query (2023, 2024, 2025)
    print("\n" + "=" * 70)
    print("TEST 1: Multi-Year Query (2023, 2024, 2025)".center(70))
    print("=" * 70)
    
    multi_year_ok = test_multi_year_query(username, password, [2023, 2024, 2025])
    
    # Test 2: Try each year individually
    print("\n" + "=" * 70)
    print("TEST 2: Individual Year Queries".center(70))
    print("=" * 70)
    
    results = {}
    for year in [2023, 2024, 2025]:
        success, elapsed = test_single_year_query(username, password, year)
        results[year] = (success, elapsed)
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY".center(70))
    print("=" * 70)
    
    print(f"\n📊 Multi-Year Query (2023-2025): {'✅ SUCCESS' if multi_year_ok else '❌ FAILED/TIMEOUT'}")
    
    print(f"\n📊 Individual Year Results:")
    for year, (success, elapsed) in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED/TIMEOUT"
        print(f"   • FY {year}: {status} ({elapsed:.2f}s)")
    
    # Recommendations
    print(f"\n💡 Recommendations:")
    if multi_year_ok:
        print("   ✓ Multi-year queries work! Can pull 2023-2025 in one request.")
    else:
        if all(success for success, _ in results.values()):
            print("   ⚠️  Multi-year query failed/timeout, but individual years work.")
            print("   → Recommendation: Query each year separately and combine results.")
        else:
            print("   ❌ Both multi-year and some individual queries failed.")
            print("   → Need to investigate further or reduce data scope.")
    
    return multi_year_ok or all(success for success, _ in results.values())


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
