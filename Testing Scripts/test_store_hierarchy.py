#!/usr/bin/env python3
"""
Test the new Store Number hierarchy to validate data type and format.
"""

import sys
from modules.utils.keyvault import get_secret
from modules.olap import execute_xmla_mdx
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def test_hierarchy(hierarchy_path):
    """Test a hierarchy and show sample values."""
    print(f"\n{'='*80}")
    print(f"Testing: {hierarchy_path}")
    print('='*80)
    
    server = "https://ednacubes.papajohns.com:10502"
    catalog = "OARS Franchise"
    username = get_secret('olap-username')
    password = get_secret('olap-password')
    
    mdx = f"""
    SELECT 
      [Measures].[TY Net Sales USD] ON COLUMNS,
    NON EMPTY 
      TopCount(Hierarchize({{{hierarchy_path}.AllMembers}}), 20) ON ROWS
    FROM [OARS Franchise]
    """
    
    print(f"\nMDX Query:")
    print(mdx)
    
    try:
        xml_response = execute_xmla_mdx(server, catalog, username, password, mdx, ssl_verify=False)
        
        # Parse response to get member names/captions
        root = ET.fromstring(xml_response)
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # Find row members
        axes = root.find('.//mdd:Axes', ns)
        if axes:
            axis1 = axes.find('.//mdd:Axis[@name="Axis1"]', ns)
            if axis1:
                members = []
                for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                    for member in tuple_elem.findall('.//mdd:Member', ns):
                        uname = member.find('mdd:UName', ns)
                        caption = member.find('mdd:Caption', ns)
                        if uname is not None and caption is not None:
                            members.append({
                                'uname': uname.text,
                                'caption': caption.text
                            })
                
                print(f"\n✓ Found {len(members)} members")
                print("\nFirst 10 sample values:")
                for i, m in enumerate(members[:10]):
                    print(f"  {i+1}. Caption: '{m['caption']}' | UName: {m['uname']}")
                
                # Check data type
                if members:
                    sample = members[0]['caption']
                    print(f"\n📊 Data Type Analysis:")
                    print(f"   Sample value: '{sample}'")
                    print(f"   Is numeric: {sample.isdigit()}")
                    print(f"   Length: {len(sample)} characters")
                    
                    # Check if all are numeric
                    all_numeric = all(m['caption'].isdigit() for m in members[:10])
                    print(f"   All samples numeric: {all_numeric}")
                
                return True
            else:
                print("✗ No Axis1 found in response")
        else:
            print("✗ No Axes found in response")
        
        return False
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False


def main():
    print("="*80)
    print("Store Hierarchy Data Type Validation")
    print("="*80)
    
    # Test the old hierarchy (should fail)
    print("\n🔴 Testing OLD hierarchy (expected to fail):")
    test_hierarchy("[Franchise].[Store Number Label].[Store Number Label]")
    
    # Test the new hierarchy
    print("\n\n🟢 Testing NEW hierarchy:")
    test_hierarchy("[Stores].[Store Number].[Store Number]")
    
    print("\n" + "="*80)
    print("✅ Test complete!")
    print("="*80)


if __name__ == "__main__":
    main()
