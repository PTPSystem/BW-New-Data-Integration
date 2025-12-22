#!/usr/bin/env python3
"""
Generic XMLA response parser with config-driven hierarchy mapping.

This demonstrates that ALL XMLA parsers can be unified into a single generic
parser since they all follow the same 2D table structure:
  - Axis0 (COLUMNS): Measures
  - Axis1 (ROWS): Dimension tuples
  - CellData: Values mapped by ordinal = row_idx * num_measures + col_idx

The ONLY cube-specific part is hierarchy name → field name mapping.
"""

import re
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Dict, List, Tuple, Optional


class GenericXMLAParser:
    """
    Generic parser for XMLA/MDX responses with config-driven hierarchy mapping.
    
    Eliminates the need for separate parse_sales_response(), parse_inventory_response(), etc.
    All cube-specific logic is externalized to YAML configuration.
    """
    
    def __init__(self, hierarchy_mappings: List[Dict[str, str]]):
        """
        Initialize parser with hierarchy mapping configuration.
        
        Args:
            hierarchy_mappings: List of {pattern, field} dicts for mapping
                                hierarchy names to DataFrame column names.
                                
        Example:
            [
                {"pattern": "Franchise.*Store", "field": "StoreNumber"},
                {"pattern": "Calendar.*Date", "field": "CalendarDate"},
                {"pattern": ".*Item_Number", "field": "ItemNumber"}
            ]
        """
        self.hierarchy_mappings = hierarchy_mappings
        self._compiled_patterns = [
            (re.compile(m['pattern']), m['field']) 
            for m in hierarchy_mappings
        ]
    
    def _match_hierarchy_to_field(self, hierarchy: str) -> Optional[str]:
        """
        Match a hierarchy name to its configured field name using regex patterns.
        
        Args:
            hierarchy: Full hierarchy name like "[Franchise].[Store Number Label]"
            
        Returns:
            Field name like "StoreNumber", or None if no match
        """
        for pattern, field in self._compiled_patterns:
            if pattern.search(hierarchy):
                return field
        return None
    
    def parse_response(self, xml_response: str, logger=None) -> pd.DataFrame:
        """
        Parse XMLA response into DataFrame using configured hierarchy mappings.
        
        This is the UNIVERSAL parser that works for ALL MDX queries.
        
        Args:
            xml_response: Raw XMLA XML response string
            logger: Optional logger for info messages
            
        Returns:
            DataFrame with dimension columns + measure columns
        """
        def log(msg: str):
            if logger:
                logger.info(msg)
        
        root = ET.fromstring(xml_response)
        
        # Define namespaces
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # 1. Parse Axis0 (COLUMNS) - Get measure names
        axis0 = root.find('.//mdd:Axis[@name="Axis0"]', ns)
        measure_names = []
        
        if axis0 is not None:
            for tuple_elem in axis0.findall('.//mdd:Tuple', ns):
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    caption_elem = member.find('mdd:Caption', ns)
                    if caption_elem is not None:
                        measure_names.append(caption_elem.text)
        
        if not measure_names:
            raise ValueError("No measures found on Axis0")
        
        log(f"Found {len(measure_names)} measures: {measure_names}")
        
        # 2. Parse Axis1 (ROWS) - Get dimension tuples
        axis1 = root.find('.//mdd:Axis[@name="Axis1"]', ns)
        row_tuples = []
        
        if axis1 is not None:
            for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                row_info = {}
                
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    hierarchy = member.get('Hierarchy', '')
                    caption_elem = member.find('mdd:Caption', ns)
                    
                    if caption_elem is not None:
                        # Use config to map hierarchy → field name
                        field_name = self._match_hierarchy_to_field(hierarchy)
                        if field_name:
                            row_info[field_name] = caption_elem.text
                        else:
                            # Fallback: use hierarchy name as-is
                            log(f"WARNING: No mapping for hierarchy '{hierarchy}', using as-is")
                            row_info[hierarchy] = caption_elem.text
                
                row_tuples.append(row_info)
        
        if not row_tuples:
            raise ValueError("No row tuples found on Axis1")
        
        log(f"Found {len(row_tuples)} row tuples")
        
        # 3. Parse CellData - Get values mapped by ordinal
        celldata = root.find('.//mdd:CellData', ns)
        cell_values = {}
        
        if celldata is not None:
            for cell in celldata.findall('.//mdd:Cell', ns):
                ordinal = int(cell.get('CellOrdinal', -1))
                value_elem = cell.find('mdd:Value', ns)
                
                if value_elem is not None and value_elem.text:
                    try:
                        cell_values[ordinal] = float(value_elem.text)
                    except ValueError:
                        cell_values[ordinal] = value_elem.text
        
        log(f"Found {len(cell_values)} cell values")
        
        # 4. Build DataFrame: Map cells to (row, measure) pairs
        data_rows = []
        num_measures = len(measure_names)
        
        for row_idx, row_tuple in enumerate(row_tuples):
            row_data = row_tuple.copy()  # Start with dimension values
            
            # Add measure values using ordinal formula
            for col_idx, measure_name in enumerate(measure_names):
                ordinal = row_idx * num_measures + col_idx
                row_data[measure_name] = cell_values.get(ordinal, None)
            
            data_rows.append(row_data)
        
        df = pd.DataFrame(data_rows)
        log(f"Built DataFrame with shape: {df.shape}")
        log(f"Columns: {list(df.columns)}")
        
        return df


def test_daily_sales():
    """Test generic parser on daily_sales_response.xml"""
    print("=" * 80)
    print("TEST: Daily Sales (OARS Franchise cube)")
    print("=" * 80)
    
    # Config for daily_sales hierarchy mappings
    hierarchy_mappings = [
        {"pattern": r"Franchise.*Store", "field": "StoreNumber"},
        {"pattern": r"Calendar.*Date", "field": "CalendarDate"}
    ]
    
    parser = GenericXMLAParser(hierarchy_mappings)
    
    with open('daily_sales_response.xml', 'r') as f:
        xml_response = f.read()
    
    df = parser.parse_response(xml_response)
    
    print("\nSample rows:")
    print(df.head(10))
    
    print("\nData types:")
    print(df.dtypes)
    
    return df


def test_inventory():
    """Test generic parser on inventory_response.xml"""
    print("\n\n")
    print("=" * 80)
    print("TEST: Inventory (OARS BI Data cube)")
    print("=" * 80)
    
    # Config for inventory hierarchy mappings
    hierarchy_mappings = [
        {"pattern": r".*Item_Number", "field": "ItemNumber"},
        {"pattern": r".*Item_Description", "field": "ItemDescription"},
        {"pattern": r"Franchise.*Store", "field": "StoreNumber"},
        {"pattern": r"Calendar.*Date", "field": "CalendarDate"}
    ]
    
    parser = GenericXMLAParser(hierarchy_mappings)
    
    with open('inventory_response.xml', 'r') as f:
        xml_response = f.read()
    
    df = parser.parse_response(xml_response)
    
    print("\nSample rows:")
    print(df.head(10))
    
    print("\nData types:")
    print(df.dtypes)
    
    return df


def main():
    print("\n🚀 GENERIC XMLA PARSER DEMONSTRATION\n")
    print("This shows that ALL parsers can be unified with config-driven hierarchy mapping.\n")
    
    df_sales = test_daily_sales()
    df_inventory = test_inventory()
    
    print("\n\n")
    print("=" * 80)
    print("✅ SUCCESS: Generic parser works for both cubes!")
    print("=" * 80)
    print("""
HOW TO INTEGRATE INTO PRODUCTION:

1. Add hierarchy_mappings to pipelines.yaml:
   
   pipelines:
     daily_sales:
       query: |
         SELECT ...
       hierarchy_mappings:
         - pattern: "Franchise.*Store"
           field: "StoreNumber"
         - pattern: "Calendar.*Date"
           field: "CalendarDate"
     
     inventory:
       query: |
         SELECT ...
       hierarchy_mappings:
         - pattern: ".*Item_Number"
           field: "ItemNumber"
         - pattern: ".*Item_Description"
           field: "ItemDescription"
         - pattern: "Franchise.*Store"
           field: "StoreNumber"
         - pattern: "Calendar.*Date"
           field: "CalendarDate"

2. Replace modules/olap.py functions:
   
   # OLD:
   from modules.olap import parse_xmla_celldata_response, parse_inventory_response
   
   # NEW:
   from generic_xmla_parser import GenericXMLAParser
   parser = GenericXMLAParser(pipeline_config['hierarchy_mappings'])
   df = parser.parse_response(xml_response)

3. Benefits:
   - Single parser for ALL cubes (no parse_sales_response, parse_inventory_response, etc.)
   - New pipelines = just add hierarchy_mappings config (no code changes)
   - Easier to maintain (one parser vs. four)
   - Same logic, different config

4. Trade-offs:
   - More complex YAML config (hierarchy patterns)
   - Less explicit code (hierarchy matching is regex-based)
   - Current approach (4 explicit parsers) is also fine for small # of cubes
""")


if __name__ == '__main__':
    main()
