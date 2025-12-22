import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import pandas as pd
import traceback

def execute_xmla_mdx(server, catalog, username, password, mdx_query, ssl_verify=False, logger=None):
    """Execute an MDX query via XMLA HTTP request and return response text."""
    xmla_url = f"{server}/xmla/default" if not server.endswith("/xmla/default") else server
    
    # Use CDATA to avoid XML escaping issues with & characters in MDX
    execute_template = f"""<?xml version="1.0" encoding="UTF-8"?>
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
    
    response = requests.post(
        xmla_url,
        data=execute_template.encode('utf-8'),
        headers=headers,
        auth=HTTPBasicAuth(username, password),
        verify=ssl_verify,
        timeout=300  # 5 minutes for large queries
    )
    
    if response.status_code != 200:
        raise Exception(f"XMLA query failed with HTTP {response.status_code}: {response.text[:500]}")
    
    return response.text

def parse_xmla_celldata_response(xml_response, logger=None):
    """
    Parse XMLA CellData format response into a pandas DataFrame.
    
    Args:
        xml_response: XML string from XMLA Execute response
        logger: Optional logger
    
    Returns:
        pandas DataFrame with columns for dimensions and measures
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        root = ET.fromstring(xml_response)
        
        # Define namespaces
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # Find the root element
        root_elem = root.find('.//mdd:root', ns)
        if root_elem is None:
            log("   Could not find mddataset:root element")
            return None
        
        # Extract axis info
        axes_info = root_elem.find('mdd:OlapInfo/mdd:AxesInfo', ns)
        axes = root_elem.find('mdd:Axes', ns)
        cell_data = root_elem.find('mdd:CellData', ns)
        
        if axes is None or cell_data is None:
            log("   Missing Axes or CellData elements")
            return None
        
        # Parse Column Axis (Axis0 - Measures)
        axis0 = axes.find('.//mdd:Axis[@name="Axis0"]', ns)
        measure_names = []
        if axis0 is not None:
            for tuple_elem in axis0.findall('.//mdd:Tuple', ns):
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    caption = member.find('mdd:Caption', ns)
                    if caption is not None:
                        measure_names.append(caption.text)
        
        log(f"   Found {len(measure_names)} measures")
        
        # Parse Row Axis (Axis1 - Store × Date)
        axis1 = axes.find('.//mdd:Axis[@name="Axis1"]', ns)
        row_tuples = []
        if axis1 is not None:
            for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                row_info = {}
                members = tuple_elem.findall('.//mdd:Member', ns)
                for member in members:
                    hierarchy = member.get('Hierarchy', '')
                    caption_elem = member.find('mdd:Caption', ns)
                    caption = caption_elem.text if caption_elem is not None else ''
                    
                    if 'Store' in hierarchy:
                        row_info['StoreNumber'] = caption
                    elif 'Calendar' in hierarchy or 'Date' in hierarchy:
                        row_info['CalendarDate'] = caption
                
                row_tuples.append(row_info)
        
        log(f"   Found {len(row_tuples)} row tuples")
        
        # Parse Cells
        cells = {}
        for cell in cell_data.findall('.//mdd:Cell', ns):
            ordinal = int(cell.get('CellOrdinal', -1))
            value_elem = cell.find('mdd:Value', ns)
            value = value_elem.text if value_elem is not None else None
            cells[ordinal] = value
        
        log(f"   Found {len(cells)} cell values")
        
        # Build DataFrame: Map cells to rows
        # CellOrdinal = row_idx * num_measures + col_idx
        num_measures = len(measure_names)
        rows = []
        
        for row_idx, row_info in enumerate(row_tuples):
            row_data = row_info.copy()
            
            for col_idx, measure_name in enumerate(measure_names):
                ordinal = row_idx * num_measures + col_idx
                value = cells.get(ordinal)
                row_data[measure_name] = value
            
            rows.append(row_data)
        
        log(f"   Built {len(rows)} data rows")
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            log("   No data rows built")
            return None
            
    except Exception as e:
        log(f"   Error parsing XMLA CellData response: {e}")
        traceback.print_exc()
        return None

def parse_sales_channel_daily_response(xml_response, logger=None):
    """
    Parse XMLA response for Sales Channel Daily query (5 dimensions, 5 measures).
    
    Args:
        xml_response: XML string from XMLA Execute response
        logger: Optional logger
    
    Returns:
        pandas DataFrame with columns:
        - StoreNumber, CalendarDate, SourceActor, SourceChannel, DayPart (dimensions)
        - TY Net Sales USD, TY Orders, Discounts USD, LY Net Sales USD, LY Orders (measures)
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        root = ET.fromstring(xml_response)
        
        # Define namespaces
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # Find the root element
        root_elem = root.find('.//mdd:root', ns)
        if root_elem is None:
            log("   Could not find mddataset:root element")
            return None
        
        # Extract axis info
        axes = root_elem.find('mdd:Axes', ns)
        cell_data = root_elem.find('mdd:CellData', ns)
        
        if axes is None or cell_data is None:
            log("   Missing Axes or CellData elements")
            return None
        
        # Parse Column Axis (Axis0 - Measures)
        axis0 = axes.find('.//mdd:Axis[@name="Axis0"]', ns)
        measure_names = []
        if axis0 is not None:
            for tuple_elem in axis0.findall('.//mdd:Tuple', ns):
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    caption = member.find('mdd:Caption', ns)
                    if caption is not None:
                        measure_names.append(caption.text)
        
        log(f"   Found {len(measure_names)} measures: {measure_names}")
        
        # Parse Row Axis (Axis1 - Store × Date × SourceActor × SourceChannel × DayPart)
        axis1 = axes.find('.//mdd:Axis[@name="Axis1"]', ns)
        row_tuples = []
        if axis1 is not None:
            for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                row_info = {}
                members = tuple_elem.findall('.//mdd:Member', ns)
                for member in members:
                    hierarchy = member.get('Hierarchy', '')
                    caption_elem = member.find('mdd:Caption', ns)
                    caption = caption_elem.text if caption_elem is not None else ''
                    
                    # Map hierarchies to dimension columns
                    if 'Store' in hierarchy:
                        row_info['StoreNumber'] = caption
                    elif 'Calendar' in hierarchy or 'Date' in hierarchy:
                        row_info['CalendarDate'] = caption
                    elif 'Source Actor' in hierarchy:
                        row_info['SourceActor'] = caption
                    elif 'Source Channel' in hierarchy:
                        row_info['SourceChannel'] = caption
                    elif 'Day Part' in hierarchy:
                        row_info['DayPart'] = caption
                
                row_tuples.append(row_info)
        
        log(f"   Found {len(row_tuples)} row tuples")
        
        # Parse Cells
        cells = {}
        for cell in cell_data.findall('.//mdd:Cell', ns):
            ordinal = int(cell.get('CellOrdinal', -1))
            value_elem = cell.find('mdd:Value', ns)
            value = value_elem.text if value_elem is not None else None
            cells[ordinal] = value
        
        log(f"   Found {len(cells)} cell values")
        
        # Build DataFrame: Map cells to rows
        # CellOrdinal = row_idx * num_measures + col_idx
        num_measures = len(measure_names)
        rows = []
        
        for row_idx, row_info in enumerate(row_tuples):
            row_data = row_info.copy()
            
            for col_idx, measure_name in enumerate(measure_names):
                ordinal = row_idx * num_measures + col_idx
                value = cells.get(ordinal)
                row_data[measure_name] = value
            
            rows.append(row_data)
        
        log(f"   Built {len(rows)} data rows")
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            log("   No data rows built")
            return None
            
    except Exception as e:
        log(f"   Error parsing Sales Channel Daily response: {e}")
        traceback.print_exc()
        return None


def parse_offers_response(xml_response, logger=None):
    """
    Parse XMLA response for Offers query (4 dimensions).
    
    Args:
        xml_response: XML string from XMLA Execute response
        logger: Optional logger
    
    Returns:
        pandas DataFrame with columns:
        - StoreNumber, CalendarDate, OfferCode, OfferPOSDescription (dimensions)
        - Measures...
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        root = ET.fromstring(xml_response)
        
        # Define namespaces
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # Find the root element
        root_elem = root.find('.//mdd:root', ns)
        if root_elem is None:
            log("   Could not find mddataset:root element")
            return None
        
        # Extract axis info
        axes = root_elem.find('mdd:Axes', ns)
        cell_data = root_elem.find('mdd:CellData', ns)
        
        if axes is None or cell_data is None:
            log("   Missing Axes or CellData elements")
            return None
        
        # Parse Column Axis (Axis0 - Measures)
        axis0 = axes.find('.//mdd:Axis[@name="Axis0"]', ns)
        measure_names = []
        if axis0 is not None:
            for tuple_elem in axis0.findall('.//mdd:Tuple', ns):
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    caption = member.find('mdd:Caption', ns)
                    if caption is not None:
                        measure_names.append(caption.text)
        
        log(f"   Found {len(measure_names)} measures: {measure_names}")
        
        # Parse Row Axis (Axis1 - Calendar × Store × Offer Code × Offer POS Description)
        axis1 = axes.find('.//mdd:Axis[@name="Axis1"]', ns)
        row_tuples = []
        if axis1 is not None:
            for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                row_info = {}
                members = tuple_elem.findall('.//mdd:Member', ns)
                for member in members:
                    hierarchy = member.get('Hierarchy', '')
                    caption_elem = member.find('mdd:Caption', ns)
                    caption = caption_elem.text if caption_elem is not None else ''
                    
                    # Map hierarchies to dimension columns
                    if 'Store' in hierarchy:
                        row_info['StoreNumber'] = caption
                    elif 'Calendar' in hierarchy or 'Date' in hierarchy:
                        row_info['CalendarDate'] = caption
                    elif 'Offer Code' in hierarchy and 'Offer Code Hierarchy' in hierarchy:
                        row_info['OfferCode'] = caption
                    elif 'Offer POS Description' in hierarchy:
                        row_info['OfferPOSDescription'] = caption
                
                row_tuples.append(row_info)
        
        log(f"   Found {len(row_tuples)} row tuples")
        
        # Parse Cells
        cells = {}
        for cell in cell_data.findall('.//mdd:Cell', ns):
            ordinal = int(cell.get('CellOrdinal', -1))
            value_elem = cell.find('mdd:Value', ns)
            value = value_elem.text if value_elem is not None else None
            cells[ordinal] = value
        
        log(f"   Found {len(cells)} cell values")
        
        # Build DataFrame: Map cells to rows
        # CellOrdinal = row_idx * num_measures + col_idx
        num_measures = len(measure_names)
        rows = []
        
        for row_idx, row_info in enumerate(row_tuples):
            row_data = row_info.copy()
            
            for col_idx, measure_name in enumerate(measure_names):
                ordinal = row_idx * num_measures + col_idx
                value = cells.get(ordinal)
                row_data[measure_name] = value
            
            rows.append(row_data)
        
        log(f"   Built {len(rows)} data rows")
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            log("   No data rows built")
            return None
            
    except Exception as e:
        log(f"   Error parsing Offers response: {e}")
        traceback.print_exc()
        return None


def parse_inventory_response(xml_response, logger=None):
    """
    Parse XMLA response for Inventory query (measure on COLUMNS, 4 dimensions on ROWS).
    
    Args:
        xml_response: XML string from XMLA Execute response
        logger: Optional logger
    
    Returns:
        pandas DataFrame with columns:
        - ItemNumber, CalendarDate, StoreNumber, ItemDescription (dimensions)
        - Qty On Hand (measure)
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        root = ET.fromstring(xml_response)
        
        # Define namespaces
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmla': 'urn:schemas-microsoft-com:xml-analysis',
            'mdd': 'urn:schemas-microsoft-com:xml-analysis:mddataset'
        }
        
        # Find the root element
        root_elem = root.find('.//mdd:root', ns)
        if root_elem is None:
            log("   Could not find mddataset:root element")
            return None
        
        # Extract axis info
        axes = root_elem.find('mdd:Axes', ns)
        cell_data = root_elem.find('mdd:CellData', ns)
        
        if axes is None or cell_data is None:
            log("   Missing Axes or CellData elements")
            return None
        
        # Parse Column Axis (Axis0 - Measures)
        axis0 = axes.find('.//mdd:Axis[@name="Axis0"]', ns)
        measure_names = []
        if axis0 is not None:
            for tuple_elem in axis0.findall('.//mdd:Tuple', ns):
                for member in tuple_elem.findall('.//mdd:Member', ns):
                    caption = member.find('mdd:Caption', ns)
                    if caption is not None:
                        measure_names.append(caption.text)
        
        log(f"   Found {len(measure_names)} measures")
        
        # Parse Row Axis (Axis1 - ItemNumber × Date × Store × ItemDescription)
        axis1 = axes.find('.//mdd:Axis[@name="Axis1"]', ns)
        row_tuples = []
        if axis1 is not None:
            for tuple_elem in axis1.findall('.//mdd:Tuple', ns):
                row_info = {}
                members = tuple_elem.findall('.//mdd:Member', ns)
                for member in members:
                    hierarchy = member.get('Hierarchy', '')
                    caption_elem = member.find('mdd:Caption', ns)
                    caption = caption_elem.text if caption_elem is not None else ''
                    
                    # Map hierarchies to dimension columns
                    if 'Item_Number' in hierarchy:
                        row_info['ItemNumber'] = caption
                    elif 'Calendar' in hierarchy or 'Date' in hierarchy:
                        row_info['CalendarDate'] = caption
                    elif 'Store' in hierarchy:
                        row_info['StoreNumber'] = caption
                    elif 'Item_Description' in hierarchy or 'Description' in hierarchy:
                        row_info['ItemDescription'] = caption
                
                row_tuples.append(row_info)
        
        log(f"   Found {len(row_tuples)} row tuples")
        
        # Parse Cells
        cells = {}
        for cell in cell_data.findall('.//mdd:Cell', ns):
            ordinal = int(cell.get('CellOrdinal', -1))
            value_elem = cell.find('mdd:Value', ns)
            value = value_elem.text if value_elem is not None else None
            cells[ordinal] = value
        
        log(f"   Found {len(cells)} cell values")
        
        # Build DataFrame: Map cells to rows
        # CellOrdinal = row_idx * num_measures + col_idx
        num_measures = len(measure_names)
        rows = []
        
        for row_idx, row_info in enumerate(row_tuples):
            row_data = row_info.copy()
            
            for col_idx, measure_name in enumerate(measure_names):
                ordinal = row_idx * num_measures + col_idx
                value = cells.get(ordinal)
                row_data[measure_name] = value
            
            rows.append(row_data)
        
        log(f"   Built {len(rows)} data rows")
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            log("   No data rows built")
            return None
            
    except Exception as e:
        log(f"   Error parsing Inventory response: {e}")
        traceback.print_exc()
        return None

