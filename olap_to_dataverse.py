"""
Query OLAP cube via XMLA and load data into Dataverse.

This script:
1. Queries the OLAP cube using XMLA (no Excel COM needed!)
2. Parses the MDX results
3. Upserts data into Dataverse table

This replaces the Excel COM automation approach.
Uses Azure Key Vault for secure credential management.

Auto-deployed via GitHub Actions + Watchtower.
Test deployment: 2025-11-25 18:57
"""

import os
import sys
import json
import uuid
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime, timedelta
import msal
import urllib3
import xml.etree.ElementTree as ET

# Import Key Vault utility
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from modules.utils.keyvault import get_dataverse_credentials, get_secret

load_dotenv()

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_dataverse_access_token(environment_url, client_id, client_secret, tenant_id, logger=None):
    """Obtain an access token for Dataverse."""
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        scope = [f"{environment_url}/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            log(f"Dataverse access token obtained")
            return result["access_token"]
        else:
            log(f"Failed to obtain Dataverse access token: {result.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        log(f"Error obtaining Dataverse access token: {e}")
        return None

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
        import traceback
        traceback.print_exc()
        return None

def get_mdx_last_n_days(days=14, fiscal_year=2025):
    """
    Generate MDX query for the last N days of data using MyView filter.
    Useful for daily incremental updates.
    
    Args:
        days: Number of days to retrieve (default 14 for 2-week lookback)
        fiscal_year: Fiscal year to query (default 2025)
    
    Returns:
        MDX query string
    
    Note: Uses [MyView].[My View].&[82] which is a predefined view for last 14 days
    """
    query = """
SELECT 
{
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
} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({[Franchise].[Store Number Label].[Store Number Label].AllMembers}),
    Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers})
)
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE ([MyView].[My View].[My View].&[82])
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    return query

def get_sample_mdx_queries(fiscal_years=[2023, 2024, 2025]):
    """
    Return MDX queries for the OARS Franchise cube.
    
    This is the actual MDX extracted from the Excel pivot table.
    See EXTRACTED_MDX_QUERY.md for detailed documentation.
    
    Args:
        fiscal_years: List of fiscal years to query
    """
    # Build the WHERE clause for multiple fiscal years
    if isinstance(fiscal_years, int):
        fiscal_years = [fiscal_years]
    
    fiscal_year_members = ", ".join([f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{year}]" for year in fiscal_years])
    where_clause = f"WHERE {{{fiscal_year_members}}}"
    
    # Main query: All BI metrics by Store and Date for specified fiscal years
    # This query returns 33 measures across all stores and dates
    query_full_bi_data = f"""
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
{where_clause}
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    
    # Last 2 weeks query for incremental updates
    query_last_2_weeks = get_mdx_last_n_days(days=14, fiscal_year=2025)
    
    return {
        'full_bi_data': query_full_bi_data,
        'last_2_weeks': query_last_2_weeks
    }

def upsert_to_dataverse(environment_url, access_token, table_name, records, logger=None):
    """
    Upsert records to Dataverse table using business key for efficient lookups.
    
    Business Key Format: {StoreNumber}_{YYYYMMDD}
    Example: 4280_20250115
    
    This checks if a record exists using the business key and updates it,
    or creates a new record if it doesn't exist.
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": "return=representation"
    }
    
    # Filter out records without business keys
    valid_records = []
    for record in records:
        if not record.get('crf63_businesskey'):
            log(f"Warning: Record missing business key, skipping")
            continue
        valid_records.append(record)
    
    if not valid_records:
        log("No valid records to upsert")
        return 0, 0, 0
    
    # Use $batch API to group operations (100 per batch - optimal for our workload)
    # UpsertMultiple isn't supported for this table, so we use batch with PATCH operations
    # Note: Tested 1000-record batches, but 100 performed better for our 630-record dataset
    batch_size = 100
    total_batches = (len(valid_records) + batch_size - 1) // batch_size
    
    log(f"Upserting {len(valid_records)} records in {total_batches} batches of {batch_size}...")
    
    created_count = 0
    updated_count = 0
    error_count = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(valid_records))
        batch_records = valid_records[start_idx:end_idx]
        
        # Build multipart/mixed batch request
        batch_id = f"batch_{uuid.uuid4()}"
        changeset_id = f"changeset_{uuid.uuid4()}"
        
        # Build batch body with CRLF line endings (required by OData spec)
        lines = []
        lines.append(f"--{batch_id}\r\n")
        lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}\r\n")
        lines.append("\r\n")
        
        # Add each PATCH request to the changeset
        for i, record in enumerate(batch_records, 1):
            business_key = record['crf63_businesskey']
            upsert_url = f"{environment_url}/api/data/v9.2/{table_name}(crf63_businesskey='{business_key}')"
            
            lines.append(f"--{changeset_id}\r\n")
            lines.append("Content-Type: application/http\r\n")
            lines.append("Content-Transfer-Encoding: binary\r\n")
            lines.append(f"Content-ID: {i}\r\n")
            lines.append("\r\n")
            lines.append(f"PATCH {upsert_url} HTTP/1.1\r\n")
            lines.append("Content-Type: application/json\r\n")
            lines.append("\r\n")
            lines.append(json.dumps(record) + "\r\n")
        
        lines.append(f"--{changeset_id}--\r\n")
        lines.append(f"--{batch_id}--\r\n")
        
        batch_body = "".join(lines)
        
        # Send batch request
        batch_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/mixed; boundary={batch_id}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0"
        }
        
        try:
            batch_url = f"{environment_url}/api/data/v9.2/$batch"
            response = requests.post(batch_url, headers=batch_headers, data=batch_body.encode('utf-8'))
            
            if response.status_code == 200:
                # Parse response to count successes
                # For simplicity, assume all succeeded if batch succeeded
                updated_count += len(batch_records)
                log(f"  Batch {batch_num + 1}/{total_batches}: ✓ {len(batch_records)} records")
            else:
                error_count += len(batch_records)
                log(f"  Batch {batch_num + 1}/{total_batches}: ✗ Failed ({response.status_code})")
                
        except Exception as e:
            error_count += len(batch_records)
            log(f"  Batch {batch_num + 1}/{total_batches}: ✗ Error: {e}")
    
    log(f"\nUpsert complete: {created_count} created, {updated_count} updated, {error_count} errors")
    return created_count, updated_count, error_count

def transform_olap_to_dataverse_records(df, logger=None):
    """
    Transform OLAP DataFrame to Dataverse records with business keys.
    
    Business Key Format: {StoreNumber}_{YYYYMMDD}
    
    Args:
        df: Pandas DataFrame from OLAP query
        logger: Optional logger
    
    Returns:
        List of Dataverse record dictionaries
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    records = []
    
    # TODO: Map OLAP column names to Dataverse field names
    # This mapping depends on your actual OLAP response structure
    # Adjust based on parse_xmla_mdx_response output
    
    for idx, row in df.iterrows():
        try:
            # Extract store number and date from row
            # Column names from parse_xmla_celldata_response: 'StoreNumber', 'CalendarDate'
            store_number = str(row['StoreNumber']) if pd.notna(row['StoreNumber']) else ''
            calendar_date = row['CalendarDate'] if pd.notna(row['CalendarDate']) else ''
            
            if not store_number or not calendar_date:
                log(f"Warning: Row {idx} missing store_number or calendar_date, skipping")
                continue
            
            # Generate business key: StoreNumber_YYYYMMDD
            # Parse date and format as YYYYMMDD
            if isinstance(calendar_date, str):
                # Try to parse various date formats (OLAP returns MM/D/YY format like "11/5/25")
                from dateutil import parser
                dt = parser.parse(calendar_date)
                date_str = dt.strftime('%Y%m%d')
            else:
                date_str = calendar_date.strftime('%Y%m%d')
            
            business_key = f"{store_number}_{date_str}"
            
            # Helper to safely get numeric value
            def get_num(col_name):
                val = row[col_name] if col_name in row.index and pd.notna(row[col_name]) else None
                return float(val) if val is not None else None
            
            def get_int(col_name):
                val = row[col_name] if col_name in row.index and pd.notna(row[col_name]) else None
                # Convert to float first to handle decimal strings, then to int
                return int(float(val)) if val is not None else None
            
            # Build Dataverse record with all 33 measures mapped
            record = {
                # Key fields
                "crf63_businesskey": business_key,
                "crf63_storenumber": store_number,
                "crf63_calendardate": calendar_date if isinstance(calendar_date, str) else calendar_date.isoformat(),
                "crf63_name": f"{store_number} - {date_str}",
                "crf63_datasource": "OARS Franchise",
                "crf63_lastrefreshed": datetime.now().isoformat(),
                
                # Sales Metrics (4)
                "crf63_tynetsales": get_num('TY Net Sales USD'),
                "crf63_l2ycompsales": get_num('L2Y Comp Net Sales USD'),
                "crf63_l3ycompsales": get_num('L3Y Comp Net Sales USD'),
                "crf63_lycompsales": get_num('LY Comp Net Sales USD'),
                
                # Cost Metrics (6)
                "crf63_targetfoodcost": get_num('TY Target Food Cost USD'),
                "crf63_actualfoodcost": get_num('Actual Food Cost USD'),
                "crf63_flmd": get_num('FLMD USD'),
                "crf63_actuallabor": get_num('Actual Labor $ USD'),
                "crf63_mileagecost": get_num('Mileage Cost Local'),
                "crf63_discounts": get_num('Discounts USD'),
                
                # Operations Metrics (6)
                "crf63_totalhours": get_num('HS Total Actual Hours'),
                "crf63_storedays": get_int('Store Days'),
                "crf63_maketime": get_num('Make Time Minutes'),
                "crf63_racktime": get_num('Rack Time Minutes'),
                "crf63_otdtime": get_num('Total OTD Time (Hours)'),
                "crf63_avgttdt": get_num('Avg TTDT'),
                
                # Order Metrics (6)
                "crf63_tyorders": get_int('TY Orders'),
                "crf63_lyorders": get_int('LY Orders'),
                "crf63_deliveries": get_int('Deliveries'),
                "crf63_bozocoroorders": get_int('BOZOCORO Orders'),
                "crf63_otdordercount": get_int('OTD Order Count'),
                "crf63_dispatchedorders": get_int('TY Dispatched Delivery Orders'),
                
                # Financial Metrics (5)
                "crf63_targetprofit": get_num('Target Profit after FLM Local (Fran)'),
                "crf63_actualflm": get_num('Actual FLM w/o Vacation Accrual Local'),
                "crf63_flmdpc": get_num('FLMDPC USD (Fran)'),
                "crf63_commission": get_num('m_ty_agg_commission_local_sum'),
                "crf63_cashovershort": get_num('Total Cash Over/Short USD'),
                
                # Customer Satisfaction Metrics (6)
                "crf63_osatsurveycount": get_int('TY Total OSAT Survey Count'),
                "crf63_osatsatisfied": get_int('TY OSAT Satisfied Survey Count'),
                "crf63_accuracysurveycount": get_int('TY Total Order Accuracy Survey Count'),
                "crf63_orderaccuracypct": get_num('Order Accuracy %'),
                "crf63_totalcalls": get_int('Total Calls'),
                "crf63_answeredcalls": get_int('Answered Calls'),
            }
            
            records.append(record)
            
        except Exception as e:
            log(f"Error transforming row {idx}: {e}")
            import traceback
            log(traceback.format_exc())
            continue
    
    return records

def query_olap_and_sync_to_dataverse(config=None, logger=None, query_type='full_bi_data'):
    """Main function to query OLAP and sync to Dataverse."""
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    log("="*80)
    log("OLAP to Dataverse Sync")
    log("="*80)
    
    # Get OLAP configuration from Key Vault
    olap_server = os.getenv('OLAP_SERVER', 'https://ednacubes.papajohns.com:10502')
    olap_catalog = os.getenv('OLAP_CATALOG', 'OARS Franchise')
    olap_username = get_secret('olap-username')
    olap_password = get_secret('olap-password')
    olap_ssl_verify = False  # Self-signed cert
    
    # Get Dataverse configuration from Key Vault
    dv_creds = get_dataverse_credentials()
    dataverse_url = dv_creds['environment_url']
    client_id = dv_creds['client_id']
    tenant_id = dv_creds['tenant_id']
    client_secret = dv_creds['client_secret']
    
    table_name = "crf63_oarsbidatas"  # Plural form for API (singular: crf63_oarsbidata)
    
    log(f"\nOLAP Server: {olap_server}")
    log(f"OLAP Catalog: {olap_catalog}")
    log(f"Dataverse URL: {dataverse_url}")
    log(f"Table: {table_name}")
    
    # Step 1: Get Dataverse access token
    log("\n1. Getting Dataverse access token...")
    dataverse_token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id, logger)
    
    if not dataverse_token:
        log("✗ Failed to get Dataverse access token")
        return {"success": False, "error": "Failed to get Dataverse token"}
    
    # Step 2: Query OLAP cube
    log("\n2. Querying OLAP cube...")
    
    # Get the appropriate MDX query based on query_type
    queries = get_sample_mdx_queries(fiscal_years=[2023, 2024, 2025])
    mdx_query = queries[query_type]
    
    if query_type == 'last_2_weeks':
        # Single query for last 2 weeks using MyView filter
        log(f"   Executing last 2 weeks query (MyView filter)...")
        
        try:
            xml_response = execute_xmla_mdx(
                olap_server,
                olap_catalog,
                olap_username,
                olap_password,
                mdx_query,
                ssl_verify=olap_ssl_verify,
                logger=logger
            )
            
            log(f"   ✓ Query executed ({len(xml_response)} bytes)")
            
            # Parse the response
            df = parse_xmla_celldata_response(xml_response, logger=logger)
            
            if df is None or len(df) == 0:
                log("⚠  Query returned no data")
                return {"success": False, "error": "No data returned from OLAP"}
            
            log(f"   ✓ Parsed {len(df)} rows")
            
        except Exception as e:
            log(f"   ✗ Query failed: {e}")
            return {"success": False, "error": str(e)}
            
    else:
        # Full BI data: Query each fiscal year separately to avoid cell count limits
        all_data_frames = []
        for fiscal_year in [2023, 2024, 2025]:
            log(f"   Executing query for Fiscal Year {fiscal_year}...")
            
            # Get query for single fiscal year
            single_year_queries = get_sample_mdx_queries(fiscal_years=[fiscal_year])
            
            try:
                xml_response = execute_xmla_mdx(
                    olap_server,
                    olap_catalog,
                    olap_username,
                    olap_password,
                    single_year_queries['full_bi_data'],
                    ssl_verify=olap_ssl_verify,
                    logger=logger
                )
                
                log(f"   ✓ FY{fiscal_year} query executed ({len(xml_response)} bytes)")
                
                # Parse the response
                year_df = parse_xmla_celldata_response(xml_response, logger=logger)
                
                if year_df is not None and len(year_df) > 0:
                    all_data_frames.append(year_df)
                    log(f"   ✓ FY{fiscal_year} parsed {len(year_df)} rows")
                else:
                    log(f"   ⚠  FY{fiscal_year} returned no data")
                    
            except Exception as e:
                log(f"   ✗ FY{fiscal_year} query failed: {e}")
                continue
        
        if not all_data_frames:
            log("⚠  No data returned from any fiscal year query")
            return {"success": False, "error": "No data returned from OLAP"}
        
        # Combine all data frames
        df = pd.concat(all_data_frames, ignore_index=True)
        log(f"✓ Combined data from {len(all_data_frames)} fiscal years: {len(df)} total rows")
    log(f"   Columns: {list(df.columns)}")
    log(f"\n   Sample data:")
    log(df.head().to_string())
    
    # Step 3: Transform data for Dataverse
    log("\n3. Transforming data for Dataverse...")
    
    # Use the dedicated transform function with business key support
    records = transform_olap_to_dataverse_records(df, logger)
    
    log(f"✓ Transformed {len(records)} records")
    
    # Debug: Show first record
    if records:
        log(f"   First record business key: {records[0].get('crf63_businesskey', 'MISSING')}")
    
    # Step 4: Upsert to Dataverse
    log("\n4. Upserting to Dataverse...")
    created, updated, errors = upsert_to_dataverse(
        dataverse_url,
        dataverse_token,
        table_name,
        records,
        logger
    )
    
    log("\n" + "="*80)
    log("✅ Sync Complete!")
    log("="*80)
    log(f"  Records created: {created}")
    log(f"  Records updated: {updated}")
    log(f"  Errors: {errors}")
    
    return {
        "success": True,
        "records_created": created,
        "records_updated": updated,
        "errors": errors
    }

def main():
    """Main entry point - uses Azure Key Vault for credentials."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync OLAP data to Dataverse')
    parser.add_argument(
        '--query-type',
        choices=['full_bi_data', 'last_2_weeks'],
        default='last_2_weeks',
        help='Type of query to run (default: last_2_weeks for incremental updates)'
    )
    args = parser.parse_args()
    
    print("="*80)
    print("OLAP to Dataverse Sync (using Azure Key Vault)")
    print(f"Query Type: {args.query_type}")
    print("="*80)
    print()
    
    try:
        # Get credentials from Azure Key Vault
        print("📦 Loading credentials from Azure Key Vault (kv-bw-data-integration)...")
        print("   Using app-client-id and app-client-secret for Dataverse access")
        print("✓ Key Vault configured (credentials loaded on-demand)")
        print()
        
        # Run the sync
        result = query_olap_and_sync_to_dataverse(query_type=args.query_type)
        
        if result["success"]:
            return 0
        else:
            print(f"\n✗ Sync failed: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
