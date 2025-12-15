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
import time
import concurrent.futures
from typing import List, Tuple
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

def load_config():
    """Load configuration from JSON file based on environment."""
    env = os.getenv('ENVIRONMENT', 'production')
    config_path = os.path.join(os.path.dirname(__file__), 'config', f'config.{env}.json')
    
    if not os.path.exists(config_path):
        # Fallback to production config
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.production.json')
    
    with open(config_path, 'r') as f:
        return json.load(f)

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

def get_graph_access_token(client_id, client_secret, tenant_id, logger=None):
    """Obtain an access token for Microsoft Graph API."""
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
        scope = ["https://graph.microsoft.com/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            log(f"Microsoft Graph access token obtained")
            return result["access_token"]
        else:
            log(f"Failed to obtain Graph access token: {result.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        log(f"Error obtaining Graph access token: {e}")
        return None

def send_email_notification(subject, body, recipients=None, is_html=False, logger=None):
    """
    Send email notification using Microsoft Graph API.
    
    Args:
        subject: Email subject line
        body: Email body content
        recipients: List of email addresses (defaults to config file recipients)
        is_html: Whether body is HTML (default: False for plain text)
        logger: Optional logger
    
    Returns:
        True if successful, False otherwise
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        # Load config to get recipients and Azure credentials
        config = load_config()
        
        # Check if email notifications are enabled
        if not config.get('email_notifications', {}).get('enabled', False):
            log("Email notifications are disabled in config")
            return False
        
        # Use provided recipients or fall back to config
        if recipients is None:
            recipients = config.get('email_notifications', {}).get('recipients', [])
        
        if not recipients:
            log("No email recipients configured")
            return False
        
        # Get Azure credentials
        tenant_id = config['azure']['tenant_id']
        client_id = config['azure']['app_client_id']
        client_secret = os.getenv('AZURE_CLIENT_SECRET')
        
        if not client_secret:
            # Try to get from Key Vault
            client_secret = get_secret('app-client-secret')
        
        if not client_secret:
            log("Failed to get Azure client secret for email")
            return False
        
        # Get Graph API access token
        access_token = get_graph_access_token(client_id, client_secret, tenant_id, logger)
        
        if not access_token:
            log("Failed to get Microsoft Graph access token")
            return False
        
        # Build recipient list
        to_recipients = [{"emailAddress": {"address": email}} for email in recipients]
        
        # Build email message
        content_type = "HTML" if is_html else "Text"
        message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": content_type,
                    "content": body
                },
                "toRecipients": to_recipients
            },
            "saveToSentItems": "true"
        }
        
        # Send email using Graph API (application permission - send as specific user)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Get sender email from config or use first recipient
        sender_email = config.get('email_notifications', {}).get('sender', recipients[0] if recipients else None)
        
        if not sender_email:
            log("No sender email configured")
            return False
        
        # Note: This requires Mail.Send application permission with admin consent
        # Sends on behalf of the specified user
        graph_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        
        response = requests.post(graph_url, headers=headers, json=message)
        
        if response.status_code == 202:
            log(f"✓ Email sent successfully to {len(recipients)} recipient(s)")
            return True
        else:
            log(f"✗ Failed to send email: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        log(f"Error sending email notification: {e}")
        import traceback
        log(traceback.format_exc())
        return False

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
        days: Number of days to retrieve (7 or 14)
        fiscal_year: Fiscal year to query (default 2025)
    
    Returns:
        MDX query string
    
    Note: 
        - Uses [MyView].[My View].&[81] for 7 days (1 week)
        - Uses [MyView].[My View].&[82] for 14 days (2 weeks)
    """
    # Map days to MyView ID
    myview_id = 81 if days == 7 else 82
    
    query = f"""
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
    [Measures].[Order Accuracy %],
    [Measures].[SMG Avg Closure],
    [Measures].[SMG Cases Opened],
    [Measures].[SMG Cases Resolved],
    [Measures].[SMG Value %],
    [Measures].[Singles],
    [Measures].[Doubles],
    [Measures].[Triples Plus],
    [Measures].[Runs],
    [Measures].[TTDT Orders],
    [Measures].[To The Door Time for Dispatch Orders],
    [Measures].[To The Door Time Minutes],
    [Measures].[TY Taste Of Food Good Survey Count],
    [Measures].[TY Total Taste Of Food Survey Count],
    [Measures].[TY Order Accuracy Good Survey Count]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({{[Franchise].[Store Number Label].[Store Number Label].AllMembers}}),
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}})
)
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE ([MyView].[My View].[My View].&[{myview_id}])
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
    # This query returns 47 measures (33 original + 14 service metrics) across all stores and dates
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
    [Measures].[Order Accuracy %],
    [Measures].[SMG Avg Closure],
    [Measures].[SMG Cases Opened],
    [Measures].[SMG Cases Resolved],
    [Measures].[SMG Value %],
    [Measures].[Singles],
    [Measures].[Doubles],
    [Measures].[Triples Plus],
    [Measures].[Runs],
    [Measures].[TTDT Orders],
    [Measures].[To The Door Time for Dispatch Orders],
    [Measures].[To The Door Time Minutes],
    [Measures].[TY Taste Of Food Good Survey Count],
    [Measures].[TY Total Taste Of Food Survey Count],
    [Measures].[TY Order Accuracy Good Survey Count]
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
    
    # Last 1 week and 2 weeks queries for incremental updates
    query_last_1_week = get_mdx_last_n_days(days=7, fiscal_year=2025)
    query_last_2_weeks = get_mdx_last_n_days(days=14, fiscal_year=2025)
    
    return {
        'full_bi_data': query_full_bi_data,
        'last_1_week': query_last_1_week,
        'last_2_weeks': query_last_2_weeks
    }


def get_sales_channel_daily_mdx():
    """
    Generate MDX query for Sales Channel Daily data.
    
    Dimensions (5):
        - Store Number Label
        - Calendar Date
        - Source Actor (Android, iOS, Desktop Web, DoorDash, etc.)
        - Source Channel (App, Web, Aggregator, Phone, Store, etc.)
        - Day Part (Lunch, Dinner, Afternoon, Evening)
    
    Measures (5):
        - TY Net Sales USD
        - TY Orders
        - Discounts USD
        - LY Net Sales USD
        - LY Orders
    
    Uses MyView 81 for last 1 week (7 days) of data.
    
    Returns:
        MDX query string
    """
    query = """SELECT {[Measures].[TY Net Sales USD],[Measures].[TY Orders],[Measures].[Discounts USD],[Measures].[LY Net Sales USD],[Measures].[LY Orders]} DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS , NON EMPTY CrossJoin(CrossJoin(CrossJoin(CrossJoin(Hierarchize({[Franchise].[Store Number Label].[Store Number Label].AllMembers}), Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers})), Hierarchize({[Source Channel].[Source Actor].[Source Actor].AllMembers})), Hierarchize({[Source Channel].[Source Channel].[Source Channel].AllMembers})), Hierarchize({[Day Part Dimension].[Day Part].[Day Part].AllMembers})) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  FROM [OARS Franchise] WHERE ([MyView].[My View].[My View].&[81]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
    return query


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
        import traceback
        traceback.print_exc()
        return None


def transform_sales_channel_daily_records(df, logger=None):
    """
    Transform Sales Channel Daily DataFrame to Dataverse records.
    
    Business Key Format: {StoreNumber}_{YYYYMMDD}_{SourceActor}_{SourceChannel}_{DayPart}
    Example: 125_20250209_Android_App_Dinner
    
    Args:
        df: Pandas DataFrame from OLAP query
        logger: Optional logger
    
    Returns:
        List of Dataverse record dictionaries for crf63_saleschanneldaily table
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    records = []
    
    for idx, row in df.iterrows():
        try:
            # Extract dimension values
            store_number = str(row.get('StoreNumber', '')) if pd.notna(row.get('StoreNumber')) else ''
            calendar_date = row.get('CalendarDate', '') if pd.notna(row.get('CalendarDate')) else ''
            source_actor = str(row.get('SourceActor', '')) if pd.notna(row.get('SourceActor')) else ''
            source_channel = str(row.get('SourceChannel', '')) if pd.notna(row.get('SourceChannel')) else ''
            day_part = str(row.get('DayPart', '')) if pd.notna(row.get('DayPart')) else ''
            
            # Skip rows with missing key fields
            if not store_number or not calendar_date:
                continue
            
            # Parse date and format as YYYYMMDD for business key
            if isinstance(calendar_date, str):
                from dateutil import parser
                dt = parser.parse(calendar_date)
                date_str = dt.strftime('%Y%m%d')
                date_iso = dt.strftime('%Y-%m-%d')
            else:
                date_str = calendar_date.strftime('%Y%m%d')
                date_iso = calendar_date.strftime('%Y-%m-%d')
            
            # Generate business key: StoreNumber_YYYYMMDD_SourceActor_SourceChannel_DayPart
            # Clean values for business key (replace spaces with underscores, remove special chars)
            actor_clean = source_actor.replace(' ', '_').replace('-', '_') if source_actor else 'Unknown'
            channel_clean = source_channel.replace(' ', '_').replace('-', '_') if source_channel else 'Unknown'
            daypart_clean = day_part.replace(' ', '_') if day_part else 'Unknown'
            
            business_key = f"{store_number}_{date_str}_{actor_clean}_{channel_clean}_{daypart_clean}"
            
            # Helper to safely get numeric value
            def get_num(col_name):
                val = row.get(col_name) if col_name in row.index and pd.notna(row.get(col_name)) else None
                return float(val) if val is not None else None
            
            def get_int(col_name):
                val = row.get(col_name) if col_name in row.index and pd.notna(row.get(col_name)) else None
                return int(float(val)) if val is not None else None
            
            # Build Dataverse record
            record = {
                # Key fields (dimensions)
                "crf63_businesskey": business_key,
                "crf63_storenumber": store_number,
                "crf63_calendardate": date_iso,
                "crf63_sourceactor": source_actor,
                "crf63_sourcechannel": source_channel,
                "crf63_daypart": day_part,
                
                # Display name
                "crf63_name": f"{store_number} - {date_str} - {source_channel} - {day_part}",
                
                # Measures
                "crf63_tynetsalesusd": get_num('TY Net Sales USD'),
                "crf63_tyorders": get_int('TY Orders'),
                "crf63_discountsusd": get_num('Discounts USD'),
                "crf63_lynetsalesusd": get_num('LY Net Sales USD'),
                "crf63_lyorders": get_int('LY Orders'),
                
                # Metadata
                "crf63_lastrefreshed": datetime.now().isoformat(),
            }
            
            records.append(record)
            
        except Exception as e:
            log(f"Error transforming row {idx}: {e}")
            import traceback
            log(traceback.format_exc())
            continue
    
    return records


def query_sales_channel_daily_and_sync(config=None, logger=None):
    """
    Query Sales Channel Daily data from OLAP and sync to Dataverse.
    
    Table: crf63_saleschanneldaily (Sales Channel Daily)
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    log("="*80)
    log("Sales Channel Daily - OLAP to Dataverse Sync")
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
    
    # Table for Sales Channel Daily data
    table_name = "crf63_saleschanneldailies"  # Plural form for API
    
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
    
    # Step 2: Query OLAP cube for Sales Channel Daily
    log("\n2. Querying OLAP cube (Sales Channel Daily)...")
    
    mdx_query = get_sales_channel_daily_mdx()
    log("   Using MyView 81 (1 week / 7 days filter)")
    
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
        
        # Parse the response using the 5-dimension parser
        df = parse_sales_channel_daily_response(xml_response, logger=logger)
        
        if df is None or len(df) == 0:
            log("⚠  Query returned no data")
            return {"success": False, "error": "No data returned from OLAP"}
        
        log(f"   ✓ Parsed {len(df)} rows")
        
    except Exception as e:
        log(f"   ✗ Query failed: {e}")
        import traceback
        log(traceback.format_exc())
        return {"success": False, "error": str(e)}
    
    log(f"   Columns: {list(df.columns)}")
    log(f"\n   Sample data:")
    log(df.head().to_string())
    
    # Step 3: Transform data for Dataverse
    log("\n3. Transforming data for Dataverse...")
    
    records = transform_sales_channel_daily_records(df, logger)
    
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
    log("✅ Sales Channel Daily Sync Complete!")
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


def upsert_to_dataverse(environment_url, access_token, table_name, records, logger=None):
    """
    ULTRA-FAST upsert using optimized batch method from load_csv.py.
    Achieves 1,800–2,600 rows/sec on production Dataverse environments.
    
    Key optimizations:
    - Batch size: 400 records (sweet spot for 2025)
    - Workers: 6 threads (no throttling)
    - Binary encoding (no string explosion)
    - Session with connection pooling
    - Retry logic for 429 errors
    - Prefer: odata.allow-upsert=true header
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    batch_url = f"{api_url}/$batch"

    # Filter valid records
    valid_records = [r for r in records if r.get("crf63_businesskey")]
    total = len(valid_records)
    if total == 0:
        log("No valid records to upsert")
        return 0, 0, 0

    # Setup session with connection pooling
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount('https://', adapter)
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Build batch function (binary encoding for speed)
    def build_batch(batch_records):
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        parts = [f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()]

        for i, rec in enumerate(batch_records, 1):
            clean_rec = {k: v for k, v in rec.items() if v is not None}
            key = clean_rec["crf63_businesskey"].replace("'", "''")
            payload = json.dumps(clean_rec, separators=(',', ':'))

            part = (
                f"--{changeset_id}\r\n"
                f"Content-Type: application/http\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                f"\r\n"
                f"PATCH {table_name}(crf63_businesskey='{key}') HTTP/1.1\r\n"
                f"Content-Type: application/json\r\n"
                f"Prefer: return=representation,odata.include-annotations=*\r\n"
                f"\r\n"
                f"{payload}\r\n"
            ).encode()
            parts.append(part)

        parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
        return b"".join(parts), batch_id

    def upsert_batch(chunk):
        body, batch_id = build_batch(chunk)
        headers = {
            "Content-Type": f"multipart/mixed; boundary={batch_id}",
            "Prefer": "odata.continue-on-error"
        }
        for _ in range(5):
            try:
                r = session.post(batch_url, headers=headers, data=body, timeout=600)
                if r.status_code in (200, 204):
                    # PATCH with return=representation returns HTTP 200 OK with entity body
                    # Check createdon vs modifiedon timestamps to distinguish create from update
                    import re
                    import json as json_module
                    from datetime import datetime
                    
                    created = 0
                    updated = 0
                    errors = 0
                    
                    # Split by changeset response boundaries
                    responses = re.split(r'--changesetresponse_[\da-f-]+', r.text)
                    
                    for resp in responses:
                        if 'HTTP/1.1 200 OK' in resp or 'HTTP/1.1 201 Created' in resp:
                            # Find JSON body - starts after double newline, extract complete JSON object
                            json_start = resp.find('\n\n{')
                            if json_start == -1:
                                json_start = resp.find('\r\n\r\n{')
                                if json_start != -1:
                                    json_start += 4
                            else:
                                json_start += 2
                            
                            if json_start > 0:
                                json_text = resp[json_start:]
                                # Find matching closing brace
                                brace_count = 0
                                json_end = 0
                                for i, char in enumerate(json_text):
                                    if char == '{': brace_count += 1
                                    elif char == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            json_end = i + 1
                                            break
                                
                                if json_end > 0:
                                    try:
                                        data = json_module.loads(json_text[:json_end])
                                        created_on = data.get('createdon', '')
                                        modified_on = data.get('modifiedon', '')
                                        
                                        if created_on and modified_on:
                                            c_time = datetime.fromisoformat(created_on.replace('Z', '+00:00'))
                                            m_time = datetime.fromisoformat(modified_on.replace('Z', '+00:00'))
                                            # If timestamps are within 2 seconds, it's a new record
                                            if abs((c_time - m_time).total_seconds()) < 2:
                                                created += 1
                                            else:
                                                updated += 1
                                        else:
                                            updated += 1
                                    except:
                                        updated += 1
                                else:
                                    updated += 1
                            else:
                                updated += 1
                        elif 'HTTP/1.1 4' in resp or 'HTTP/1.1 5' in resp:
                            errors += 1
                    
                    # Verify counts add up, if not use simple status code counting as fallback
                    if created + updated + errors != len(chunk):
                        # Fallback: count by status codes (more reliable than JSON parsing)
                        created = r.text.count('HTTP/1.1 201 Created')
                        updated = r.text.count('HTTP/1.1 200 OK')
                        errors = len(chunk) - (created + updated)
                    
                    return {"created": created, "updated": updated, "errors": errors}
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 5)))
                    continue
            except Exception as e:
                log(f"\n⚠️  Batch exception: {str(e)[:200]}")
                time.sleep(3)
        return {"created": 0, "updated": 0, "errors": len(chunk)}

    # Create batches and process
    batch_size = 400
    batches = [valid_records[i:i + batch_size] for i in range(0, total, batch_size)]
    log(f"Fast upserting {total:,} records in {len(batches)} batches of {batch_size} (6 parallel threads)")

    total_created = 0
    total_updated = 0
    total_errors = 0
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        for future in concurrent.futures.as_completed([ex.submit(upsert_batch, c) for c in batches]):
            result = future.result()
            total_created += result["created"]
            total_updated += result["updated"]
            total_errors += result["errors"]
            processed = total_created + total_updated
            rate = processed / (time.time() - start_time) if time.time() - start_time > 0 else 0
            log(f"\r  Progress: {processed:,}/{total:,} records ({total_created:,} created, {total_updated:,} updated) | {rate:,.0f} rows/sec")

    elapsed = time.time() - start_time
    log(f"\nFast upsert complete: {total_created:,} created, {total_updated:,} updated, {total_errors:,} errors in {elapsed:.1f}s → {(total_created+total_updated)/elapsed:,.0f} rows/sec")
    return total_created, total_updated, total_errors

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
                
                # Service Metrics - SMG (4)
                "crf63_smgavgclosure": get_num('SMG Avg Closure'),
                "crf63_smgcasesopened": get_int('SMG Cases Opened'),
                "crf63_smgcasesresolved": get_int('SMG Cases Resolved'),
                "crf63_smgvaluepct": get_num('SMG Value %'),
                
                # Service Metrics - Delivery Performance (4)
                "crf63_singles": get_int('Singles'),
                "crf63_doubles": get_int('Doubles'),
                "crf63_triplesplus": get_int('Triples Plus'),
                "crf63_runs": get_int('Runs'),
                
                # Service Metrics - TTDT (3)
                "crf63_ttdtorders": get_int('TTDT Orders'),
                "crf63_todoortimedispatch": get_num('To The Door Time for Dispatch Orders'),
                "crf63_todoortimeminutes": get_num('To The Door Time Minutes'),
                
                # Service Metrics - Taste Surveys (3)
                "crf63_tasteoffoodgood": get_int('TY Taste Of Food Good Survey Count'),
                "crf63_tasteoffoodtotal": get_int('TY Total Taste Of Food Survey Count'),
                "crf63_orderaccuracygood": get_int('TY Order Accuracy Good Survey Count'),
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
    
    if query_type in ['last_1_week', 'last_2_weeks']:
        # Single query using MyView filter (1 week or 2 weeks)
        week_label = "1 week" if query_type == 'last_1_week' else "2 weeks"
        myview_id = "81" if query_type == 'last_1_week' else "82"
        log(f"   Executing last {week_label} query (MyView {myview_id} filter)...")
        
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
        '--mdx-query',
        choices=['daily_sales', 'sales_channel', 'services', 'all'],
        default='daily_sales',
        help='Type of data to sync: daily_sales (OARS BI Data), sales_channel (Sales Channel Daily), services (future), or all (sync all query types)'
    )
    parser.add_argument(
        '--mdx-length',
        choices=['1_week', '2_week', 'full'],
        default='2_week',
        help='Time range for the query: 1_week (7 days), 2_week (14 days), or full (all fiscal years)'
    )
    parser.add_argument(
        '--send-email',
        choices=['yes', 'no'],
        default='no',
        help='Send email notification after sync completes (default: no)'
    )
    args = parser.parse_args()
    
    # Determine the actual query type based on both mdx-query and mdx-length
    if args.mdx_length == 'full':
        # Full historical data for the specified query type
        if args.mdx_query == 'daily_sales':
            query_type = 'full_bi_data'
        elif args.mdx_query == 'sales_channel':
            query_type = 'sales_channel_full'  # Will need to implement
        else:
            query_type = 'full_bi_data'
    elif args.mdx_length == '1_week':
        # 1 week of data (MyView 81)
        if args.mdx_query == 'daily_sales':
            query_type = 'last_1_week'
        elif args.mdx_query == 'sales_channel':
            query_type = 'sales_channel_daily'
        else:
            query_type = 'last_1_week'
    else:  # 2_week
        # 2 weeks of data (MyView 82)
        if args.mdx_query == 'daily_sales':
            query_type = 'last_2_weeks'
        elif args.mdx_query == 'sales_channel':
            query_type = 'sales_channel_2weeks'  # Will need to implement
        else:
            query_type = 'last_2_weeks'
    
    send_email = (args.send_email == 'yes')
    
    print("="*80)
    print("OLAP to Dataverse Sync (using Azure Key Vault)")
    print(f"MDX Query Type: {args.mdx_query}")
    if args.mdx_query == 'daily_sales':
        print(f"MDX Length: {args.mdx_length}")
    print(f"Send Email: {args.send_email}")
    print("="*80)
    print()
    
    try:
        # Get credentials from Azure Key Vault
        print("📦 Loading credentials from Azure Key Vault (kv-bw-data-integration)...")
        print("   Using app-client-id and app-client-secret for Dataverse access")
        print("✓ Key Vault configured (credentials loaded on-demand)")
        print()
        
        # Run the appropriate sync based on query type
        if args.mdx_query == 'all':
            # Sync all query types
            print("🔄 Syncing all query types...\n")
            results = []
            
            # 1. Daily Sales
            print("=" * 80)
            print("1. Syncing Daily Sales (OARS BI Data)")
            print("=" * 80)
            if args.mdx_length == 'full':
                ds_query_type = 'full_bi_data'
            elif args.mdx_length == '1_week':
                ds_query_type = 'last_1_week'
            else:
                ds_query_type = 'last_2_weeks'
            result_ds = query_olap_and_sync_to_dataverse(query_type=ds_query_type)
            results.append(('daily_sales', result_ds))
            
            # 2. Sales Channel
            print("\n" + "=" * 80)
            print("2. Syncing Sales Channel Daily")
            print("=" * 80)
            result_sc = query_sales_channel_daily_and_sync()
            results.append(('sales_channel', result_sc))
            
            # Aggregate results
            total_created = sum(r[1].get('records_created', 0) for r in results if r[1].get('success'))
            total_updated = sum(r[1].get('records_updated', 0) for r in results if r[1].get('success'))
            total_errors = sum(r[1].get('errors', 0) for r in results if r[1].get('success'))
            all_success = all(r[1].get('success', False) for r in results)
            
            result = {
                'success': all_success,
                'records_created': total_created,
                'records_updated': total_updated,
                'errors': total_errors,
                'details': results
            }
            
            print("\n" + "=" * 80)
            print("✅ ALL SYNCS COMPLETE")
            print("=" * 80)
            for query_name, query_result in results:
                status = "✓" if query_result.get('success') else "✗"
                print(f"{status} {query_name}: {query_result.get('records_updated', 0)} records")
            
        elif args.mdx_query == 'sales_channel':
            # Use the dedicated Sales Channel Daily sync function
            # TODO: Add support for different time ranges (1_week, 2_week, full)
            result = query_sales_channel_daily_and_sync()
        else:
            # Use the standard OARS BI Data sync function
            result = query_olap_and_sync_to_dataverse(query_type=query_type)
        
        # Send email notification if requested
        if send_email:
            print("\n📧 Sending email notification...")
            
            if result["success"]:
                subject = "✅ OLAP to Dataverse Sync Completed Successfully"
                body = f"""
OLAP to Dataverse sync completed successfully.

Summary:
- MDX Query Type: {args.mdx_query}
- MDX Length: {args.mdx_length if args.mdx_query == 'daily_sales' else 'N/A'}
- Records Created: {result.get('records_created', 0)}
- Records Updated: {result.get('records_updated', 0)}
- Errors: {result.get('errors', 0)}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

The data has been synchronized to Dataverse.
                """.strip()
            else:
                subject = "❌ OLAP to Dataverse Sync Failed"
                body = f"""
OLAP to Dataverse sync failed.

Error: {result.get('error', 'Unknown error')}
MDX Query Type: {args.mdx_query}
MDX Length: {args.mdx_length if args.mdx_query == 'daily_sales' else 'N/A'}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Please check the logs for more details.
                """.strip()
            
            email_sent = send_email_notification(subject, body)
            if email_sent:
                print("✓ Email notification sent")
            else:
                print("⚠ Failed to send email notification")
        
        if result["success"]:
            return 0
        else:
            print(f"\n✗ Sync failed: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to send error notification
        if send_email:
            try:
                subject = "❌ OLAP to Dataverse Sync Error"
                body = f"""
OLAP to Dataverse sync encountered an unexpected error.

Error: {str(e)}
MDX Query Type: {args.mdx_query}
MDX Length: {args.mdx_length if args.mdx_query == 'daily_sales' else 'N/A'}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Stack trace:
{traceback.format_exc()}
                """.strip()
                send_email_notification(subject, body)
            except:
                pass
        
        return 1

if __name__ == "__main__":
    sys.exit(main())
