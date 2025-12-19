#!/usr/bin/env python3
"""
Populate the 14 new service columns in crf63_oarsbidata with 2024 and 2025 data.

This script:
1. Queries OLAP cube for Service metrics (Store × Date dimension)
2. Updates existing records in crf63_oarsbidata with the new service column values
3. Uses batch operations for performance (similar to load_csv.py)

Run after add_service_columns.py completes.
"""

import os
import sys
import uuid
import json
import time
import concurrent.futures
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.utils.keyvault import get_dataverse_credentials, get_secret
import requests
import msal

def get_service_metrics_mdx(fiscal_years=[2024, 2025]):
    """
    Generate MDX query for Service metrics only.
    Returns Store × Date dimension with 29 service measures.
    """
    fiscal_year_members = ", ".join([f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{year}]" for year in fiscal_years])
    where_clause = f"WHERE {{{fiscal_year_members}}}"
    
    return f"""
SELECT {{
    [Measures].[Make Time Minutes],
    [Measures].[Rack Time Minutes],
    [Measures].[Total OTD Time (Hours)],
    [Measures].[Deliveries],
    [Measures].[BOZOCORO Orders],
    [Measures].[OTD Order Count],
    [Measures].[Total Cash Over/Short USD],
    [Measures].[TY Total OSAT Survey Count],
    [Measures].[TY OSAT Satisfied Survey Count],
    [Measures].[Total Calls],
    [Measures].[Answered Calls],
    [Measures].[Mileage Cost Local],
    [Measures].[TY Total Order Accuracy Survey Count],
    [Measures].[Order Accuracy %],
    [Measures].[SMG Avg Closure],
    [Measures].[SMG Cases Opened],
    [Measures].[SMG Cases Resolved],
    [Measures].[SMG Value %],
    [Measures].[Doubles],
    [Measures].[Singles],
    [Measures].[Triples Plus],
    [Measures].[Runs],
    [Measures].[TTDT Orders],
    [Measures].[TY Dispatched Delivery Orders],
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

def execute_xmla_mdx(server, catalog, username, password, mdx_query, ssl_verify=False):
    """Execute MDX query via XMLA"""
    xmla_url = f"{server}/xmla/default" if not server.endswith("/xmla/default") else server
    
    xmla_request = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
            <Command>
                <Statement><![CDATA[{mdx_query}]]></Statement>
            </Command>
            <Properties>
                <PropertyList>
                    <Catalog>{catalog}</Catalog>
                    <Format>Multidimensional</Format>
                    <AxisFormat>TupleFormat</AxisFormat>
                </PropertyList>
            </Properties>
        </Execute>
    </soap:Body>
</soap:Envelope>"""
    
    response = requests.post(
        xmla_url,
        data=xmla_request.encode('utf-8'),
        headers={'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'urn:schemas-microsoft-com:xml-analysis:Execute'},
        auth=requests.auth.HTTPBasicAuth(username, password),
        verify=ssl_verify,
        timeout=300
    )
    
    response.raise_for_status()
    return response.text

def parse_service_response(xml_response):
    """Parse XMLA response and return list of records with service metrics"""
    import xml.etree.ElementTree as ET
    
    root = ET.fromstring(xml_response)
    
    # Define namespaces
    ns = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        'xsd': 'http://www.w3.org/2001/XMLSchema',
        'mddataset': 'urn:schemas-microsoft-com:xml-analysis:mddataset',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
    
    # Extract measure names from AxisInfo
    measures = []
    axis0 = root.find('.//mddataset:Axis[@name="Axis0"]', ns)
    if axis0:
        for tuple_elem in axis0.findall('.//mddataset:Tuple', ns):
            member = tuple_elem.find('.//mddataset:Member', ns)
            if member is not None:
                caption = member.find('.//mddataset:Caption', ns)
                if caption is not None:
                    measures.append(caption.text)
    
    # Extract row dimensions (Store Number, Calendar Date)
    rows = []
    axis1 = root.find('.//mddataset:Axis[@name="Axis1"]', ns)
    if axis1:
        for tuple_elem in axis1.findall('.//mddataset:Tuple', ns):
            members = tuple_elem.findall('.//mddataset:Member', ns)
            if len(members) >= 2:
                store = members[0].find('.//mddataset:Caption', ns).text
                date = members[1].find('.//mddataset:Caption', ns).text
                rows.append({'Store': store, 'Date': date})
    
    # Extract cell values
    cell_data = root.find('.//mddataset:CellData', ns)
    cells = []
    if cell_data is not None:
        for cell in cell_data.findall('.//mddataset:Cell', ns):
            value_elem = cell.find('.//mddataset:Value', ns)
            cells.append(value_elem.text if value_elem is not None else None)
    
    # Build records
    records = []
    for row_idx, row_info in enumerate(rows):
        record = {
            'store_number': row_info['Store'],
            'calendar_date': row_info['Date'],
            'service_metrics': {}
        }
        
        # Map cell values to measures
        for measure_idx, measure_name in enumerate(measures):
            cell_idx = row_idx * len(measures) + measure_idx
            if cell_idx < len(cells):
                record['service_metrics'][measure_name] = cells[cell_idx]
        
        records.append(record)
    
    print(f"✓ Parsed {len(records)} records with {len(measures)} service measures")
    return records

def transform_to_dataverse_updates(records):
    """Transform OLAP records to Dataverse update payloads"""
    updates = []
    
    def to_float(val):
        if val in (None, '', '-', ' '): return None
        try:
            return float(str(val).replace(',', ''))
        except:
            return None
    
    def to_int(val):
        f = to_float(val)
        return int(f) if f is not None else None
    
    for rec in records:
        sm = rec['service_metrics']
        
        # Parse date
        date_str = rec['calendar_date']
        try:
            from dateutil import parser
            dt = parser.parse(date_str)
            date_key = dt.strftime('%Y%m%d')
        except:
            continue
        
        # Business key for lookup
        business_key = f"{rec['store_number']}_{date_key}"
        
        # Map to Dataverse columns (only the NEW columns)
        update_payload = {
            "crf63_businesskey": business_key,
            # NEW Service columns (14 total)
            "crf63_smgavgclosure": to_float(sm.get('SMG Avg Closure')),
            "crf63_smgcasesopened": to_int(sm.get('SMG Cases Opened')),
            "crf63_smgcasesresolved": to_int(sm.get('SMG Cases Resolved')),
            "crf63_smgvaluepct": to_float(sm.get('SMG Value %')),
            "crf63_singles": to_int(sm.get('Singles')),
            "crf63_doubles": to_int(sm.get('Doubles')),
            "crf63_triplesplus": to_int(sm.get('Triples Plus')),
            "crf63_runs": to_int(sm.get('Runs')),
            "crf63_ttdtorders": to_int(sm.get('TTDT Orders')),
            "crf63_todoortimedispatch": to_float(sm.get('To The Door Time for Dispatch Orders')),
            "crf63_todoortimeminutes": to_float(sm.get('To The Door Time Minutes')),
            "crf63_tasteoffoodgood": to_int(sm.get('TY Taste Of Food Good Survey Count')),
            "crf63_tasteoffoodtotal": to_int(sm.get('TY Total Taste Of Food Survey Count')),
            "crf63_orderaccuracygood": to_int(sm.get('TY Order Accuracy Good Survey Count')),
        }
        
        # Remove None values
        update_payload = {k: v for k, v in update_payload.items() if v is not None}
        
        if len(update_payload) > 1:  # More than just business_key
            updates.append(update_payload)
    
    print(f"✓ Transformed {len(updates)} update payloads")
    return updates

def batch_update_dataverse(environment_url, access_token, table_name, updates):
    """Batch update existing records with service metrics"""
    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    batch_url = f"{api_url}/$batch"
    
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount('https://', adapter)
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    
    def build_batch(batch_records):
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        parts = [f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()]
        
        for i, rec in enumerate(batch_records, 1):
            key = rec["crf63_businesskey"].replace("'", "''")
            payload = json.dumps(rec, separators=(',', ':'))
            
            part = (
                f"--{changeset_id}\r\n"
                f"Content-Type: application/http\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                f"\r\n"
                f"PATCH {table_name}(crf63_businesskey='{key}') HTTP/1.1\r\n"
                f"Content-Type: application/json\r\n"
                f"\r\n"
                f"{payload}\r\n"
            ).encode()
            parts.append(part)
        
        parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
        return b"".join(parts), batch_id
    
    def update_batch(chunk):
        body, batch_id = build_batch(chunk)
        headers = {"Content-Type": f"multipart/mixed; boundary={batch_id}"}
        
        for _ in range(5):
            try:
                r = session.post(batch_url, headers=headers, data=body, timeout=600)
                if r.status_code in (200, 204):
                    success = r.text.count("HTTP/1.1 204") + r.text.count("HTTP/1.1 200")
                    return success
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 5)))
                    continue
            except:
                time.sleep(3)
        return 0
    
    batch_size = 400
    batches = [updates[i:i + batch_size] for i in range(0, len(updates), batch_size)]
    print(f"\nUpdating {len(updates):,} records in {len(batches)} batches...")
    
    processed = 0
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        for future in concurrent.futures.as_completed([ex.submit(update_batch, c) for c in batches]):
            processed += future.result()
            rate = processed / (time.time() - start_time) if time.time() - start_time > 0 else 0
            print(f"\r  Progress: {processed:,}/{len(updates):,} | {rate:,.0f} rows/sec", end='')
    
    elapsed = time.time() - start_time
    print(f"\n✓ Updated {processed:,} records in {elapsed:.1f}s")
    return processed

def main():
    print("="*80)
    print("Populate Service Columns in crf63_oarsbidata")
    print("Querying OLAP for 2024 and 2025 service metrics")
    print("="*80)
    print()
    
    try:
        # Get credentials
        print("1. Loading credentials from Azure Key Vault...")
        creds = get_dataverse_credentials()
        olap_server = os.getenv('OLAP_SERVER', 'https://ednacubes.papajohns.com:10502')
        olap_catalog = os.getenv('OLAP_CATALOG', 'OARS Franchise')
        olap_username = get_secret("olap-username")
        olap_password = get_secret("olap-password")
        print("✓ Credentials loaded")
        
        # Get Dataverse token
        print("\n2. Getting Dataverse access token...")
        app = msal.ConfidentialClientApplication(
            creds['client_id'],
            authority=f"https://login.microsoftonline.com/{creds['tenant_id']}",
            client_credential=creds['client_secret']
        )
        token_result = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])
        dataverse_token = token_result["access_token"]
        print("✓ Token obtained")
        
        # Query OLAP
        print("\n3. Querying OLAP cube for service metrics (2024-2025)...")
        mdx = get_service_metrics_mdx([2024, 2025])
        xml_response = execute_xmla_mdx(
            olap_server,
            olap_catalog,
            olap_username,
            olap_password,
            mdx,
            ssl_verify=False
        )
        print(f"✓ Query executed ({len(xml_response):,} bytes)")
        
        # Parse response
        print("\n4. Parsing OLAP response...")
        records = parse_service_response(xml_response)
        
        # Transform to updates
        print("\n5. Transforming to Dataverse update payloads...")
        updates = transform_to_dataverse_updates(records)
        
        # Batch update
        print("\n6. Updating Dataverse records...")
        updated = batch_update_dataverse(
            creds['environment_url'],
            dataverse_token,
            'crf63_oarsbidatas',
            updates
        )
        
        print("\n" + "="*80)
        print("✓ Population Complete!")
        print("="*80)
        print(f"  Records updated: {updated:,}")
        print("\nNext step: Update olap_to_dataverse.py MDX query to include service metrics")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
