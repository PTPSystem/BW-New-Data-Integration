import pandas as pd
from datetime import datetime
import traceback
from dateutil import parser

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
            log(traceback.format_exc())
            continue
    
    return records


def transform_offers_records(df, logger=None):
    """
    Transform Offers DataFrame to Dataverse records.
    
    Business Key Format: {StoreNumber}_{YYYYMMDD}_{OfferCode}
    Example: 125_20250209_12345
    
    Args:
        df: Pandas DataFrame from OLAP query
        logger: Optional logger
    
    Returns:
        List of Dataverse record dictionaries for crf63_offers table
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
            offer_code = str(row.get('OfferCode', '')) if pd.notna(row.get('OfferCode')) else ''
            offer_pos_desc = str(row.get('OfferPOSDescription', '')) if pd.notna(row.get('OfferPOSDescription')) else ''
            
            # Skip rows with missing key fields
            if not store_number or not calendar_date or not offer_code:
                continue
            
            # Parse date and format as YYYYMMDD for business key
            if isinstance(calendar_date, str):
                dt = parser.parse(calendar_date)
                date_str = dt.strftime('%Y%m%d')
                date_iso = dt.strftime('%Y-%m-%d')
            else:
                date_str = calendar_date.strftime('%Y%m%d')
                date_iso = calendar_date.strftime('%Y-%m-%d')
            
            # Generate business key: StoreNumber_YYYYMMDD_OfferCode
            # Clean values for business key
            offer_code_clean = offer_code.replace(' ', '_').replace('-', '_')
            
            business_key = f"{store_number}_{date_str}_{offer_code_clean}"
            
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
                "crf63_offercode": offer_code,
                "crf63_offerposdescription": offer_pos_desc,
                
                # Display name
                "crf63_name": f"{store_number} - {date_str} - {offer_code}",
                
                # Measures
                "crf63_redeemedcount": get_int('Redeemed Count'),
                "crf63_discountamountusd": get_num('Discount Amount USD'),
                "crf63_grossmarginusd": get_num('Gross Margin USD'),
                "crf63_ordermixpercent": get_num('Order Mix %'),
                "crf63_salesmixusdpercent": get_num('Sales Mix USD %'),
                "crf63_netsalesusd": get_num('Net Sales USD'),
                "crf63_ordercount": get_int('Order Count'),
                "crf63_targetfoodcostusd": get_num('Target Food Cost USD'),
                
                # Metadata
                "crf63_lastrefreshed": datetime.now().isoformat(),
            }
            
            records.append(record)
            
        except Exception as e:
            log(f"Error transforming row {idx}: {e}")
            log(traceback.format_exc())
            continue
    
    return records

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
            log(traceback.format_exc())
            continue
    
    return records
