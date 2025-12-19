import os
import sys
import argparse
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Import local modules
from modules.utils.keyvault import get_secret, get_dataverse_credentials
from modules.utils.config import load_config
from modules.olap import execute_xmla_mdx, parse_xmla_celldata_response, parse_sales_channel_daily_response, parse_offers_response
from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.notifications import send_email_notification
from modules.mdx_queries import get_sample_mdx_queries, get_sales_channel_daily_mdx, get_offers_mdx, get_daily_sales_mdx
from modules.transformers import transform_olap_to_dataverse_records, transform_sales_channel_daily_records, transform_offers_records
import pandas as pd

load_dotenv()

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

def query_offers_and_sync(config=None, logger=None, days=7):
    """
    Query Offers data from OLAP and sync to Dataverse.
    
    Table: crf63_offers (Offers)
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    log("="*80)
    log("Offers - OLAP to Dataverse Sync")
    log("="*80)
    
    # Get OLAP configuration from Key Vault
    olap_server = os.getenv('OLAP_SERVER', 'https://ednacubes.papajohns.com:10502')
    olap_catalog = os.getenv('OLAP_CATALOG_OFFERS', 'Offers')  # Note: Different catalog for Offers
    olap_username = get_secret('olap-username')
    olap_password = get_secret('olap-password')
    olap_ssl_verify = False  # Self-signed cert
    
    # Get Dataverse configuration from Key Vault
    dv_creds = get_dataverse_credentials()
    dataverse_url = dv_creds['environment_url']
    client_id = dv_creds['client_id']
    tenant_id = dv_creds['tenant_id']
    client_secret = dv_creds['client_secret']
    
    # Table for Offers data
    # The table name for API calls should be the plural collection name
    # In create_offers_table.py, we set CollectionName to "crf63_offerses" (implicitly or explicitly)
    # Let's verify what the collection name actually is.
    # Usually it's schema name + 'es' or 's'.
    # If schema is crf63_offers, collection is likely crf63_offerses.
    table_name = "crf63_offerses"
    
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
    
    # Step 2: Query OLAP cube for Offers
    log("\n2. Querying OLAP cube (Offers)...")
    
    mdx_query = get_offers_mdx(days=days)
    week_label = "1 week" if days == 7 else "2 weeks"
    log(f"   Using MyView (last {week_label})")
    
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
        
        # Parse the response using the Offers parser
        df = parse_offers_response(xml_response, logger=logger)
        
        if df is None or len(df) == 0:
            log("⚠  Query returned no data")
            return {"success": False, "error": "No data returned from OLAP"}
        
        log(f"   ✓ Parsed {len(df)} rows")
        
    except Exception as e:
        log(f"   ✗ Query failed: {e}")
        log(traceback.format_exc())
        return {"success": False, "error": str(e)}
    
    log(f"   Columns: {list(df.columns)}")
    log(f"\n   Sample data:")
    log(df.head().to_string())
    
    # Step 3: Transform data for Dataverse
    log("\n3. Transforming data for Dataverse...")
    
    records = transform_offers_records(df, logger)
    
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
    log("✅ Offers Sync Complete!")
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
    # For incremental daily_sales ranges, allow service measures to be toggled.
    if query_type == 'last_1_week':
        mdx_query = get_daily_sales_mdx(days=7)
    elif query_type == 'last_2_weeks':
        mdx_query = get_daily_sales_mdx(days=14)
    else:
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
    
    parser = argparse.ArgumentParser(description='Sync OLAP data to Dataverse')
    parser.add_argument(
        '--mdx-query',
        choices=['daily_sales', 'sales_channel', 'offers', 'all'],
        default='daily_sales',
        help='Type of data to sync: daily_sales (OARS BI Data + Service measures), sales_channel (Sales Channel Daily), offers (Offers), or all (sync all query types)'
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
        elif args.mdx_query == 'offers':
            query_type = 'offers_full' # Will need to implement
        else:
            query_type = 'full_bi_data'
    elif args.mdx_length == '1_week':
        # 1 week of data (MyView 81)
        if args.mdx_query == 'daily_sales':
            query_type = 'last_1_week'
        elif args.mdx_query == 'sales_channel':
            query_type = 'sales_channel_daily'
        elif args.mdx_query == 'offers':
            query_type = 'offers_daily'
        else:
            query_type = 'last_1_week'
    else:  # 2_week
        # 2 weeks of data (MyView 82)
        if args.mdx_query == 'daily_sales':
            query_type = 'last_2_weeks'
        elif args.mdx_query == 'sales_channel':
            query_type = 'sales_channel_2weeks'  # Will need to implement
        elif args.mdx_query == 'offers':
            query_type = 'offers_2weeks'
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
            
            # 3. Offers
            print("\n" + "=" * 80)
            print("3. Syncing Offers")
            print("=" * 80)
            days = 7 if args.mdx_length == '1_week' else 14
            result_offers = query_offers_and_sync(days=days)
            results.append(('offers', result_offers))
            
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
        elif args.mdx_query == 'offers':
            # Use the dedicated Offers sync function
            days = 7 if args.mdx_length == '1_week' else 14
            result = query_offers_and_sync(days=days)
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
