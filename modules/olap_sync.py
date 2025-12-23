import os
import sys
import argparse
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Import local modules
from modules.utils.keyvault import get_secret, get_dataverse_credentials
from modules.utils.config import load_config
from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.notifications import send_email_notification
from modules.pipeline_config import load_pipelines, load_mapping, render_mdx_template
from modules.pipeline_runner import run_mdx_to_df, transform_df_to_records
import pandas as pd

load_dotenv()

# All legacy query functions (query_sales_channel_daily_and_sync, query_offers_and_sync, etc.)
# have been replaced by the config-driven pipeline system in run_pipeline_by_name().
# See pipelines/pipelines.yaml for pipeline definitions.

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
    
    # Show help if no arguments provided
    if len(sys.argv) == 1:
        sys.argv.append('--help')
    
    # Load available pipelines dynamically from config
    pipelines = load_pipelines()
    available_pipelines = list(pipelines.keys())
    
    parser = argparse.ArgumentParser(description='Sync OLAP data to Dataverse')
    parser.add_argument(
        '--query',
        choices=available_pipelines + ['all'],
        default='daily_sales',
        help=f'Pipeline to sync. Available: {", ".join(available_pipelines)}, or "all" for all pipelines'
    )
    parser.add_argument(
        '--length',
        choices=['1wk', '2wk'],
        default='2wk',
        help='Time range: 1wk or 2wk (default)'
    )
    parser.add_argument(
        '--fy',
        type=int,
        default=None,
        help='Fiscal year slice (e.g., 2023). Overrides MyView-based --length slicing.'
    )
    parser.add_argument(
        '--fp',
        type=int,
        default=None,
        help='Fiscal period number (1-13). Requires --fy.'
    )
    parser.add_argument(
        '--email',
        choices=['yes', 'no'],
        default='no',
        help='Send email notification after sync completes (default: no)'
    )
    parser.add_argument(
        '--pipeline',
        default=None,
        help='(Advanced) Run a config-driven pipeline by name from pipelines/pipelines.yaml'
    )
    parser.add_argument(
        '--print-mdx',
        action='store_true',
        help='Print the rendered MDX query before executing (debugging)'
    )
    args = parser.parse_args()

    def run_pipeline_by_name(
        pipeline_name: str,
        length: str,
        fiscal_year: int | None = None,
        period: int | None = None,
    ):
        cfg = load_config()
        pipelines = load_pipelines()
        if pipeline_name not in pipelines:
            raise SystemExit(f"Unknown pipeline '{pipeline_name}'. Available: {', '.join(sorted(pipelines.keys()))}")

        p = pipelines[pipeline_name]
        mapping = load_mapping(p.mapping_path)

        if period is not None and fiscal_year is None:
            raise SystemExit("--fp requires --fy")

        if fiscal_year is not None:
            if pipeline_name in ('offers', 'sales_channel'):
                # Both Offers and Sales Channel use 13-4 calendar dimensions.
                if period is not None:
                    if period < 1 or period > 13:
                        raise SystemExit("--fp must be between 1 and 13")
                    slicer = (
                        f"[13-4 Calendar].[d_Year].[d_Year].&[{int(fiscal_year)}],"
                        f"[13-4 Calendar].[d_Period].[d_Period].&[{int(period)}]"
                    )
                else:
                    slicer = f"[13-4 Calendar].[d_Year].[d_Year].&[{int(fiscal_year)}]"
            else:
                # Other pipelines use regular Calendar hierarchy
                if period is not None:
                    # For pipelines with regular calendar, period is ignored but don't error
                    print(f"⚠️  --fp is only supported for offers and sales_channel pipelines. Ignoring for {pipeline_name}.")
                slicer = f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{int(fiscal_year)}]"

            mdx = render_mdx_template(p.mdx, {"slicer": slicer})
        elif length in ('1wk', '2wk'):
            # VBO cube uses different MyView filter names
            if p.catalog == 'VBO':
                myview_filter = 'Last 7 Days' if length == '1wk' else 'Last 14 Days'
                slicer = f"[MyView].[My View].[My View].&[{myview_filter}]"
            else:
                myview_id = 81 if length == '1wk' else 82
                if pipeline_name == 'offers':
                    # Keep historical behavior (MyView + 13-4 All) as the default for offers.
                    slicer = (
                        f"([MyView].[My View].[My View].&[{myview_id}],"
                        "[13-4 Calendar].[Alternate Calendar Hierarchy].[All])"
                    )
                else:
                    slicer = f"[MyView].[My View].[My View].&[{myview_id}]"

            mdx = render_mdx_template(p.mdx, {"myview_id": myview_id if p.catalog != 'VBO' else myview_filter, "slicer": slicer})
        else:
            raise SystemExit(f"Unknown length '{length}'")

        olap_server = os.getenv('OLAP_SERVER', cfg.get('olap', {}).get('server', 'https://ednacubes.papajohns.com:10502'))
        olap_catalog = p.catalog or os.getenv('OLAP_CATALOG', cfg.get('olap', {}).get('catalog', 'OARS Franchise'))
        olap_ssl_verify = bool(cfg.get('olap', {}).get('ssl_verify', False))
        olap_username = get_secret('olap-username')
        olap_password = get_secret('olap-password')

        dv_creds = get_dataverse_credentials()
        dataverse_url = dv_creds['environment_url']
        client_id = dv_creds['client_id']
        tenant_id = dv_creds['tenant_id']
        client_secret = dv_creds['client_secret']

        print(f"Running pipeline: {p.name}")
        print(f"OLAP Catalog: {olap_catalog}")
        print(f"Dataverse Table: {mapping.get('table')}")

        if args.print_mdx:
            print("\n--- Rendered MDX ---")
            print(mdx)
            print("--- End MDX ---\n")

        dataverse_token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
        df = run_mdx_to_df(
            xmla_server=olap_server,
            catalog=olap_catalog,
            username=olap_username,
            password=olap_password,
            mdx=mdx,
            parser=p.parser,
            hierarchy_mappings=p.hierarchy_mappings,
            ssl_verify=olap_ssl_verify,
        )

        if df is None or len(df) == 0:
            raise SystemExit("No data returned from OLAP")

        records = transform_df_to_records(df, mapping)
        alternate_key = mapping.get('alternate_key', 'crf63_businesskey')
        created, updated, errors = upsert_to_dataverse(dataverse_url, dataverse_token, mapping['table'], records, alternate_key)
        return {"success": True, "records_created": created, "records_updated": updated, "errors": errors}

    # If pipeline is explicitly provided, run it (advanced escape hatch).
    if args.pipeline:
        result = run_pipeline_by_name(args.pipeline, args.length, args.fy, args.fp)
        print(f"Done: {result['records_created']} created, {result['records_updated']} updated, {result['errors']} errors")
        return 0 if result.get('success') else 1
    
    send_email = (args.email == 'yes')
    
    # Determine the actual mode being used
    if args.fy is not None:
        if args.fp is not None:
            mode_description = f"FY{args.fy} Period {args.fp}"
        else:
            mode_description = f"FY{args.fy} (full year)"
    else:
        mode_description = args.length
    
    print("="*80)
    print("OLAP to Dataverse Sync (using Azure Key Vault)")
    print(f"Query: {args.query}")
    print(f"Mode: {mode_description}")
    print(f"Email: {args.email}")
    print("="*80)
    print()
    
    try:
        # Get credentials from Azure Key Vault
        print("📦 Loading credentials from Azure Key Vault (kv-bw-data-integration)...")
        print("   Using app-client-id and app-client-secret for Dataverse access")
        print("✓ Key Vault configured (credentials loaded on-demand)")
        print()
        
        # All pipelines are now directly accessible by name from pipelines.yaml
        # No more hardcoded query_to_pipeline mapping needed!

        if args.query == 'all':
            print("🔄 Syncing all pipelines...\n")
            results = []
            for pipeline_name in pipelines.keys():
                print("=" * 80)
                print(f"Syncing {pipeline_name}...")
                print("=" * 80)
                results.append((pipeline_name, run_pipeline_by_name(pipeline_name, args.length, args.fy, args.fp)))

            total_created = sum(r[1].get('records_created', 0) for r in results)
            total_updated = sum(r[1].get('records_updated', 0) for r in results)
            total_errors = sum(r[1].get('errors', 0) for r in results)

            result = {
                'success': all(r[1].get('success', False) for r in results),
                'records_created': total_created,
                'records_updated': total_updated,
                'errors': total_errors,
                'details': results,
            }
        else:
            # Use pipeline name directly from --query argument
            result = run_pipeline_by_name(args.query, args.length, args.fy, args.fp)

        
        # Send email notification if requested
        if send_email:
            print("\n📧 Sending email notification...")
            
            if result["success"]:
                subject = "✅ OLAP to Dataverse Sync Completed Successfully"
                body = f"""
OLAP to Dataverse sync completed successfully.

Summary:
            - Query: {args.query}
            - Length: {args.length}
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
Query: {args.query}
Length: {args.length}
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
            Query: {args.query}
            Length: {args.length}
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
