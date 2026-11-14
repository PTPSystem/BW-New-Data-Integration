import os
import sys
import argparse
import traceback
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

# Import local modules
from modules.utils.keyvault import get_secret, get_atscale_api_token
from modules.utils.config import load_config
from modules.neon_sink import spec_from_mapping, upsert_records
from modules.pipeline_config import load_pipelines, load_mapping
from modules.notifications import send_email_notification
from modules.pipeline_runner import run_mdx_to_df, run_sql_to_df, transform_df_to_records
from modules.length_slicer import (
    LengthError,
    length_arg,
    render_time_sliced_mdx,
    render_time_sliced_sql,
)
import pandas as pd

load_dotenv()

# All legacy query functions (query_sales_channel_daily_and_sync, query_offers_and_sync, etc.)
# have been replaced by the config-driven pipeline system in run_pipeline_by_name().
# See pipelines/pipelines.yaml for pipeline definitions.

def query_olap_and_sync_to_dataverse(config=None, logger=None, query_type='full_bi_data'):

    """Query OLAP and upsert the transformed rows into Neon Postgres."""
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    log("="*80)
    log("OLAP to Neon Postgres Sync")
    log("="*80)
    
    # Get OLAP configuration from Key Vault
    olap_server = os.getenv('OLAP_SERVER', 'https://ednacubes.papajohns.com:10502')
    olap_catalog = os.getenv('OLAP_CATALOG', 'OARS Franchise')
    olap_username = get_secret('olap-username')
    olap_password = get_secret('olap-password')
    olap_ssl_verify = False  # Self-signed cert
    
    table_name = "crf63_oarsbidatas"
    
    log(f"\nOLAP Server: {olap_server}")
    log(f"OLAP Catalog: {olap_catalog}")
    log(f"Neon table: {table_name}")
    
    # Step 1: Query OLAP cube
    log("\n1. Querying OLAP cube...")
    
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
    
    # Step 2: Transform data (unchanged record shape)
    log("\n2. Transforming data...")
    
    # Use the dedicated transform function with business key support
    records = transform_olap_to_dataverse_records(df, logger)
    
    log(f"✓ Transformed {len(records)} records")
    
    # Debug: Show first record
    if records:
        log(f"   First record business key: {records[0].get('crf63_businesskey', 'MISSING')}")
    
    # Step 3: Upsert to Neon. daily_sales mapping is the schema for this legacy load.
    log("\n3. Upserting to Neon Postgres...")
    mapping = load_mapping(load_pipelines()["daily_sales"].mapping_path)
    created, updated, errors = upsert_records(records, spec_from_mapping(mapping), logger=logger)
    
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
    
    def _length_arg(value: str) -> str:
        try:
            return length_arg(value)
        except LengthError as exc:
            raise argparse.ArgumentTypeError(str(exc)) from exc

    parser = argparse.ArgumentParser(description='Sync OLAP data to Neon Postgres')
    parser.add_argument(
        '--query',
        choices=available_pipelines + ['all'],
        default='daily_sales',
        help=f'Pipeline to sync. Available: {", ".join(available_pipelines)}, or "all" for all pipelines'
    )
    parser.add_argument(
        '--length',
        type=_length_arg,
        default='2wk',
        help=(
            'Time range: 1wk, 2wk (default), or Nday/Ndays '
            '(e.g. 3day, 30days). 7day and 14day use the same MyView slices as 1wk and 2wk.'
        ),
    )
    parser.add_argument(
        '--fy',
        type=int,
        default=None,
        help='Fiscal year slice (e.g., 2023). Overrides --length slicing.'
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
        fiscal_year: Optional[int] = None,
        period: Optional[int] = None,
    ):
        cfg = load_config()
        pipelines = load_pipelines()
        if pipeline_name not in pipelines:
            raise SystemExit(f"Unknown pipeline '{pipeline_name}'. Available: {', '.join(sorted(pipelines.keys()))}")

        p = pipelines[pipeline_name]
        mapping = load_mapping(p.mapping_path)
        olap_cfg = cfg.get("olap") or {}
        backend = (
            os.getenv("OLAP_BACKEND")
            or olap_cfg.get("backend")
            or "atscale"
        ).lower()
        use_atscale = backend == "atscale" and bool(p.sql)

        print(f"Running pipeline: {p.name}")
        print(f"Backend: {backend}")
        print(f"Neon table: {mapping.get('table')}")

        if use_atscale:
            try:
                sql = render_time_sliced_sql(
                    p.sql,
                    length,
                    fiscal_year=fiscal_year,
                    period=period,
                )
            except LengthError as exc:
                raise SystemExit(str(exc)) from exc

            atscale_cfg = olap_cfg.get("atscale") or {}
            api_url = atscale_cfg.get("api_url") or "https://atscale.papajohns.com/v1/query"
            project = (
                p.atscale_project
                or atscale_cfg.get("project")
                or "OARS"
            )
            environment = atscale_cfg.get("environment") or "default"
            timeout = atscale_cfg.get("timeout") or "300s"
            print(f"AtScale project: {project}")

            if args.print_mdx:
                print("\n--- Rendered SQL ---")
                print(sql)
                print("--- End SQL ---\n")

            token = get_atscale_api_token()
            df = run_sql_to_df(
                sql=sql,
                token=token,
                api_url=api_url,
                project=project,
                environment=environment,
                timeout=timeout,
            )
        else:
            if backend == "atscale" and not p.sql:
                raise SystemExit(
                    f"Pipeline '{pipeline_name}' has no SQL template; "
                    "set olap.backend=xmla for legacy MDX or add a sql: block."
                )
            try:
                mdx = render_time_sliced_mdx(
                    p.mdx,
                    pipeline_name,
                    length,
                    fiscal_year=fiscal_year,
                    period=period,
                )
            except LengthError as exc:
                raise SystemExit(str(exc)) from exc

            olap_server = os.getenv(
                "OLAP_SERVER",
                olap_cfg.get("server", "https://ednacubes.papajohns.com:10502"),
            )
            olap_catalog = (
                p.catalog
                or os.getenv("OLAP_CATALOG", olap_cfg.get("catalog", "OARS Franchise"))
            )
            olap_ssl_verify = bool(olap_cfg.get("ssl_verify", False))
            olap_username = get_secret("olap-username")
            olap_password = get_secret("olap-password")
            print(f"OLAP Catalog: {olap_catalog}")

            if args.print_mdx:
                print("\n--- Rendered MDX ---")
                print(mdx)
                print("--- End MDX ---\n")

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
            print("⚠️  No data returned from OLAP; skipping upsert")
            return {
                "success": True,
                "records_created": 0,
                "records_updated": 0,
                "errors": 0,
            }

        records = transform_df_to_records(df, mapping)
        created, updated, errors = upsert_records(records, spec_from_mapping(mapping))
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
    print("OLAP to Neon Postgres Sync (using Azure Key Vault)")
    print(f"Query: {args.query}")
    print(f"Mode: {mode_description}")
    print(f"Email: {args.email}")
    print("="*80)
    print()
    
    try:
        # Get credentials from Azure Key Vault
        print("📦 Loading credentials from Azure Key Vault (kv-bw-data-integration)...")
        print("   Using Key Vault for OLAP credentials; Neon uses DATABASE_URL")
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
                subject = "✅ OLAP to Neon Sync Completed Successfully"
                body = f"""
OLAP to Neon Postgres sync completed successfully.

Summary:
            - Query: {args.query}
            - Length: {args.length}
- Records Created: {result.get('records_created', 0)}
- Records Updated: {result.get('records_updated', 0)}
- Errors: {result.get('errors', 0)}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

The data has been synchronized to Neon Postgres.
                """.strip()
            else:
                subject = "❌ OLAP to Neon Sync Failed"
                body = f"""
OLAP to Neon Postgres sync failed.

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
                subject = "❌ OLAP to Neon Sync Error"
                body = f"""
OLAP to Neon Postgres sync encountered an unexpected error.

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
