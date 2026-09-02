"""Download Papa John's labor files and upsert day-part hours to Dataverse."""
import argparse
import logging
import os
import sys
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.labor_processing import run_labor_processing
from modules.notifications import send_email_notification
from modules.utils.config import load_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Download PPJ labor ZIPs and load day-part hours")
    parser.add_argument("--date", default="", help="Target file date YYYYMMDD (default: latest on the portal)")
    parser.add_argument("--skip-dataverse", action="store_true", help="Download and process only; do not upsert")
    parser.add_argument("--keep-temp", action="store_true", help="Leave downloaded/extracted files in place")
    parser.add_argument("--csv", default="", help="Optional CSV output path")
    parser.add_argument("--email", choices=["yes", "no"], default="no", help="Send a summary email")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )
    logger = logging.getLogger("labor_processing")

    print("=" * 80)
    print("Labor file download → day-part hours")
    print(f"Date: {args.date or 'latest'}")
    print(f"Dataverse: {'skip' if args.skip_dataverse else 'upsert'}")
    print("=" * 80)

    try:
        config = load_config()
        result = run_labor_processing(
            config,
            logger,
            target_date=args.date or None,
            skip_dataverse=args.skip_dataverse,
            keep_temp=args.keep_temp,
            csv_path=args.csv or None,
        )
        print(
            f"Done: date={result.get('target_date')} "
            f"downloaded={result.get('download_count')} "
            f"records={result.get('record_count')} "
            f"created={result.get('records_created', 0)} "
            f"updated={result.get('records_updated', 0)} "
            f"errors={result.get('errors', 0)}"
        )
        missing = result.get("missing_stores") or []
        if missing:
            print(f"Missing store files: {', '.join(missing)}")

        if args.email == "yes":
            if result.get("success"):
                subject = "✅ Labor processing completed"
                body = f"""
Labor file download completed.

Date: {result.get('target_date')}
Downloaded: {result.get('download_count')}
Extracted: {result.get('extract_count')}
Records: {result.get('record_count')}
Created: {result.get('records_created', 0)}
Updated: {result.get('records_updated', 0)}
Errors: {result.get('errors', 0)}
Missing stores: {', '.join(missing) if missing else 'none'}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""".strip()
            else:
                subject = "❌ Labor processing failed"
                body = f"Labor processing failed.\n\nResult: {result}"
            send_email_notification(subject, body, logger=logger)

        return 0 if result.get("success") else 1
    except Exception as exc:
        print(f"\n✗ Labor processing failed: {exc}")
        traceback.print_exc()
        if args.email == "yes":
            send_email_notification(
                "❌ Labor processing error",
                f"Labor processing encountered an unexpected error.\n\n{exc}\n\n{traceback.format_exc()}",
                logger=logger,
            )
        return 1


if __name__ == "__main__":
    sys.exit(main())
