"""
Sales Forecast entry point.

Reads historical sales from Dataverse (no SharePoint / Excel files needed),
runs ARIMA per-store weekly forecasts, breaks down to day-of-week + daypart
using the sales channel distribution, and upserts to crf63_bwsalesforecasts.

Usage:
    .venv/bin/python sales_forecast.py
    .venv/bin/python sales_forecast.py --dryrun   # forecast only, no writes
"""
import sys
import os
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.sales_forecast import main
from modules.utils.config import load_config


def setup_logging():
    config = load_config()
    level = getattr(logging, config.get("logging", {}).get("level", "INFO"), logging.INFO)
    fmt = config.get("logging", {}).get("format", "%(asctime)s - %(levelname)s - %(module)s - %(message)s")
    logging.basicConfig(level=level, format=fmt)
    # Suppress verbose Azure SDK HTTP tracing
    for noisy in ("azure", "urllib3", "msal"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger("sales_forecast")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the sales forecast pipeline.")
    parser.add_argument("--dryrun", action="store_true", help="Run forecast but skip writing to Dataverse")
    args = parser.parse_args()

    logger = setup_logging()
    result = main(logger, dry_run=args.dryrun)
    if result.get("success"):
        dry = " (dry run — not written to Dataverse)" if result.get("dry_run") else ""
        logger.info(
            f"Sales forecast complete: {result['forecast_count']} records "
            f"for {result.get('stores', '?')} stores over weeks {result.get('weeks', [])}{dry}"
        )
        sys.exit(0)
    else:
        logger.error(f"Sales forecast failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)
