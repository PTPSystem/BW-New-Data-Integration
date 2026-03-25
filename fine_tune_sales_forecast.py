"""
Fine-tune sales forecast models entry point.

Reads historical weekly sales from Dataverse, uses auto_arima (AIC-guided stepwise search)
to find the best ARIMA/SARIMA parameters per store, and saves the best configuration to
config/sales_forecast_parameters.json for use by sales_forecast.py.

Run this periodically (e.g. quarterly) or when adding new stores.

Usage:
    .venv/bin/python fine_tune_sales_forecast.py
"""
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.fine_tune_sales_forecast import main
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
    logger = setup_logging()
    result = main(logger)
    if result.get("success"):
        logger.info(
            f"Fine-tuning complete: {result['stores_tuned']}/{result['stores_total']} stores tuned "
            f"({result['arima_count']} ARIMA, {result['sarima_count']} SARIMA)"
            + (f" | avg MAPE {result['avg_mape']:.2f}%" if result.get("avg_mape") else "")
        )
        sys.exit(0)
    else:
        logger.error(f"Fine-tuning failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)
