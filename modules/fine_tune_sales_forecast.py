"""
Fine-tune sales forecasting models on a per-store level using auto_arima.

Reads historical weekly sales from Dataverse (crf63_oarsbidatas + crf63_bwfiscalcalendars),
uses the last 12 weeks as a hold-out test set, trains on the rest, and saves the best
parameters per store to config/sales_forecast_parameters.json.

Run periodically (e.g. quarterly) to keep model configurations fresh for new stores
or when store sales patterns change significantly.
"""

import json
import os
import numpy as np
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

warnings.filterwarnings("ignore")
from pathlib import Path

from pmdarima import auto_arima

from modules.utils.keyvault import get_dataverse_credentials
from modules.utils.config import load_config
from modules.dataverse import get_dataverse_access_token
from modules.sales_forecast import get_fiscal_calendar, get_weekly_sales_history


# ---------------------------------------------------------------------------
# MAPE helper
# ---------------------------------------------------------------------------

def _calculate_mape(y_true, y_pred):
    """Mean Absolute Percentage Error; returns inf when any true value is 0."""
    if np.any(y_true == 0):
        return float("inf")
    return float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100)


# ---------------------------------------------------------------------------
# auto_arima for one store
# ---------------------------------------------------------------------------

def tune_store_model(store_data, store_id, logger=None):
    """
    Use auto_arima (AIC-guided stepwise search) to find the best ARIMA/SARIMA
    configuration for one store. Uses last 12 weeks as hold-out test set.
    Returns a result dict or None if no valid model found.
    """
    def log_warning(msg):
        if logger:
            logger.warning(msg)

    if len(store_data) < 24:
        log_warning(f"Store {store_id}: only {len(store_data)} weeks — skipping (need ≥24)")
        return None

    train = store_data["sales"].values[:-12]
    test = store_data["sales"].values[-12:]

    model = None

    # Try seasonal (SARIMA) first when we have ≥104 training weeks (≥2 full years)
    if len(train) >= 104:
        try:
            model = auto_arima(
                train,
                seasonal=True, m=52,
                stepwise=True,
                information_criterion="aic",
                max_p=5, max_q=3, max_P=2, max_Q=2,
                error_action="ignore",
                suppress_warnings=True,
            )
        except Exception:
            model = None

    # Fall back to non-seasonal
    if model is None:
        try:
            model = auto_arima(
                train,
                seasonal=False,
                stepwise=True,
                information_criterion="aic",
                max_p=5, max_q=3,
                error_action="ignore",
                suppress_warnings=True,
            )
        except Exception:
            log_warning(f"Store {store_id}: auto_arima failed")
            return None

    try:
        fc = model.predict(12)
        mape = _calculate_mape(test, fc)
    except Exception:
        log_warning(f"Store {store_id}: forecast step failed")
        return None

    order = list(model.order)
    seasonal_order = list(model.seasonal_order)  # always present; (0,0,0,0) for non-seasonal
    is_sarima = any(v != 0 for v in seasonal_order[:3])  # P, D, or Q > 0
    model_type = "SARIMA" if is_sarima else "ARIMA"

    result = {
        "model_type": model_type,
        "order": order,
        "test_mape": mape,
        "training_weeks": len(train),
        "test_weeks": len(test),
        "tuned_date": datetime.now().isoformat(),
    }
    if is_sarima:
        result["seasonal_order"] = seasonal_order
    return result


# ---------------------------------------------------------------------------
# Parallel worker (top-level so it's picklable by multiprocessing)
# ---------------------------------------------------------------------------

def _tune_store_worker(args):
    """Multiprocessing worker — logger not passed (can't be pickled)."""
    store_id, store_data = args
    return store_id, tune_store_model(store_data, store_id, logger=None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(logger):
    config = load_config()
    credentials = get_dataverse_credentials()
    environment_url = credentials["environment_url"]
    access_token = get_dataverse_access_token(
        environment_url,
        credentials["client_id"],
        credentials["client_secret"],
        credentials["tenant_id"],
        logger,
    )

    logger.info("Downloading crf63_bwfiscalcalendars...")
    fiscal_calendar = get_fiscal_calendar(environment_url, access_token, logger)
    logger.info(f"  crf63_bwfiscalcalendars done. ({len(fiscal_calendar)} days)")

    logger.info("Downloading crf63_oarsbidatas...")
    weekly_sales = get_weekly_sales_history(environment_url, access_token, fiscal_calendar, logger)
    stores = weekly_sales["store"].unique()
    logger.info(f"  crf63_oarsbidatas done. ({len(stores)} stores, {len(weekly_sales)} store-weeks)")

    workers = min(os.cpu_count() or 4, len(stores))
    logger.info(f"Tuning {len(stores)} stores (parallel, {workers} workers)...")

    store_args = [
        (store_id, weekly_sales[weekly_sales["store"] == store_id].reset_index(drop=True))
        for store_id in sorted(stores)
    ]

    raw_results = {}  # store_id -> result
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_tune_store_worker, args): args[0] for args in store_args}
        for future in as_completed(futures):
            store_id, result = future.result()
            raw_results[store_id] = result
            if result:
                logger.info(f"  Tuning store {store_id}... done. ({result['model_type']}, MAPE {result['test_mape']:.2f}%)")
            else:
                logger.warning(f"  Store {store_id}: insufficient data — using default parameters")

    tuned = {}
    arima_count = sarima_count = default_count = 0

    for store_id in sorted(raw_results):
        result = raw_results[store_id]
        store_data_len = next(len(a[1]) for a in store_args if a[0] == store_id)
        if result:
            tuned[str(store_id)] = result
            if result["model_type"] == "SARIMA":
                sarima_count += 1
            else:
                arima_count += 1
        else:
            tuned[str(store_id)] = {
                "model_type": "ARIMA",
                "order": [5, 1, 0],
                "test_mape": None,
                "training_weeks": store_data_len,
                "test_weeks": 0,
                "tuned_date": datetime.now().isoformat(),
                "note": "default_parameters_insufficient_data",
            }
            default_count += 1

    output_file = Path(__file__).parent.parent / "config" / "sales_forecast_parameters.json"
    output_file.parent.mkdir(exist_ok=True)
    logger.info(f"Updating sales_forecast_parameters.json...")
    with open(output_file, "w") as f:
        json.dump(tuned, f, indent=2)

    successful = len(tuned) - default_count
    valid_mapes = [v["test_mape"] for v in tuned.values() if v.get("test_mape") is not None]
    avg = f"{np.mean(valid_mapes):.2f}%" if valid_mapes else "n/a"
    logger.info(f"  sales_forecast_parameters.json done. ({successful}/{len(stores)} tuned — {arima_count} ARIMA, {sarima_count} SARIMA | avg MAPE {avg})")

    return {
        "success": True,
        "stores_tuned": successful,
        "stores_total": len(stores),
        "arima_count": arima_count,
        "sarima_count": sarima_count,
        "avg_mape": float(np.mean(valid_mapes)) if valid_mapes else None,
    }
