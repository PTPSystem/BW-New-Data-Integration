"""
Sales Forecast Module - BW New Data Integration

Architecture:
  1. Fetch fiscal calendar from crf63_bwfiscalcalendars (replaces BI Dimensions.xlsx)
  2. Fetch full daily sales history from crf63_oarsbidatas, aggregate to weekly per store
     (replaces BI At Scale Import xlsx files)
  3. Fetch last 10 complete fiscal weeks from crf63_saleschanneldailies, aggregate by
     store / day-of-week / daypart to build a distribution percentage grid
     (replaces BI Sales Channel - Daily.csv)
  4. Run ARIMA/SARIMA per-store weekly forecast
  5. Distribute each weekly forecast → daily × daypart using the distribution grid
  6. Upsert results to crf63_bwsalesforecasts (respecting crf63_overwrite flags)
"""

import os
import json
import uuid
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX

from modules.utils.keyvault import get_dataverse_credentials
from modules.utils.config import load_config
from modules.dataverse import get_dataverse_access_token


# ---------------------------------------------------------------------------
# Dataverse helpers
# ---------------------------------------------------------------------------

def _query_paged(environment_url, access_token, url, logger=None):
    """Fetch all pages from a Dataverse OData query and return combined value list."""
    def log(msg):
        if logger:
            logger.debug(msg)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": "odata.maxpagesize=5000",
    }

    records = []
    next_url = url
    page = 1
    while next_url:
        response = requests.get(next_url, headers=headers, timeout=120)
        if response.status_code != 200:
            if logger:
                logger.error(f"Query failed (page {page}): {response.status_code} - {response.text[:300]}")
            break
        data = response.json()
        batch = data.get("value", [])
        records.extend(batch)
        log(f"  Page {page}: +{len(batch)} records (total {len(records)})")
        next_url = data.get("@odata.nextLink")
        page += 1

    return records


# ---------------------------------------------------------------------------
# Step 1: Fiscal calendar
# ---------------------------------------------------------------------------

def get_fiscal_calendar(environment_url, access_token, logger=None):
    """
    Query crf63_bwfiscalcalendars and return a DataFrame with columns:
      date (date), fiscal_year_label (str), fiscal_week_num (int),
      fiscal_week_label (str e.g. 'Y2026W12')
    """
    url = (
        f"{environment_url}/api/data/v9.2/crf63_bwfiscalcalendars"
        f"?$select=crf63_date,crf63_fiscalyearlabel,crf63_fiscalweeknumber,crf63_fiscalyearnumber"
        f"&$orderby=crf63_date asc"
    )
    records = _query_paged(environment_url, access_token, url, logger)

    if not records:
        raise RuntimeError("Fiscal calendar is empty — cannot proceed")

    df = pd.DataFrame(records)
    # crf63_date is like "2023-01-08T06:00:00Z" (UTC midnight CST); extract as UTC date
    df["date"] = pd.to_datetime(df["crf63_date"], utc=True).dt.date
    df["fiscal_year_label"] = df["crf63_fiscalyearlabel"].astype(str)
    df["fiscal_year_num"] = df["crf63_fiscalyearnumber"].astype(int)
    df["fiscal_week_num"] = df["crf63_fiscalweeknumber"].astype(int)
    df["fiscal_week_label"] = df["fiscal_year_label"] + "W" + df["fiscal_week_num"].apply(lambda w: f"{w:02d}")

    df = df[["date", "fiscal_year_label", "fiscal_year_num", "fiscal_week_num", "fiscal_week_label"]].drop_duplicates("date")

    return df


# ---------------------------------------------------------------------------
# Step 2: Weekly sales history
# ---------------------------------------------------------------------------

def get_weekly_sales_history(environment_url, access_token, fiscal_calendar, logger=None):
    """
    Query crf63_oarsbidatas for all daily sales, merge with fiscal calendar,
    and aggregate to weekly totals per store.

    Returns DataFrame with columns: store, fiscal_week_label, fiscal_year_num,
    fiscal_week_num, sales (sum of crf63_tynetsales)
    """
    url = (
        f"{environment_url}/api/data/v9.2/crf63_oarsbidatas"
        f"?$select=crf63_storenumber,crf63_calendardate,crf63_tynetsales"
        f"&$orderby=crf63_calendardate asc"
    )
    records = _query_paged(environment_url, access_token, url, logger)

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["crf63_calendardate"], utc=True).dt.date
    # Normalise zero-padded store numbers → plain integer string (matches distribution grid)
    df["store"] = df["crf63_storenumber"].astype(str).apply(lambda x: str(int(x)) if x.strip().strip("0") else "0")
    df["sales"] = pd.to_numeric(df["crf63_tynetsales"], errors="coerce").fillna(0.0)

    # Join fiscal week info
    df = df.merge(
        fiscal_calendar[["date", "fiscal_week_label", "fiscal_year_num", "fiscal_week_num"]],
        on="date",
        how="inner",
    )

    # Aggregate to weekly
    weekly = (
        df.groupby(["store", "fiscal_week_label", "fiscal_year_num", "fiscal_week_num"])
        .agg(sales=("sales", "sum"))
        .reset_index()
        .sort_values(["store", "fiscal_year_num", "fiscal_week_num"])
        .reset_index(drop=True)
    )

    return weekly


# ---------------------------------------------------------------------------
# Step 3: Distribution grid (store / DOW / daypart percentages)
# ---------------------------------------------------------------------------

def get_distribution_grid(environment_url, access_token, fiscal_calendar, logger=None):
    """
    Query crf63_saleschanneldailies for the last 10 complete fiscal weeks.
    Sum crf63_tynetsalesusd across all source channels (App, Web, PapaCall, etc.)
    to get total sales per store / calendar date / daypart.

    Compute what % of a store's weekly total falls on each (day_of_week, daypart)
    combination.  Day-of-week uses 1=Monday … 7=Sunday.

    Store numbers in this table are zero-padded (e.g. "000126"); they are
    normalised to plain integers ("126") to match crf63_oarsbidatas.

    Returns: dict  {store_str: {(dow_int, daypart_str): pct_float}}
    """
    # Pull last 11 weeks of data (we'll filter to 10 complete below)
    cutoff = (datetime.utcnow() - timedelta(weeks=11)).strftime("%Y-%m-%dT00:00:00Z")
    url = (
        f"{environment_url}/api/data/v9.2/crf63_saleschanneldailies"
        f"?$select=crf63_storenumber,crf63_calendardate,crf63_daypart,crf63_tynetsalesusd"
        f"&$filter=crf63_calendardate ge {cutoff}"
        f"&$orderby=crf63_calendardate asc"
    )
    records = _query_paged(environment_url, access_token, url, logger)

    if not records:
        if logger:
            logger.warning("No sales channel daily data available — distribution grid is empty")
        return {}

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["crf63_calendardate"], utc=True).dt.date
    # Normalise zero-padded store numbers → plain integer string
    df["store"] = df["crf63_storenumber"].astype(str).apply(lambda x: str(int(x)) if x.strip("0") else "0")
    df["daypart"] = df["crf63_daypart"].astype(str).str.strip()
    df["sales"] = pd.to_numeric(df["crf63_tynetsalesusd"], errors="coerce").fillna(0.0)
    df["dow"] = pd.to_datetime(df["date"].astype(str)).dt.dayofweek + 1  # 1=Mon, 7=Sun

    # Merge with fiscal calendar to identify complete weeks
    df = df.merge(
        fiscal_calendar[["date", "fiscal_week_label", "fiscal_year_num", "fiscal_week_num"]],
        on="date",
        how="inner",
    )

    # Identify complete fiscal weeks (those with exactly 7 distinct dates in this dataset)
    today = datetime.utcnow().date()
    week_info = (
        df.groupby("fiscal_week_label")["date"]
        .agg(["nunique", "max"])
        .reset_index()
    )
    week_info.columns = ["fiscal_week_label", "day_count", "max_date"]
    complete_weeks = week_info[
        (week_info["day_count"] == 7) & (week_info["max_date"] < today)
    ]["fiscal_week_label"].tolist()

    # Take the most recent 10 complete weeks
    complete_weeks_sorted = sorted(complete_weeks)[-10:]

    if logger:
        if complete_weeks_sorted:
            pass
        else:
            logger.warning("  No complete fiscal weeks found in sales channel data")
            return {}

    df_recent = df[df["fiscal_week_label"].isin(complete_weeks_sorted)].copy()

    # Sum across all source channels: total sales per store / date / daypart
    summary = (
        df_recent.groupby(["store", "dow", "daypart"])["sales"]
        .sum()
        .reset_index()
    )

    # Store totals over the 10-week window
    store_totals = df_recent.groupby("store")["sales"].sum().reset_index()
    store_totals.columns = ["store", "store_total"]

    summary = summary.merge(store_totals, on="store")
    summary["pct"] = summary["sales"] / summary["store_total"]
    summary = summary[summary["pct"] > 0]

    # Build lookup dict
    distribution = {}
    for _, row in summary.iterrows():
        store = row["store"]
        distribution.setdefault(store, {})[(int(row["dow"]), row["daypart"])] = float(row["pct"])

    return distribution


# ---------------------------------------------------------------------------
# Step 4: Determine future fiscal weeks to forecast
# ---------------------------------------------------------------------------

def get_next_fiscal_weeks(fiscal_calendar, n_weeks, logger=None):
    """
    Return a list of (fiscal_week_label, [dates_in_week]) for the next N fiscal
    weeks that haven't started yet relative to today (UTC).

    The returned dates are Python date objects sorted ascending.
    """
    today = datetime.utcnow().date()

    week_dates = (
        fiscal_calendar.groupby("fiscal_week_label")
        .apply(lambda g: g.sort_values("fiscal_week_num"), include_groups=False)
        .reset_index(drop=True)
    )

    agg = (
        fiscal_calendar.groupby(
            ["fiscal_week_label", "fiscal_year_num", "fiscal_week_num"]
        )
        .agg(min_date=("date", "min"), max_date=("date", "max"))
        .reset_index()
    )

    # Future weeks: weeks whose first day is after today
    future = (
        agg[agg["min_date"] > today]
        .sort_values(["fiscal_year_num", "fiscal_week_num"])
        .head(n_weeks)
    )

    result = []
    for _, row in future.iterrows():
        label = row["fiscal_week_label"]
        dates = sorted(
            fiscal_calendar[fiscal_calendar["fiscal_week_label"] == label]["date"].tolist()
        )
        result.append((label, dates))

    return result


# ---------------------------------------------------------------------------
# Step 5: ARIMA / SARIMA per-store weekly forecast
# ---------------------------------------------------------------------------

def _load_tuned_parameters(logger=None):
    """Load pre-tuned ARIMA parameters from config/sales_forecast_parameters.json."""
    params_file = Path(__file__).parent.parent / "config" / "sales_forecast_parameters.json"
    if not params_file.exists():
        return None
    try:
        with open(params_file, "r") as f:
            params = json.load(f)
        return params
    except Exception as e:
        if logger:
            logger.warning(f"Could not load tuned parameters: {e}")
        return None


def _forecast_store(store_data, store_id, tuned_params, n_forecast, logger=None):
    """
    Run ARIMA or SARIMA on a store's weekly sales series.
    Returns list of n_forecast non-negative float values.
    """
    if len(store_data) < 4:
        return [0.0] * n_forecast

    store_key = str(store_id)
    if tuned_params and store_key in tuned_params:
        params = tuned_params[store_key]
        model_type = params.get("model_type", "ARIMA")
        order = tuple(params["order"])
    else:
        model_type = "ARIMA"
        order = (5, 1, 0)

    try:
        if model_type == "SARIMA":
            seasonal_order = tuple(params["seasonal_order"])
            model = SARIMAX(store_data["sales"], order=order, seasonal_order=seasonal_order)
            fit = model.fit(disp=False)
        else:
            model = ARIMA(store_data["sales"], order=order)
            fit = model.fit()

        fc = [max(0.0, float(v)) for v in fit.forecast(n_forecast)]
        if logger:
            logger.debug(f"  Store {store_id} {model_type}{order}: {[round(v, 2) for v in fc]}")
        return fc

    except Exception as e:
        if logger:
            logger.warning(f"  Forecast failed for store {store_id}: {e}. Using zeros.")
        return [0.0] * n_forecast


# ---------------------------------------------------------------------------
# Step 6: Overwrite protection
# ---------------------------------------------------------------------------

def _get_overwrite_keys(environment_url, access_token, week_labels, logger=None):
    """Return set of (store, week, daypart, dow) tuples that have overwrite=true."""
    if not week_labels:
        return set()

    week_filters = " or ".join(
        [f"crf63_forecastedfiscalweek eq '{w}'" for w in week_labels]
    )
    url = (
        f"{environment_url}/api/data/v9.2/crf63_bwsalesforecasts"
        f"?$filter=crf63_overwrite eq true and ({week_filters})"
        f"&$select=crf63_forecastedstore,crf63_forecastedfiscalweek,"
        f"crf63_forecasteddaypart,crf63_forecasteddow"
    )
    records = _query_paged(environment_url, access_token, url, logger)

    keys = set()
    for r in records:
        keys.add((
            str(r["crf63_forecastedstore"]),
            r["crf63_forecastedfiscalweek"],
            r["crf63_forecasteddaypart"],
            int(r["crf63_forecasteddow"]),
        ))

    return keys


# ---------------------------------------------------------------------------
# Step 7: Batch upsert to crf63_bwsalesforecasts
# ---------------------------------------------------------------------------

def _upsert_records(environment_url, access_token, records, overwrite_keys, logger=None):
    """
    Batch PATCH upsert using the composite alternate key:
      (crf63_forecasteddaypart, crf63_forecasteddow,
       crf63_forecastedfiscalweek, crf63_forecastedstore)

    Records that exist in overwrite_keys are skipped.
    Returns True if all batches succeeded.
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    table = "crf63_bwsalesforecasts"
    api_base = f"{environment_url}/api/data/v9.2"

    # Filter out overwrite-protected records
    filtered, skipped = [], 0
    for r in records:
        key = (
            str(r["crf63_forecastedstore"]),
            r["crf63_forecastedfiscalweek"],
            r["crf63_forecasteddaypart"],
            int(r["crf63_forecasteddow"]),
        )
        if key in overwrite_keys:
            skipped += 1
        else:
            filtered.append(r)

    if skipped:
        log(f"  Skipped {skipped} overwrite-protected records")

    if not filtered:
        log("  No records to upsert")
        return True

    batch_size = 50
    total = len(filtered)
    success_batches = 0
    total_batches = (total + batch_size - 1) // batch_size

    for batch_num, start in enumerate(range(0, total, batch_size), 1):
        batch = filtered[start : start + batch_size]
        batch_id = uuid.uuid4().hex
        changeset_id = uuid.uuid4().hex

        lines = []
        lines.append(f"--batch_{batch_id}")
        lines.append(f"Content-Type: multipart/mixed;boundary=changeset_{changeset_id}")
        lines.append("")

        for i, record in enumerate(batch):
            store = record["crf63_forecastedstore"]
            week = record["crf63_forecastedfiscalweek"]
            daypart = record["crf63_forecasteddaypart"]
            dow = int(record["crf63_forecasteddow"])

            lines.append(f"--changeset_{changeset_id}")
            lines.append("Content-Type: application/http")
            lines.append("Content-Transfer-Encoding: binary")
            lines.append(f"Content-ID: {i + 1}")
            lines.append("")
            lines.append(
                f"PATCH /api/data/v9.2/{table}"
                f"(crf63_forecasteddaypart='{daypart}',"
                f"crf63_forecasteddow={dow},"
                f"crf63_forecastedfiscalweek='{week}',"
                f"crf63_forecastedstore='{store}') HTTP/1.1"
            )
            lines.append("Content-Type: application/json;type=entry")
            lines.append("")
            lines.append(json.dumps(record))
            lines.append("")

        lines.append(f"--changeset_{changeset_id}--")
        lines.append("")
        lines.append(f"--batch_{batch_id}--")

        body = "\r\n".join(lines).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/mixed;boundary=batch_{batch_id}",
            "Accept": "multipart/mixed",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

        response = requests.post(f"{api_base}/$batch", headers=headers, data=body, timeout=120)

        if response.status_code == 200:
            text = response.text
            ok = (
                text.count("HTTP/1.1 200 OK")
                + text.count("HTTP/1.1 201 Created")
                + text.count("HTTP/1.1 204 No Content")
            )
            if ok == len(batch):
                success_batches += 1
            else:
                log(f"  WARNING: {len(batch) - ok} records failed in batch {batch_num}")
        else:
            log(f"  Batch {batch_num}/{total_batches} HTTP error: {response.status_code}")

    return success_batches == total_batches


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main(logger=None, dry_run=False):
    """
    Run the full sales forecast pipeline:
      1. Load fiscal calendar from Dataverse
      2. Load & aggregate weekly sales history from Dataverse
      3. Build daypart distribution grid from Dataverse
      4. Run ARIMA forecasts per store
      5. Break down weekly forecasts using distribution grid
      6. Upsert to crf63_bwsalesforecasts

    Returns dict with success, forecast_count, weeks, stores keys.
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    config = load_config()
    forecast_horizon = config.get("sales_forecast", {}).get("forecast_horizon_weeks", 5)

    # Credentials
    dv_creds = get_dataverse_credentials()
    environment_url = dv_creds["environment_url"]

    # Dataverse access token
    access_token = get_dataverse_access_token(
        environment_url,
        dv_creds["client_id"],
        dv_creds["client_secret"],
        dv_creds["tenant_id"],
        logger,
    )
    if not access_token:
        return {"success": False, "error": "Failed to obtain Dataverse access token"}

    # --- 1. Fiscal calendar
    log("Downloading crf63_bwfiscalcalendars...")
    try:
        fiscal_calendar = get_fiscal_calendar(environment_url, access_token, logger)
    except Exception as e:
        return {"success": False, "error": f"Fiscal calendar load failed: {e}"}
    log(f"  crf63_bwfiscalcalendars done. ({len(fiscal_calendar)} days)")

    # --- 2. Weekly sales history
    log("Downloading crf63_oarsbidatas...")
    try:
        weekly_df = get_weekly_sales_history(environment_url, access_token, fiscal_calendar, logger)
    except Exception as e:
        return {"success": False, "error": f"Sales history load failed: {e}"}
    log(f"  crf63_oarsbidatas done. ({weekly_df['store'].nunique()} stores, {len(weekly_df)} store-weeks)")

    # --- 3. Daypart distribution grid
    log("Downloading crf63_saleschanneldailies...")
    try:
        distribution = get_distribution_grid(environment_url, access_token, fiscal_calendar, logger)
    except Exception as e:
        return {"success": False, "error": f"Distribution grid build failed: {e}"}
    if not distribution:
        return {"success": False, "error": "Distribution grid is empty — no recent sales channel data"}
    log(f"  crf63_saleschanneldailies done. ({len(distribution)} stores)")

    # --- 4. Forecast weeks
    next_weeks = get_next_fiscal_weeks(fiscal_calendar, forecast_horizon, logger)
    if not next_weeks:
        return {"success": False, "error": "No future fiscal weeks found in calendar"}
    week_labels = [w[0] for w in next_weeks]
    log(f"Forecasting weeks: {', '.join(week_labels)}")

    # --- 5. ARIMA forecasts
    tuned_params = _load_tuned_parameters(logger)
    stores = weekly_df["store"].unique()

    store_forecasts = {}  # {store: {week_label: float}}
    for store in stores:
        store_data = weekly_df[weekly_df["store"] == store].reset_index(drop=True)
        fc_values = _forecast_store(store_data, store, tuned_params, forecast_horizon, logger)
        store_forecasts[store] = dict(zip(week_labels, fc_values))
        log(f"  Forecasting store {store}... done.")

    # --- 6. Build detailed forecast records
    today = datetime.utcnow().date()
    all_records = []

    for store, weekly_fc in store_forecasts.items():
        store_dist = distribution.get(store, {})
        if not store_dist:
            if logger:
                logger.debug(f"  No distribution data for store {store}, skipping")
            continue

        for week_label, _ in next_weeks:
            total_sales = weekly_fc.get(week_label, 0.0)
            if total_sales <= 0:
                continue

            for (dow, daypart), pct in store_dist.items():
                forecasted_sales = total_sales * pct
                if forecasted_sales <= 0:
                    continue

                all_records.append({
                    "crf63_forecastedfiscalweek": week_label,
                    "crf63_forecastedstore": str(store),
                    "crf63_forecasteddow": int(dow),
                    "crf63_forecasteddaypart": daypart,
                    "crf63_forecastedsales": round(float(forecasted_sales), 4),
                    "crf63_dateofforecast": today.isoformat(),
                })

    if not all_records:
        log("No forecast records to write")
        return {"success": True, "forecast_count": 0, "weeks": week_labels, "stores": 0}

    # --- 7. Upsert to Dataverse ----------------------------------------------
    if dry_run:
        log(f"\n[7/7] Dry run — skipping upsert of {len(all_records)} records")
        return {
            "success": True,
            "forecast_count": len(all_records),
            "weeks": week_labels,
            "stores": len(store_forecasts),
            "dry_run": True,
        }

    log("\n[7/7] Upserting forecast records to crf63_bwsalesforecasts...")

    overwrite_keys = _get_overwrite_keys(environment_url, access_token, week_labels, logger)
    success = _upsert_records(environment_url, access_token, all_records, overwrite_keys, logger)

    result = {
        "success": success,
        "forecast_count": len(all_records),
        "weeks": week_labels,
        "stores": len(store_forecasts),
    }

    log(
        f"\n{'SUCCESS' if success else 'PARTIAL FAILURE'}: "
        f"{len(all_records)} records for {len(store_forecasts)} stores "
        f"over {len(week_labels)} weeks"
    )
    return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    _logger = logging.getLogger(__name__)
    main(_logger)
