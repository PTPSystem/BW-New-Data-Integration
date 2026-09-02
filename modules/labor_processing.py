"""Download Papa John's labor ZIPs from files.papajohns.com and load day-part hours.

Ported from Beachwood-Data-Integration/modules/labor_processing.py.

Flow:
  1. Log in to https://files.papajohns.com
  2. Download the latest (or configured) pay_summ/summary ZIP per store
  3. Extract timecard + employee-detail CSVs
  4. Split punch hours into day parts (Morning / Lunch / ...)
  5. Upsert aggregated hours to Dataverse crf63_labortimes
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dt_parser

from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.utils.config import load_config
from modules.utils.keyvault import get_dataverse_credentials, get_labor_files_credentials

FILE_NAME_RE = re.compile(r"(\d{6}\.(?:pay_summ|summary)\.(\d{8})\.\d+\.zip)")
DEFAULT_STORE_EXCEL = (
    "/Users/howardshen/Library/CloudStorage/OneDrive-SharedLibraries-globalpacmgt.com/"
    "IT Project - General/BI Import/BI Dimensions.xlsx"
)
TZINFOS = {
    "PDT": timezone(timedelta(hours=-7)),
    "EDT": timezone(timedelta(hours=-4)),
    "CDT": timezone(timedelta(hours=-5)),
    "MDT": timezone(timedelta(hours=-6)),
    "PST": timezone(timedelta(hours=-8)),
    "EST": timezone(timedelta(hours=-5)),
    "CST": timezone(timedelta(hours=-6)),
    "MST": timezone(timedelta(hours=-7)),
}


def _log(logger, msg: str, level: str = "info") -> None:
    if logger:
        getattr(logger, level)(msg)
    else:
        print(msg)


def normalize_store_number(store: Any) -> str:
    """Strip leading zeros so store numbers match other Dataverse tables."""
    value = str(store).strip()
    if value.isdigit():
        return str(int(value))
    return value


def inject_labor_credentials(config: Dict[str, Any], logger=None) -> Dict[str, Any]:
    """Fill username/password/base_url from Key Vault when config leaves them blank."""
    labor = config.setdefault("labor_processing", {})
    creds = get_labor_files_credentials()
    if not labor.get("username"):
        labor["username"] = creds.get("files-username")
    if not labor.get("password"):
        labor["password"] = creds.get("files-password")
    if creds.get("files-url") and not labor.get("base_url"):
        labor["base_url"] = creds["files-url"]
    if not labor.get("username") or not labor.get("password"):
        raise RuntimeError(
            "Labor portal credentials missing. Set files-username and files-password in Key Vault."
        )
    _log(logger, "Loaded labor portal credentials from Key Vault")
    return labor


def get_store_numbers_from_excel(config: Dict[str, Any], logger=None) -> List[str]:
    """Read expected store numbers from BI Dimensions.xlsx, Stores tab."""
    labor = config.get("labor_processing", {})
    excel_path = labor.get("store_list_excel") or DEFAULT_STORE_EXCEL
    if not excel_path or not os.path.exists(excel_path):
        raise FileNotFoundError(f"Store list Excel not found: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name="Stores")
    column = "Stores Number 0" if "Stores Number 0" in df.columns else None
    if column is None:
        for candidate in df.columns:
            if "store" in str(candidate).lower() and "number" in str(candidate).lower():
                column = candidate
                break
    if column is None:
        raise ValueError(f"'Stores Number 0' column not found in Stores tab of {excel_path}")

    store_numbers = df[column].dropna().astype(str).str.zfill(6).unique().tolist()
    _log(logger, f"Loaded {len(store_numbers)} store numbers from {excel_path}")
    return store_numbers


def get_store_numbers_from_dataverse(config: Dict[str, Any], logger=None) -> List[str]:
    """Distinct store numbers already loaded into Dataverse (clock-in or OARS)."""
    dv_creds = get_dataverse_credentials()
    token = get_dataverse_access_token(
        dv_creds["environment_url"],
        dv_creds["client_id"],
        dv_creds["client_secret"],
        dv_creds["tenant_id"],
        logger,
    )
    if not token:
        raise RuntimeError("Could not get Dataverse token for store list")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }
    tables = config.get("dataverse", {}).get("table_names", {})
    candidates = [
        tables.get("clock_in_out", "crf63_bw_clockinouts"),
        tables.get("olap_bi_data", "crf63_oarsbidatas"),
        tables.get("store_hours", "crf63_storeoperatinghours"),
    ]
    api = f"{dv_creds['environment_url'].rstrip('/')}/api/data/v9.2"
    last_error = None
    for table in candidates:
        url = f"{api}/{table}?$select=crf63_storenumber&$top=5000"
        try:
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code != 200:
                last_error = f"{table}: HTTP {response.status_code}"
                continue
            stores = {
                str(row.get("crf63_storenumber", "")).zfill(6)
                for row in response.json().get("value", [])
                if row.get("crf63_storenumber") is not None
            }
            stores.discard("000000")
            if stores:
                _log(logger, f"Loaded {len(stores)} store numbers from Dataverse {table}")
                return sorted(stores)
        except Exception as exc:
            last_error = str(exc)
    raise RuntimeError(f"Could not load store numbers from Dataverse: {last_error}")


def get_expected_store_numbers(config: Dict[str, Any], logger=None) -> List[str]:
    """Prefer local BI Dimensions.xlsx; fall back to Dataverse."""
    try:
        return get_store_numbers_from_excel(config, logger)
    except Exception as excel_error:
        _log(logger, f"Store list Excel unavailable ({excel_error}); trying Dataverse", "warning")
        try:
            return get_store_numbers_from_dataverse(config, logger)
        except Exception as dv_error:
            _log(logger, f"Store list Dataverse unavailable ({dv_error}); skipping missing-store check", "warning")
            return []


def get_file_list(config: Dict[str, Any], session: requests.Session, logger=None):
    """Log in, list portal files, and return the latest ZIP per store for the target date."""
    labor = config["labor_processing"]
    base_url = labor["base_url"]
    login_url = labor["login_url"]
    config_date = labor.get("config_date") or ""

    login_page = session.get(login_url)
    if login_page.status_code != 200:
        raise Exception(f"Failed to access login page. Status code: {login_page.status_code}")

    login_response = session.post(
        login_url,
        data={"username": labor["username"], "password": labor["password"]},
    )
    if login_response.status_code != 200 or "Login" in login_response.url:
        raise Exception(f"Form-based login failed. Status code: {login_response.status_code}")

    response = session.get(base_url)
    if response.status_code != 200:
        raise Exception(f"Failed to access file list page. Status code: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    if config_date:
        if not re.match(r"^\d{8}$", config_date):
            raise ValueError(f"CONFIG_DATE must be in YYYYMMDD format, got: {config_date}")
        target_date = config_date
        _log(logger, f"Using CONFIG_DATE: {target_date}")
    else:
        dates = set()
        for link in soup.find_all("a", href=True):
            match = FILE_NAME_RE.search(link.text.strip())
            if match:
                dates.add(match.group(2))
        if not dates:
            raise Exception("No matching files found in the file list.")
        target_date = max(dates)
        _log(logger, f"Latest date found: {target_date}")

    file_pattern = re.compile(rf"(\d{{6}}\.(?:pay_summ|summary)\.{target_date}\.\d+\.zip)")
    files_by_store: Dict[str, Tuple[str, str, str]] = {}
    found_stores = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        file_name = link.text.strip()
        if not file_pattern.search(file_name):
            continue
        store_number = file_name[:6]
        found_stores.add(store_number)
        timestamp = file_name.split(".")[3]
        if not href.startswith("http"):
            href = base_url + href.lstrip("/")
        if store_number not in files_by_store or timestamp > files_by_store[store_number][0]:
            files_by_store[store_number] = (timestamp, file_name, href)

    files = [(file_name, href) for _, file_name, href in files_by_store.values()]
    if not files:
        raise Exception(f"No files found for target date: {target_date}")

    expected_stores = set(get_expected_store_numbers(config, logger))
    missing_stores = sorted(expected_stores - found_stores) if expected_stores else []
    if missing_stores:
        _log(logger, f"Missing files for stores: {', '.join(missing_stores)}", "warning")
    elif expected_stores:
        _log(logger, "All expected store files found")

    return files, target_date, missing_stores


def download_files(files: Sequence[Tuple[str, str]], session: requests.Session, config: Dict[str, Any], logger=None) -> int:
    download_dir = config["labor_processing"]["download_dir"]
    download_count = 0
    for file_name, file_url in files:
        file_path = os.path.join(download_dir, file_name)
        _log(logger, f"Downloading {file_name} from {file_url}...", "debug")
        response = session.get(file_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    handle.write(chunk)
            download_count += 1
        else:
            _log(logger, f"Failed to download {file_name}. Status code: {response.status_code}", "error")
    return download_count


def extract_files(files: Sequence[Tuple[str, str]], config: Dict[str, Any], logger=None) -> int:
    download_dir = config["labor_processing"]["download_dir"]
    extract_dir = config["labor_processing"]["extract_dir"]
    extract_count = 0
    for file_name, _ in files:
        file_path = os.path.join(download_dir, file_name)
        extract_path = os.path.join(extract_dir, file_name.replace(".zip", ""))
        os.makedirs(extract_path, exist_ok=True)
        _log(logger, f"Extracting {file_name}...", "debug")
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)
        extract_count += 1
    return extract_count


def load_employee_names(target_date_str: str, config: Dict[str, Any], logger=None) -> Dict[str, str]:
    extract_dir = config["labor_processing"]["extract_dir"]
    employee_names: Dict[str, str] = {}
    file_pattern = re.compile(rf"\d{{6}}\.p_hs_employee_detail\.{target_date_str}_\d+\.csv")

    for root, _dirs, files in os.walk(extract_dir):
        for file in files:
            if not file_pattern.search(file):
                continue
            file_path = os.path.join(root, file)
            _log(logger, f"Loading employee names from {file_path}...", "debug")
            try:
                with open(file_path, "r", encoding="utf-8") as handle:
                    reader = csv.reader(handle, delimiter="\t")
                    for row in reader:
                        if len(row) <= 5:
                            _log(logger, f"Row too short, skipping: {row}", "warning")
                            continue
                        employee_id = row[3].strip()
                        first_name = row[4].strip()
                        last_name = row[5].strip()
                        employee_names[employee_id] = f"{first_name} {last_name}".strip()
            except Exception as exc:
                _log(logger, f"Error loading employee names from {file_path}: {exc}", "error")

    _log(logger, f"Retrieved {len(employee_names)} employee names", "debug")
    return employee_names


def decimal_to_time(decimal_hours: float) -> Tuple[int, int]:
    hours = int(decimal_hours)
    minutes = int((decimal_hours - hours) * 60)
    return hours, minutes


def decimal_to_datetime(date_str: str, decimal_hours: float) -> datetime:
    hours, minutes = decimal_to_time(decimal_hours)
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    return base_date + timedelta(hours=hours, minutes=minutes)


def extract_date_from_filename(filename: str) -> str:
    date_match = re.search(r"(\d{8})", filename)
    if date_match:
        date_str = date_match.group(1)
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    raise ValueError(f"Could not extract date from filename: {filename}")


def get_day_part(time, config: Dict[str, Any]) -> str:
    day_parts = config["labor_processing"]["day_parts"]
    if isinstance(time, datetime):
        decimal_hours = time.hour + time.minute / 60.0
    else:
        decimal_hours = time

    for part, (start, end) in day_parts.items():
        if start <= decimal_hours < end:
            return part
    return "Late Night"


def _parse_punch_time(value: str, logger=None, label: str = "time", source: str = "") -> Optional[datetime]:
    try:
        parsed = dt_parser.parse(value, tzinfos=TZINFOS).replace(tzinfo=None)
        _log(logger, f"Parsed {label}: {value} as {parsed}", "debug")
        return parsed
    except (ValueError, TypeError):
        try:
            parsed = datetime.strptime(value.split(".")[0], "%Y-%m-%d %H:%M:%S")
            _log(logger, f"Assuming PDT for {label} {value} in {source}", "warning")
            return parsed
        except ValueError:
            _log(logger, f"Invalid {label} format in {source}: {value}. Skipping row.", "warning")
            return None


def process_labor_time(target_date_str: str, config: Dict[str, Any], logger=None) -> List[Dict[str, Any]]:
    """Split timecard punches into day-part hour records (unaggregated)."""
    extract_dir = config["labor_processing"]["extract_dir"]
    target_date = f"{target_date_str[:4]}-{target_date_str[4:6]}-{target_date_str[6:8]}"
    target_date_dt = datetime.strptime(target_date, "%Y-%m-%d")
    next_day_8am = target_date_dt + timedelta(days=1, hours=8)
    employee_names = load_employee_names(target_date_str, config, logger)
    new_records: List[Dict[str, Any]] = []
    file_pattern = re.compile(rf"\d{{6}}\.p_hs_timecard_record\.{target_date_str}_\d+\.csv")

    for root, _dirs, files in os.walk(extract_dir):
        for file in files:
            if not file_pattern.search(file):
                continue
            file_path = os.path.join(root, file)
            _log(logger, f"Processing {file_path}...", "debug")
            try:
                labor_date = extract_date_from_filename(file)
                with open(file_path, "r", encoding="utf-8") as handle:
                    reader = csv.reader(handle, delimiter="\t")
                    for row in reader:
                        if len(row) < 14:
                            _log(logger, f"Row too short, skipping: {row}", "warning")
                            continue
                        store_number = row[0]
                        employee_id = row[10].strip()
                        clock_in_str = row[7].strip()
                        clock_out_str = row[8].strip()
                        regular_time = float(row[12] or 0)
                        overtime = float(row[13] or 0)

                        clock_in_dt = _parse_punch_time(clock_in_str, logger, "clock-in", file)
                        clock_out_dt = _parse_punch_time(clock_out_str, logger, "clock-out", file)
                        if clock_in_dt is None or clock_out_dt is None:
                            continue

                        if clock_in_dt > next_day_8am:
                            _log(
                                logger,
                                f"Skipping record in {file} with clock-in {clock_in_str} after 8 AM next day ({next_day_8am})",
                            )
                            continue

                        if clock_out_dt > next_day_8am:
                            _log(
                                logger,
                                f"Capping clock-out in {file} from {clock_out_str} to {next_day_8am} (8 AM next day)",
                            )
                            original_duration = (clock_out_dt - clock_in_dt).total_seconds() / 3600.0
                            capped_duration = (next_day_8am - clock_in_dt).total_seconds() / 3600.0
                            if original_duration > 0:
                                total_hours = (regular_time + overtime) / 60.0 * (capped_duration / original_duration)
                            else:
                                total_hours = 0
                            clock_out_dt = next_day_8am
                        else:
                            total_hours = (regular_time + overtime) / 60.0

                        if clock_out_dt < clock_in_dt:
                            clock_out_dt += timedelta(days=1)

                        current_time = clock_in_dt
                        remaining_hours = total_hours

                        while remaining_hours > 0 and current_time < clock_out_dt:
                            day_part = get_day_part(current_time, config)
                            day_part_end = clock_out_dt
                            for part, (start, end) in config["labor_processing"]["day_parts"].items():
                                if part != day_part:
                                    continue
                                if start == 0.0 and end == 8.0:
                                    end = 32.0
                                day_part_end = decimal_to_datetime(labor_date, end)
                                if end >= 24.0:
                                    day_part_end = decimal_to_datetime(labor_date, end - 24.0) + timedelta(days=1)
                                break

                            time_in_part_end = min(clock_out_dt, day_part_end)
                            hours_in_part = (time_in_part_end - current_time).total_seconds() / 3600.0
                            new_records.append(
                                {
                                    "store_number": store_number,
                                    "employee_id": employee_id,
                                    "employee_name": employee_names.get(employee_id, "Unknown"),
                                    "day_part": day_part,
                                    "total_hours": round(hours_in_part, 2),
                                    "date": labor_date,
                                }
                            )
                            remaining_hours -= hours_in_part
                            current_time = time_in_part_end
            except Exception as exc:
                _log(logger, f"Error processing {file_path}: {exc}", "error")

    _log(logger, f"Processed {len(new_records)} day-part slices for {target_date}")
    return new_records


def aggregate_labor_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sum hours for the Dataverse key store + employee + date + day part."""
    grouped: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for record in records:
        key = (
            str(record["store_number"]),
            str(record["employee_id"]),
            str(record["date"]),
            str(record["day_part"]),
        )
        if key not in grouped:
            grouped[key] = {
                "store_number": record["store_number"],
                "employee_id": record["employee_id"],
                "employee_name": record.get("employee_name", "Unknown"),
                "day_part": record["day_part"],
                "total_hours": 0.0,
                "date": record["date"],
            }
        grouped[key]["total_hours"] = round(grouped[key]["total_hours"] + float(record["total_hours"]), 2)
        if grouped[key]["employee_name"] == "Unknown" and record.get("employee_name"):
            grouped[key]["employee_name"] = record["employee_name"]
    return list(grouped.values())


def records_to_dataverse(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.now().isoformat()
    payload = []
    for record in records:
        store = normalize_store_number(record["store_number"])
        work_date = record["date"]
        date_key = work_date.replace("-", "")
        day_part = record["day_part"]
        employee_id = str(record["employee_id"])
        payload.append(
            {
                "crf63_businesskey": f"{store}_{employee_id}_{date_key}_{day_part}",
                "crf63_storenumber": store,
                "crf63_employeeid": employee_id,
                "crf63_employeename": record.get("employee_name") or "Unknown",
                "crf63_daypart": day_part,
                "crf63_totalhours": float(record["total_hours"]),
                "crf63_workdate": work_date,
                "crf63_name": f"{store} - {work_date} - {record.get('employee_name') or employee_id} - {day_part}",
                "crf63_lastrefreshed": now,
                "crf63_datasource": "labor_processing",
            }
        )
    return payload


def write_labor_csv(records: Sequence[Dict[str, Any]], output_file: str, logger=None) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_file)) or ".", exist_ok=True)
    fieldnames = ["store_number", "employee_id", "employee_name", "day_part", "total_hours", "date"]
    with open(output_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow({key: record.get(key, "") for key in fieldnames})
    _log(logger, f"Wrote {len(records)} records to {output_file}")


def cleanup_temp_dirs(config: Dict[str, Any], logger=None) -> None:
    for key in ("download_dir", "extract_dir"):
        path = config["labor_processing"].get(key)
        if not path:
            continue
        _log(logger, f"Cleaning up {path}...")
        try:
            shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
        except Exception as exc:
            _log(logger, f"Error cleaning {path}: {exc}", "error")


def upsert_labor_to_dataverse(records: Sequence[Dict[str, Any]], config: Dict[str, Any], logger=None) -> Dict[str, int]:
    table_name = (
        config.get("labor_processing", {}).get("dataverse_table")
        or config.get("dataverse", {}).get("table_names", {}).get("labor_times")
        or "crf63_labortimes"
    )
    dv_creds = get_dataverse_credentials()
    token = get_dataverse_access_token(
        dv_creds["environment_url"],
        dv_creds["client_id"],
        dv_creds["client_secret"],
        dv_creds["tenant_id"],
        logger,
    )
    if not token:
        raise RuntimeError("Failed to get Dataverse access token")

    payload = records_to_dataverse(records)
    created, updated, errors = upsert_to_dataverse(
        dv_creds["environment_url"],
        token,
        table_name,
        payload,
        "crf63_businesskey",
        logger,
    )
    return {"created": created, "updated": updated, "errors": errors}


def run_labor_processing(
    config: Optional[Dict[str, Any]] = None,
    logger=None,
    *,
    target_date: Optional[str] = None,
    skip_dataverse: bool = False,
    keep_temp: bool = False,
    csv_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the full labor download → process → Dataverse flow."""
    config = config or load_config()
    inject_labor_credentials(config, logger)
    labor = config["labor_processing"]
    if target_date:
        labor["config_date"] = target_date

    os.makedirs(labor["download_dir"], exist_ok=True)
    os.makedirs(labor["extract_dir"], exist_ok=True)

    _log(logger, "Starting labor processing")
    session = requests.Session()
    session.auth = (labor["username"], labor["password"])
    session.verify = bool(labor.get("ssl_verify", False))
    if not session.verify:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        _log(logger, "TLS verification disabled for files.papajohns.com (labor_processing.ssl_verify=false)")

    files, resolved_date, missing_stores = get_file_list(config, session, logger)
    if not files:
        return {
            "success": True,
            "download_count": 0,
            "extract_count": 0,
            "record_count": 0,
            "missing_stores": missing_stores,
            "target_date": resolved_date,
        }

    download_count = download_files(files, session, config, logger)
    _log(logger, f"Downloaded {download_count} files")
    extract_count = extract_files(files, config, logger)
    _log(logger, f"Extracted {extract_count} files")

    raw_records = process_labor_time(resolved_date, config, logger)
    records = aggregate_labor_records(raw_records)
    _log(logger, f"Aggregated to {len(records)} store/employee/day-part rows")

    output_file = csv_path or labor.get("output_file")
    if output_file:
        write_labor_csv(records, output_file, logger)

    upsert_result = {"created": 0, "updated": 0, "errors": 0}
    if skip_dataverse:
        _log(logger, "Skipping Dataverse upsert")
    else:
        upsert_result = upsert_labor_to_dataverse(records, config, logger)
        _log(
            logger,
            f"Dataverse upsert: {upsert_result['created']} created, "
            f"{upsert_result['updated']} updated, {upsert_result['errors']} errors",
        )

    if not keep_temp:
        cleanup_temp_dirs(config, logger)

    _log(logger, "Labor processing completed successfully")
    return {
        "success": upsert_result.get("errors", 0) == 0 if not skip_dataverse else True,
        "download_count": download_count,
        "extract_count": extract_count,
        "record_count": len(records),
        "raw_record_count": len(raw_records),
        "missing_stores": missing_stores,
        "target_date": resolved_date,
        "records_created": upsert_result.get("created", 0),
        "records_updated": upsert_result.get("updated", 0),
        "errors": upsert_result.get("errors", 0),
    }


def main(config=None, logger=None):
    """Backward-compatible entry used by the old orchestrator."""
    return run_labor_processing(config, logger)
