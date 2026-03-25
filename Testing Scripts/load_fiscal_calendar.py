#!/usr/bin/env python3
"""Back-populate crf63_bwfiscalcalendars with fiscal calendar rows.

Fiscal calendar rules:
  - The fiscal year starts on the Monday nearest to December 29 of the
    prior calendar year.
  - Each fiscal year has 13 periods of 4 weeks (52 weeks = 364 days).
  - When the Monday nearest to Dec 29 falls in the *next* calendar year
    (i.e. Dec 29 is a Fri/Sat/Sun), the current FY gets a 53rd week added
    to its last period (P13 becomes 5 weeks, 371 days total).

4-4-5 calendar (parallel calendar, 12 periods per year):
  - P01: W01-W04  P02: W05-W08  P03: W09-W13 (5 wk, end of Q1)
  - P04: W14-W17  P05: W18-W21  P06: W22-W26 (5 wk, end of Q2)
  - P07: W27-W30  P08: W31-W34  P09: W35-W39 (5 wk, end of Q3)
  - P10: W40-W43  P11: W44-W47  P12: W48-W52 (5 wk, end of Q4)
  - In a 53-week year P12 absorbs week 53 (becomes 6 weeks).
  - Quarters: Q1=P1-3, Q2=P4-6, Q3=P7-9, Q4=P10-12

13-4 calendar quarters: Q1=P1-3, Q2=P4-6, Q3=P7-9, Q4=P10-13

Usage:
  source .venv/bin/activate && python load_fiscal_calendar.py --confirm
  source .venv/bin/activate && python load_fiscal_calendar.py --dry-run
  source .venv/bin/activate && python load_fiscal_calendar.py --fy 2020 2021 2022 --confirm
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date, timedelta, datetime
from typing import List

import requests

from modules.utils.keyvault import get_dataverse_credentials
from modules.dataverse import get_dataverse_access_token

TABLE = "crf63_bwfiscalcalendars"
API_VERSION = "v9.2"
BATCH_SIZE = 100          # records per $batch changeset
REQUEST_DELAY = 0.05      # seconds between batch posts


# ---------------------------------------------------------------------------
# Fiscal calendar math
# ---------------------------------------------------------------------------

def _nearest_monday_to_dec29(year: int) -> date:
    """Return the Monday nearest to December 29 of *year*.

    If Dec 29 falls Mon-Thu we go back to that or the preceding Monday.
    If Dec 29 falls Fri-Sun we jump to the next Monday (in January).
    """
    anchor = date(year, 12, 29)
    dow = anchor.weekday()   # Mon=0 … Sun=6
    if dow <= 3:
        return anchor - timedelta(days=dow)
    else:
        return anchor + timedelta(days=7 - dow)


def fiscal_year_start(fy: int) -> date:
    """First day (Monday) of fiscal year *fy*."""
    return _nearest_monday_to_dec29(fy - 1)


def fiscal_year_end(fy: int) -> date:
    """Last day (Sunday) of fiscal year *fy*."""
    return fiscal_year_start(fy + 1) - timedelta(days=1)


def fiscal_year_weeks(fy: int) -> int:
    """52 or 53 weeks."""
    return (fiscal_year_start(fy + 1) - fiscal_year_start(fy)).days // 7


def _13_4_period(week: int) -> int:
    """13-4 fiscal period (1-13) from fiscal week number."""
    return min(13, (week - 1) // 4 + 1)


def _13_4_quarter(period: int) -> int:
    """13-4 fiscal quarter (1-4) from period number."""
    if period <= 3:
        return 1
    elif period <= 6:
        return 2
    elif period <= 9:
        return 3
    return 4


# Cumulative end-weeks for all 12 periods of the 4-4-5 calendar.
# P03, P06, P09, P12 are 5-week periods; all others are 4-week periods.
_445_END_WEEKS = [4, 8, 13, 17, 21, 26, 30, 34, 39, 43, 47, 52]


def _445_period(week: int) -> int:
    """4-4-5 fiscal period (1-12) from fiscal week number."""
    for p, end_wk in enumerate(_445_END_WEEKS, 1):
        if week <= end_wk:
            return p
    return 12   # week 53 (53-week year) stays in P12


def _445_quarter(period: int) -> int:
    """4-4-5 fiscal quarter (1-4) from 4-4-5 period number."""
    return (period - 1) // 3 + 1


def generate_fiscal_calendar(fy: int) -> List[dict]:
    """Return one Dataverse record dict per day in fiscal year *fy*."""
    start = fiscal_year_start(fy)
    end = fiscal_year_end(fy)
    weeks = fiscal_year_weeks(fy)

    records: List[dict] = []
    day_of_year = 1
    current = start

    while current <= end:
        week = (day_of_year - 1) // 7 + 1

        p_13_4 = _13_4_period(week)
        q_13_4 = _13_4_quarter(p_13_4)

        p_445 = _445_period(week)
        q_445 = _445_quarter(p_445)

        # Dataverse stores datetimes at 06:00 UTC (= midnight CST, UTC-6).
        date_str = current.strftime("%Y-%m-%dT06:00:00Z")

        records.append({
            "crf63_date":                  date_str,
            "crf63_dayofyear":             day_of_year,
            "crf63_calendardayofyear":     current.timetuple().tm_yday,
            "crf63_fiscalyearnumber":      fy,
            "crf63_fiscalyearlabel":       f"Y{fy}",
            "crf63_fiscalperiodnumber":    p_13_4,
            "crf63_fiscalperiodlabel":     f"P{p_13_4:02d}",
            "crf63_fiscalweeknumber":      week,
            "crf63_fiscalweeklabel":       f"W{week:02d}",
            "crf63_fiscalquarternumber":   q_13_4,
            "crf63_fiscalquarterlabel":    f"Q{q_13_4}",      # no zero-pad (matches existing data)
            "crf63__445fiscalperiodnumber":  p_445,
            "crf63__445fiscalperiodlabel":   f"P{p_445:02d}",
            "crf63__445fiscalquarternumber": q_445,
            "crf63__445fiscalquarterlabel":  f"Q{q_445:02d}",  # zero-padded (matches existing data)
        })

        current += timedelta(days=1)
        day_of_year += 1

    print(f"  FY{fy}: {fiscal_year_start(fy)} → {fiscal_year_end(fy)}  ({weeks} weeks, {len(records)} days)")
    return records


# ---------------------------------------------------------------------------
# Dataverse helpers
# ---------------------------------------------------------------------------

def _get_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Content-Type": "application/json",
    }


def fetch_existing_dates(api_url: str, headers: dict, start: date, end: date) -> set:
    """Return a set of calendar dates already present in the table for [start, end]."""
    existing = set()
    start_str = start.strftime("%Y-%m-%dT00:00:00Z")
    end_str   = (end + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    url = (
        f"{api_url}/{TABLE}"
        f"?$select=crf63_date"
        f"&$filter=crf63_date ge {start_str} and crf63_date lt {end_str}"
        f"&$top=5000"
    )
    while url:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code != 200:
            print(f"  ⚠ Failed to fetch existing dates: HTTP {resp.status_code} {resp.text[:200]}")
            return existing
        data = resp.json()
        for row in data.get("value", []):
            dt_str = row.get("crf63_date", "")[:10]  # "YYYY-MM-DD"
            if dt_str:
                existing.add(dt_str)
        url = data.get("@odata.nextLink")
    return existing


def _batch_create(api_url: str, token: str, records: List[dict]) -> tuple:
    """POST a list of records to Dataverse using $batch.

    Returns (created, errors).
    """
    created = errors = 0
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, 1):
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())

        lines: List[str] = []
        lines.append(f"--{batch_id}")
        lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
        lines.append("")
        lines.append("")

        for idx, rec in enumerate(batch, 1):
            import json as _json
            lines.append(f"--{changeset_id}")
            lines.append("Content-Type: application/http")
            lines.append("Content-Transfer-Encoding: binary")
            lines.append(f"Content-ID: {idx}")
            lines.append("")
            lines.append(f"POST {api_url}/{TABLE} HTTP/1.1")
            lines.append("Content-Type: application/json")
            lines.append("")
            lines.append(_json.dumps(rec))
            lines.append("")

        lines.append(f"--{changeset_id}--")
        lines.append(f"--{batch_id}--")
        lines.append("")

        body = "\r\n".join(lines).encode("utf-8")
        batch_headers = {
            "Authorization": f"Bearer {token}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": f"multipart/mixed; boundary={batch_id}",
            "Prefer": "odata.continue-on-error",
        }

        for attempt in range(5):
            try:
                r = requests.post(f"{api_url}/$batch", headers=batch_headers, data=body, timeout=(10, 300))
                if r.status_code in (200, 202):
                    created += len(batch)
                    break
                print(f"    Batch {batch_num} attempt {attempt+1}/5: HTTP {r.status_code} {r.text[:200]}")
            except requests.exceptions.RequestException as exc:
                print(f"    Batch {batch_num} attempt {attempt+1}/5: {exc}")
            time.sleep(2 ** attempt)
        else:
            errors += len(batch)
            print(f"    ✗ Batch {batch_num} failed after 5 attempts — {len(batch)} records NOT created")

        if batch_num % 5 == 0:
            print(f"    Progress: {min(batch_num * BATCH_SIZE, len(records))}/{len(records)} records processed")

        time.sleep(REQUEST_DELAY)

    return created, errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Back-populate crf63_bwfiscalcalendars (FY2020+)")
    parser.add_argument(
        "--fy",
        nargs="+",
        type=int,
        default=[2020, 2021, 2022],
        metavar="YEAR",
        help="Fiscal years to load (default: 2020 2021 2022)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required to actually write to Dataverse",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate records and print summary without writing",
    )
    args = parser.parse_args()

    if not args.confirm and not args.dry_run:
        raise SystemExit(
            "Pass --confirm to write to Dataverse, or --dry-run to preview only."
        )

    print("=" * 70)
    print("FISCAL CALENDAR BACKFILL")
    print(f"Fiscal years: {args.fy}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 70)

    # Print FY boundary summary
    print("\nFiscal year boundaries:")
    for fy in sorted(args.fy):
        weeks = fiscal_year_weeks(fy)
        print(f"  FY{fy}: {fiscal_year_start(fy)}  →  {fiscal_year_end(fy)}  ({weeks} weeks)")

    if args.dry_run:
        print("\nDry run — generating records without writing...")
        total = 0
        for fy in sorted(args.fy):
            recs = generate_fiscal_calendar(fy)
            total += len(recs)
            # Sample first and last record
            if recs:
                print(f"    First: {recs[0]['crf63_date'][:10]}  FY={recs[0]['crf63_fiscalyearnumber']} "
                      f"P{recs[0]['crf63_fiscalperiodnumber']} W{recs[0]['crf63_fiscalweeknumber']} Q{recs[0]['crf63_fiscalquarternumber']}")
                print(f"    Last:  {recs[-1]['crf63_date'][:10]}  FY={recs[-1]['crf63_fiscalyearnumber']} "
                      f"P{recs[-1]['crf63_fiscalperiodnumber']} W{recs[-1]['crf63_fiscalweeknumber']} Q{recs[-1]['crf63_fiscalquarternumber']}")
        print(f"\nTotal records that would be created: {total:,}")
        print("✅ Dry run complete")
        return 0

    # --- Live run ---
    creds = get_dataverse_credentials()
    dv_url = creds["environment_url"]
    token = get_dataverse_access_token(dv_url, creds["client_id"], creds["client_secret"], creds["tenant_id"])
    api_url = dv_url.rstrip("/") + f"/api/data/{API_VERSION}"
    headers = _get_headers(token)

    def refresh_token():
        nonlocal token
        token = get_dataverse_access_token(dv_url, creds["client_id"], creds["client_secret"], creds["tenant_id"])
        headers["Authorization"] = f"Bearer {token}"

    grand_created = grand_errors = 0

    for fy in sorted(args.fy):
        print(f"\n==> FY{fy}")
        all_records = generate_fiscal_calendar(fy)
        if not all_records:
            print("  No records generated — skipping.")
            continue

        fy_start = fiscal_year_start(fy)
        fy_end   = fiscal_year_end(fy)

        print(f"  Checking for existing records in {fy_start} – {fy_end} ...")
        existing_dates = fetch_existing_dates(api_url, headers, fy_start, fy_end)
        print(f"  Already present: {len(existing_dates)} dates")

        # Filter to only records not yet in Dataverse
        new_records = [
            r for r in all_records
            if r["crf63_date"][:10] not in existing_dates
        ]
        print(f"  To create: {len(new_records)} records")

        if not new_records:
            print("  ✓ All dates already present — nothing to do.")
            continue

        refresh_token()
        print(f"  Creating {len(new_records)} records in batches of {BATCH_SIZE} ...")
        created, errors = _batch_create(api_url, token, new_records)
        print(f"  ✓ FY{fy}: {created} created, {errors} errors")
        grand_created += created
        grand_errors  += errors

    print("\n" + "=" * 70)
    print(f"✅ Complete — {grand_created} created, {grand_errors} errors")
    return 0 if grand_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
