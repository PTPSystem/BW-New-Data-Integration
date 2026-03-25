#!/usr/bin/env python3
"""
normalize_store_numbers.py

Removes leading zeros from store numbers in Dataverse tables.

Background:
    Papa John's changed the store number format in the OLAP/MDX feed from
    zero-padded (e.g. 003716, 000128) to plain integers (e.g. 3716, 128).
    Existing historical records still have the old padded format, which means
    new UPSERT operations will create duplicate records instead of updating them.

    This script finds all records whose crf63_storenumber starts with '0',
    strips the leading zeros, and patches both the store number field AND the
    crf63_businesskey (which embeds the store number) using the record's
    PRIMARY KEY so the alternate key itself can safely change.

Tables covered (all sourced from the OARS Franchise OLAP cube):
    oarsbi       → crf63_oarsbidatas
    sales_channel → crf63_saleschanneldailies
    inventory    → crf63_inventories
    offers       → crf63_offerses
    clockinout   → crf63_bw_clockinouts

Usage:
    # Preview what would change (no writes):
    .venv/bin/python normalize_store_numbers.py --dry-run

    # Preview a specific table only:
    .venv/bin/python normalize_store_numbers.py --dry-run --table oarsbi

    # Apply to all tables:
    .venv/bin/python normalize_store_numbers.py

    # Apply to a specific table:
    .venv/bin/python normalize_store_numbers.py --table oarsbi
    .venv/bin/python normalize_store_numbers.py --table sales_channel
    .venv/bin/python normalize_store_numbers.py --table inventory
    .venv/bin/python normalize_store_numbers.py --table offers
    .venv/bin/python normalize_store_numbers.py --table clockinout
"""

import os
import sys
import json
import time
import uuid
import argparse
import requests
import msal
import concurrent.futures
from collections import defaultdict
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.utils.keyvault import get_dataverse_credentials

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Table configuration
# Each entry describes how to find, build keys, and patch a table.
# ──────────────────────────────────────────────────────────────────────────────

TABLE_CONFIG = {
    "oarsbi": {
        "display_name":  "OARS BI Data",
        "table":         "crf63_oarsbidatas",
        "primary_key":   "crf63_oarsbidataid",
        "store_field":   "crf63_storenumber",
        "bk_field":      "crf63_businesskey",
        "date_field":    "crf63_calendardate",
        # Extra fields needed to reconstruct the business key (beyond store + date)
        "extra_bk_fields": [],
        # Returns new business key given: new_store (str), date_yyyymmdd (str), extras (dict)
        "bk_builder": lambda new_store, date_yyyymmdd, extras:
            f"{new_store}_{date_yyyymmdd}",
    },
    "sales_channel": {
        "display_name":  "Sales Channel Daily",
        "table":         "crf63_saleschanneldailies",
        "primary_key":   "crf63_saleschanneldailyid",
        "store_field":   "crf63_storenumber",
        "bk_field":      "crf63_businesskey",
        "date_field":    "crf63_calendardate",
        "extra_bk_fields": ["crf63_sourceactor", "crf63_sourcechannel", "crf63_daypart"],
        "bk_builder": lambda new_store, date_yyyymmdd, extras:
            f"{new_store}_{date_yyyymmdd}_{extras['crf63_sourceactor']}_{extras['crf63_sourcechannel']}_{extras['crf63_daypart']}",
    },
    "inventory": {
        "display_name":  "Inventory",
        "table":         "crf63_inventories",
        "primary_key":   "crf63_inventoryid",
        "store_field":   "crf63_storenumber",
        "bk_field":      "crf63_businesskey",
        "date_field":    "crf63_calendardate",
        "extra_bk_fields": ["crf63_itemnumber"],
        "bk_builder": lambda new_store, date_yyyymmdd, extras:
            f"{new_store}_{date_yyyymmdd}_{extras['crf63_itemnumber']}",
    },
    "offers": {
        "display_name":  "Offers",
        "table":         "crf63_offerses",
        "primary_key":   "crf63_offersid",
        "store_field":   "crf63_storenumber",
        "bk_field":      "crf63_businesskey",
        "date_field":    "crf63_calendardate",
        "extra_bk_fields": ["crf63_offercode"],
        "bk_builder": lambda new_store, date_yyyymmdd, extras:
            f"{new_store}_{date_yyyymmdd}_{extras['crf63_offercode']}",
    },
    "clockinout": {
        "display_name":  "Clock In/Out",
        "table":         "crf63_bw_clockinouts",
        "primary_key":   "crf63_bw_clockinoutid",
        "store_field":   "crf63_storenumber",
        "bk_field":      "crf63_businesskey",
        "date_field":    "crf63_calendardate",
        "extra_bk_fields": ["crf63_employeename", "crf63_systemuserid"],
        "bk_builder": lambda new_store, date_yyyymmdd, extras:
            f"{new_store}_{date_yyyymmdd}_{extras['crf63_employeename']}_{extras['crf63_systemuserid']}",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────────────

def get_token(creds: dict) -> str:
    app = msal.ConfidentialClientApplication(
        creds["client_id"],
        authority=f"https://login.microsoftonline.com/{creds['tenant_id']}",
        client_credential=creds["client_secret"],
    )
    result = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Failed to get token: {result.get('error_description', result)}")
    return result["access_token"]


# ──────────────────────────────────────────────────────────────────────────────
# Query
# ──────────────────────────────────────────────────────────────────────────────

def fetch_padded_records(api_url: str, headers: dict, cfg: dict) -> list:
    """
    Fetch all records where crf63_storenumber starts with '0'.
    Handles OData server-side paging automatically.
    """
    pk           = cfg["primary_key"]
    store_field  = cfg["store_field"]
    bk_field     = cfg["bk_field"]
    date_field   = cfg["date_field"]
    extras       = cfg["extra_bk_fields"]

    select = ",".join([pk, store_field, bk_field, date_field] + extras)
    # Do NOT use $top — Dataverse won't return @odata.nextLink when $top is set,
    # causing pagination to silently stop after the first 5,000 records.
    # Instead, use the Prefer header to set page size and rely on nextLink.
    url = (
        f"{api_url}/{cfg['table']}"
        f"?$filter=startswith({store_field},'0')"
        f"&$select={select}"
    )
    page_headers = {**headers, "Prefer": "odata.maxpagesize=5000"}

    all_records = []
    page = 0
    while url:
        page += 1
        resp = requests.get(url, headers=page_headers, timeout=120)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Query failed (HTTP {resp.status_code}):\n{resp.text[:600]}"
            )
        data = resp.json()
        batch = data.get("value", [])
        all_records.extend(batch)
        print(f"  Page {page}: {len(batch):,} records  (running total: {len(all_records):,})", flush=True)
        url = data.get("@odata.nextLink")

    return all_records


# ──────────────────────────────────────────────────────────────────────────────
# Business-key reconstruction
# ──────────────────────────────────────────────────────────────────────────────

def _date_to_yyyymmdd(date_val: str) -> str:
    """Convert Dataverse ISO date string (2024-01-15T00:00:00Z) → '20240115'."""
    if not date_val:
        return ""
    return date_val[:10].replace("-", "")


def compute_new_values(rec: dict, cfg: dict) -> tuple[str, str]:
    """
    Returns (new_store_number, new_business_key) with leading zeros stripped.
    """
    old_store     = rec[cfg["store_field"]]
    new_store     = old_store.lstrip("0") or "0"   # guard against all-zero strings
    date_yyyymmdd = _date_to_yyyymmdd(rec.get(cfg["date_field"], ""))
    extras        = {f: rec.get(f, "") for f in cfg["extra_bk_fields"]}
    new_bk        = cfg["bk_builder"](new_store, date_yyyymmdd, extras)
    return new_store, new_bk


# ──────────────────────────────────────────────────────────────────────────────
# Batch PATCH (via primary key, NOT alternate key)
# ──────────────────────────────────────────────────────────────────────────────

def batch_patch_records(
    api_url: str,
    headers: dict,
    cfg: dict,
    records: list,
    batch_size: int = 400,
    max_workers: int = 6,
) -> tuple[int, int]:
    """
    PATCH records using their primary GUID key so the alternate key
    (crf63_businesskey) can safely be changed in the same operation.
    """
    table     = cfg["table"]
    pk        = cfg["primary_key"]
    store_fld = cfg["store_field"]
    bk_fld    = cfg["bk_field"]
    batch_url = f"{api_url}/$batch"

    # Build patch payloads
    patches = []
    for rec in records:
        new_store, new_bk = compute_new_values(rec, cfg)
        patches.append({
            "guid":    rec[pk],
            "payload": {store_fld: new_store, bk_fld: new_bk},
        })

    total   = len(patches)
    batches = [patches[i : i + batch_size] for i in range(0, total, batch_size)]

    total_ok  = 0
    total_err = 0
    start     = time.time()

    def do_batch(chunk: list) -> tuple[int, int]:
        batch_id     = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        parts        = [
            f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()
        ]

        for i, p in enumerate(chunk, 1):
            payload_json = json.dumps(p["payload"], separators=(",", ":"))
            part = (
                f"--{changeset_id}\r\n"
                f"Content-Type: application/http\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                f"\r\n"
                f"PATCH {api_url}/{table}({p['guid']}) HTTP/1.1\r\n"
                f"Content-Type: application/json\r\n"
                f"\r\n"
                f"{payload_json}\r\n"
            ).encode()
            parts.append(part)

        parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
        body = b"".join(parts)

        req_headers = {
            **headers,
            "Content-Type": f"multipart/mixed; boundary={batch_id}",
            "Prefer":        "odata.continue-on-error",
        }

        for _attempt in range(4):
            try:
                r = requests.post(batch_url, headers=req_headers, data=body, timeout=600)
                if r.status_code in (200, 204):
                    ok  = r.text.count("HTTP/1.1 204 No Content")
                    err = sum(
                        r.text.count(f"HTTP/1.1 {code}")
                        for code in ["400", "401", "403", "404", "409", "412", "500", "502", "503"]
                    )
                    # Surface a sample error for debugging
                    if err:
                        for part in r.text.split("\nHTTP/1.1 "):
                            first = part.split("\n", 1)[0]
                            if first[:1] in ("4", "5"):
                                print(f"\n  ⚠ Sample error: HTTP/1.1 {part[:400]}", flush=True)
                                break
                    return ok, err
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    print(f"\n  Rate limited — waiting {retry_after}s …", flush=True)
                    time.sleep(retry_after)
                    continue
                print(f"\n  ⚠ Batch HTTP {r.status_code}: {r.text[:400]}", flush=True)
            except Exception as exc:
                print(f"\n  ⚠ Request error: {exc}", flush=True)
                time.sleep(3)

        return 0, len(chunk)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(do_batch, b): b for b in batches}
        for future in concurrent.futures.as_completed(futures):
            ok, err = future.result()
            total_ok  += ok
            total_err += err
            processed  = total_ok + total_err
            elapsed    = time.time() - start
            rate       = total_ok / elapsed if elapsed > 0 else 0
            print(
                f"\r  Progress: {processed:,}/{total:,} | "
                f"✓ {total_ok:,} updated | ✗ {total_err:,} errors | "
                f"{rate:,.0f} rows/sec   ",
                end="",
                flush=True,
            )

    elapsed = time.time() - start
    print(
        f"\n  Done: {total_ok:,} updated, {total_err:,} errors "
        f"in {elapsed:.1f}s → {total_ok/elapsed if elapsed else 0:,.0f} rows/sec"
    )
    return total_ok, total_err


# ──────────────────────────────────────────────────────────────────────────────
# Main logic per table
# ──────────────────────────────────────────────────────────────────────────────

def process_table(api_url: str, headers: dict, key: str, cfg: dict, dry_run: bool) -> None:
    print(f"\n{'='*72}")
    print(f"  {cfg['display_name']}  →  {cfg['table']}")
    print(f"{'='*72}")

    # 1. Fetch zero-padded records
    print(f"\nQuerying for zero-padded store numbers …")
    records = fetch_padded_records(api_url, headers, cfg)

    if not records:
        print("  ✓ No zero-padded store numbers found — nothing to do.")
        return

    # 2. Build change summary
    change_map: dict[str, tuple[str, int]] = {}  # old_store → (new_store, count)
    for rec in records:
        old_store = rec[cfg["store_field"]]
        new_store = old_store.lstrip("0") or "0"
        entry = change_map.setdefault(old_store, [new_store, 0])
        entry[1] += 1

    print(f"\n  Found {len(records):,} records across {len(change_map):,} unique store numbers.\n")
    print(f"  {'OLD STORE':<15}  {'NEW STORE':<15}  {'RECORDS':>10}")
    print(f"  {'-'*15}  {'-'*15}  {'-'*10}")
    for old_store in sorted(change_map)[:30]:
        new_store, count = change_map[old_store]
        print(f"  {old_store:<15}  {new_store:<15}  {count:>10,}")
    if len(change_map) > 30:
        print(f"  … and {len(change_map) - 30} more unique store numbers")

    if dry_run:
        print(f"\n  [DRY RUN] Would update {len(records):,} records. Re-run without --dry-run to apply.")
        return

    # 3. Apply patches
    print(f"\nApplying updates …")
    batch_patch_records(api_url, headers, cfg, records)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Normalize Dataverse store numbers — strip leading zeros."
    )
    parser.add_argument(
        "--table",
        choices=[*TABLE_CONFIG.keys(), "all"],
        default="all",
        help="Which table to process (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Dataverse",
    )
    args = parser.parse_args()

    table_keys = list(TABLE_CONFIG.keys()) if args.table == "all" else [args.table]

    if args.dry_run:
        print("🔍 DRY RUN — no changes will be written to Dataverse.\n")
    else:
        print("⚡ LIVE RUN — changes WILL be written to Dataverse.\n")

    # Auth
    print("Authenticating …")
    creds   = get_dataverse_credentials()
    token   = get_token(creds)
    api_url = f"{creds['environment_url'].rstrip('/')}/api/data/v9.2"
    headers = {
        "Authorization":    f"Bearer {token}",
        "Accept":           "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version":    "4.0",
    }
    print("  ✓ Token obtained.\n")

    for key in table_keys:
        process_table(api_url, headers, key, TABLE_CONFIG[key], dry_run=args.dry_run)

    print(f"\n{'='*72}")
    print("  All done.")
    print(f"{'='*72}\n")


if __name__ == "__main__":
    main()
