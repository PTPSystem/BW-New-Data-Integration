#!/usr/bin/env python3
"""Full refresh (delete + backfill) for FY2023–FY2025.

This script is intentionally explicit and safe:
- Deletes all records from the target Dataverse tables.
- Backfills by querying OLAP per fiscal year (FY2023, FY2024, FY2025) to avoid huge single queries.

Supported pipelines:
- daily_sales  (OARS Franchise)
- sales_channel (OARS Franchise)

Offers is not included here because it uses a different cube/catalog and currently
relies on MyView-based filtering.

Usage examples:
  source .venv/bin/activate && python full_refresh_fy2023_2025.py --targets sales_channel --confirm-delete --confirm-fy
  source .venv/bin/activate && python full_refresh_fy2023_2025.py --targets all --confirm-delete --confirm-fy --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import requests

from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.utils.keyvault import get_secret
from modules.pipeline_config import load_mapping, load_pipelines
from modules.pipeline_runner import run_mdx_to_df, transform_df_to_records
from modules.utils.keyvault import get_dataverse_credentials
from modules.utils.config import load_config


FY_YEARS = [2023, 2024, 2025]


@dataclass(frozen=True)
class Target:
    pipeline_name: str
    table: str
    id_field: str


TARGETS: Dict[str, Target] = {
    # Note: plural API name + primary key field name
    "daily_sales": Target("daily_sales", "crf63_oarsbidatas", "crf63_oarsbidataid"),
    "sales_channel": Target("sales_channel", "crf63_saleschanneldailies", "crf63_saleschanneldailyid"),
}


def _batch_delete(
    *,
    api_url: str,
    table: str,
    id_field: str,
    headers: Dict[str, str],
    refresh_auth: callable,
    fetch_top: int = 5000,
    delete_batch_size: int = 100,
    max_workers: int = 8,
) -> int:
    deleted = 0
    start = time.time()

    def _sleep_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> None:
        delay = min(cap, base ** attempt)
        time.sleep(delay)

    while True:
        fetch_url = f"{api_url}/{table}?$select={id_field}&$top={fetch_top}"
        resp = None
        for attempt in range(0, 6):
            try:
                resp = requests.get(fetch_url, headers=headers, timeout=(10, 120))
                if resp.status_code == 200:
                    break
                if resp.status_code in (401, 403):
                    print("  Auth expired during fetch; refreshing token...")
                    refresh_auth()
                    continue
                print(f"  Fetch retry {attempt+1}/6: HTTP {resp.status_code} {resp.text[:200]}")
            except requests.exceptions.RequestException as e:
                print(f"  Fetch retry {attempt+1}/6: {type(e).__name__}: {e}")
            _sleep_backoff(attempt)

        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Fetch failed for {table} after retries")

        records = resp.json().get("value", [])
        if not records:
            break

        ids = [r[id_field] for r in records if id_field in r]
        if not ids:
            break

        batches = [ids[i : i + delete_batch_size] for i in range(0, len(ids), delete_batch_size)]

        def delete_one_batch(batch_ids: List[str]) -> int:
            batch_id = str(uuid.uuid4())
            changeset_id = str(uuid.uuid4())

            lines: List[str] = []
            lines.append(f"--{batch_id}")
            lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
            lines.append("")
            lines.append("")

            for idx, rid in enumerate(batch_ids, 1):
                lines.append(f"--{changeset_id}")
                lines.append("Content-Type: application/http")
                lines.append("Content-Transfer-Encoding: binary")
                lines.append(f"Content-ID: {idx}")
                lines.append("")
                lines.append(f"DELETE {api_url}/{table}({rid}) HTTP/1.1")
                lines.append("Content-Length: 0")
                lines.append("")
                lines.append("")

            lines.append(f"--{changeset_id}--")
            lines.append(f"--{batch_id}--")
            lines.append("")

            body = "\r\n".join(lines).encode("utf-8")
            batch_headers = {"Content-Type": f"multipart/mixed; boundary={batch_id}"}
            full_headers = {**headers, **batch_headers}
            for attempt in range(0, 6):
                try:
                    r = requests.post(f"{api_url}/$batch", headers=full_headers, data=body, timeout=(10, 600))
                    if r.status_code in (200, 202, 204):
                        return len(batch_ids)
                    if r.status_code in (401, 403):
                        print("  Auth expired during delete; refreshing token...")
                        refresh_auth()
                        full_headers = {**headers, **batch_headers}
                        continue
                    print(
                        f"  Delete batch retry {attempt+1}/6: HTTP {r.status_code} {r.text[:200]}"
                    )
                except requests.exceptions.RequestException as e:
                    print(f"  Delete batch retry {attempt+1}/6: {type(e).__name__}: {e}")
                _sleep_backoff(attempt)
            # Non-fatal: leave these rows for the next fetch loop.
            return 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(delete_one_batch, b) for b in batches]
            for fut in as_completed(futures):
                try:
                    deleted += fut.result()
                except Exception as e:
                    # Keep going; remaining rows will be retried on the next loop.
                    print(f"  Delete worker error (continuing): {type(e).__name__}: {e}")

        elapsed = time.time() - start
        rate = (deleted / elapsed) if elapsed > 0 else 0
        print(f"  Deleted so far: {deleted:,} ({rate:,.0f} rows/sec)")

    return deleted


def _inject_fiscal_year_where(mdx: str, fiscal_year: int) -> str:
    """Append/replace the WHERE clause with a Fiscal_Year filter.

    We rely on the cube hierarchy used in the legacy script:
    [Calendar].[Calendar Hierarchy].[Fiscal_Year].&[YYYY]
    """
    where = f"WHERE ([Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{fiscal_year}])"
    upper = mdx.upper()
    idx = upper.rfind("WHERE")
    if idx == -1:
        return mdx.rstrip() + "\n" + where + "\n"
    return mdx[:idx].rstrip() + "\n" + where + "\n"


def _myview_ids_for_sales_channel_backfill() -> List[int]:
    """Return MyView IDs to iterate for a historical backfill.

    Today we only have validated semantics for:
      - 81: last 1wk
      - 82: last 2wk

    The OLAP server has returned empty results when attempting Fiscal_Year/Fiscal_Month
    slicing alongside MyView. Until we have a verified member for FY slicing in this cube,
    we fall back to a conservative MyView-based backfill.

    NOTE: Extend this list once we confirm stable historical MyView IDs.
    """

    return [81, 82]


def main() -> int:
    parser = argparse.ArgumentParser(description="Full refresh delete + FY2023–FY2025 backfill")
    parser.add_argument(
        "--targets",
        choices=["daily_sales", "sales_channel", "all"],
        default="all",
        help="Which tables to refresh",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required acknowledgement to delete all records in target tables",
    )
    parser.add_argument(
        "--confirm-fy",
        action="store_true",
        help="Required acknowledgement to run FY2023–FY2025 backfill",
    )
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Skip Dataverse delete phase (useful if you bulk-deleted server-side)",
    )
    parser.add_argument(
        "--sales-channel-mode",
        choices=["fy", "myview"],
        default="myview",
        help="Backfill mode for sales_channel. 'fy' attempts FY2023–FY2025 slicing; 'myview' uses validated MyView runs. Default: myview",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen, without deleting or upserting",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path. Defaults to logs/full_refresh_fy2023_2025_<timestamp>.log",
    )
    parser.add_argument(
        "--delete-workers",
        type=int,
        default=8,
        help="Parallel workers for delete (lower is more reliable). Default: 8",
    )
    parser.add_argument(
        "--delete-batch-size",
        type=int,
        default=100,
        help="Records per $batch changeset during delete. Default: 100",
    )
    parser.add_argument(
        "--delete-fetch-top",
        type=int,
        default=5000,
        help="How many IDs to fetch per loop during delete. Default: 5000",
    )
    args = parser.parse_args()

    os.makedirs("logs", exist_ok=True)
    if args.log_file:
        log_path = args.log_file
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join("logs", f"full_refresh_fy2023_2025_{ts}.log")

    class _Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                try:
                    s.write(data)
                except Exception:
                    pass
            self.flush()

        def flush(self):
            for s in self.streams:
                try:
                    s.flush()
                except Exception:
                    pass

    log_fh = open(log_path, "a", buffering=1, encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, log_fh)
    sys.stderr = _Tee(sys.__stderr__, log_fh)
    print(f"Log file: {log_path}")

    if not args.skip_delete and not args.confirm_delete:
        raise SystemExit("Refusing to run: pass --confirm-delete to delete all records (or use --skip-delete).")
    if not args.confirm_fy:
        raise SystemExit("Refusing to run: pass --confirm-fy to run FY backfill.")

    selected: List[Target]
    if args.targets == "all":
        selected = [TARGETS["daily_sales"], TARGETS["sales_channel"]]
    else:
        selected = [TARGETS[args.targets]]

    cfg = load_config()
    pipelines = load_pipelines()

    dv_creds = get_dataverse_credentials()
    dataverse_url = dv_creds["environment_url"]
    client_id = dv_creds["client_id"]
    tenant_id = dv_creds["tenant_id"]
    client_secret = dv_creds["client_secret"]

    token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
    api_url = f"{dataverse_url.rstrip('/')}/api/data/v9.2"
    headers = {
        "Authorization": f"Bearer {token}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": "odata.continue-on-error",
    }

    def refresh_auth() -> str:
        nonlocal token
        token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
        headers["Authorization"] = f"Bearer {token}"
        return token

    olap_server = os.getenv("OLAP_SERVER", cfg.get("olap", {}).get("server", "https://ednacubes.papajohns.com:10502"))
    olap_ssl_verify = bool(cfg.get("olap", {}).get("ssl_verify", False))
    olap_username = get_secret("olap-username")
    olap_password = get_secret("olap-password")

    print("=" * 80)
    print("FULL REFRESH FY2023–FY2025")
    print(f"Targets: {', '.join([t.pipeline_name for t in selected])}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 80)

    for t in selected:
        print(f"\n==> Deleting all records from {t.table} ...")
        if args.skip_delete:
            print("  (skip-delete) assuming table already emptied server-side")
        elif args.dry_run:
            print("  (dry-run) skip delete")
        else:
            refresh_auth()
            deleted = _batch_delete(
                api_url=api_url,
                table=t.table,
                id_field=t.id_field,
                headers=headers,
                refresh_auth=refresh_auth,
                fetch_top=max(1, int(args.delete_fetch_top)),
                delete_batch_size=max(1, int(args.delete_batch_size)),
                max_workers=max(1, int(args.delete_workers)),
            )
            print(f"  ✅ Deleted {deleted:,} rows from {t.table}")

        p = pipelines.get(t.pipeline_name)
        if not p:
            raise SystemExit(f"Missing pipeline '{t.pipeline_name}' in pipelines.yaml")
        mapping = load_mapping(p.mapping_path)

        print(f"\n==> Backfilling {t.pipeline_name} for FY2023–FY2025 ...")

        # sales_channel currently has validated MyView-based slicing only.
        if t.pipeline_name == "sales_channel" and args.sales_channel_mode == "myview":
            myviews = _myview_ids_for_sales_channel_backfill()
            print(f"  sales_channel: running MyView backfill over IDs: {myviews}")
            for mv in myviews:
                print(f"    MyView {mv}: querying OLAP ...")
                mdx_mv = p.mdx.replace("${myview_id}", str(mv))
                if args.dry_run:
                    print("      (dry-run) skip query/upsert")
                    continue

                df = run_mdx_to_df(
                    xmla_server=olap_server,
                    catalog=p.catalog,
                    username=olap_username,
                    password=olap_password,
                    mdx=mdx_mv,
                    parser=p.parser,
                    ssl_verify=olap_ssl_verify,
                )
                if df is None or len(df) == 0:
                    print("      ⚠ no rows")
                    continue

                refresh_auth()
                records = transform_df_to_records(df, mapping)
                created, updated, errors = upsert_to_dataverse(dataverse_url, token, mapping["table"], records)
                print(f"      ✓ {created} created, {updated} updated, {errors} errors")
        else:
            for fy in FY_YEARS:
                print(f"  FY{fy}: querying OLAP ...")
                mdx_fy = _inject_fiscal_year_where(p.mdx, fy)
                if args.dry_run:
                    print("    (dry-run) skip query/upsert")
                    continue

                refresh_auth()
                df = run_mdx_to_df(
                    xmla_server=olap_server,
                    catalog=p.catalog,
                    username=olap_username,
                    password=olap_password,
                    mdx=mdx_fy,
                    parser=p.parser,
                    ssl_verify=olap_ssl_verify,
                )
                if df is None or len(df) == 0:
                    print(f"    ⚠ No rows returned for FY{fy}")
                    continue

                records = transform_df_to_records(df, mapping)
                created, updated, errors = upsert_to_dataverse(dataverse_url, token, mapping["table"], records)
                print(f"    ✓ FY{fy}: {created} created, {updated} updated, {errors} errors")

    print("\n✅ Full refresh complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
