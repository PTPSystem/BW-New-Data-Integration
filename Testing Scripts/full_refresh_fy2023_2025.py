#!/usr/bin/env python3
"""Full refresh (truncate + backfill) for FY2023–FY2025.

This script is intentionally explicit and safe:
- Truncates the target Neon Postgres tables (DATABASE_URL).
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
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from modules.neon_sink import spec_from_mapping, truncate_table, upsert_records
from modules.utils.keyvault import get_secret
from modules.pipeline_config import load_mapping, load_pipelines
from modules.pipeline_runner import run_mdx_to_df, transform_df_to_records
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
        help="Required acknowledgement to truncate the target Neon tables",
    )
    parser.add_argument(
        "--confirm-fy",
        action="store_true",
        help="Required acknowledgement to run FY2023–FY2025 backfill",
    )
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Skip the Neon truncate (upsert into the existing table)",
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
        help="Unused. Kept so older commands still parse. Neon refresh truncates the table.",
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
        p = pipelines.get(t.pipeline_name)
        if not p:
            raise SystemExit(f"Missing pipeline '{t.pipeline_name}' in pipelines.yaml")
        mapping = load_mapping(p.mapping_path)
        table_spec = spec_from_mapping(mapping)

        print(f"\n==> Truncating Neon table {table_spec.name} ...")
        if args.skip_delete:
            print("  (skip-delete) leaving existing Neon rows in place")
        elif args.dry_run:
            print("  (dry-run) skip truncate")
        else:
            truncate_table(table_spec)
            print(f"  ✅ Truncated {table_spec.name}")

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

                records = transform_df_to_records(df, mapping)
                created, updated, errors = upsert_records(records, table_spec)
                print(f"      ✓ {created} created, {updated} updated, {errors} errors")
        else:
            for fy in FY_YEARS:
                print(f"  FY{fy}: querying OLAP ...")
                mdx_fy = _inject_fiscal_year_where(p.mdx, fy)
                if args.dry_run:
                    print("    (dry-run) skip query/upsert")
                    continue

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
                created, updated, errors = upsert_records(records, table_spec)
                print(f"    ✓ FY{fy}: {created} created, {updated} updated, {errors} errors")

    print("\n✅ Full refresh complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
