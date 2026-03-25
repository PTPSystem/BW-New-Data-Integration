#!/usr/bin/env python3
"""Backfill FY2020–FY2021 across all pipelines.

This script is additive (no delete phase) — it upserts records for FY2020 and
FY2021 without touching any existing data.

Supported pipelines:
  daily_sales     (OARS Franchise — Calendar hierarchy)
  sales_channel   (OARS Franchise — 13-4 Calendar)
  offers          (Offers cube     — 13-4 Calendar)
  inventory       (OARS Franchise — Calendar hierarchy)
  clock_in_out    (VBO             — Calendar hierarchy)

Usage examples:
  source .venv/bin/activate && python backfill_fy2020_2021.py --confirm-fy
  source .venv/bin/activate && python backfill_fy2020_2021.py --targets daily_sales --confirm-fy
  source .venv/bin/activate && python backfill_fy2020_2021.py --confirm-fy --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List

from modules.dataverse import get_dataverse_access_token, upsert_to_dataverse
from modules.utils.keyvault import get_secret, get_dataverse_credentials
from modules.pipeline_config import load_mapping, load_pipelines, render_mdx_template
from modules.pipeline_runner import run_mdx_to_df, transform_df_to_records
from modules.utils.config import load_config


FY_YEARS = [2020, 2021]

# Pipelines that use the 13-4 Calendar dimension for FY slicing.
_13_4_PIPELINES = {"offers", "sales_channel"}


@dataclass(frozen=True)
class Target:
    pipeline_name: str


ALL_TARGETS = [
    Target("daily_sales"),
    Target("sales_channel"),
    Target("offers"),
    Target("inventory"),
    Target("clock_in_out"),
]

TARGET_MAP: Dict[str, Target] = {t.pipeline_name: t for t in ALL_TARGETS}


def _build_fy_mdx(pipeline_name: str, mdx_template: str, fiscal_year: int) -> str:
    """Render an MDX template with a fiscal-year slicer appropriate for the pipeline."""
    if pipeline_name in _13_4_PIPELINES:
        slicer = f"[13-4 Calendar].[d_Year].[d_Year].&[{fiscal_year}]"
    else:
        slicer = f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{fiscal_year}]"
    return render_mdx_template(mdx_template, {"slicer": slicer})


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill FY2020–FY2021 across all pipelines")
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=list(TARGET_MAP.keys()) + ["all"],
        default=["all"],
        help="Which pipelines to backfill (default: all)",
    )
    parser.add_argument(
        "--confirm-fy",
        action="store_true",
        help="Required acknowledgement to run FY2020–FY2021 backfill",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without querying OLAP or upserting",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path. Defaults to logs/backfill_fy2020_2021_<timestamp>.log",
    )
    args = parser.parse_args()

    if not args.confirm_fy:
        raise SystemExit("Refusing to run: pass --confirm-fy to run FY2020–FY2021 backfill.")

    os.makedirs("logs", exist_ok=True)
    if args.log_file:
        log_path = args.log_file
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join("logs", f"backfill_fy2020_2021_{ts}.log")

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

    # Resolve selected targets
    if "all" in args.targets:
        selected: List[Target] = ALL_TARGETS
    else:
        selected = [TARGET_MAP[name] for name in args.targets]

    cfg = load_config()
    pipelines = load_pipelines()

    dv_creds = get_dataverse_credentials()
    dataverse_url = dv_creds["environment_url"]
    client_id = dv_creds["client_id"]
    tenant_id = dv_creds["tenant_id"]
    client_secret = dv_creds["client_secret"]

    token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)

    def refresh_auth() -> str:
        nonlocal token
        token = get_dataverse_access_token(dataverse_url, client_id, client_secret, tenant_id)
        return token

    olap_server = os.getenv(
        "OLAP_SERVER",
        cfg.get("olap", {}).get("server", "https://ednacubes.papajohns.com:10502"),
    )
    olap_ssl_verify = bool(cfg.get("olap", {}).get("ssl_verify", False))
    olap_username = get_secret("olap-username")
    olap_password = get_secret("olap-password")

    print("=" * 80)
    print("BACKFILL FY2020–FY2021")
    print(f"Pipelines: {', '.join([t.pipeline_name for t in selected])}")
    print(f"Fiscal years: {FY_YEARS}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 80)

    total_created = total_updated = total_errors = 0

    for t in selected:
        p = pipelines.get(t.pipeline_name)
        if not p:
            raise SystemExit(f"Missing pipeline '{t.pipeline_name}' in pipelines.yaml")
        mapping = load_mapping(p.mapping_path)

        print(f"\n==> Backfilling {t.pipeline_name} for {FY_YEARS} ...")

        for fy in FY_YEARS:
            print(f"  FY{fy}: querying OLAP ...")

            if args.dry_run:
                print(f"    (dry-run) skip query/upsert for {t.pipeline_name} FY{fy}")
                continue

            mdx = _build_fy_mdx(t.pipeline_name, p.mdx, fy)

            df = run_mdx_to_df(
                xmla_server=olap_server,
                catalog=p.catalog,
                username=olap_username,
                password=olap_password,
                mdx=mdx,
                parser=p.parser,
                ssl_verify=olap_ssl_verify,
            )

            if df is None or len(df) == 0:
                print(f"    ⚠ No rows returned for {t.pipeline_name} FY{fy}")
                continue

            print(f"    {len(df):,} rows returned; upserting to Dataverse ...")
            refresh_auth()
            records = transform_df_to_records(df, mapping)
            created, updated, errors = upsert_to_dataverse(
                dataverse_url, token, mapping["table"], records
            )
            print(f"    ✓ FY{fy}: {created} created, {updated} updated, {errors} errors")
            total_created += created
            total_updated += updated
            total_errors += errors

    print("\n" + "=" * 80)
    if args.dry_run:
        print("✅ Dry run complete — no data was written")
    else:
        print(
            f"✅ Backfill complete — "
            f"{total_created} created, {total_updated} updated, {total_errors} errors"
        )
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
