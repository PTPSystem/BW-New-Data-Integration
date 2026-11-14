#!/usr/bin/env python3
"""One-time copy of ETL history from Dataverse into Neon Postgres.

Reads every table the ETL writes (OLAP pipelines, labor times, and sales
forecasts), following Dataverse ``@odata.nextLink`` pages, and upserts them
into Neon using DATABASE_URL. The load is safe to re-run: the default is an
upsert on the primary key. ``--truncate`` empties each Neon table first.

This does not change extraction. It only copies rows that are already in
Dataverse onto the new write target.

Examples:
  python scripts/backfill_dataverse_to_neon.py --dry-run
  python scripts/backfill_dataverse_to_neon.py
  python scripts/backfill_dataverse_to_neon.py --truncate --batch-size 1000

``--dry-run`` reads Dataverse and prints row counts. It does not connect to Neon.
Do not commit DATABASE_URL. A dedicated Neon project is provisioned separately;
until then leave DATABASE_URL unset and use --dry-run only when Dataverse auth
is available.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.dataverse_backfill import run_backfill


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Page Dataverse and log row counts. Do not open DATABASE_URL or write to Neon.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate each Neon table before loading it (full reload). Default is a primary-key upsert.",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per Neon upsert batch (default: 500)")
    parser.add_argument(
        "--page-size",
        type=int,
        default=5000,
        help="Dataverse odata.maxpagesize hint (default: 5000)",
    )
    args = parser.parse_args(argv)
    if args.batch_size < 1 or args.page_size < 1:
        parser.error("--batch-size and --page-size must be positive")

    if not args.dry_run and not (os.environ.get("DATABASE_URL") or "").strip():
        print(
            "DATABASE_URL is not set. Export a Neon connection string "
            "(postgresql://<role>:<password>@<endpoint>.<region>.aws.neon.tech/<database>?sslmode=require) "
            "or pass --dry-run to count Dataverse rows without writing.",
            file=sys.stderr,
        )
        return 2

    result = run_backfill(
        dry_run=args.dry_run,
        truncate=args.truncate,
        batch_size=args.batch_size,
        page_size=args.page_size,
    )
    print("")
    print("Per-table counts:")
    for table in result["tables"]:
        print(
            f"  {table['table']}: fetched={table['fetched']} "
            f"created={table['created']} updated={table['updated']} errors={table['errors']}"
        )
    return 0 if result["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
