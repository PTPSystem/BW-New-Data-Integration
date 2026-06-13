#!/usr/bin/env python3
"""
Beachwood Daily Power BI semantic model query helper (Layer B).

Uses modules/powerbi.py + service principal from Key Vault.

Usage:
  python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset max-date
  python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset profitability --days 14
  python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset store --store 349 --days 14
  python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --dax "EVALUATE ROW(\"x\", 1)"
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

from modules.powerbi import execute_dax_query, get_powerbi_config
from modules.powerbi_queries import (
    get_max_sales_date_dax,
    get_store_paflmd_dax,
    get_store_profitability_ranking_dax,
    get_stores_paflmd_dax,
)


PRESETS = ("max-date", "profitability", "store", "stores")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query Beachwood Daily Power BI semantic model (DAX)"
    )
    parser.add_argument(
        "--preset",
        choices=PRESETS,
        default="profitability",
        help="Built-in DAX query (default: profitability ranking)",
    )
    parser.add_argument("--days", type=int, default=14, help="Lookback days for date filter")
    parser.add_argument("--store", default="", help="Single store number (for --preset store)")
    parser.add_argument("--stores", default="", help="Comma-separated stores (for --preset stores)")
    parser.add_argument("--limit", type=int, default=50, help="Max rows to print (table format)")
    parser.add_argument("--dax", default="", help="Raw DAX query (overrides --preset)")
    parser.add_argument("--format", choices=["table", "csv", "json"], default="table")
    args = parser.parse_args()

    config = get_powerbi_config()
    print(
        f"Beachwood Daily | workspace {config['workspace_id']} | dataset {config['dataset_id']}",
        file=sys.stderr,
    )

    if args.dax:
        dax = args.dax
    elif args.preset == "max-date":
        dax = get_max_sales_date_dax()
    elif args.preset == "store":
        if not args.store:
            print("❌ --store is required for --preset store", file=sys.stderr)
            sys.exit(2)
        dax = get_store_paflmd_dax(args.store, days=args.days)
    elif args.preset == "stores":
        stores = [s.strip() for s in args.stores.split(",") if s.strip()]
        if not stores:
            print("❌ --stores is required for --preset stores", file=sys.stderr)
            sys.exit(2)
        dax = get_stores_paflmd_dax(stores, days=args.days)
    else:
        dax = get_store_profitability_ranking_dax(days=args.days)

    print(f"Executing DAX ({args.preset or 'custom'})...", file=sys.stderr)
    df = execute_dax_query(dax)

    if df is None or df.empty:
        print("No rows returned.", file=sys.stderr)
        sys.exit(0)

    if args.format == "table":
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(df.head(args.limit).to_string(index=False))
        print(f"\nTotal rows: {len(df)}", file=sys.stderr)
    elif args.format == "csv":
        print(df.to_csv(index=False))
    else:
        print(df.to_json(orient="records", indent=2))


if __name__ == "__main__":
    main()