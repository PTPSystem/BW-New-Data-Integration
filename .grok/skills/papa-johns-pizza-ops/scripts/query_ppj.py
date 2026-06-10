#!/usr/bin/env python3
"""
Papa John's PPJ / OARS Semantic Model Query Helper (Current Repo)

Lightweight CLI for the papa-johns-pizza-ops skill and agent.

Designed for BW-New-Data-Integration (the current active repo).

Usage:
  python .grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py --last-n-days 14 --stores 1334 --format table
  python .grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py --master --fiscal-years 2025 --stores 1334
"""

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
import pandas as pd

# Current repo modules
from modules.olap import execute_xmla_mdx, parse_xmla_celldata_response
from modules.mdx_queries import get_mdx_last_n_days, get_sample_mdx_queries
from modules.utils.keyvault import get_secret


def get_creds():
    """Load OLAP credentials from Key Vault (primary) with env fallback."""
    load_dotenv()

    server = os.getenv("OLAP_SERVER", "https://ednacubes.papajohns.com:10502")
    ssl_verify = os.getenv("OLAP_SSL_VERIFY", "false").lower() == "true"

    # Primary: Key Vault (kv-bw-data-integration)
    try:
        username = get_secret("olap-username")
        password = get_secret("olap-password")
        if username and password:
            print("✓ Loaded OLAP credentials from Key Vault (kv-bw-data-integration)", file=sys.stderr)
            return server, username, password, ssl_verify
    except Exception as e:
        print(f"⚠ Key Vault lookup failed: {e}", file=sys.stderr)
        print("   Run 'az login' and ensure you have Key Vault Secrets User role.", file=sys.stderr)

    # Fallback to environment (for .env overrides during testing)
    username = os.getenv("OLAP_USERNAME")
    password = os.getenv("OLAP_PASSWORD")

    if username and password:
        print("✓ Using OLAP credentials from environment variables", file=sys.stderr)
        return server, username, password, ssl_verify

    print("\n❌ No OLAP credentials available.", file=sys.stderr)
    print("   Primary: Secrets 'olap-username' and 'olap-password' in kv-bw-data-integration", file=sys.stderr)
    print("   Fallback: Set OLAP_USERNAME / OLAP_PASSWORD in .env", file=sys.stderr)
    print("   Make sure: az login", file=sys.stderr)
    return server, None, None, ssl_verify


def main():
    parser = argparse.ArgumentParser(description="Query Papa John's OARS semantic model (current repo)")
    parser.add_argument("--catalog", default="OARS Franchise", help="Cube catalog")
    parser.add_argument("--master", action="store_true", help="Use full fiscal year master query (can be large)")
    parser.add_argument("--fiscal-years", default="2025", help="Comma sep fiscal years (for --master)")
    parser.add_argument("--last-n-days", type=int, default=14, help="Use efficient MyView last-N-days query (recommended)")
    parser.add_argument("--stores", default="", help="Comma-separated store numbers for post-filter")
    parser.add_argument("--format", choices=["table", "csv", "json"], default="table")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    server, user, pw, verify = get_creds()
    if not user or not pw:
        sys.exit(2)

    # Build MDX
    if args.master:
        years = [int(y.strip()) for y in args.fiscal_years.split(",") if y.strip()]
        mdx = get_sample_mdx_queries(fiscal_years=years)['full_bi_data']
        print(f"Using full master query for years {years}", file=sys.stderr)
    else:
        mdx = get_mdx_last_n_days(days=args.last_n_days)
        print(f"Using last {args.last_n_days} days query (MyView)", file=sys.stderr)

    print(f"Executing against {server} / {args.catalog} ...", file=sys.stderr)

    xml_response = execute_xmla_mdx(server, args.catalog, user, pw, mdx, ssl_verify=verify)
    df = parse_xmla_celldata_response(xml_response)

    if df is None or df.empty:
        print("No rows returned.", file=sys.stderr)
        sys.exit(0)

    # Optional store filter
    if args.stores:
        wanted = {s.strip() for s in args.stores.split(",") if s.strip()}
        if 'StoreNumber' in df.columns:
            df = df[df['StoreNumber'].astype(str).isin(wanted)]

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
