#!/usr/bin/env python3
"""
Pull each AtScale SQL pipeline locally, then compare to current Dataverse rows.

Usage (from repo root):
  python "Testing Scripts/compare_atscale_to_dataverse.py" --length 1day
  python "Testing Scripts/compare_atscale_to_dataverse.py" --pipeline clock_in_out --length 3day

Writes under Testing Scripts/atscale_compare_out/<timestamp>/:
  <pipeline>.sql
  <pipeline>_atscale.csv
  <pipeline>_dataverse.csv
  <pipeline>_diff.csv
  summary.txt
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import pandas as pd
import requests
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules.atscale import query_atscale_sql_df
from modules.dataverse import get_dataverse_access_token
from modules.length_slicer import LengthError, render_time_sliced_sql, sql_date_window
from modules.pipeline_config import load_mapping, load_pipelines
from modules.pipeline_runner import transform_df_to_records
from modules.utils.keyvault import get_atscale_api_token, get_dataverse_credentials

DEFAULT_PIPELINES = (
    "daily_sales",
    "sales_channel",
    "offers",
    "inventory",
    "clock_in_out",
)

# Metadata / keys we don't treat as measure mismatches
SKIP_COMPARE_FIELDS = {
    "crf63_name",
    "crf63_datasource",
    "crf63_lastrefreshed",
    "@odata.etag",
    "@odata.context",
}


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_config() -> Dict[str, Any]:
    path = REPO_ROOT / "config.yaml"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _measure_columns(mapping: Dict[str, Any]) -> List[str]:
    return [str(spec["dataverse"]) for spec in (mapping.get("measures") or {}).values() if spec.get("dataverse")]


def _field_columns(mapping: Dict[str, Any]) -> List[str]:
    return [str(spec["dataverse"]) for spec in (mapping.get("fields") or {}).values() if spec.get("dataverse")]


def _fetch_dataverse_by_dates(
    *,
    environment_url: str,
    access_token: str,
    table: str,
    start: date,
    end: date,
    select: Sequence[str],
) -> List[Dict[str, Any]]:
    """Page Dataverse rows whose CalendarDate falls in [start, end] inclusive."""
    # Dates are stored as noon UTC; filter with half-open day bounds.
    start_iso = f"{start.isoformat()}T00:00:00Z"
    end_exclusive = (end + timedelta(days=1)).isoformat() + "T00:00:00Z"
    filt = (
        f"crf63_calendardate ge {start_iso} and "
        f"crf63_calendardate lt {end_exclusive}"
    )
    cols = list(dict.fromkeys([*select, "crf63_businesskey", "crf63_calendardate"]))
    select_q = ",".join(cols)
    url = (
        f"{environment_url.rstrip('/')}/api/data/v9.2/{table}"
        f"?$select={quote(select_q, safe=',')}"
        f"&$filter={quote(filt, safe=' ')}"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": "odata.maxpagesize=5000",
    }

    rows: List[Dict[str, Any]] = []
    next_url: Optional[str] = url
    page = 1
    while next_url:
        r = requests.get(next_url, headers=headers, timeout=180)
        if r.status_code != 200:
            raise RuntimeError(f"Dataverse query failed ({r.status_code}): {r.text[:400]}")
        payload = r.json()
        batch = payload.get("value") or []
        rows.extend(batch)
        _log(f"  Dataverse page {page}: +{len(batch)} (total {len(rows)})")
        next_url = payload.get("@odata.nextLink")
        page += 1
    return rows


def _records_to_df(records: List[Dict[str, Any]], key: str) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=[key])
    df = pd.DataFrame(records)
    if key in df.columns:
        df = df.set_index(key, drop=False)
    return df


def _norm_num(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, str) and not v.strip():
        return None
    try:
        return float(v)
    except Exception:
        return None


def _norm_str(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    return s if s else None


def _nums_close(a: Optional[float], b: Optional[float], tol: float = 0.02) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        # Treat missing vs 0 as match (Dataverse often omits zeros)
        if (a is None and b == 0.0) or (b is None and a == 0.0):
            return True
        return False
    if abs(a - b) <= tol:
        return True
    # relative tolerance for larger values
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) / scale <= 0.001


def compare_pipeline(
    *,
    pipeline_name: str,
    length: str,
    out_dir: Path,
    as_of: Optional[date],
    atscale_token: str,
    dv_url: str,
    dv_token: str,
    api_url: str,
    environment: str,
    timeout: str,
) -> Dict[str, Any]:
    pipelines = load_pipelines()
    if pipeline_name not in pipelines:
        raise SystemExit(f"Unknown pipeline '{pipeline_name}'")
    p = pipelines[pipeline_name]
    if not p.sql:
        raise SystemExit(f"Pipeline '{pipeline_name}' has no sql: block")

    mapping = load_mapping(p.mapping_path)
    table = mapping["table"]
    alt_key = mapping.get("alternate_key", "crf63_businesskey")
    measure_cols = _measure_columns(mapping)
    field_cols = _field_columns(mapping)

    start, end = sql_date_window(length, as_of=as_of)
    sql = render_time_sliced_sql(p.sql, length, as_of=as_of)
    sql_path = out_dir / f"{pipeline_name}.sql"
    sql_path.write_text(sql + "\n", encoding="utf-8")
    _log(f"\n===== {pipeline_name} =====")
    _log(f"Window: {start} → {end}")
    _log(f"Wrote SQL → {sql_path}")

    project = p.atscale_project or "OARS"
    _log(f"Querying AtScale project={project}…")
    df = query_atscale_sql_df(
        sql,
        atscale_token,
        api_url=api_url,
        project=project,
        environment=environment,
        timeout=timeout,
    )
    atscale_csv = out_dir / f"{pipeline_name}_atscale.csv"
    df.to_csv(atscale_csv, index=False)
    _log(f"AtScale rows: {len(df)} → {atscale_csv}")

    records = transform_df_to_records(df, mapping)
    atscale_df = _records_to_df(records, alt_key)

    select_cols = field_cols + measure_cols + [alt_key]
    _log(f"Querying Dataverse table={table}…")
    dv_rows = _fetch_dataverse_by_dates(
        environment_url=dv_url,
        access_token=dv_token,
        table=table,
        start=start,
        end=end,
        select=select_cols,
    )
    dv_csv = out_dir / f"{pipeline_name}_dataverse.csv"
    pd.DataFrame(dv_rows).to_csv(dv_csv, index=False)
    _log(f"Dataverse rows: {len(dv_rows)} → {dv_csv}")
    dv_df = _records_to_df(dv_rows, alt_key)

    atscale_keys = set(atscale_df.index.astype(str)) if len(atscale_df) else set()
    dv_keys = set(dv_df.index.astype(str)) if len(dv_df) else set()
    only_atscale = sorted(atscale_keys - dv_keys)
    only_dv = sorted(dv_keys - atscale_keys)
    shared = sorted(atscale_keys & dv_keys)

    diffs: List[Dict[str, Any]] = []
    matched = 0
    measure_mismatches = 0
    field_mismatches = 0

    compare_fields = [c for c in field_cols if c not in SKIP_COMPARE_FIELDS]
    compare_measures = [c for c in measure_cols if c not in SKIP_COMPARE_FIELDS]

    for key in shared:
        a_row = atscale_df.loc[key]
        d_row = dv_df.loc[key]
        # If duplicate index, take first
        if isinstance(a_row, pd.DataFrame):
            a_row = a_row.iloc[0]
        if isinstance(d_row, pd.DataFrame):
            d_row = d_row.iloc[0]

        row_ok = True
        for col in compare_fields:
            av = _norm_str(a_row.get(col) if hasattr(a_row, "get") else a_row[col] if col in a_row.index else None)
            dv = _norm_str(d_row.get(col) if hasattr(d_row, "get") else d_row[col] if col in d_row.index else None)
            # Date fields: compare date part only
            if col.endswith("calendardate") or col == "crf63_calendardate":
                av = (av or "")[:10] or None
                dv = (dv or "")[:10] or None
            if av != dv:
                # StoreNumber sometimes int vs str
                if av is not None and dv is not None and av.lstrip("0") == dv.lstrip("0"):
                    continue
                row_ok = False
                field_mismatches += 1
                diffs.append(
                    {
                        "business_key": key,
                        "kind": "field",
                        "column": col,
                        "atscale": av,
                        "dataverse": dv,
                    }
                )

        for col in compare_measures:
            av = _norm_num(a_row.get(col) if hasattr(a_row, "get") else a_row[col] if col in a_row.index else None)
            dv = _norm_num(d_row.get(col) if hasattr(d_row, "get") else d_row[col] if col in d_row.index else None)
            if not _nums_close(av, dv):
                row_ok = False
                measure_mismatches += 1
                diffs.append(
                    {
                        "business_key": key,
                        "kind": "measure",
                        "column": col,
                        "atscale": av,
                        "dataverse": dv,
                        "delta": None if av is None or dv is None else av - dv,
                    }
                )

        if row_ok:
            matched += 1

    for key in only_atscale:
        diffs.append({"business_key": key, "kind": "only_atscale", "column": "", "atscale": "", "dataverse": ""})
    for key in only_dv:
        diffs.append({"business_key": key, "kind": "only_dataverse", "column": "", "atscale": "", "dataverse": ""})

    diff_df = pd.DataFrame(diffs)
    diff_path = out_dir / f"{pipeline_name}_diff.csv"
    diff_df.to_csv(diff_path, index=False)

    summary = {
        "pipeline": pipeline_name,
        "table": table,
        "start": str(start),
        "end": str(end),
        "atscale_rows": len(atscale_keys),
        "dataverse_rows": len(dv_keys),
        "shared": len(shared),
        "matched": matched,
        "only_atscale": len(only_atscale),
        "only_dataverse": len(only_dv),
        "field_mismatches": field_mismatches,
        "measure_mismatches": measure_mismatches,
        "diff_rows": len(diff_df),
    }
    _log(
        f"Result: shared={summary['shared']} matched={matched} "
        f"only_atscale={summary['only_atscale']} only_dv={summary['only_dataverse']} "
        f"measure_mismatches={measure_mismatches} → {diff_path}"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare AtScale SQL pipelines to Dataverse")
    parser.add_argument(
        "--pipeline",
        action="append",
        dest="pipelines",
        help="Pipeline name (repeatable). Default: all SQL pipelines.",
    )
    parser.add_argument("--length", default="1day", help="Date window (e.g. 1day, 3day, 1wk)")
    parser.add_argument(
        "--as-of",
        default=None,
        help="Anchor date YYYY-MM-DD (default: today). Window ends yesterday relative to this.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output directory (default: Testing Scripts/atscale_compare_out/<timestamp>)",
    )
    args = parser.parse_args()

    try:
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
    except ValueError as exc:
        raise SystemExit(f"Invalid --as-of: {exc}") from exc

    names = args.pipelines or list(DEFAULT_PIPELINES)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out) if args.out else (REPO_ROOT / "Testing Scripts" / "atscale_compare_out" / stamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = _load_config()
    olap_cfg = cfg.get("olap") or {}
    atscale_cfg = olap_cfg.get("atscale") or {}
    api_url = atscale_cfg.get("api_url") or "https://atscale.papajohns.com/v1/query"
    environment = atscale_cfg.get("environment") or "default"
    timeout = atscale_cfg.get("timeout") or "300s"

    _log(f"Output → {out_dir}")
    _log(f"Pipelines: {', '.join(names)}")
    _log(f"Length: {args.length}")

    atscale_token = get_atscale_api_token()
    dv = get_dataverse_credentials()
    dv_token = get_dataverse_access_token(
        dv["environment_url"], dv["client_id"], dv["client_secret"], dv["tenant_id"]
    )
    if not dv_token:
        raise SystemExit("Failed to get Dataverse token")

    summaries = []
    for name in names:
        try:
            summaries.append(
                compare_pipeline(
                    pipeline_name=name,
                    length=args.length,
                    out_dir=out_dir,
                    as_of=as_of,
                    atscale_token=atscale_token,
                    dv_url=dv["environment_url"],
                    dv_token=dv_token,
                    api_url=api_url,
                    environment=environment,
                    timeout=timeout,
                )
            )
        except LengthError as exc:
            raise SystemExit(str(exc)) from exc
        except Exception as exc:
            _log(f"FAILED {name}: {exc}")
            summaries.append({"pipeline": name, "error": str(exc)})

    lines = ["AtScale vs Dataverse comparison", f"out={out_dir}", ""]
    for s in summaries:
        if "error" in s:
            lines.append(f"{s['pipeline']}: ERROR {s['error']}")
            continue
        lines.append(
            f"{s['pipeline']}: atscale={s['atscale_rows']} dv={s['dataverse_rows']} "
            f"shared={s['shared']} matched={s['matched']} "
            f"only_atscale={s['only_atscale']} only_dv={s['only_dataverse']} "
            f"measure_mismatches={s['measure_mismatches']}"
        )
    summary_text = "\n".join(lines) + "\n"
    (out_dir / "summary.txt").write_text(summary_text, encoding="utf-8")
    _log("\n" + summary_text)


if __name__ == "__main__":
    main()
