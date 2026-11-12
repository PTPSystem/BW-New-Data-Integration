"""AtScale public SQL API client (replaces legacy ednacubes XMLA for OARS)."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests

DEFAULT_API_URL = "https://atscale.papajohns.com/v1/query"
DEFAULT_PROJECT = "OARS"
DEFAULT_ENVIRONMENT = "default"
DEFAULT_TIMEOUT = "300s"


def _log(logger, msg: str) -> None:
    if logger:
        logger.info(msg)
    else:
        print(msg)


def execute_atscale_sql(
    sql: str,
    token: str,
    *,
    api_url: str = DEFAULT_API_URL,
    project: str = DEFAULT_PROJECT,
    environment: str = DEFAULT_ENVIRONMENT,
    timeout: str = DEFAULT_TIMEOUT,
    use_aggregates: bool = True,
    gen_aggregates: bool = False,
    use_local_cache: bool = True,
    use_aggregate_cache: bool = True,
    max_retries: int = 2,
    retry_backoff_base: float = 5.0,
    logger=None,
) -> Dict[str, Any]:
    """POST a SQL query to AtScale and return the raw JSON payload."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "language": "SQL",
        "query": sql,
        "fakeResults": False,
        "dryRun": False,
        "useLocalCache": use_local_cache,
        "useAggregateCache": use_aggregate_cache,
        "timeout": timeout,
        "context": {
            "environment": {"id": environment},
            "project": {"name": project},
            "aggregation": {
                "useAggregates": use_aggregates,
                "genAggregates": gen_aggregates,
            },
        },
    }

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            # HTTP timeout slightly above AtScale query timeout string
            http_timeout = 360 if timeout.endswith("s") else 360
            response = requests.post(
                api_url,
                headers=headers,
                json=body,
                timeout=http_timeout,
                verify=True,
            )
            if response.status_code != 200:
                raise RuntimeError(
                    f"AtScale query HTTP {response.status_code}: {response.text[:500]}"
                )
            payload = response.json()
            meta = payload.get("metadata") or {}
            if meta.get("succeeded") is False:
                raise RuntimeError(
                    f"AtScale query failed: {meta.get('error-message') or payload}"
                )
            return payload
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = retry_backoff_base * (2**attempt)
                _log(logger, f"AtScale request failed (attempt {attempt + 1}): {exc}; retry in {wait}s")
                time.sleep(wait)
            else:
                break
    raise last_exc or RuntimeError("AtScale query failed")


def atscale_payload_to_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Convert AtScale /v1/query JSON into a DataFrame with caption column names."""
    meta = payload.get("metadata") or {}
    columns_meta: Sequence[Dict[str, Any]] = meta.get("columns") or []
    names = [c.get("name") or f"c{i}" for i, c in enumerate(columns_meta)]
    rows: List[List[Any]] = payload.get("data") or []
    if not names and rows:
        width = max(len(r) for r in rows)
        names = [f"c{i}" for i in range(width)]
    df = pd.DataFrame(rows, columns=names if names else None)
    return df


# Map AtScale SQL captions → pipeline mapping field names used by Dataverse transforms.
DEFAULT_COLUMN_RENAMES = {
    "Store Number": "StoreNumber",
    "Calendar Date": "CalendarDate",
    "Source Actor": "SourceActor",
    "Source Channel": "SourceChannel",
    "Day Part": "DayPart",
    "Offer Code": "OfferCode",
    "Offer POS Description": "OfferPOSDescription",
    "Item_Number": "ItemNumber",
    "Item_Description": "ItemDescription",
    "Employee Name": "EmployeeName",
    "System User ID": "SystemUserID",
    "Employee_Name": "EmployeeName",
    "System_User_ID": "SystemUserID",
    "Employee_Name Level": "EmployeeName",
    "System_User_ID Level": "SystemUserID",
}


def query_atscale_sql_df(
    sql: str,
    token: str,
    *,
    column_renames: Optional[Dict[str, str]] = None,
    logger=None,
    **kwargs,
) -> pd.DataFrame:
    """Execute SQL and return a DataFrame ready for pipeline mapping."""
    _log(logger, f"AtScale SQL ({len(sql)} chars)…")
    payload = execute_atscale_sql(sql, token, logger=logger, **kwargs)
    df = atscale_payload_to_dataframe(payload)
    renames = {**DEFAULT_COLUMN_RENAMES, **(column_renames or {})}
    present = {src: dst for src, dst in renames.items() if src in df.columns}
    if present:
        df = df.rename(columns=present)
    wall = (payload.get("metadata") or {}).get("wall-time")
    _log(logger, f"AtScale returned {len(df)} rows (wall={wall}s)")
    return df
