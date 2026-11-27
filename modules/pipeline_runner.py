from __future__ import annotations

from datetime import datetime
from string import Formatter
from typing import Any, Dict, List, Optional

import pandas as pd
from dateutil import parser as dt_parser

from modules.olap import (
    execute_xmla_mdx,
    parse_xmla_celldata_response,
    parse_sales_channel_daily_response,
    parse_offers_response,
    parse_inventory_response,
)
from modules.atscale import query_atscale_sql_df
from modules.generic_xmla_parser import GenericXMLAParser


# Legacy custom parsers - kept for backward compatibility and as fallback.
# These are no longer used by default when hierarchy_mappings are provided.
# See modules/generic_xmla_parser.py for the unified generic parser.


_PARSERS = {
    "celldata": parse_xmla_celldata_response,
    "sales_channel_daily": parse_sales_channel_daily_response,
    "offers": parse_offers_response,
    "inventory": parse_inventory_response,
}


def run_sql_to_df(
    *,
    sql: str,
    token: str,
    api_url: str,
    project: str,
    environment: str = "default",
    timeout: str = "300s",
    logger=None,
) -> pd.DataFrame:
    """Run AtScale SQL and return a DataFrame with mapping-friendly column names."""
    return query_atscale_sql_df(
        sql,
        token,
        api_url=api_url,
        project=project,
        environment=environment,
        timeout=timeout,
        logger=logger,
    )


def run_mdx_to_df(
    *,
    xmla_server: str,
    catalog: str,
    username: str,
    password: str,
    mdx: str,
    parser: str,
    hierarchy_mappings: Optional[List[Dict[str, str]]] = None,
    ssl_verify: bool = False,
    logger=None,
) -> pd.DataFrame:
    # If hierarchy_mappings provided, use generic parser
    if hierarchy_mappings:
        xml = execute_xmla_mdx(
            xmla_server,
            catalog,
            username,
            password,
            mdx,
            ssl_verify=ssl_verify,
            logger=logger,
        )
        
        generic_parser = GenericXMLAParser(hierarchy_mappings)
        df = generic_parser.parse_response(xml, logger=logger)
        return df if df is not None else pd.DataFrame()
    
    # Otherwise, use legacy custom parsers
    parse_fn = _PARSERS.get(parser)
    if not parse_fn:
        raise ValueError(f"Unknown parser '{parser}'. Known: {sorted(_PARSERS.keys())}")

    xml = execute_xmla_mdx(
        xmla_server,
        catalog,
        username,
        password,
        mdx,
        ssl_verify=ssl_verify,
        logger=logger,
    )

    df = parse_fn(xml, logger=logger)
    if df is None:
        return pd.DataFrame()
    return df


def _is_scalar_na(value: Any) -> bool:
    """True for None, NaN, pd.NA, and NaT. Non-scalars are not treated as NA."""
    if value is None:
        return True
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if getattr(result, "shape", None) not in (None, ()):
        return False
    return bool(result)


def _missing_business_key_value(value: Any) -> bool:
    """Null or blank values cannot form a stable alternate key."""
    if _is_scalar_na(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _business_key_field_names(fmt: str) -> List[str]:
    names: List[str] = []
    for _, field_name, _, _ in Formatter().parse(fmt):
        if not field_name:
            continue
        base = field_name.split(".", 1)[0].split("[", 1)[0]
        if base and base not in names:
            names.append(base)
    return names


def _coerce(value: Any, typ: str):
    if _is_scalar_na(value):
        return None

    if typ == "string":
        s = str(value)
        return s

    if typ == "date":
        # Date-only semantics: OLAP provides a date (often midnight). 
        # 
        # FIX for timezone shift issue: Use 12PM (noon) UTC instead of midnight.
        # 
        # Problem: Dataverse DateOnly fields with UserLocal behavior convert UTC to user timezone.
        # - Midnight UTC (00:00:00Z) appears as previous day in negative UTC offset timezones (e.g., EST)
        # - Example: 2026-01-26T00:00:00Z displays as 2026-01-25T19:00:00 EST = wrong date (1/25)
        # 
        # Solution: Use noon UTC (12:00:00Z) as the timestamp.
        # - Noon UTC works for all timezones from UTC-12 to UTC+12
        # - Example: 2026-01-26T12:00:00Z displays as 2026-01-26T07:00:00 EST = correct date (1/26)
        # 
        # This works for both UserLocal (current) and TimeZoneIndependent (future) behaviors.
        try:
            if isinstance(value, str):
                dt = dt_parser.parse(value)
            else:
                dt = value
            
            # Set time to noon (12:00 PM) UTC and format with 'Z' suffix
            dt_noon = dt.replace(hour=12, minute=0, second=0, microsecond=0)
            return dt_noon.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    if typ == "int":
        try:
            return int(float(str(value).replace(",", "")))
        except Exception:
            return None

    if typ == "decimal":
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            return None

    if typ == "datetime":
        # Full datetime with time component
        try:
            if isinstance(value, str):
                dt = dt_parser.parse(value)
            else:
                dt = value
            # Return ISO 8601 format with timezone (UTC assumed if not specified)
            if dt.tzinfo is None:
                # Assume UTC if no timezone info
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                return dt.isoformat()
        except Exception:
            return None

    raise ValueError(f"Unsupported type '{typ}'")


def transform_df_to_records(
    df: pd.DataFrame,
    mapping: Dict[str, Any],
    logger=None,
    stats: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    table = mapping.get("table")
    if not table:
        raise ValueError("Mapping missing 'table'")

    fields = mapping.get("fields") or {}
    measures = mapping.get("measures") or {}
    bk = mapping.get("business_key") or {}
    bk_format = bk.get("format")
    if not bk_format:
        raise ValueError("Mapping missing business_key.format")
    bk_fields = _business_key_field_names(bk_format)

    def log(msg: str):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    records: List[Dict[str, Any]] = []
    now_iso = datetime.now().isoformat()
    # Rows with a null or blank business-key field are skipped, not given a
    # placeholder. A synthetic token such as "NONE" would mint a new alternate
    # key, collapse distinct source rows onto one record, and orphan that row
    # if a real code arrives later. Complete rows keep the key they already have.
    skipped_missing_key = 0
    max_skip_logs = 5

    for _, row in df.iterrows():
        # Skip aggregate rows where any dimension value is 'All'.
        # Compare only real scalars: pd.NA == 'All' raises TypeError.
        skip_row = False
        for src in fields.keys():
            raw = row.get(src)
            if _is_scalar_na(raw):
                continue
            if raw == 'All':
                skip_row = True
                break
        if skip_row:
            continue
            
        record: Dict[str, Any] = {}

        # Dimensions/fields
        template_values: Dict[str, Any] = {}
        template_raw_values: Dict[str, Any] = {}
        for src, spec in fields.items():
            dv = spec.get("dataverse")
            typ = spec.get("type", "string")
            v = row.get(src)
            template_raw_values[src] = v
            cv = _coerce(v, typ)
            if cv is None:
                continue
            record[str(dv)] = cv
            template_values[src] = cv

        # Measures
        for src, spec in measures.items():
            dv = spec.get("dataverse")
            typ = spec.get("type", "decimal")
            default = spec.get("default")
            v = row.get(src)
            cv = _coerce(v, typ)
            if cv is None and default is not None:
                cv = default
            if cv is None:
                continue
            record[str(dv)] = cv

        # Business key: if template uses {CalendarDate:%Y%m%d}, ensure CalendarDate is a datetime.
        # Also normalize to date-only to avoid timezone/day-shift surprises.
        if "CalendarDate" in template_values and "CalendarDate" in template_raw_values:
            raw_date = template_raw_values.get("CalendarDate")
            try:
                dt = dt_parser.parse(raw_date) if isinstance(raw_date, str) else raw_date
                # Normalize to the date part (still a datetime for strftime formatting)
                template_values["CalendarDate"] = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            except Exception:
                try:
                    dt = dt_parser.parse(str(raw_date))
                    template_values["CalendarDate"] = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                except Exception:
                    pass

        missing = [
            name for name in bk_fields
            if _missing_business_key_value(template_values.get(name))
        ]
        if missing:
            skipped_missing_key += 1
            if skipped_missing_key <= max_skip_logs:
                log(
                    "⚠️  Skipping row missing business-key field(s) "
                    f"{', '.join(missing)} ({_business_key_snapshot(bk_fields, template_values, template_raw_values)})"
                )
            continue

        try:
            business_key = bk_format.format(**template_values)
        except (KeyError, ValueError, IndexError) as exc:
            skipped_missing_key += 1
            if skipped_missing_key <= max_skip_logs:
                log(
                    "⚠️  Skipping row; business key format failed "
                    f"({exc}): {_business_key_snapshot(bk_fields, template_values, template_raw_values)}"
                )
            continue
        record[mapping.get("alternate_key", "crf63_businesskey")] = business_key

        # Common metadata
        record.setdefault("crf63_lastrefreshed", now_iso)
        if "crf63_name" not in record and "StoreNumber" in template_values and "CalendarDate" in template_values:
            try:
                dt = template_values["CalendarDate"]
                date_str = dt.strftime("%Y%m%d") if hasattr(dt, "strftime") else str(dt)
                record["crf63_name"] = f"{template_values['StoreNumber']} - {date_str}"
            except Exception:
                pass

        if record.get(mapping.get("alternate_key", "crf63_businesskey")):
            records.append(record)

    if skipped_missing_key:
        log(
            f"⚠️  Skipped {skipped_missing_key} row(s) with a null or blank business-key "
            f"field for {table}. Those rows were not upserted; keys for complete rows are unchanged."
        )
    if stats is not None:
        stats["skipped_missing_key"] = skipped_missing_key
    log(f"✓ Transformed {len(records)} records for {table}")
    return records


def _business_key_snapshot(
    fields: List[str],
    template_values: Dict[str, Any],
    template_raw_values: Dict[str, Any],
) -> str:
    parts: List[str] = []
    for name in fields:
        raw = template_raw_values.get(name)
        if isinstance(raw, str) and raw.strip() == "":
            parts.append(f"{name}=<blank>")
            continue
        if _missing_business_key_value(template_values.get(name)):
            parts.append(f"{name}=<null>")
            continue
        shown = template_values.get(name)
        if hasattr(shown, "strftime"):
            shown = shown.strftime("%Y-%m-%d")
        parts.append(f"{name}={shown}")
    return ", ".join(parts)
