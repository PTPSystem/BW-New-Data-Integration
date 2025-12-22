from __future__ import annotations

from datetime import datetime
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


def _coerce(value: Any, typ: str):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if typ == "string":
        s = str(value)
        return s

    if typ == "date":
        # Date-only semantics: OLAP provides a date (often midnight). Dataverse UI can
        # shift DateTime values based on timezone; we always emit a pure YYYY-MM-DD.
        try:
            if isinstance(value, str):
                dt = dt_parser.parse(value)
            else:
                dt = value
            # Strip any time component by formatting only the date portion.
            return dt.strftime("%Y-%m-%d")
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

    raise ValueError(f"Unsupported type '{typ}'")


def transform_df_to_records(
    df: pd.DataFrame,
    mapping: Dict[str, Any],
    logger=None,
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

    def log(msg: str):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    records: List[Dict[str, Any]] = []
    now_iso = datetime.now().isoformat()

    for _, row in df.iterrows():
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

        business_key = bk_format.format(**template_values)
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

    log(f"✓ Transformed {len(records)} records for {table}")
    return records
