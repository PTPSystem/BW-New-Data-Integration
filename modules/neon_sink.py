"""Neon Postgres write target for ETL loads.

Extraction and record shaping stay in the existing pipeline code. This module
only creates the destination tables and upserts records that already use the
Dataverse table and column names.

The connection string comes from DATABASE_URL. sslmode=require is added when
the URL does not already demand SSL. Credentials are never logged.
"""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil import parser as dt_parser

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# Logical types used in pipelines/mappings/*.yaml -> Postgres types.
PG_TYPES = {
    "string": "text",
    "text": "text",
    "date": "date",
    "datetime": "timestamptz",
    "int": "integer",
    "integer": "integer",
    "decimal": "numeric",
    "number": "numeric",
    "boolean": "boolean",
    "bool": "boolean",
}

# Dataverse Decimal precision is the scale. The table scripts that set a range
# use ±100000000000, which numeric(14,2) can store. It is not the Money type.
_DECIMAL = "numeric(14,2)"
_DECIMAL_NOTE = (
    "Dataverse Decimal precision 2, range ±100000000000. "
    "Not the Money type. numeric(14,2) covers that range."
)
_INTEGER_NOTE = "Dataverse Whole Number, range -2147483648 to 2147483647."
_TS_NOTE = (
    "Dataverse DateAndTime. Stored as timestamptz because DateTimeBehavior "
    "is not set on this column in the create script."
)
_UNCONFIRMED = "Dataverse attribute metadata for this column is not in the repo."


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    pg_type: str
    nullable: bool = True
    comment: str = ""


@dataclass(frozen=True)
class TableSpec:
    """One ETL destination table.

    ``optional_columns`` are part of the schema but may be absent on the
    Dataverse source (the historical backfill drops them if the entity
    rejects the $select).
    """

    name: str
    columns: Tuple[ColumnSpec, ...]
    primary_key: Tuple[str, ...]
    optional_columns: Tuple[str, ...] = ()
    comment: str = ""
    primary_key_name: str = ""

    def column(self, name: str) -> ColumnSpec:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(name)


def quote_ident(name: str) -> str:
    """Quote a simple SQL identifier. Rejects anything that could change the statement."""
    if not isinstance(name, str) or not _IDENT.fullmatch(name):
        raise ValueError(f"Refusing unsafe SQL identifier: {name!r}")
    return f'"{name}"'


def ensure_sslmode(url: str) -> str:
    """Return ``url`` with sslmode=require unless a stricter mode is already set."""
    parts = urlsplit(url.strip())
    if parts.scheme not in {"postgres", "postgresql"}:
        raise ValueError("DATABASE_URL must be a postgresql:// connection string")
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    mode = (query.get("sslmode") or "").lower()
    if mode not in {"require", "verify-ca", "verify-full"}:
        query["sslmode"] = "require"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def database_url() -> str:
    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        raise RuntimeError(
            "DATABASE_URL is not set. Point it at the dedicated Neon project "
            "(postgresql://<role>:<password>@<endpoint>.<region>.aws.neon.tech/<database>?sslmode=require)."
        )
    return ensure_sslmode(raw)


def _pg_type(logical: str) -> str:
    key = (logical or "string").strip().lower()
    try:
        return PG_TYPES[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported mapping type '{logical}'") from exc


def value_kind(pg_type: str) -> str:
    """Map a DDL type such as varchar(20) or numeric(14,2) to a coerce family."""
    base = pg_type.split("(", 1)[0].strip().lower()
    kinds = {
        "text": "text",
        "varchar": "text",
        "character varying": "text",
        "numeric": "numeric",
        "decimal": "numeric",
        "integer": "int",
        "int": "int",
        "bigint": "int",
        "smallint": "int",
        "date": "date",
        "timestamptz": "timestamptz",
        "timestamp": "timestamptz",
        "boolean": "boolean",
    }
    try:
        return kinds[base]
    except KeyError as exc:
        raise ValueError(f"Unsupported Postgres type '{pg_type}'") from exc


def _string(name: str, length: int, *, nullable: bool = True, extra: str = "") -> ColumnSpec:
    note = f"Dataverse String({length})."
    if extra:
        note = f"{note} {extra}"
    return ColumnSpec(name, f"varchar({length})", nullable, note)


def _decimal(name: str, *, extra: str = "") -> ColumnSpec:
    note = _DECIMAL_NOTE if not extra else f"{_DECIMAL_NOTE} {extra}"
    return ColumnSpec(name, _DECIMAL, True, note)


def _integer(name: str, *, extra: str = "") -> ColumnSpec:
    note = _INTEGER_NOTE if not extra else f"{_INTEGER_NOTE} {extra}"
    return ColumnSpec(name, "integer", True, note)


def _date_only(name: str, *, required: bool, extra: str) -> ColumnSpec:
    return ColumnSpec(name, "date", not required, extra)


def _timestamp(name: str) -> ColumnSpec:
    return ColumnSpec(name, "timestamptz", True, _TS_NOTE)


def _column_catalog() -> Dict[Tuple[str, str], ColumnSpec]:
    """Dataverse attribute metadata taken from dataverse_table_creation/."""
    date_oars = _date_only(
        "crf63_calendardate",
        required=True,
        extra="Dataverse DateOnly with TimeZoneIndependent behavior (create_oarsbidata_table.py). ApplicationRequired.",
    )
    date_required = _date_only(
        "crf63_calendardate",
        required=True,
        extra="Dataverse DateOnly, ApplicationRequired. DateTimeBehavior is not set in the create script.",
    )
    date_optional = _date_only(
        "crf63_calendardate",
        required=False,
        extra="Dataverse DateOnly. RequiredLevel None in the create script.",
    )

    columns: List[Tuple[str, ColumnSpec]] = []

    def add(table: str, col: ColumnSpec) -> None:
        columns.append((table, col))

    oars = "crf63_oarsbidatas"
    add(oars, _string(
        "crf63_businesskey", 50, nullable=False,
        extra="Alternate key crf63_businesskey_key. RequiredLevel in the add script is None; NOT NULL because it is the primary key.",
    ))
    add(oars, _string("crf63_storenumber", 20))
    add(oars, date_oars)
    for name in (
        "crf63_tynetsales", "crf63_l2ycompsales", "crf63_l3ycompsales", "crf63_lycompsales",
        "crf63_targetfoodcost", "crf63_actualfoodcost", "crf63_flmd", "crf63_actuallabor",
        "crf63_mileagecost", "crf63_discounts", "crf63_totalhours", "crf63_maketime",
        "crf63_racktime", "crf63_otdtime", "crf63_avgttdt", "crf63_targetprofit",
        "crf63_actualflm", "crf63_flmdpc", "crf63_commission", "crf63_cashovershort",
        "crf63_orderaccuracypct", "crf63_smgavgclosure", "crf63_smgvaluepct",
        "crf63_todoortimedispatch", "crf63_todoortimeminutes",
    ):
        add(oars, _decimal(name))
    for name in (
        "crf63_storedays", "crf63_tyorders", "crf63_lyorders", "crf63_deliveries",
        "crf63_bozocoroorders", "crf63_otdordercount", "crf63_dispatchedorders",
        "crf63_osatsurveycount", "crf63_osatsatisfied", "crf63_accuracysurveycount",
        "crf63_totalcalls", "crf63_answeredcalls", "crf63_smgcasesopened",
        "crf63_smgcasesresolved", "crf63_singles", "crf63_doubles", "crf63_triplesplus",
        "crf63_runs", "crf63_ttdtorders", "crf63_tasteoffoodgood", "crf63_tasteoffoodtotal",
        "crf63_orderaccuracygood",
    ):
        add(oars, _integer(name))
    add(oars, _string("crf63_name", 100, extra="Primary name. RequiredLevel None in the create script."))
    add(oars, _string("crf63_datasource", 50))
    add(oars, _timestamp("crf63_lastrefreshed"))

    channel = "crf63_saleschanneldailies"
    add(channel, _string(
        "crf63_businesskey", 250, nullable=False,
        extra="Alternate key crf63_saleschanneldaily_businesskey_key. NOT NULL because it is the primary key.",
    ))
    add(channel, _string("crf63_storenumber", 20))
    add(channel, date_required)
    add(channel, _string("crf63_sourceactor", 100))
    add(channel, _string("crf63_sourcechannel", 100))
    add(channel, _string("crf63_daypart", 50))
    add(channel, _decimal("crf63_tynetsalesusd"))
    add(channel, _integer("crf63_tyorders"))
    add(channel, _decimal("crf63_discountsusd"))
    add(channel, _decimal("crf63_lynetsalesusd"))
    add(channel, _integer("crf63_lyorders"))
    add(channel, _string("crf63_name", 200, extra="Primary name. RequiredLevel None in the create script."))
    add(channel, _timestamp("crf63_lastrefreshed"))

    offers = "crf63_offerses"
    offers_decimal = (
        "Dataverse Decimal precision 2 in create_offers_table.py. "
        "Min/max are omitted there; numeric(14,2) uses the same range as the other ETL decimals. Not Money."
    )
    offers_int = "Dataverse Integer in create_offers_table.py. Explicit min/max omitted; whole numbers are 32-bit."
    add(offers, _string(
        "crf63_businesskey", 200, nullable=False,
        extra="Alternate key crf63_offers_key. ApplicationRequired.",
    ))
    add(offers, _string("crf63_storenumber", 50))
    add(offers, date_optional)
    add(offers, _string("crf63_offercode", 100))
    add(offers, _string("crf63_offerposdescription", 200))
    add(offers, _integer("crf63_redeemedcount", extra=offers_int))
    for name in (
        "crf63_discountamountusd", "crf63_grossmarginusd", "crf63_ordermixpercent",
        "crf63_salesmixusdpercent", "crf63_netsalesusd", "crf63_targetfoodcostusd",
    ):
        add(offers, ColumnSpec(name, _DECIMAL, True, offers_decimal))
    add(offers, _integer("crf63_ordercount", extra=offers_int))
    add(offers, _string("crf63_name", 200, extra="Primary name. RequiredLevel None in the create script."))
    add(offers, _timestamp("crf63_lastrefreshed"))

    inventory = "crf63_inventories"
    add(inventory, _string(
        "crf63_businesskey", 600, nullable=False,
        extra="Alternate key crf63_businesskey_key. RequiredLevel None; NOT NULL because it is the primary key.",
    ))
    add(inventory, _string("crf63_itemnumber", 100))
    add(inventory, _string("crf63_storenumber", 100))
    add(inventory, date_optional)
    add(inventory, _string("crf63_itemdescription", 500))
    add(inventory, _decimal("crf63_qtyonhand"))
    add(inventory, _string("crf63_name", 200, extra="Primary name. RequiredLevel None in the create script."))
    add(inventory, _timestamp("crf63_lastrefreshed"))

    clock = "crf63_bw_clockinouts"
    add(clock, _string(
        "crf63_businesskey", 300, nullable=False,
        extra=(
            "Alternate key crf63_businesskey_key. The create script documents "
            "{Store}_{YYYYMMDD}_{EmployeeName}; the live mapping also appends SystemUserID. "
            "NOT NULL because it is the primary key."
        ),
    ))
    add(clock, _string("crf63_storenumber", 20))
    add(clock, date_required)
    add(clock, _string("crf63_employeename", 200))
    add(clock, _string("crf63_systemuserid", 50, extra="Added by add_clockinout_columns.py."))
    add(clock, _timestamp("crf63_clockintime"))
    add(clock, _timestamp("crf63_clockouttime"))
    for name in (
        "crf63_regularhours", "crf63_overtimehours", "crf63_totalhours",
        "crf63_totalpay", "crf63_regularpay", "crf63_overtimepay",
    ):
        add(clock, _decimal(name))
    add(clock, _string("crf63_name", 300, extra="Primary name. RequiredLevel None in the create script."))
    add(clock, _string("crf63_datasource", 100))
    add(clock, _timestamp("crf63_lastrefreshed"))

    labor = "crf63_labortimes"
    add(labor, _string(
        "crf63_businesskey", 300, nullable=False,
        extra="Alternate key crf63_businesskey_key. Format {Store}_{EmployeeID}_{YYYYMMDD}_{DayPart}. NOT NULL because it is the primary key.",
    ))
    add(labor, _string("crf63_storenumber", 20))
    add(labor, _string("crf63_employeeid", 50))
    add(labor, _string("crf63_employeename", 200))
    add(labor, _string("crf63_daypart", 50))
    add(labor, _decimal("crf63_totalhours"))
    add(labor, _date_only(
        "crf63_workdate",
        required=True,
        extra="Dataverse DateOnly, ApplicationRequired (create_labor_times_table.py).",
    ))
    add(labor, _string("crf63_name", 300, extra="Primary name. RequiredLevel None in the create script."))
    add(labor, _timestamp("crf63_lastrefreshed"))
    add(labor, _string("crf63_datasource", 100))

    forecast = "crf63_bwsalesforecasts"
    add(forecast, ColumnSpec(
        "crf63_forecasteddaypart", "text", False,
        f"{_UNCONFIRMED} Treated as an unbounded string. NOT NULL because it is part of the alternate key.",
    ))
    add(forecast, ColumnSpec(
        "crf63_forecasteddow", "integer", False,
        f"{_UNCONFIRMED} The writer sends int(dow). Assumed Dataverse Whole Number (32-bit), not a confirmed range.",
    ))
    add(forecast, ColumnSpec(
        "crf63_forecastedfiscalweek", "text", False,
        f"{_UNCONFIRMED} Week label such as the strings the forecast writer stores. Length not declared. NOT NULL as part of the alternate key.",
    ))
    add(forecast, ColumnSpec(
        "crf63_forecastedstore", "text", False,
        f"{_UNCONFIRMED} Store number stored as text by the writer. Length not declared. NOT NULL as part of the alternate key.",
    ))
    add(forecast, ColumnSpec(
        "crf63_forecastedsales", "numeric(18,4)", True,
        f"{_UNCONFIRMED} modules/sales_forecast.py rounds to 4 decimal places. Dataverse Precision was not found, so this is not confirmed as Decimal or Money.",
    ))
    add(forecast, ColumnSpec(
        "crf63_dateofforecast", "date", True,
        f"{_UNCONFIRMED} The writer sends date.isoformat(). DateOnly versus DateAndTime was not declared; stored as date.",
    ))
    add(forecast, ColumnSpec(
        "crf63_overwrite", "boolean", True,
        f"{_UNCONFIRMED} The old read filtered crf63_overwrite eq true, so this is a Two Options column. Nullable because some environments may omit it.",
    ))
    return {(table, col.name): col for table, col in columns}


def _table_notes() -> Dict[str, Tuple[str, str]]:
    """Table comment and Dataverse alternate-key schema name, when the repo names it."""
    return {
        "crf63_oarsbidatas": (
            "Entity set for logical table crf63_oarsbidata.",
            "crf63_businesskey_key",
        ),
        "crf63_saleschanneldailies": (
            "Entity set for logical table crf63_saleschanneldaily.",
            "crf63_saleschanneldaily_businesskey_key",
        ),
        "crf63_offerses": (
            "Entity set for the Offers table.",
            "crf63_offers_key",
        ),
        "crf63_inventories": (
            "Entity set for logical table crf63_inventory.",
            "crf63_businesskey_key",
        ),
        "crf63_bw_clockinouts": (
            "Entity set for logical table crf63_bw_clockinout.",
            "crf63_businesskey_key",
        ),
        "crf63_labortimes": (
            "Entity set for logical table crf63_labortime.",
            "crf63_businesskey_key",
        ),
        "crf63_bwsalesforecasts": (
            "No create script in the repo. The composite key is the one the Dataverse PATCH used: daypart, dow, fiscal week, store. The alternate-key schema name is not in the repo.",
            "",
        ),
    }


def spec_from_mapping(mapping: Mapping[str, Any]) -> TableSpec:
    """Build a Postgres table spec from a pipeline mapping file.

    Column names stay on the ``dataverse:`` keys the transformer already emits.
    The mapping key is historical; the values are the Neon column names.
    """
    table = str(mapping.get("table") or "").strip()
    if not table:
        raise ValueError("Mapping is missing 'table'")
    pk_name = str(mapping.get("alternate_key") or "crf63_businesskey").strip()

    columns: List[ColumnSpec] = []
    seen = set()

    catalog = _column_catalog()
    notes, pk_constraint = _table_notes().get(table, ("", ""))

    def add(name: Any, fallback_type: str) -> None:
        if name is None:
            return
        col_name = str(name).strip()
        if not col_name or col_name in seen:
            return
        quote_ident(col_name)
        seen.add(col_name)
        known = catalog.get((table, col_name))
        if known is not None:
            columns.append(known)
            return
        columns.append(ColumnSpec(
            col_name,
            fallback_type,
            True,
            "Pipeline mapping type only. Dataverse length, precision, and required level were not found in the repo.",
        ))

    add(pk_name, "text")
    for section in ("fields", "measures", "metadata"):
        block = mapping.get(section) or {}
        if not isinstance(block, dict):
            continue
        for spec in block.values():
            if not isinstance(spec, dict) or not spec.get("dataverse"):
                continue
            add(spec.get("dataverse"), _pg_type(str(spec.get("type") or "string")))
    for name in ("crf63_name", "crf63_datasource", "crf63_lastrefreshed"):
        if (table, name) in catalog:
            add(name, "text")

    quote_ident(table)
    return TableSpec(
        name=table,
        columns=tuple(columns),
        primary_key=(pk_name,),
        comment=notes,
        primary_key_name=pk_constraint,
    )


def with_table_name(spec: TableSpec, name: str) -> TableSpec:
    renamed = str(name or "").strip()
    quote_ident(renamed)
    return TableSpec(
        name=renamed,
        columns=spec.columns,
        primary_key=spec.primary_key,
        optional_columns=spec.optional_columns,
        comment=spec.comment,
        primary_key_name=spec.primary_key_name,
    )


def _spec_from_catalog(
    name: str,
    column_names: Sequence[str],
    primary_key: Sequence[str],
    optional_columns: Sequence[str] = (),
) -> TableSpec:
    catalog = _column_catalog()
    notes, pk_constraint = _table_notes()[name]
    return TableSpec(
        name=name,
        columns=tuple(catalog[(name, column)] for column in column_names),
        primary_key=tuple(primary_key),
        optional_columns=tuple(optional_columns),
        comment=notes,
        primary_key_name=pk_constraint,
    )


LABOR_TIMES = _spec_from_catalog(
    "crf63_labortimes",
    (
        "crf63_businesskey",
        "crf63_storenumber",
        "crf63_employeeid",
        "crf63_employeename",
        "crf63_daypart",
        "crf63_totalhours",
        "crf63_workdate",
        "crf63_name",
        "crf63_lastrefreshed",
        "crf63_datasource",
    ),
    ("crf63_businesskey",),
)

# Composite alternate key used by the sales-forecast PATCH:
# (daypart, dow, fiscal week, store). crf63_overwrite is read to skip protected
# rows and is copied by the historical backfill when Dataverse has the column.
SALES_FORECAST = _spec_from_catalog(
    "crf63_bwsalesforecasts",
    (
        "crf63_forecasteddaypart",
        "crf63_forecasteddow",
        "crf63_forecastedfiscalweek",
        "crf63_forecastedstore",
        "crf63_forecastedsales",
        "crf63_dateofforecast",
        "crf63_overwrite",
    ),
    (
        "crf63_forecasteddaypart",
        "crf63_forecasteddow",
        "crf63_forecastedfiscalweek",
        "crf63_forecastedstore",
    ),
    optional_columns=("crf63_overwrite",),
)


def etl_write_targets() -> List[TableSpec]:
    """Every table the ETL writes, in pipeline order, then labor and forecast."""
    from modules.pipeline_config import load_mapping, load_pipelines

    specs: List[TableSpec] = []
    seen = set()
    for pipeline in load_pipelines().values():
        spec = spec_from_mapping(load_mapping(pipeline.mapping_path))
        if spec.name in seen:
            continue
        seen.add(spec.name)
        specs.append(spec)
    for spec in (LABOR_TIMES, SALES_FORECAST):
        if spec.name not in seen:
            specs.append(spec)
            seen.add(spec.name)
    return specs


def _column_ddl(col: ColumnSpec, *, forced_not_null: bool = False) -> str:
    """Column declaration. The comment is on the previous line so the separating comma stays live."""
    null_sql = " NOT NULL" if (forced_not_null or not col.nullable) else ""
    declaration = f"    {quote_ident(col.name)} {col.pg_type}{null_sql}"
    if not col.comment:
        return declaration
    note = " ".join(col.comment.split())
    return f"    -- {note}\n{declaration}"


def render_create_table(spec: TableSpec) -> str:
    pk = set(spec.primary_key)
    column_sql = [_column_ddl(col, forced_not_null=col.name in pk) for col in spec.columns]
    pk_cols = ", ".join(quote_ident(name) for name in spec.primary_key)
    if spec.primary_key_name:
        column_sql.append(
            f"    CONSTRAINT {quote_ident(spec.primary_key_name)} PRIMARY KEY ({pk_cols})"
        )
    else:
        column_sql.append(f"    PRIMARY KEY ({pk_cols})")
    body = ",\n".join(column_sql)
    return f"CREATE TABLE IF NOT EXISTS {quote_ident(spec.name)} (\n{body}\n);"


def render_add_columns(spec: TableSpec) -> List[str]:
    table = quote_ident(spec.name)
    return [
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {quote_ident(col.name)} {col.pg_type};"
        for col in spec.columns
    ]


def render_ddl(specs: Optional[Sequence[TableSpec]] = None) -> str:
    chosen = list(specs) if specs is not None else etl_write_targets()
    parts = [
        "-- Neon Postgres 18 schema for project beachwood-integration (aws-us-west-2).",
        "-- Generated by: python -m modules.neon_sink --print-ddl",
        "-- Checked in as sql/neon_schema.sql. This statement list is not executed by the ETL.",
        "-- Apply it with a direct (non-pooler) connection when the role should not create tables at load time.",
        "--",
        "-- Approximation rules:",
        "-- * Table names are Dataverse entity-set names. GUID primary keys, owner, statecode,",
        "--   statuscode, and audit columns are omitted because the ETL does not write them.",
        "-- * The Dataverse alternate key is the primary key. That is the ON CONFLICT target.",
        "--   No other Dataverse indexes are declared in this repo, so none are created.",
        "-- * Dataverse Decimal precision is the scale. Precision 2 with range ±100000000000",
        "--   is numeric(14,2). These columns are Decimal, not Money.",
        "-- * Dataverse Whole Number is integer (32-bit).",
        "-- * DateOnly is date. DateAndTime is timestamptz.",
        "",
    ]
    for spec in chosen:
        if spec.comment:
            parts.append(f"-- {spec.name}: {spec.comment}")
        parts.append(render_create_table(spec))
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def coerce_value(value: Any, pg_type: str) -> Any:
    """Adapt a transformer or Dataverse value to a psycopg parameter."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None

    kind = value_kind(pg_type)
    if kind == "text":
        return str(value)

    if kind == "int":
        return int(value)

    if kind == "numeric":
        return Decimal(str(value))

    if kind == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "t"}

    if kind == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        parsed = dt_parser.parse(str(value))
        return parsed.date()

    if kind == "timestamptz":
        if isinstance(value, datetime):
            parsed_dt = value
        else:
            parsed_dt = dt_parser.parse(str(value))
        if parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
        return parsed_dt

    raise ValueError(f"Unsupported Postgres type '{pg_type}'")


def _row_values(record: Mapping[str, Any], spec: TableSpec) -> Tuple[Any, ...]:
    return tuple(coerce_value(record.get(col.name), col.pg_type) for col in spec.columns)


def _missing_primary_key(record: Mapping[str, Any], spec: TableSpec) -> bool:
    for name in spec.primary_key:
        value = record.get(name)
        if value is None or value == "":
            return True
    return False


def build_upsert_sql(spec: TableSpec, *, null_overwrites: bool = False) -> str:
    """INSERT ... ON CONFLICT DO UPDATE.

    By default a NULL in the incoming row does not wipe a stored value. That
    matches the Dataverse PATCH, which omitted null properties. Set
    ``null_overwrites`` after a truncate when the batch is a full reload.
    """
    table = quote_ident(spec.name)
    column_list = ", ".join(quote_ident(col.name) for col in spec.columns)
    placeholders = ", ".join(["%s"] * len(spec.columns))
    conflict = ", ".join(quote_ident(name) for name in spec.primary_key)
    pk = set(spec.primary_key)
    assignments = []
    for col in spec.columns:
        if col.name in pk:
            continue
        target = f"{table}.{quote_ident(col.name)}"
        incoming = f"EXCLUDED.{quote_ident(col.name)}"
        if null_overwrites:
            assignments.append(f"{quote_ident(col.name)} = {incoming}")
        else:
            assignments.append(f"{quote_ident(col.name)} = COALESCE({incoming}, {target})")
    if not assignments:
        action = "DO NOTHING"
    else:
        action = "DO UPDATE SET " + ", ".join(assignments)
    return (
        f"INSERT INTO {table} ({column_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict}) {action} "
        f"RETURNING (xmax = 0)"
    )


def _log(logger, message: str) -> None:
    if logger is None:
        print(message)
    elif hasattr(logger, "info"):
        logger.info(message)
    else:
        logger(message)


@contextmanager
def connect(url: Optional[str] = None):
    """Open a Neon connection. Prepared statements are off so a pooled URL still works."""
    import psycopg

    conn = psycopg.connect(url or database_url(), prepare_threshold=None)
    try:
        yield conn
    finally:
        conn.close()


def ensure_table(conn, spec: TableSpec) -> None:
    with conn.cursor() as cur:
        cur.execute(render_create_table(spec))
        for statement in render_add_columns(spec):
            cur.execute(statement)
    conn.commit()


def truncate_table(spec: TableSpec, *, conn=None) -> None:
    own = conn is None
    if own:
        ctx = connect()
        conn = ctx.__enter__()
    try:
        ensure_table(conn, spec)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {quote_ident(spec.name)}")
        conn.commit()
    finally:
        if own:
            ctx.__exit__(None, None, None)


def _chunks(rows: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    step = max(1, int(size))
    for start in range(0, len(rows), step):
        yield rows[start : start + step]


def upsert_records(
    records: Sequence[Mapping[str, Any]],
    spec: TableSpec,
    *,
    batch_size: int = 500,
    truncate: bool = False,
    null_overwrites: bool = False,
    dry_run: bool = False,
    logger=None,
    connection_factory: Optional[Callable] = None,
) -> Tuple[int, int, int]:
    """Upsert records. Returns (created, updated, errors).

    ``dry_run`` does not open a connection. A failed batch counts as errors
    for that batch and the load continues, matching the previous Dataverse writer.
    """
    incoming = list(records or [])
    valid: List[Mapping[str, Any]] = []
    errors = 0
    for record in incoming:
        if _missing_primary_key(record, spec):
            errors += 1
            continue
        valid.append(record)

    if dry_run:
        _log(logger, f"{spec.name}: dry-run, {len(valid)} rows would be upserted ({errors} skipped)")
        return 0, 0, errors

    if not valid and not truncate:
        _log(logger, f"{spec.name}: no rows to upsert")
        return 0, 0, errors

    sql = build_upsert_sql(spec, null_overwrites=null_overwrites or truncate)
    created = 0
    updated = 0
    factory = connection_factory or connect

    with factory() as conn:
        ensure_table(conn, spec)
        if truncate:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {quote_ident(spec.name)}")
            conn.commit()
            _log(logger, f"{spec.name}: truncated before load")

        batches = list(_chunks(valid, batch_size)) or [[]]
        if not valid:
            batches = []
        for index, batch in enumerate(batches, 1):
            values = [_row_values(record, spec) for record in batch]
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, values, returning=True)
                    flags = [bool(row[0]) for row in cur]
                conn.commit()
            except Exception as exc:
                conn.rollback()
                errors += len(batch)
                _log(logger, f"{spec.name}: batch {index}/{len(batches)} failed ({len(batch)} rows): {exc}")
                continue
            batch_created = sum(1 for flag in flags if flag)
            created += batch_created
            updated += len(flags) - batch_created
            _log(
                logger,
                f"{spec.name}: batch {index}/{len(batches)} "
                f"created={batch_created} updated={len(flags) - batch_created}",
            )

    _log(logger, f"{spec.name}: created={created} updated={updated} errors={errors}")
    return created, updated, errors


def fetch_overwrite_keys(week_labels: Sequence[str], *, logger=None) -> set:
    """Sales-forecast rows flagged crf63_overwrite that must not be replaced."""
    labels = [str(label) for label in week_labels if label]
    if not labels:
        return set()
    sql = (
        "SELECT crf63_forecastedstore, crf63_forecastedfiscalweek, "
        "crf63_forecasteddaypart, crf63_forecasteddow "
        f"FROM {quote_ident(SALES_FORECAST.name)} "
        "WHERE crf63_overwrite IS TRUE AND crf63_forecastedfiscalweek = ANY(%s)"
    )
    keys = set()
    with connect() as conn:
        ensure_table(conn, SALES_FORECAST)
        with conn.cursor() as cur:
            cur.execute(sql, (labels,))
            for store, week, daypart, dow in cur.fetchall():
                keys.add((str(store), week, daypart, int(dow)))
    _log(logger, f"{SALES_FORECAST.name}: {len(keys)} overwrite-protected keys")
    return keys


def _main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Print Neon DDL for the ETL tables")
    parser.add_argument("--print-ddl", action="store_true", help="Write CREATE TABLE statements to stdout")
    args = parser.parse_args(argv)
    if args.print_ddl:
        print(render_ddl(), end="")
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
