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
    "int": "bigint",
    "integer": "bigint",
    "decimal": "numeric",
    "number": "numeric",
    "boolean": "boolean",
    "bool": "boolean",
}

# Columns the record builder always may set, even when a mapping omits them.
_IMPLIED_COLUMNS = (
    ("crf63_name", "text"),
    ("crf63_datasource", "text"),
    ("crf63_lastrefreshed", "timestamptz"),
)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    pg_type: str


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

    def add(name: Any, pg_type: str) -> None:
        if name is None:
            return
        col_name = str(name).strip()
        if not col_name or col_name in seen:
            return
        quote_ident(col_name)
        seen.add(col_name)
        columns.append(ColumnSpec(col_name, pg_type))

    add(pk_name, "text")
    for section in ("fields", "measures", "metadata"):
        block = mapping.get(section) or {}
        if not isinstance(block, dict):
            continue
        for spec in block.values():
            if not isinstance(spec, dict) or not spec.get("dataverse"):
                continue
            add(spec.get("dataverse"), _pg_type(str(spec.get("type") or "string")))
    for name, pg_type in _IMPLIED_COLUMNS:
        add(name, pg_type)

    quote_ident(table)
    return TableSpec(name=table, columns=tuple(columns), primary_key=(pk_name,))


def with_table_name(spec: TableSpec, name: str) -> TableSpec:
    renamed = str(name or "").strip()
    quote_ident(renamed)
    return TableSpec(
        name=renamed,
        columns=spec.columns,
        primary_key=spec.primary_key,
        optional_columns=spec.optional_columns,
    )


LABOR_TIMES = TableSpec(
    name="crf63_labortimes",
    columns=(
        ColumnSpec("crf63_businesskey", "text"),
        ColumnSpec("crf63_storenumber", "text"),
        ColumnSpec("crf63_employeeid", "text"),
        ColumnSpec("crf63_employeename", "text"),
        ColumnSpec("crf63_daypart", "text"),
        ColumnSpec("crf63_totalhours", "numeric"),
        ColumnSpec("crf63_workdate", "date"),
        ColumnSpec("crf63_name", "text"),
        ColumnSpec("crf63_lastrefreshed", "timestamptz"),
        ColumnSpec("crf63_datasource", "text"),
    ),
    primary_key=("crf63_businesskey",),
)

# Composite alternate key used by the sales-forecast PATCH:
# (daypart, dow, fiscal week, store). crf63_overwrite is read to skip protected
# rows and is copied by the historical backfill when Dataverse has the column.
SALES_FORECAST = TableSpec(
    name="crf63_bwsalesforecasts",
    columns=(
        ColumnSpec("crf63_forecasteddaypart", "text"),
        ColumnSpec("crf63_forecasteddow", "bigint"),
        ColumnSpec("crf63_forecastedfiscalweek", "text"),
        ColumnSpec("crf63_forecastedstore", "text"),
        ColumnSpec("crf63_forecastedsales", "numeric"),
        ColumnSpec("crf63_dateofforecast", "date"),
        ColumnSpec("crf63_overwrite", "boolean"),
    ),
    primary_key=(
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


def render_create_table(spec: TableSpec) -> str:
    column_sql = [f"    {quote_ident(col.name)} {col.pg_type}" for col in spec.columns]
    pk = ", ".join(quote_ident(name) for name in spec.primary_key)
    column_sql.append(f"    PRIMARY KEY ({pk})")
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
        "-- Neon tables for the Beachwood ETL.",
        "-- Column names match the Dataverse logical names the transformers already emit.",
        "-- Apply with a direct (non-pooler) DATABASE_URL, or let the loader create them.",
        "",
    ]
    for spec in chosen:
        parts.append(render_create_table(spec))
        parts.extend(render_add_columns(spec))
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def coerce_value(value: Any, pg_type: str) -> Any:
    """Adapt a transformer or Dataverse value to a psycopg parameter."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None

    if pg_type == "text":
        return str(value)

    if pg_type == "bigint":
        return int(value)

    if pg_type == "numeric":
        return Decimal(str(value))

    if pg_type == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "t"}

    if pg_type == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        parsed = dt_parser.parse(str(value))
        return parsed.date()

    if pg_type == "timestamptz":
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
