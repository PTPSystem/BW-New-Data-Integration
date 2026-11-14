"""Copy historical Dataverse rows into the Neon tables the ETL now writes.

Read-only against Dataverse: pages each entity with ``@odata.nextLink`` and
reuses the repo's Key Vault Dataverse auth. Writes are upserts on the same
primary keys as the live load, so a re-run updates existing rows.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

from modules.neon_sink import (
    TableSpec,
    etl_write_targets,
    quote_ident,
    upsert_records,
)


def _log(logger, message: str) -> None:
    if logger is None:
        print(message)
    elif hasattr(logger, "info"):
        logger.info(message)
    else:
        logger(message)


def entity_select_url(environment_url: str, spec: TableSpec, columns: Sequence[str]) -> str:
    table = quote_ident(spec.name).strip('"')
    select = ",".join(columns)
    base = environment_url.rstrip("/")
    return f"{base}/api/data/v9.2/{table}?$select={select}"


def _odata_error_text(response: requests.Response) -> str:
    try:
        body = response.json()
    except Exception:
        return response.text[:500]
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        return str(error.get("message") or error)
    return response.text[:500]


def iter_odata_pages(
    session: requests.Session,
    start_url: str,
    *,
    page_size: int = 5000,
    timeout: int = 300,
) -> Iterable[List[Mapping[str, Any]]]:
    """Yield each ``value`` page, following ``@odata.nextLink`` until it is absent."""
    url: Optional[str] = start_url
    prefer = f"odata.maxpagesize={max(1, int(page_size))}"
    while url:
        response = session.get(
            url,
            headers={"Prefer": prefer},
            timeout=timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Dataverse read failed ({response.status_code}) for {url.split('?', 1)[0]}: "
                f"{_odata_error_text(response)}"
            )
        payload = response.json()
        rows = payload.get("value") or []
        if not isinstance(rows, list):
            raise RuntimeError("Dataverse page did not contain a value array")
        yield rows
        nxt = payload.get("@odata.nextLink")
        url = str(nxt) if nxt else None


def _selectable_columns(spec: TableSpec, dropped: Sequence[str]) -> List[str]:
    skip = set(dropped)
    return [col.name for col in spec.columns if col.name not in skip]


def clean_dataverse_row(row: Mapping[str, Any], spec: TableSpec) -> Dict[str, Any]:
    """Keep mapped columns and drop OData annotations."""
    return {col.name: row.get(col.name) for col in spec.columns if col.name in row}


def iter_entity_rows(
    session: requests.Session,
    environment_url: str,
    spec: TableSpec,
    *,
    page_size: int = 5000,
    logger=None,
) -> Iterable[List[Dict[str, Any]]]:
    """Page one entity. Optional columns are dropped once if Dataverse rejects them."""
    dropped: List[str] = []
    columns = _selectable_columns(spec, dropped)
    url = entity_select_url(environment_url, spec, columns)
    try:
        pages = iter_odata_pages(session, url, page_size=page_size)
        first = next(pages, None)
    except RuntimeError as exc:
        optional = [name for name in spec.optional_columns if name in columns]
        if not optional:
            raise
        message = str(exc)
        if not any(name in message for name in optional) and "400" not in message:
            raise
        dropped = list(optional)
        _log(logger, f"{spec.name}: Dataverse has no {', '.join(dropped)}; continuing without them")
        columns = _selectable_columns(spec, dropped)
        url = entity_select_url(environment_url, spec, columns)
        pages = iter_odata_pages(session, url, page_size=page_size)
        first = next(pages, None)

    if first is not None:
        yield [clean_dataverse_row(row, spec) for row in first]
    for page in pages:
        yield [clean_dataverse_row(row, spec) for row in page]


def _dataverse_session(environment_url: str, token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
    )
    return session


def run_backfill(
    *,
    dry_run: bool = False,
    truncate: bool = False,
    batch_size: int = 500,
    page_size: int = 5000,
    logger=None,
    specs: Optional[Sequence[TableSpec]] = None,
    session: Optional[requests.Session] = None,
    environment_url: Optional[str] = None,
    upsert: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Load every ETL destination table from Dataverse into Neon.

    ``dry_run`` reads Dataverse and logs row counts, and does not open Neon.
    ``truncate`` empties each Neon table before its first batch (full reload).
    Without ``truncate``, primary-key upserts make the load safe to repeat.
    """
    chosen = list(specs) if specs is not None else etl_write_targets()
    writer = upsert or upsert_records
    close_session = False
    if session is None:
        from modules.dataverse import get_dataverse_access_token
        from modules.utils.keyvault import get_dataverse_credentials

        creds = get_dataverse_credentials()
        environment_url = creds["environment_url"]
        token = get_dataverse_access_token(
            environment_url,
            creds["client_id"],
            creds["client_secret"],
            creds["tenant_id"],
            logger,
        )
        if not token:
            raise RuntimeError("Failed to obtain a Dataverse access token")
        session = _dataverse_session(environment_url, token)
        close_session = True
    elif not environment_url:
        raise ValueError("environment_url is required when session is injected")

    _log(
        logger,
        f"Backfill {'dry-run' if dry_run else 'load'}: {len(chosen)} tables, "
        f"batch_size={batch_size}, page_size={page_size}, truncate={truncate}",
    )

    summary: List[Dict[str, Any]] = []
    total_fetched = total_created = total_updated = total_errors = 0
    try:
        for spec in chosen:
            _log(logger, f"== {spec.name} ==")
            fetched = 0
            created = updated = errors = 0
            pending: List[Dict[str, Any]] = []
            did_truncate = False

            def flush(force_truncate: bool = False) -> None:
                nonlocal created, updated, errors, pending, did_truncate
                if dry_run or not pending:
                    pending = []
                    return
                use_truncate = force_truncate and truncate and not did_truncate
                batch_created, batch_updated, batch_errors = writer(
                    pending,
                    spec,
                    batch_size=batch_size,
                    truncate=use_truncate,
                    logger=logger,
                )
                created += batch_created
                updated += batch_updated
                errors += batch_errors
                did_truncate = did_truncate or use_truncate
                pending = []

            for page in iter_entity_rows(
                session,
                environment_url,
                spec,
                page_size=page_size,
                logger=logger,
            ):
                fetched += len(page)
                _log(logger, f"{spec.name}: fetched {fetched} rows")
                if dry_run:
                    continue
                pending.extend(page)
                while len(pending) >= batch_size:
                    chunk = pending[:batch_size]
                    pending = pending[batch_size:]
                    use_truncate = truncate and not did_truncate
                    batch_created, batch_updated, batch_errors = writer(
                        chunk,
                        spec,
                        batch_size=batch_size,
                        truncate=use_truncate,
                        logger=logger,
                    )
                    created += batch_created
                    updated += batch_updated
                    errors += batch_errors
                    did_truncate = did_truncate or use_truncate
            flush(force_truncate=True)
            if truncate and not dry_run and fetched == 0 and not did_truncate:
                writer([], spec, truncate=True, logger=logger)
                did_truncate = True

            table_result = {
                "table": spec.name,
                "fetched": fetched,
                "created": created,
                "updated": updated,
                "errors": errors,
                "dry_run": dry_run,
            }
            summary.append(table_result)
            total_fetched += fetched
            total_created += created
            total_updated += updated
            total_errors += errors
            if dry_run:
                _log(logger, f"{spec.name}: dry-run fetched {fetched} rows (no Neon writes)")
            else:
                _log(
                    logger,
                    f"{spec.name}: done fetched={fetched} created={created} "
                    f"updated={updated} errors={errors}",
                )
    finally:
        if close_session:
            session.close()

    _log(
        logger,
        "Backfill complete: "
        f"tables={len(summary)} fetched={total_fetched} created={total_created} "
        f"updated={total_updated} errors={total_errors}",
    )
    return {
        "tables": summary,
        "fetched": total_fetched,
        "created": total_created,
        "updated": total_updated,
        "errors": total_errors,
        "dry_run": dry_run,
    }
