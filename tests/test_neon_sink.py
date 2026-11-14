"""Unit tests for the Neon write path. No database connection."""

import os
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.neon_sink import (
    LABOR_TIMES,
    SALES_FORECAST,
    build_upsert_sql,
    coerce_value,
    database_url,
    ensure_sslmode,
    etl_write_targets,
    quote_ident,
    render_create_table,
    spec_from_mapping,
    upsert_records,
)
from modules.pipeline_config import load_mapping, load_pipelines


class SslModeTests(unittest.TestCase):
    def test_adds_require_when_missing(self):
        url = ensure_sslmode("postgresql://role:secret@ep.example.aws.neon.tech/neondb")
        self.assertIn("sslmode=require", url)
        self.assertNotIn("secret", url.split("?", 1)[-1])

    def test_keeps_verify_full(self):
        original = "postgresql://role:secret@ep.example.aws.neon.tech/neondb?sslmode=verify-full"
        self.assertEqual(ensure_sslmode(original), original)

    def test_replaces_disable(self):
        url = ensure_sslmode("postgresql://role:secret@ep.example.aws.neon.tech/neondb?sslmode=disable")
        self.assertIn("sslmode=require", url)
        self.assertNotIn("sslmode=disable", url)

    def test_database_url_required(self):
        previous = os.environ.pop("DATABASE_URL", None)
        try:
            with self.assertRaises(RuntimeError):
                database_url()
        finally:
            if previous is not None:
                os.environ["DATABASE_URL"] = previous

    def test_database_url_reads_env_without_logging_secret(self):
        previous = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = "postgresql://role:secret@ep.example.aws.neon.tech/neondb"
        try:
            resolved = database_url()
        finally:
            if previous is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous
        self.assertTrue(resolved.startswith("postgresql://role:secret@"))
        self.assertIn("sslmode=require", resolved)


class SchemaTests(unittest.TestCase):
    def test_rejects_unsafe_identifier(self):
        with self.assertRaises(ValueError):
            quote_ident('crf63_businesskey"; drop table t;--')

    def test_daily_sales_mapping_keeps_dataverse_names_and_types(self):
        mapping = load_mapping(load_pipelines()["daily_sales"].mapping_path)
        spec = spec_from_mapping(mapping)
        self.assertEqual(spec.name, "crf63_oarsbidatas")
        self.assertEqual(spec.primary_key, ("crf63_businesskey",))
        types = {col.name: col.pg_type for col in spec.columns}
        self.assertEqual(types["crf63_storenumber"], "text")
        self.assertEqual(types["crf63_calendardate"], "date")
        self.assertEqual(types["crf63_tynetsales"], "numeric")
        self.assertEqual(types["crf63_storedays"], "bigint")
        self.assertEqual(types["crf63_lastrefreshed"], "timestamptz")
        ddl = render_create_table(spec)
        self.assertIn('PRIMARY KEY ("crf63_businesskey")', ddl)
        self.assertIn('"crf63_tynetsales" numeric', ddl)

    def test_etl_targets_cover_every_write_table(self):
        names = [spec.name for spec in etl_write_targets()]
        self.assertEqual(
            names,
            [
                "crf63_oarsbidatas",
                "crf63_saleschanneldailies",
                "crf63_offerses",
                "crf63_inventories",
                "crf63_bw_clockinouts",
                "crf63_labortimes",
                "crf63_bwsalesforecasts",
            ],
        )

    def test_upsert_sql_preserves_nulls_and_uses_conflict_target(self):
        sql = build_upsert_sql(LABOR_TIMES)
        self.assertIn('ON CONFLICT ("crf63_businesskey") DO UPDATE SET', sql)
        self.assertIn('COALESCE(EXCLUDED."crf63_totalhours", "crf63_labortimes"."crf63_totalhours")', sql)
        self.assertIn("RETURNING (xmax = 0)", sql)
        forecast = build_upsert_sql(SALES_FORECAST, null_overwrites=True)
        self.assertIn(
            'ON CONFLICT ("crf63_forecasteddaypart", "crf63_forecasteddow", '
            '"crf63_forecastedfiscalweek", "crf63_forecastedstore")',
            forecast,
        )
        self.assertIn('"crf63_forecastedsales" = EXCLUDED."crf63_forecastedsales"', forecast)
        self.assertNotIn("COALESCE(EXCLUDED.\"crf63_forecastedsales\"", forecast)


class CoerceTests(unittest.TestCase):
    def test_date_keeps_the_calendar_day_from_noon_utc(self):
        self.assertEqual(coerce_value("2026-01-26T12:00:00Z", "date"), date(2026, 1, 26))

    def test_timestamptz_assumes_utc_when_naive(self):
        value = coerce_value("2026-01-26T15:04:05", "timestamptz")
        self.assertEqual(value, datetime(2026, 1, 26, 15, 4, 5, tzinfo=timezone.utc))

    def test_numeric_and_bool(self):
        self.assertEqual(coerce_value("12.50", "numeric"), Decimal("12.50"))
        self.assertIs(coerce_value(True, "boolean"), True)
        self.assertIs(coerce_value("false", "boolean"), False)
        self.assertIsNone(coerce_value("", "text"))


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self._returned = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.statements.append(sql)

    def executemany(self, sql, values, returning=False):
        rows = list(values)
        self.conn.statements.append(sql)
        self.conn.batches.append(rows)
        if self.conn.fail:
            raise RuntimeError("boom")
        # First row of the first batch looks like an insert; the rest look like updates.
        self._returned = [(index == 0 and len(self.conn.batches) == 1,) for index in range(len(rows))]

    def __iter__(self):
        return iter(self._returned)


class _Conn:
    def __init__(self, fail=False):
        self.statements = []
        self.batches = []
        self.commits = 0
        self.rollbacks = 0
        self.fail = fail

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class UpsertTests(unittest.TestCase):
    def test_dry_run_does_not_open_a_connection(self):
        def explode():
            raise AssertionError("connection opened")

        created, updated, errors = upsert_records(
            [{"crf63_businesskey": "1"}],
            LABOR_TIMES,
            dry_run=True,
            connection_factory=explode,
        )
        self.assertEqual((created, updated, errors), (0, 0, 0))

    def test_missing_primary_key_is_an_error_and_is_not_sent(self):
        conn = _Conn()
        created, updated, errors = upsert_records(
            [
                {"crf63_businesskey": "keep", "crf63_totalhours": 1},
                {"crf63_totalhours": 9},
            ],
            LABOR_TIMES,
            connection_factory=lambda: conn,
        )
        self.assertEqual(created, 1)
        self.assertEqual(updated, 0)
        self.assertEqual(errors, 1)
        self.assertEqual(len(conn.batches), 1)
        self.assertEqual(len(conn.batches[0]), 1)
        self.assertTrue(any(sql.startswith("CREATE TABLE") for sql in conn.statements))

    def test_batch_failure_counts_errors_and_rolls_back(self):
        conn = _Conn(fail=True)
        created, updated, errors = upsert_records(
            [{"crf63_businesskey": "a"}, {"crf63_businesskey": "b"}],
            LABOR_TIMES,
            batch_size=2,
            connection_factory=lambda: conn,
        )
        self.assertEqual((created, updated, errors), (0, 0, 2))
        self.assertEqual(conn.rollbacks, 1)


if __name__ == "__main__":
    unittest.main()
