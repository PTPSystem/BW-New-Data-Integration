"""Unit tests for the Dataverse → Neon backfill. No live services."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.dataverse_backfill import iter_entity_rows, iter_odata_pages, run_backfill
from modules.neon_sink import ColumnSpec, SALES_FORECAST, TableSpec
from scripts.backfill_dataverse_to_neon import main as backfill_main


class _Response:
    def __init__(self, payload, status=200):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _Session:
    def __init__(self, routes):
        self.routes = list(routes)
        self.urls = []

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        status, payload = self.routes.pop(0)
        return _Response(payload, status)

    def close(self):
        return None


TINY = TableSpec(
    name="crf63_oarsbidatas",
    columns=(
        ColumnSpec("crf63_businesskey", "text"),
        ColumnSpec("crf63_storenumber", "text"),
    ),
    primary_key=("crf63_businesskey",),
)


class PagingTests(unittest.TestCase):
    def test_follows_next_link_until_absent(self):
        session = _Session(
            [
                (
                    200,
                    {
                        "value": [{"crf63_businesskey": "a"}],
                        "@odata.nextLink": "https://example.crm.dynamics.com/api/data/v9.2/page2",
                    },
                ),
                (200, {"value": [{"crf63_businesskey": "b"}, {"crf63_businesskey": "c"}]}),
            ]
        )
        pages = list(
            iter_odata_pages(
                session,
                "https://example.crm.dynamics.com/api/data/v9.2/crf63_oarsbidatas?$select=crf63_businesskey",
            )
        )
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[1][1]["crf63_businesskey"], "c")
        self.assertTrue(session.urls[1].endswith("/page2"))

    def test_optional_column_is_dropped_after_a_400(self):
        session = _Session(
            [
                (
                    400,
                    {"error": {"message": "Could not find a property named 'crf63_overwrite'"}},
                ),
                (200, {"value": [{"crf63_forecastedstore": "126", "crf63_forecastedsales": 10}]}),
            ]
        )
        pages = list(
            iter_entity_rows(
                session,
                "https://example.crm.dynamics.com",
                SALES_FORECAST,
                page_size=100,
            )
        )
        self.assertEqual(len(pages), 1)
        self.assertNotIn("crf63_overwrite", pages[0][0])
        self.assertNotIn("crf63_overwrite", session.urls[1])
        self.assertIn("crf63_forecastedsales", session.urls[1])

    def test_strips_odata_annotations(self):
        session = _Session(
            [
                (
                    200,
                    {
                        "value": [
                            {
                                "@odata.etag": "W/\"1\"",
                                "crf63_businesskey": "s_20260126",
                                "crf63_storenumber": "126",
                            }
                        ]
                    },
                )
            ]
        )
        rows = list(iter_entity_rows(session, "https://example.crm.dynamics.com", TINY))
        self.assertEqual(rows, [[{"crf63_businesskey": "s_20260126", "crf63_storenumber": "126"}]])


class RunTests(unittest.TestCase):
    def test_dry_run_counts_rows_and_does_not_write(self):
        session = _Session(
            [
                (
                    200,
                    {
                        "value": [{"crf63_businesskey": "a"}],
                        "@odata.nextLink": "https://example.crm.dynamics.com/next",
                    },
                ),
                (200, {"value": [{"crf63_businesskey": "b"}]}),
            ]
        )
        calls = []

        def upsert(*args, **kwargs):
            calls.append((args, kwargs))
            return (1, 0, 0)

        result = run_backfill(
            dry_run=True,
            truncate=True,
            batch_size=1,
            specs=[TINY],
            session=session,
            environment_url="https://example.crm.dynamics.com",
            upsert=upsert,
        )
        self.assertEqual(calls, [])
        self.assertEqual(result["fetched"], 2)
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["tables"][0]["table"], "crf63_oarsbidatas")

    def test_load_upserts_in_batches_and_truncates_once(self):
        session = _Session(
            [
                (200, {"value": [{"crf63_businesskey": "a"}, {"crf63_businesskey": "b"}]}),
            ]
        )
        calls = []

        def upsert(records, spec, **kwargs):
            calls.append((len(records), spec.name, kwargs.get("truncate")))
            return (len(records), 0, 0)

        result = run_backfill(
            dry_run=False,
            truncate=True,
            batch_size=1,
            specs=[TINY],
            session=session,
            environment_url="https://example.crm.dynamics.com",
            upsert=upsert,
        )
        self.assertEqual(calls, [(1, "crf63_oarsbidatas", True), (1, "crf63_oarsbidatas", False)])
        self.assertEqual(result["created"], 2)
        self.assertEqual(result["errors"], 0)

    def test_cli_refuses_to_write_without_database_url(self):
        previous = os.environ.pop("DATABASE_URL", None)
        try:
            code = backfill_main([])
        finally:
            if previous is not None:
                os.environ["DATABASE_URL"] = previous
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
