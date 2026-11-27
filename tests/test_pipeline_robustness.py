"""Null business keys are skipped, and one pipeline failure does not stop the rest."""
import os
import sys
import unittest
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.olap_sync import (
    _sync_email,
    outcome_for_transformed_rows,
    run_pipelines_isolated,
)
from modules.pipeline_config import load_mapping, load_pipelines
from modules.pipeline_runner import transform_df_to_records

_PIPELINES = load_pipelines()
OFFERS = load_mapping(_PIPELINES["offers"].mapping_path)
SALES_CHANNEL = load_mapping(_PIPELINES["sales_channel"].mapping_path)


def _offers_row(**overrides):
    row = {
        "StoreNumber": "125",
        "CalendarDate": "2026-11-26",
        "OfferCode": "PROMO1",
        "OfferPOSDescription": "Large",
        "Redeemed Count": 2,
    }
    row.update(overrides)
    return row


class MissingBusinessKeyTests(unittest.TestCase):
    def test_null_or_blank_offer_code_is_skipped_and_real_keys_stay_put(self):
        df = pd.DataFrame([
            _offers_row(),
            _offers_row(StoreNumber="126", OfferCode=None, OfferPOSDescription="Mystery"),
            _offers_row(StoreNumber="127", OfferCode=float("nan")),
            _offers_row(StoreNumber="128", OfferCode=pd.NA),
            _offers_row(StoreNumber="129", OfferCode=""),
            _offers_row(StoreNumber="130", OfferCode="   "),
            _offers_row(StoreNumber="131", OfferCode="A-1"),
            _offers_row(StoreNumber="132", OfferCode="NONE"),
            _offers_row(StoreNumber="133", OfferCode=0),
            _offers_row(StoreNumber="134", OfferCode=" PROMO"),
            _offers_row(StoreNumber="135", OfferPOSDescription=None),
        ])
        stats = {}
        records = transform_df_to_records(df, OFFERS, stats=stats)
        keys = [r["crf63_businesskey"] for r in records]

        self.assertEqual(keys, [
            "125_20261126_PROMO1",
            "131_20261126_A-1",
            "132_20261126_NONE",
            "133_20261126_0",
            "134_20261126_ PROMO",
            "135_20261126_PROMO1",
        ])
        self.assertEqual(stats["skipped_missing_key"], 5)
        for store in ("126", "127", "128", "129", "130"):
            self.assertFalse(any(key.startswith(store + "_") for key in keys))
        self.assertFalse(any(key.endswith("_NONE") and key.startswith("126_") for key in keys))
        desc_row = next(r for r in records if r["crf63_businesskey"].startswith("135_"))
        self.assertNotIn("crf63_offerposdescription", desc_row)

    def test_missing_offer_code_column_skips_every_row(self):
        df = pd.DataFrame([
            {
                "StoreNumber": "125",
                "CalendarDate": "2026-11-26",
                "OfferPOSDescription": "X",
                "Redeemed Count": 1,
            },
        ])
        stats = {}
        records = transform_df_to_records(df, OFFERS, stats=stats)
        self.assertEqual(records, [])
        self.assertEqual(stats["skipped_missing_key"], 1)

    def test_all_member_row_is_ignored_even_when_offer_code_is_na(self):
        df = pd.DataFrame([
            _offers_row(StoreNumber="All", OfferCode=pd.NA),
            _offers_row(),
        ])
        stats = {}
        records = transform_df_to_records(df, OFFERS, stats=stats)
        self.assertEqual(
            [r["crf63_businesskey"] for r in records],
            ["125_20261126_PROMO1"],
        )
        self.assertEqual(stats["skipped_missing_key"], 0)

    def test_sales_channel_row_missing_daypart_is_skipped(self):
        df = pd.DataFrame([
            {
                "StoreNumber": "125",
                "CalendarDate": "2026-11-26",
                "SourceActor": "Guest",
                "SourceChannel": "Web",
                "DayPart": "Lunch",
                "TY Orders": 4,
            },
            {
                "StoreNumber": "126",
                "CalendarDate": "2026-11-26",
                "SourceActor": "Guest",
                "SourceChannel": "Web",
                "DayPart": None,
                "TY Orders": 4,
            },
        ])
        records = transform_df_to_records(df, SALES_CHANNEL)
        self.assertEqual(
            [r["crf63_businesskey"] for r in records],
            ["125_20261126_Guest_Web_Lunch"],
        )

    def test_every_row_unkeyed_fails_the_pipeline_result(self):
        outcome = outcome_for_transformed_rows([], 0, 0, 0, 12)
        self.assertFalse(outcome["success"])
        self.assertIn("All 12", outcome["error"])
        self.assertEqual(outcome["rows_skipped_missing_key"], 12)

    def test_partial_skips_still_succeed(self):
        outcome = outcome_for_transformed_rows([{"crf63_businesskey": "125_20261126_PROMO1"}], 0, 1, 0, 3)
        self.assertTrue(outcome["success"])
        self.assertEqual(outcome["rows_skipped_missing_key"], 3)
        self.assertNotIn("error", outcome)


class IsolatedPipelineTests(unittest.TestCase):
    def test_offer_failure_still_runs_inventory_and_clock(self):
        calls = []

        def run_one(name):
            calls.append(name)
            if name == "offers":
                raise KeyError("OfferCode")
            return {
                "success": True,
                "records_created": 1,
                "records_updated": 2,
                "errors": 0,
                "rows_skipped_missing_key": 0,
            }

        names = ["daily_sales", "sales_channel", "offers", "inventory", "clock_in_out"]
        result = run_pipelines_isolated(names, run_one)

        self.assertEqual(calls, names)
        self.assertFalse(result["success"])
        self.assertEqual(result["records_created"], 4)
        self.assertIn("offers: KeyError: 'OfferCode'", result["error"])
        by_name = dict(result["details"])
        self.assertFalse(by_name["offers"]["success"])
        self.assertIn("OfferCode", by_name["offers"]["traceback"])
        self.assertTrue(by_name["inventory"]["success"])
        self.assertTrue(by_name["clock_in_out"]["success"])

        subject, body = _sync_email(SimpleNamespace(query="all", length="2day"), result)
        self.assertIn("Failed", subject)
        self.assertIn("inventory: ok", body)
        self.assertIn("clock_in_out: ok", body)
        self.assertIn("offers: FAILED", body)
        self.assertIn("KeyError: 'OfferCode'", body)

    def test_system_exit_does_not_stop_the_next_pipeline(self):
        def run_one(name):
            if name == "offers":
                raise SystemExit("pipeline stopped")
            return {"success": True, "records_created": 0, "records_updated": 5, "errors": 0}

        result = run_pipelines_isolated(["offers", "inventory"], run_one)
        self.assertFalse(result["success"])
        self.assertEqual(result["records_updated"], 5)
        self.assertTrue(dict(result["details"])["inventory"]["success"])

    def test_skip_warning_email_on_success(self):
        result = run_pipelines_isolated(
            ["offers", "inventory"],
            lambda name: {
                "success": True,
                "records_created": 10,
                "records_updated": 0,
                "errors": 0,
                "rows_skipped_missing_key": 4 if name == "offers" else 0,
            },
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["rows_skipped_missing_key"], 4)
        subject, body = _sync_email(SimpleNamespace(query="all", length="2day"), result)
        self.assertIn("skipped 4", subject)
        self.assertIn("unchanged", body)
        self.assertNotIn("Failed", subject)


if __name__ == "__main__":
    unittest.main()
