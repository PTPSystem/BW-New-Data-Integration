"""Unit tests for labor file processing helpers (no network)."""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.labor_processing import (
    FILE_NAME_RE,
    aggregate_labor_records,
    decimal_to_datetime,
    extract_date_from_filename,
    get_day_part,
    normalize_store_number,
    records_to_dataverse,
)


DAY_PARTS = {
    "labor_processing": {
        "day_parts": {
            "Morning": [8.0, 10.0],
            "Lunch": [10.0, 14.0],
            "Afternoon": [14.0, 17.0],
            "Dinner": [17.0, 21.0],
            "Evening": [21.0, 24.0],
            "Late Night": [0.0, 8.0],
        }
    }
}


class NormalizeStoreTests(unittest.TestCase):
    def test_strips_leading_zeros(self):
        self.assertEqual(normalize_store_number("000126"), "126")
        self.assertEqual(normalize_store_number("1334"), "1334")

    def test_non_numeric_passthrough(self):
        self.assertEqual(normalize_store_number("STORE-A"), "STORE-A")


class DayPartTests(unittest.TestCase):
    def test_decimal_hours(self):
        self.assertEqual(get_day_part(8.0, DAY_PARTS), "Morning")
        self.assertEqual(get_day_part(10.0, DAY_PARTS), "Lunch")
        self.assertEqual(get_day_part(15.5, DAY_PARTS), "Afternoon")
        self.assertEqual(get_day_part(18.0, DAY_PARTS), "Dinner")
        self.assertEqual(get_day_part(22.0, DAY_PARTS), "Evening")
        self.assertEqual(get_day_part(2.0, DAY_PARTS), "Late Night")

    def test_datetime(self):
        self.assertEqual(get_day_part(datetime(2026, 9, 1, 11, 30), DAY_PARTS), "Lunch")
        self.assertEqual(get_day_part(datetime(2026, 9, 1, 21, 0), DAY_PARTS), "Evening")


class FilenameTests(unittest.TestCase):
    def test_extract_date(self):
        self.assertEqual(
            extract_date_from_filename("000126.p_hs_timecard_record.20260901_1.csv"),
            "2026-09-01",
        )

    def test_portal_zip_pattern(self):
        match = FILE_NAME_RE.search("000126.pay_summ.20260901.153045.zip")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(2), "20260901")
        self.assertIsNone(FILE_NAME_RE.search("readme.txt"))


class DecimalDatetimeTests(unittest.TestCase):
    def test_hours_and_next_day(self):
        self.assertEqual(decimal_to_datetime("2026-09-01", 8.5), datetime(2026, 9, 1, 8, 30))
        self.assertEqual(decimal_to_datetime("2026-09-01", 0.0), datetime(2026, 9, 1, 0, 0))


class AggregateAndMapTests(unittest.TestCase):
    def test_aggregates_same_key(self):
        records = [
            {
                "store_number": "000126",
                "employee_id": "99",
                "employee_name": "Jane Doe",
                "day_part": "Lunch",
                "total_hours": 1.5,
                "date": "2026-09-01",
            },
            {
                "store_number": "000126",
                "employee_id": "99",
                "employee_name": "Jane Doe",
                "day_part": "Lunch",
                "total_hours": 0.75,
                "date": "2026-09-01",
            },
            {
                "store_number": "000126",
                "employee_id": "99",
                "employee_name": "Jane Doe",
                "day_part": "Dinner",
                "total_hours": 2.0,
                "date": "2026-09-01",
            },
        ]
        aggregated = aggregate_labor_records(records)
        by_part = {row["day_part"]: row["total_hours"] for row in aggregated}
        self.assertEqual(len(aggregated), 2)
        self.assertEqual(by_part["Lunch"], 2.25)
        self.assertEqual(by_part["Dinner"], 2.0)

    def test_dataverse_mapping(self):
        payload = records_to_dataverse(
            [
                {
                    "store_number": "000126",
                    "employee_id": "99",
                    "employee_name": "Jane Doe",
                    "day_part": "Lunch",
                    "total_hours": 2.25,
                    "date": "2026-09-01",
                }
            ]
        )
        self.assertEqual(len(payload), 1)
        row = payload[0]
        self.assertEqual(row["crf63_storenumber"], "126")
        self.assertEqual(row["crf63_businesskey"], "126_99_20260901_Lunch")
        self.assertEqual(row["crf63_workdate"], "2026-09-01")
        self.assertEqual(row["crf63_totalhours"], 2.25)
        self.assertEqual(row["crf63_datasource"], "labor_processing")


if __name__ == "__main__":
    unittest.main()
