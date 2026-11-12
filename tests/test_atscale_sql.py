"""Unit tests for AtScale SQL date window helpers."""
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.length_slicer import (
    length_to_day_count,
    render_time_sliced_sql,
    sql_date_window,
)


class SqlWindowTests(unittest.TestCase):
    def test_length_days(self):
        self.assertEqual(length_to_day_count("1wk"), 7)
        self.assertEqual(length_to_day_count("2wk"), 14)
        self.assertEqual(length_to_day_count("2day"), 2)

    def test_sql_window_ends_yesterday(self):
        start, end = sql_date_window("2day", as_of=date(2026, 11, 12))
        self.assertEqual(end, date(2026, 11, 11))
        self.assertEqual(start, date(2026, 11, 10))

    def test_render_sql_dates(self):
        sql = render_time_sliced_sql(
            "WHERE `Calendar Date` BETWEEN DATE '${start_date}' AND DATE '${end_date}'",
            "1wk",
            as_of=date(2026, 11, 12),
        )
        self.assertIn("DATE '2026-11-05'", sql)
        self.assertIn("DATE '2026-11-11'", sql)


if __name__ == "__main__":
    unittest.main()
