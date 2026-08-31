"""Unit tests for --length 1wk/2wk and Nday slicing."""
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.length_slicer import (
    LengthError,
    apply_last_n_days_subselect,
    last_complete_business_date,
    last_n_calendar_days_set,
    length_arg,
    parse_length,
    render_time_sliced_mdx,
)
from modules.pipeline_config import load_pipelines

_AS_OF = date(2026, 8, 30)


class ParseLengthTests(unittest.TestCase):
    def test_weeks(self):
        one = parse_length("1wk")
        self.assertEqual(one.kind, "week")
        self.assertEqual(one.weeks, 1)
        self.assertEqual(one.myview_id, 81)

        two = parse_length("2WK")
        self.assertEqual(two.kind, "week")
        self.assertEqual(two.weeks, 2)
        self.assertEqual(two.myview_id, 82)

    def test_seven_and_fourteen_days_map_to_myview(self):
        self.assertEqual(parse_length("7day").myview_id, 81)
        self.assertEqual(parse_length("7days").myview_id, 81)
        self.assertEqual(parse_length("14day").myview_id, 82)
        self.assertEqual(parse_length("14DAYS").myview_id, 82)

    def test_arbitrary_days(self):
        parsed = parse_length("30day")
        self.assertEqual(parsed.kind, "days")
        self.assertEqual(parsed.days, 30)
        self.assertIsNone(parsed.myview_id)

        self.assertEqual(parse_length("1day").days, 1)
        self.assertEqual(parse_length("3days").days, 3)
        self.assertEqual(parse_length("90day").days, 90)

    def test_length_arg_normalizes(self):
        self.assertEqual(length_arg("2WK"), "2wk")
        self.assertEqual(length_arg("30DAYS"), "30day")
        self.assertEqual(length_arg("7day"), "1wk")
        self.assertEqual(length_arg("14days"), "2wk")

    def test_invalid(self):
        for bad in ("", "week", "3wk", "0day", "367day", "30", "day30", "1d"):
            with self.subTest(bad=bad):
                with self.assertRaises(LengthError):
                    parse_length(bad)


class LastNDaysMdxTests(unittest.TestCase):
    def test_last_complete_day_is_yesterday(self):
        self.assertEqual(last_complete_business_date(date(2026, 8, 30)), date(2026, 8, 29))

    def test_set_expression_uses_explicit_date_members(self):
        # Run date Aug 30 → 2 complete days are Aug 28 and Aug 29.
        expr = last_n_calendar_days_set(2, as_of=_AS_OF)
        self.assertEqual(
            expr,
            "{[Calendar].[Calendar Date].[Calendar Date].&[2026-08-28], "
            "[Calendar].[Calendar Date].[Calendar Date].&[2026-08-29]}",
        )
        self.assertNotIn(".&[2026-08-30]", expr)
        self.assertNotIn("LastPeriods", expr)
        self.assertNotIn("Tail", expr)

    def test_thirty_day_range_is_inclusive(self):
        expr = last_n_calendar_days_set(30, as_of=_AS_OF)
        self.assertIn(".&[2026-07-31]", expr)
        self.assertIn(".&[2026-08-29]", expr)
        self.assertNotIn(".&[2026-08-30]", expr)
        self.assertEqual(expr.count(".&["), 30)

    def test_subselect_wraps_from_clause(self):
        mdx = "SELECT ... FROM [OARS Franchise] WHERE ([MyView].[My View].[All])"
        wrapped = apply_last_n_days_subselect(mdx, 3, as_of=_AS_OF)
        self.assertIn("FROM (SELECT {[Calendar].[Calendar Date].[Calendar Date].&[2026-08-27]", wrapped)
        self.assertIn(".&[2026-08-29]} ON 0 FROM [OARS Franchise])", wrapped)
        self.assertIn("WHERE ([MyView].[My View].[All])", wrapped)
        self.assertNotIn("LastPeriods", wrapped)


class RenderPipelineMdxTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipelines = load_pipelines()

    def test_1wk_uses_myview_81(self):
        mdx = render_time_sliced_mdx(
            self.pipelines["daily_sales"].mdx, "daily_sales", "1wk"
        )
        self.assertIn("[MyView].[My View].[My View].&[81]", mdx)
        self.assertNotIn("LastPeriods", mdx)
        self.assertNotIn("Tail", mdx)

    def test_30day_uses_subselect_not_myview_week(self):
        mdx = render_time_sliced_mdx(
            self.pipelines["daily_sales"].mdx,
            "daily_sales",
            "30day",
            as_of=_AS_OF,
        )
        self.assertIn(".&[2026-07-31]", mdx)
        self.assertIn(".&[2026-08-29]", mdx)
        self.assertNotIn(".&[2026-08-30]", mdx)
        self.assertIn("FROM (SELECT", mdx)
        self.assertIn("[MyView].[My View].[All]", mdx)
        self.assertNotIn("LastPeriods", mdx)
        self.assertNotIn("[MyView].[My View].[My View].&[81]", mdx)
        self.assertNotIn("[MyView].[My View].[My View].&[82]", mdx)

    def test_7day_matches_1wk(self):
        week = render_time_sliced_mdx(
            self.pipelines["sales_channel"].mdx, "sales_channel", "1wk"
        )
        days = render_time_sliced_mdx(
            self.pipelines["sales_channel"].mdx, "sales_channel", "7day"
        )
        self.assertEqual(week, days)

    def test_offers_keeps_13_4_all_for_day_slice(self):
        mdx = render_time_sliced_mdx(
            self.pipelines["offers"].mdx, "offers", "5day", as_of=_AS_OF
        )
        self.assertIn("[13-4 Calendar].[Alternate Calendar Hierarchy].[All]", mdx)
        self.assertIn(".&[2026-08-25]", mdx)
        self.assertIn(".&[2026-08-29]", mdx)
        self.assertNotIn(".&[2026-08-30]", mdx)
        self.assertIn("FROM (SELECT", mdx)
        self.assertIn("FROM [Offers])", mdx)
        self.assertNotIn("LastPeriods", mdx)

    def test_clock_in_out_wraps_vbo_cube(self):
        mdx = render_time_sliced_mdx(
            self.pipelines["clock_in_out"].mdx, "clock_in_out", "3day", as_of=_AS_OF
        )
        self.assertIn("FROM (SELECT {[Calendar].[Calendar Date].[Calendar Date].&[2026-08-27]", mdx)
        self.assertIn("FROM [VBO])", mdx)
        self.assertNotIn("LastPeriods", mdx)

    def test_fy_overrides_length(self):
        mdx = render_time_sliced_mdx(
            self.pipelines["daily_sales"].mdx,
            "daily_sales",
            "30day",
            fiscal_year=2024,
        )
        self.assertIn("[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[2024]", mdx)
        self.assertNotIn("LastPeriods", mdx)

    def test_fp_requires_fy(self):
        with self.assertRaises(LengthError):
            render_time_sliced_mdx(
                self.pipelines["offers"].mdx, "offers", "1wk", period=3
            )


if __name__ == "__main__":
    unittest.main()
