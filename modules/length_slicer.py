"""Parse --length values and render time-slice MDX (MyView weeks or last-N-days)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from modules.pipeline_config import render_mdx_template

_LENGTH_WEEK = frozenset({"1wk", "2wk"})
_DAY_RE = re.compile(r"^(\d+)days?$")
_FROM_CUBE_RE = re.compile(r"FROM\s+(\[[^\]]+\])", re.IGNORECASE)

MYVIEW_1WK = 81
MYVIEW_2WK = 82
MAX_DAYS = 366


class LengthError(ValueError):
    """Invalid --length value."""


@dataclass(frozen=True)
class ParsedLength:
    """Normalized --length value.

    Week slices use cube MyView members (81 = last 7 days, 82 = last 14 days).
    Day slices use explicit Calendar Date members (AtScale does not support
    LastPeriods or Tail).
    """

    kind: str  # "week" or "days"
    weeks: Optional[int] = None
    days: Optional[int] = None

    @property
    def myview_id(self) -> Optional[int]:
        if self.kind != "week":
            return None
        return MYVIEW_1WK if self.weeks == 1 else MYVIEW_2WK


def parse_length(value: str) -> ParsedLength:
    """Parse CLI --length into a week MyView slice or an N-day slice.

    Accepted forms:
      1wk, 2wk
      Nday / Ndays  (e.g. 1day, 3day, 30days)

    7day/7days and 14day/14days map to the same MyView members as 1wk and 2wk.
    """
    if value is None:
        raise LengthError("Length is required")

    raw = str(value).strip().lower()
    if not raw:
        raise LengthError("Length is required")

    if raw in _LENGTH_WEEK:
        return ParsedLength(kind="week", weeks=int(raw[0]))

    match = _DAY_RE.fullmatch(raw)
    if not match:
        raise LengthError(
            f"Unknown length '{value}'. Use 1wk, 2wk, or Nday/Ndays (e.g. 3day, 30days)."
        )

    days = int(match.group(1))
    if days < 1 or days > MAX_DAYS:
        raise LengthError(f"Day length must be between 1 and {MAX_DAYS}, got {days}.")

    if days == 7:
        return ParsedLength(kind="week", weeks=1)
    if days == 14:
        return ParsedLength(kind="week", weeks=2)
    return ParsedLength(kind="days", days=days)


def length_arg(value: str) -> str:
    """argparse type: validate --length and return a normalized lowercase string."""
    parsed = parse_length(value)
    if parsed.kind == "week":
        return f"{parsed.weeks}wk"
    return f"{parsed.days}day"


def last_n_calendar_days_set(days: int, as_of: Optional[date] = None) -> str:
    """MDX set of the last N calendar dates ending on as_of (default: today).

    Uses explicit ``&[YYYY-MM-DD]`` members. AtScale's XMLA endpoint does not
    fully support LastPeriods or Tail, which is how an SSAS last-N slice
    would normally be written.
    """
    n = int(days)
    end = as_of or date.today()
    members = [
        "[Calendar].[Calendar Date].[Calendar Date]."
        f"&[{(end - timedelta(days=n - 1 - offset)).isoformat()}]"
        for offset in range(n)
    ]
    return "{" + ", ".join(members) + "}"


def apply_last_n_days_subselect(
    mdx: str, days: int, as_of: Optional[date] = None
) -> str:
    """Wrap FROM [Cube] in a subselect that restricts Calendar Date to last N days.

    Calendar Date is already on ROWS in pipeline MDX, so it cannot also appear in
    WHERE. A FROM subselect is the AtScale-safe way to slice the same hierarchy.
    """
    match = _FROM_CUBE_RE.search(mdx)
    if not match:
        raise LengthError("MDX is missing a FROM [Cube] clause")
    cube = match.group(1)
    replacement = (
        f"FROM (SELECT {last_n_calendar_days_set(days, as_of=as_of)} ON 0 FROM {cube})"
    )
    return _FROM_CUBE_RE.sub(replacement, mdx, count=1)


def myview_slicer(pipeline_name: str, myview_id: int) -> str:
    if pipeline_name == "offers":
        return (
            f"([MyView].[My View].[My View].&[{myview_id}],"
            "[13-4 Calendar].[Alternate Calendar Hierarchy].[All])"
        )
    return f"[MyView].[My View].[My View].&[{myview_id}]"


def unrestricted_slicer(pipeline_name: str) -> str:
    """WHERE clause that does not cap the date window (MyView All).

    Used with an N-day FROM subselect so MyView 81/82 cannot shrink the range.
    """
    if pipeline_name == "offers":
        return (
            "([MyView].[My View].[All],"
            "[13-4 Calendar].[Alternate Calendar Hierarchy].[All])"
        )
    return "[MyView].[My View].[All]"


def fiscal_slicer(pipeline_name: str, fiscal_year: int, period: Optional[int] = None) -> str:
    if period is not None and fiscal_year is None:
        raise LengthError("--fp requires --fy")

    if pipeline_name in ("offers", "sales_channel"):
        if period is not None:
            if period < 1 or period > 13:
                raise LengthError("--fp must be between 1 and 13")
            return (
                f"[13-4 Calendar].[d_Year].[d_Year].&[{int(fiscal_year)}],"
                f"[13-4 Calendar].[d_Period].[d_Period].&[{int(period)}]"
            )
        return f"[13-4 Calendar].[d_Year].[d_Year].&[{int(fiscal_year)}]"

    if period is not None:
        print(
            f"⚠️  --fp is only supported for offers and sales_channel pipelines. "
            f"Ignoring for {pipeline_name}."
        )
    return f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{int(fiscal_year)}]"


def render_time_sliced_mdx(
    mdx_template: str,
    pipeline_name: str,
    length: str,
    fiscal_year: Optional[int] = None,
    period: Optional[int] = None,
    as_of: Optional[date] = None,
) -> str:
    """Render pipeline MDX with --fy/--fp or --length time slicing."""
    if period is not None and fiscal_year is None:
        raise LengthError("--fp requires --fy")

    if fiscal_year is not None:
        slicer = fiscal_slicer(pipeline_name, fiscal_year, period)
        return render_mdx_template(mdx_template, {"slicer": slicer})

    parsed = parse_length(length)
    if parsed.kind == "week":
        myview_id = parsed.myview_id
        slicer = myview_slicer(pipeline_name, myview_id)
        return render_mdx_template(
            mdx_template, {"myview_id": myview_id, "slicer": slicer}
        )

    slicer = unrestricted_slicer(pipeline_name)
    mdx = render_mdx_template(mdx_template, {"slicer": slicer})
    return apply_last_n_days_subselect(mdx, parsed.days, as_of=as_of)
