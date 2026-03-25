#!/usr/bin/env python3
"""Spot-check load_fiscal_calendar.py against known Dataverse FY2023 data."""
from load_fiscal_calendar import generate_fiscal_calendar, fiscal_year_start, fiscal_year_end, fiscal_year_weeks

recs = generate_fiscal_calendar(2023)
print(f"FY2023: {fiscal_year_start(2023)} -> {fiscal_year_end(2023)} ({fiscal_year_weeks(2023)} weeks, {len(recs)} days)")
print()

# Expected values extracted from live Dataverse inspection output above.
# (day_of_year, exp_period, exp_week, exp_q, exp_445p, exp_445q)
checks = [
    (1,   1,  1, 1,  1, 1),   # P01 start
    (28,  1,  4, 1,  1, 1),   # last day P01
    (29,  2,  5, 1,  2, 1),   # first day P02
    (84,  3, 12, 1,  3, 1),   # last day P03
    (85,  4, 13, 2,  3, 1),   # P04 start — 445 still P03 Q1
    (91,  4, 13, 2,  3, 1),   # W13, end of 445 P03 / Q1
    (92,  4, 14, 2,  4, 2),   # W14, start of 445 P04 / Q2
    (112, 4, 16, 2,  4, 2),   # last day P04
    (113, 5, 17, 2,  4, 2),   # first day P05
    (168, 6, 24, 2,  6, 2),   # last day P06
    (169, 7, 25, 3,  6, 2),   # P07 start — 445 still P06 Q2
    (252, 9, 36, 3,  9, 3),   # last day P09
    (253, 10,37, 4,  9, 3),   # P10 start — 445 still P09 Q3
    (364,13, 52, 4, 12, 4),   # day 364
    (365,13, 53, 4, 12, 4),   # day 365 (53rd week)
]

all_ok = True
print(f"{'Day':>4}  {'Date':12}  {'P':>4}  {'W':>4}  {'Q':>3}  {'445P':>5}  {'445Q':>5}  OK?")
for day, ep, ew, eq, e445p, e445q in checks:
    r = recs[day - 1]
    ok = (r['crf63_fiscalperiodnumber'] == ep and
          r['crf63_fiscalweeknumber'] == ew and
          r['crf63_fiscalquarternumber'] == eq and
          r['crf63__445fiscalperiodnumber'] == e445p and
          r['crf63__445fiscalquarternumber'] == e445q)
    all_ok = all_ok and ok
    flag = "OK" if ok else "FAIL"
    date_str = r['crf63_date'][:10]
    print(f"{day:>4}  {date_str}  P={r['crf63_fiscalperiodnumber']:>2}(e{ep})  "
          f"W={r['crf63_fiscalweeknumber']:>2}(e{ew})  Q={r['crf63_fiscalquarternumber']}(e{eq})  "
          f"445P={r['crf63__445fiscalperiodnumber']}(e{e445p})  445Q={r['crf63__445fiscalquarternumber']}(e{e445q})  {flag}")

print()
print("All checks passed!" if all_ok else "FAILURES DETECTED — check above!")
