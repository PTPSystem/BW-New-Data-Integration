"""DAX query builders for Beachwood Daily (Layer B)."""


def get_max_sales_date_dax() -> str:
    return """
EVALUATE
ROW(
    "MaxSalesDate",
    CALCULATE(
        MAX('DailyAtScale'[Calendar Date]),
        NOT ISBLANK('DailyAtScale'[TY Net Sales USD])
    )
)
"""


def get_store_paflmd_dax(store_number: str, days: int = 14) -> str:
    store = store_number.zfill(6) if store_number.isdigit() else store_number
    return f"""
EVALUATE
ROW(
    "Store", "{store}",
    "PaFLMD",
        CALCULATE(
            [PaFLMD],
            'DailyAtScale'[Store Number Label] = "{store}",
            'Calendar'[Date] >= TODAY() - {days}
        ),
    "Estimate Profit Loss Period",
        CALCULATE(
            [Estimate Profit Loss Period],
            'DailyAtScale'[Store Number Label] = "{store}",
            'Calendar'[Date] >= TODAY() - {days}
        ),
    "Net Profit",
        CALCULATE(
            [Net Profit],
            'DailyAtScale'[Store Number Label] = "{store}",
            'Calendar'[Date] >= TODAY() - {days}
        )
)
"""


def get_store_profitability_ranking_dax(days: int = 14, top_n: int = 45) -> str:
    return f"""
EVALUATE
TOPN(
    {top_n},
    ADDCOLUMNS(
        SUMMARIZECOLUMNS(
            'DailyAtScale'[Store Number Label],
            FILTER(
                ALL('Calendar'[Date]),
                'Calendar'[Date] >= TODAY() - {days}
            ),
            "PaFLMD", [PaFLMD],
            "Estimate Profit Loss Period", [Estimate Profit Loss Period],
            "Net Profit", [Net Profit]
        ),
        "Net Sales", [TY Net Sales USD]
    ),
    [PaFLMD],
    ASC
)
"""


def get_stores_paflmd_dax(stores: list[str], days: int = 14) -> str:
    labels = ", ".join(f'"{s.zfill(6)}"' for s in stores if s)
    return f"""
EVALUATE
ADDCOLUMNS(
    SUMMARIZECOLUMNS(
        'DailyAtScale'[Store Number Label],
        FILTER(
            ALL('Calendar'[Date]),
            'Calendar'[Date] >= TODAY() - {days}
        ),
        FILTER(
            ALL('DailyAtScale'[Store Number Label]),
            'DailyAtScale'[Store Number Label] IN {{{labels}}}
        ),
        "PaFLMD", [PaFLMD],
        "Estimate Profit Loss Period", [Estimate Profit Loss Period],
        "Net Profit", [Net Profit]
    ),
    "Net Sales", [TY Net Sales USD]
)
"""