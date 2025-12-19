def get_mdx_last_n_days(days=14, fiscal_year=2025):
    """
    Generate MDX query for the last N days of data using MyView filter.
    Useful for daily incremental updates.
    
    Args:
        days: Number of days to retrieve (7 or 14)
        fiscal_year: Fiscal year to query (default 2025)
    
    Returns:
        MDX query string
    
    Note: 
        - Uses [MyView].[My View].&[81] for 7 days (1 week)
        - Uses [MyView].[My View].&[82] for 14 days (2 weeks)
    """
    # Map days to MyView ID
    myview_id = 81 if days == 7 else 82
    
    query = f"""
SELECT 
{{
    [Measures].[TY Net Sales USD],
    [Measures].[L2Y Comp Net Sales USD],
    [Measures].[L3Y Comp Net Sales USD],
    [Measures].[LY Comp Net Sales USD],
    [Measures].[TY Target Food Cost USD],
    [Measures].[Actual Food Cost USD],
    [Measures].[FLMD USD],
    [Measures].[Target Profit after FLM Local (Fran)],
    [Measures].[Actual FLM w/o Vacation Accrual Local],
    [Measures].[Actual Labor $ USD],
    [Measures].[HS Total Actual Hours],
    [Measures].[Store Days],
    [Measures].[Make Time Minutes],
    [Measures].[TY Orders],
    [Measures].[Rack Time Minutes],
    [Measures].[Total OTD Time (Hours)],
    [Measures].[Deliveries],
    [Measures].[BOZOCORO Orders],
    [Measures].[OTD Order Count],
    [Measures].[Total Cash Over/Short USD],
    [Measures].[LY Orders],
    [Measures].[TY Total OSAT Survey Count],
    [Measures].[TY OSAT Satisfied Survey Count],
    [Measures].[Total Calls],
    [Measures].[Answered Calls],
    [Measures].[FLMDPC USD (Fran)],
    [Measures].[m_ty_agg_commission_local_sum],
    [Measures].[TY Dispatched Delivery Orders],
    [Measures].[Avg TTDT],
    [Measures].[Mileage Cost Local],
    [Measures].[Discounts USD],
    [Measures].[TY Total Order Accuracy Survey Count],
    [Measures].[Order Accuracy %],
    [Measures].[SMG Avg Closure],
    [Measures].[SMG Cases Opened],
    [Measures].[SMG Cases Resolved],
    [Measures].[SMG Value %],
    [Measures].[Singles],
    [Measures].[Doubles],
    [Measures].[Triples Plus],
    [Measures].[Runs],
    [Measures].[TTDT Orders],
    [Measures].[To The Door Time for Dispatch Orders],
    [Measures].[To The Door Time Minutes],
    [Measures].[TY Taste Of Food Good Survey Count],
    [Measures].[TY Total Taste Of Food Survey Count],
    [Measures].[TY Order Accuracy Good Survey Count]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({{[Franchise].[Store Number Label].[Store Number Label].AllMembers}}),
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}})
)
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
WHERE ([MyView].[My View].[My View].&[{myview_id}])
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    return query


def get_daily_sales_mdx(days=14):
    """Daily sales (OARS BI Data) incremental query.

    Daily sales in this repo always includes the service measures.
    """
    return get_mdx_last_n_days(days=days)

def get_sample_mdx_queries(fiscal_years=[2023, 2024, 2025]):
    """
    Return MDX queries for the OARS Franchise cube.
    
    This is the actual MDX extracted from the Excel pivot table.
    See EXTRACTED_MDX_QUERY.md for detailed documentation.
    
    Args:
        fiscal_years: List of fiscal years to query
    """
    # Build the WHERE clause for multiple fiscal years
    if isinstance(fiscal_years, int):
        fiscal_years = [fiscal_years]
    
    fiscal_year_members = ", ".join([f"[Calendar].[Calendar Hierarchy].[Fiscal_Year].&[{year}]" for year in fiscal_years])
    where_clause = f"WHERE {{{fiscal_year_members}}}"
    
    # Main query: All BI metrics by Store and Date for specified fiscal years
    # This query returns 47 measures (33 original + 14 service metrics) across all stores and dates
    query_full_bi_data = f"""
SELECT 
{{
    [Measures].[TY Net Sales USD],
    [Measures].[L2Y Comp Net Sales USD],
    [Measures].[L3Y Comp Net Sales USD],
    [Measures].[LY Comp Net Sales USD],
    [Measures].[TY Target Food Cost USD],
    [Measures].[Actual Food Cost USD],
    [Measures].[FLMD USD],
    [Measures].[Target Profit after FLM Local (Fran)],
    [Measures].[Actual FLM w/o Vacation Accrual Local],
    [Measures].[Actual Labor $ USD],
    [Measures].[HS Total Actual Hours],
    [Measures].[Store Days],
    [Measures].[Make Time Minutes],
    [Measures].[TY Orders],
    [Measures].[Rack Time Minutes],
    [Measures].[Total OTD Time (Hours)],
    [Measures].[Deliveries],
    [Measures].[BOZOCORO Orders],
    [Measures].[OTD Order Count],
    [Measures].[Total Cash Over/Short USD],
    [Measures].[LY Orders],
    [Measures].[TY Total OSAT Survey Count],
    [Measures].[TY OSAT Satisfied Survey Count],
    [Measures].[Total Calls],
    [Measures].[Answered Calls],
    [Measures].[FLMDPC USD (Fran)],
    [Measures].[m_ty_agg_commission_local_sum],
    [Measures].[TY Dispatched Delivery Orders],
    [Measures].[Avg TTDT],
    [Measures].[Mileage Cost Local],
    [Measures].[Discounts USD],
    [Measures].[TY Total Order Accuracy Survey Count],
    [Measures].[Order Accuracy %],
    [Measures].[SMG Avg Closure],
    [Measures].[SMG Cases Opened],
    [Measures].[SMG Cases Resolved],
    [Measures].[SMG Value %],
    [Measures].[Singles],
    [Measures].[Doubles],
    [Measures].[Triples Plus],
    [Measures].[Runs],
    [Measures].[TTDT Orders],
    [Measures].[To The Door Time for Dispatch Orders],
    [Measures].[To The Door Time Minutes],
    [Measures].[TY Taste Of Food Good Survey Count],
    [Measures].[TY Total Taste Of Food Survey Count],
    [Measures].[TY Order Accuracy Good Survey Count]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
    Hierarchize({{[Franchise].[Store Number Label].[Store Number Label].AllMembers}}),
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}})
) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [OARS Franchise]
{where_clause}
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    
    # Last 1 week and 2 weeks queries for incremental updates
    query_last_1_week = get_mdx_last_n_days(days=7, fiscal_year=2025)
    query_last_2_weeks = get_mdx_last_n_days(days=14, fiscal_year=2025)
    
    return {
        'full_bi_data': query_full_bi_data,
        'last_1_week': query_last_1_week,
        'last_2_weeks': query_last_2_weeks
    }


def get_sales_channel_daily_mdx():
    """
    Generate MDX query for Sales Channel Daily data.
    
    Dimensions (5):
        - Store Number Label
        - Calendar Date
        - Source Actor (Android, iOS, Desktop Web, DoorDash, etc.)
        - Source Channel (App, Web, Aggregator, Phone, Store, etc.)
        - Day Part (Lunch, Dinner, Afternoon, Evening)
    
    Measures (5):
        - TY Net Sales USD
        - TY Orders
        - Discounts USD
        - LY Net Sales USD
        - LY Orders
    
    Uses MyView 81 for last 1 week (7 days) of data.
    
    Returns:
        MDX query string
    """
    query = """SELECT {[Measures].[TY Net Sales USD],[Measures].[TY Orders],[Measures].[Discounts USD],[Measures].[LY Net Sales USD],[Measures].[LY Orders]} DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS , NON EMPTY CrossJoin(CrossJoin(CrossJoin(CrossJoin(Hierarchize({[Franchise].[Store Number Label].[Store Number Label].AllMembers}), Hierarchize({[Calendar].[Calendar Date].[Calendar Date].AllMembers})), Hierarchize({[Source Channel].[Source Actor].[Source Actor].AllMembers})), Hierarchize({[Source Channel].[Source Channel].[Source Channel].AllMembers})), Hierarchize({[Day Part Dimension].[Day Part].[Day Part].AllMembers})) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  FROM [OARS Franchise] WHERE ([MyView].[My View].[My View].&[81]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
    return query


def get_offers_mdx(days=7):
    """
    Generate MDX query for Offers data.
    
    Args:
        days: Number of days to retrieve (7 or 14). Default 7.
    
    Returns:
        MDX query string
    """
    # Map days to MyView ID
    myview_id = 81 if days == 7 else 82
    
    query = f"""
SELECT {{
    [Measures].[Redeemed Count],
    [Measures].[Discount Amount USD],
    [Measures].[Gross Margin USD],
    [Measures].[Order Mix %],
    [Measures].[Sales Mix USD %],
    [Measures].[Net Sales USD],
    [Measures].[Order Count],
    [Measures].[Target Food Cost USD]
}} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS, 
NON EMPTY CrossJoin(CrossJoin(CrossJoin(
    Hierarchize({{[Calendar].[Calendar Date].[Calendar Date].AllMembers}}), 
    Hierarchize({{[Stores].[Store Number].[Store Number].AllMembers}})), 
    Hierarchize({{[Offer Code].[Offer Code Hierarchy].[Offer Code Level].AllMembers}})), 
    Hierarchize({{[Offer Code].[Offer POS Description].[Offer POS Description].AllMembers}})) 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  
FROM [Offers] 
WHERE ([MyView].[My View].[My View].&[{myview_id}],[13-4 Calendar].[Alternate Calendar Hierarchy].[All]) 
CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""
    return query

