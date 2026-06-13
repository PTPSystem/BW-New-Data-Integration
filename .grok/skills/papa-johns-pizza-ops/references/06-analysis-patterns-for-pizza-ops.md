# Analysis Patterns for Papa John's Ops (Current Repo)

## Preferred Recent Data Queries
Use `get_mdx_last_n_days(days=14)` or `days=7` from `modules/mdx_queries.py`. These use MyView filters and are efficient for most day-to-day analysis.

## Common Patterns

**Store performance last 2 weeks**
- Call `get_mdx_last_n_days(days=14)`
- Post-filter to specific stores in the resulting DataFrame (StoreNumber column)
- Compute food cost %, comp deltas, TTDT trends, etc.

**Food cost investigation**
Pull Actual Food Cost USD + TY Net Sales USD + Target Food Cost USD (and comp versions when available).
Calculate % and variance after fetching.

**Kitchen + Delivery efficiency**
Make Time Minutes + Rack Time Minutes + relevant order/delivery counts + TTDT variants.
Look at per-order or per-$ metrics.

**Expanded CX metrics**
Include the SMG and Taste of Food survey measures when the question involves service quality.

**Profitability / least profitable stores (use PaFLMD)**
- **Preferred (Layer B)**: `query_beachwood_daily.py --preset profitability --days 14`
- Or module: `execute_dax_query(get_store_profitability_ranking_dax(days=14))`
- Rank by `[PaFLMD]`, `[Estimate Profit Loss Period]`, or `[Net Profit]`.
- **Fallback (cube only)**: `query_ppj.py` + compute `PaFLMD = Sales − FLMD` per store.
- Do **not** use `Target Profit after FLM Local (Fran)` for profitability ranking.

**Single-store profit deep dive**
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset store --store 349 --days 14
```
Combine with Layer A ops metrics (Make/Rack, food cost) from `query_ppj.py`.

## Reproducibility
When answering, show the helper function used (e.g. `get_mdx_last_n_days(days=14)`) or the command to the `query_ppj.py` script so the user (or you later) can re-run it exactly.

See the query execution guide and the measures catalog for details.
