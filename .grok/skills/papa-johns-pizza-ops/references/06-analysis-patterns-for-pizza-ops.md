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
- **Preferred**: Query **Beachwood Daily** semantic model (Layer B) for `[PaFLMD]`, `[Estimate Profit Loss Period]`, or `[Net Profit]`. Rank stores ascending.
- **Fallback (cube only)**: Pull `TY Net Sales USD` + `FLMD USD`, compute `PaFLMD = Sales − FLMD` per store, rank lowest. This is contribution after prime costs, not full GL profit.
- Do **not** use `Target Profit after FLM Local (Fran)` for profitability ranking — that is a target/budget measure, not PaFLMD-based profit.

## Reproducibility
When answering, show the helper function used (e.g. `get_mdx_last_n_days(days=14)`) or the command to the `query_ppj.py` script so the user (or you later) can re-run it exactly.

See the query execution guide and the measures catalog for details.
