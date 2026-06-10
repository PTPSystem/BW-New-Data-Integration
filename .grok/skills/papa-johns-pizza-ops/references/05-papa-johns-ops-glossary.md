# Papa John's Pizza Operations Glossary (Current Implementation)

## Key Terms from the OARS Model

**FLMD / FLM / FLMDPC**
Food + Labor + Management/Direct costs. One of the primary controllable "prime cost" metrics.

**PaFLMD (Profit after FLMD)**
DAX measure in the **Beachwood Daily** Power BI semantic model (not the raw OARS cube). Formula: `Revenue − FLMD` = `TY Net Sales USD − FLMD USD`. Represents dollars left after prime/direct costs — the X variable in store profit regressions. Use PaFLMD (not `Target Profit after FLM`) for profitability ranking and pacing. See `08-beachwood-daily-semantic-model.md`.

**OTD / TTDT**
Order To Door / Time To Door. The delivery customer experience metric. Several variants now exist in the cube (Avg TTDT, To The Door Time Minutes, To The Door Time for Dispatch Orders, etc.).

**Make Time / Rack Time**
Kitchen production stages. Make = assembly/build. Rack = holding/finishing. Used heavily for labor scheduling and efficiency analysis.

**BOZOCORO**
Internal order classification (promo/bundle/channel code with distinct economics).

**MyView**
Special filter dimension used for efficient recent-data pulls (81 = last 7 days, 82 = last 14 days). Preferred for incremental work in the current codebase.

**SMG**
Newer service / guest experience metrics (SMG Avg Closure, Cases Opened/Resolved, Value %).

**Comp Measures**
LY / L2Y / L3Y Comp Net Sales, Orders, etc. Pre-calculated comparable periods — always prefer these.

**Taste Of Food / Order Accuracy Good surveys**
Expanded customer experience tracking beyond basic OSAT.

See the full measures catalog for exact unique names used in MDX.
