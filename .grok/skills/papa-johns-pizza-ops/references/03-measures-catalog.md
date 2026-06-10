# OARS Franchise Measures Catalog — Business Meanings (Current Repo)

This is the primary reference for measure selection and interpretation.

The current codebase exposes the original ~33 measures + additional service / experience metrics (SMG, taste of food, more granular TTDT, etc.). Total often 47+ in full queries.

## Core Sales & Comps
- **TY Net Sales USD**, **LY Comp Net Sales USD**, **L2Y Comp...**, **L3Y Comp...** — Standard this-year vs comparable prior periods.

## Food Cost (Prime Cost #1)
- **Actual Food Cost USD**
- **TY Target Food Cost USD**

Business meaning: Actual vs theoretical/standard cost of ingredients for what was sold. Critical for variance analysis (portion control, waste, menu mix, recipe accuracy).

## Labor & FLMD (Prime Cost #2) — Layer A (OARS cube)
- **Actual Labor $ USD**
- **HS Total Actual Hours**
- **FLMD USD** / **FLMDPC USD (Fran)**
- Target/Actual Profit after FLM variants

These are the main controllable costs watched daily at store and franchise level.

## PaFLMD & Profit — Layer B (Beachwood Daily semantic model)

These are **DAX measures**, not native OARS cube fields. See `08-beachwood-daily-semantic-model.md`.

- **PaFLMD** — `Sum(TY Net Sales USD) - Sum(FLMD USD)`. Profit-after-FLMD contribution dollars. **Primary metric for profitability analysis.**
- **LY PaFLMD** / **Comp PaFLMD** — Comp versions.
- **Estimate Profit Loss Period** — Estimated profit/loss from PaFLMD vs store break-even regression.
- **Net Profit** — Actual GL net profit (`GL Report` table).
- **Estimated Break Even** — PaFLMD threshold where estimated profit = 0.

**Cube fallback**: compute `TY Net Sales USD - FLMD USD` when Layer B is unavailable.

## Kitchen Production Efficiency
- **Make Time Minutes**
- **Rack Time Minutes**

Used to compute minutes per order or per dollar. Rising values with flat volume = kitchen bottleneck.

## Delivery Performance
- **Total OTD Time (Hours)**
- **Avg TTDT** / **To The Door Time Minutes** / **To The Door Time for Dispatch Orders**
- **Deliveries**, **OTD Order Count**, **TY Dispatched Delivery Orders**

Key customer promise and operational cost driver.

## Order Volume & Mix
- **TY Orders**, **LY Orders**
- **BOZOCORO Orders**
- Newer: **Singles**, **Doubles**, **Triples Plus**, **Runs**

## Financial & Other
- **Discounts USD**
- **Mileage Cost Local**
- **Total Cash Over/Short USD**
- **m_ty_agg_commission_local_sum**

## Customer Experience & Quality (Expanded)
- OSAT related (Total / Satisfied counts)
- **Order Accuracy %** and related survey counts
- **Total Calls** / **Answered Calls**
- Newer service metrics:
  - **SMG Avg Closure**, **SMG Cases Opened**, **SMG Cases Resolved**, **SMG Value %**
  - **TY Taste Of Food Good Survey Count**, **TY Total Taste Of Food Survey Count**
  - **TY Order Accuracy Good Survey Count**

## How to Use
Before writing MDX, look up the exact unique name here. Prefer pulling the relevant TY + comp measures. Compute ratios (food cost %, labor %, TTDT per order, etc.) after fetching the raw numbers.

See `modules/mdx_queries.py` for the current master lists of measures used in production queries.

For the most up-to-date list, run the refresh script or inspect a recent full query result.
