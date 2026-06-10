# Beachwood Daily Power BI Semantic Model (Layer B)

## What This Is

**Beachwood Daily** is the published Power BI / Fabric semantic model used for franchise daily ops dashboards. It sits **on top of** the OARS cube data and adds DAX measures for profitability, goals, and pacing.

- **Repo**: `Beachwood-PowerBI` (sibling workspace)
- **Model path**: `PBIP/Beachwood Daily.SemanticModel/`
- **Primary fact table**: `DailyAtScale` (OARS measures imported/transformed)
- **Profit / GL table**: `GL Report` (`Net Profit` from general ledger categories)
- **Store attributes**: `Stores` (per-store PaFLMD→profit regression coefficients)

This repo (`BW-New-Data-Integration`) currently queries **Layer A only** (OARS cube). Profit questions that reference **PaFLMD** require **Layer B** unless you compute the cube equivalent.

## PaFLMD — The Key Profitability Driver

**PaFLMD** = **P**rofit **a**fter **FLMD** = revenue left after prime/direct costs.

### DAX definition (authoritative, from `DailyAtScale.tmdl`)

```dax
PaFLMD = Sum(DailyAtScale[TY Net Sales USD]) - Sum(DailyAtScale[FLMD USD])
```

Business meaning: dollars remaining after Food + Labor + Management/Direct costs. Higher PaFLMD = more room for profit after covering controllable prime costs.

### Cube equivalent (when only Layer A is available)

```text
PaFLMD ≈ TY Net Sales USD − FLMD USD
```

Aggregate by store and period after pulling from `get_mdx_last_n_days()`.

### Related DAX measures (Beachwood Daily only)

| Measure | Purpose |
|---------|---------|
| `LY PaFLMD` | Prior-year comp version |
| `Comp PaFLMD` | Period-over-period % change |
| `PaFLMD(c)` | Calculated column: per-row `TY Net Sales USD - FLMD USD` |
| `Estimated Break Even` | Store-level break-even PaFLMD threshold (from regression) |
| `Estimate Profit Loss Period` | `(PaFLMD - Estimated Break Even) × Avg(PaFLMD to Profit Slope)` |
| `Estimate Profit Loss Period Dyn` | Dynamic regression variant |
| `Dynamic PaFLMD to Profit Slope 2` | LINESTX slope of Net Profit vs PaFLMD |
| `Net Profit` | Actual GL net profit (`GL Report` table) |

### How profit is modeled

Beachwood fits a **linear regression** per store:

```text
Net Profit ≈ Slope × PaFLMD + Intercept
```

Store coefficients live in `Stores[PaFLMD to Profit Slope]` and `Stores[PaFLMD to Profit Intercept]`. Break-even PaFLMD is `-Intercept / Slope`.

When actual GL `Net Profit` is blank, the model falls back to `Estimate Profit Loss Period`.

## When to Use Which Layer

| Question | Layer | Measure |
|----------|-------|---------|
| Food cost %, labor hours, Make/Rack time | A (OARS cube) | `Actual Food Cost USD`, `HS Total Actual Hours`, etc. |
| PaFLMD, profit pacing, least profitable stores | **B (Beachwood Daily)** | `PaFLMD`, `Estimate Profit Loss Period`, `Net Profit` |
| Quick PaFLMD proxy without PBI connection | A (compute) | `TY Net Sales USD - FLMD USD` |

## Querying Layer B (Not Yet in BW-New-Data-Integration)

To query the published semantic model directly you need:

1. **Power BI / Fabric XMLA endpoint** for the workspace hosting **Beachwood Daily**
2. **Entra ID auth** (service principal or user with dataset read + XMLA permissions)
3. **DAX** (not MDX) for measures like `[PaFLMD]`

Example DAX pattern (once endpoint is configured):

```dax
EVALUATE
SUMMARIZECOLUMNS(
    Stores[Stores Number 0],
    "PaFLMD", [PaFLMD],
    "Est Profit", [Estimate Profit Loss Period],
    "Net Profit", [Net Profit]
)
```

Store in Key Vault when available: workspace ID, dataset ID, XMLA endpoint URL, and service principal credentials.

## Ranking Least Profitable Stores

Preferred approach (Layer B):

1. Filter to the desired period (calendar / fiscal week).
2. Rank stores by **`Estimate Profit Loss Period`** or **`Net Profit`** (ascending).
3. Use **`PaFLMD`** as the explanatory driver (contribution margin after FLMD).

Fallback (Layer A only):

1. Pull `TY Net Sales USD` and `FLMD USD` per store.
2. Compute `PaFLMD = Sales - FLMD`.
3. Rank lowest PaFLMD — this identifies worst **contribution after prime costs**, not full GL profit.