# Beachwood Daily Power BI Semantic Model (Layer B)

## What This Is

**Beachwood Daily** is the published Power BI / Fabric semantic model used for franchise daily ops dashboards. It sits **on top of** the OARS cube data and adds DAX measures for profitability, goals, and pacing.

- **Repo**: `Beachwood-PowerBI` (sibling workspace)
- **Model path**: `PBIP/Beachwood Daily.SemanticModel/`
- **Primary fact table**: `DailyAtScale` (OARS measures imported/transformed)
- **Profit / GL table**: `GL Report` (`Net Profit` from general ledger categories)
- **Store attributes**: `Stores` (per-store PaFLMD→profit regression coefficients)

## Live Connection (Wired in BW-New-Data-Integration)

| Item | Value |
|------|-------|
| **Workspace** | Shared Workspace |
| **Workspace ID** | `ba0545ee-6dee-4757-b5c2-c5946cd9e320` |
| **Dataset** | Beachwood Daily |
| **Dataset ID** | `6fd26600-b245-404f-86e4-5841e1c88e9c` |
| **API** | `https://api.powerbi.com/v1.0/myorg` |
| **Auth** | Service principal `ar-bw-data-integration` |
| **SP object ID** | `a9591855-4270-4364-9b09-190e89671e5b` |
| **Client ID** | `d056223e-f0de-4b16-b4e0-fec2a24109ff` |
| **Required workspace role** | **Member** (Viewer cannot execute DAX) |

**Code:**
- `modules/powerbi.py` — token + `execute_dax_query()`
- `modules/powerbi_queries.py` — DAX builders
- `.grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py` — CLI

```bash
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset profitability --days 14
```

## PaFLMD — The Key Profitability Driver

**PaFLMD** = **P**rofit **a**fter **FLMD** = revenue left after prime/direct costs.

### DAX definition (authoritative, from `DailyAtScale.tmdl`)

```dax
PaFLMD = Sum(DailyAtScale[TY Net Sales USD]) - Sum(DailyAtScale[FLMD USD])
```

### Cube equivalent (Layer A fallback)

```text
PaFLMD ≈ TY Net Sales USD − FLMD USD
```

### Related DAX measures (Beachwood Daily only)

| Measure | Purpose |
|---------|---------|
| `LY PaFLMD` | Prior-year comp version |
| `Comp PaFLMD` | Period-over-period % change |
| `Estimated Break Even` | Store-level break-even PaFLMD threshold |
| `Estimate Profit Loss Period` | Estimated profit/loss from PaFLMD vs break-even |
| `Net Profit` | Actual GL net profit (`GL Report` table) |
| `Dynamic PaFLMD to Profit Slope 2` | LINESTX slope of Net Profit vs PaFLMD |

## Ranking Least / Most Profitable Stores

**Preferred (Layer B):**
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset profitability --days 14
```

Rank by `PaFLMD`, `Estimate Profit Loss Period`, or `Net Profit`.

## Setup Checklist (for new environments)

1. Semantic model in a **shared workspace** (not My Workspace).
2. Add `ar-bw-data-integration` as **Member** via Workspace access.
3. Enable Power BI admin setting: **Allow service principals to use Power BI APIs**.
4. Optional: store `powerbi-workspace-id` and `powerbi-dataset-id` in Key Vault `kv-bw-data-integration`.