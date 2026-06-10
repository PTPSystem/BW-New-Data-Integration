---
name: papa-johns-pizza-ops
description: Expert for Papa John's pizza operations analysis powered by the live PPJ/OARS Power BI semantic model (OARS Franchise cube and related on ednacubes.papajohns.com). Use for store performance, sales trends, food/labor/FLMD cost analysis, production & delivery times (Make/Rack/OTD), customer satisfaction (OSAT, order accuracy), promos/offers impact, labor efficiency, and root-cause questions. Phrases: "papa johns", "pizza ops", "why is store X food cost high", "comp sales this week", "OTD performance", "FLMD variance", "OARS cube".
---

# Papa John's Pizza Operations Agent (Current BW-New-Data-Integration)

This skill gives you deep, always-up-to-date knowledge of Papa John's franchise operations through the **OARS semantic model** (the Power BI / Analysis Services cubes that power daily operations BI).

**This is built for the current repository** (`BW-New-Data-Integration`).

## Primary Use Cases
- Root cause analysis on sales, costs, times, and satisfaction for individual stores or groups.
- Period-over-period and comp comparisons.
- Food cost % and variance to target (prime cost control).
- Labor hours, make-line efficiency (Make Time / Rack Time), delivery (OTD / TTDT) performance.
- Promo/offer lift, order accuracy, OSAT drivers, new SMG / Taste of Food metrics.
- Store ranking, outlier detection, and "what changed" questions.

## How This Works (Two Data Layers)
1. **Knowledge Layer** (the `references/` files in this skill): Curated business definitions, MDX/DAX patterns, Papa John's ops glossary, and schema docs.
2. **Layer A — PPJ OARS cube (raw)**: MDX against `OARS Franchise` on `ednacubes.papajohns.com` via `modules/olap.py` + `modules/mdx_queries.py`. Use for base measures (sales, FLMD, labor, times, OSAT, etc.).
3. **Layer B — Beachwood Daily Power BI semantic model (derived)**: DAX measures like **PaFLMD**, profit estimates, goals, and regressions live in the published **Beachwood Daily** model (`Beachwood-PowerBI` repo). **Profitability questions must use this layer** — PaFLMD is not a native OARS cube measure.
4. When PaFLMD is needed but only the cube is available, compute the equivalent: `TY Net Sales USD - FLMD USD` (matches the DAX definition in `DailyAtScale`).

## Connection & Execution (Current Infrastructure)
- **OARS cube (Layer A)**: `https://ednacubes.papajohns.com:10502` / catalog `OARS Franchise`
- **Beachwood Daily semantic model (Layer B)**: Published Fabric/Power BI dataset **Beachwood Daily** — query via Power BI XMLA endpoint (not yet wired in this repo; see `08-beachwood-daily-semantic-model.md`)
- **Auth (preferred)**: Secrets `olap-username` and `olap-password` in Azure Key Vault **`kv-bw-data-integration`**.
- **Modern client**:
  - `modules/olap.py` → `execute_xmla_mdx(...)` and `parse_xmla_celldata_response(...)`
  - `modules/mdx_queries.py` → `get_mdx_last_n_days()`, `get_sample_mdx_queries()`, sales channel, offers, etc. (now 47+ measures)
- **Preferred execution for this skill**: Use the helper script `python .grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py ...`

See full details: [01-connection-and-infrastructure.md](./references/01-connection-and-infrastructure.md)

## Key Vault (Current)
- Vault: `kv-bw-data-integration`
- OLAP secrets: `olap-username`, `olap-password`
- Always run `az login` first for local development (enables DefaultAzureCredential).

## Quick Reference: Core Measures
See the full catalog with business meanings: [03-measures-catalog.md](./references/03-measures-catalog.md)

Major groups include the original 33 + newer service metrics (SMG, Singles/Doubles/Triples, Taste of Food surveys, etc.).

## References (Load These)
- [01-connection-and-infrastructure.md](./references/01-connection-and-infrastructure.md)
- [02-cubes-and-schema.md](./references/02-cubes-and-schema.md)
- [03-measures-catalog.md](./references/03-measures-catalog.md)
- [04-dimensions-and-hierarchies.md](./references/04-dimensions-and-hierarchies.md)
- [05-papa-johns-ops-glossary.md](./references/05-papa-johns-ops-glossary.md)
- [06-analysis-patterns-for-pizza-ops.md](./references/06-analysis-patterns-for-pizza-ops.md)
- [07-query-execution-guide.md](./references/07-query-execution-guide.md)
- [08-beachwood-daily-semantic-model.md](./references/08-beachwood-daily-semantic-model.md) — **PaFLMD, profit, Beachwood Daily DAX measures**

## Recommended Workflow
1. Read the relevant reference docs (especially measures catalog + patterns).
2. Identify measures + dimension filters.
3. Generate MDX using helpers from `modules/mdx_queries.py` (or craft one).
4. Execute via the skill's `query_ppj.py` script (it handles Key Vault + modern parser).
5. Interpret results using business meanings in the catalog.
6. Deliver actionable franchise ops insight.

## Keeping Knowledge Fresh
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/refresh_knowledge.py
```

## Security
- All access is read-only analytics.
- The agent inherits the current shell's `az login` / identity + network context (VPN if required for ednacubes).

When you ask Papa John's pizza ops questions while in this repo, this skill + the references should activate automatically. Use the `papa-pizza-ops-analyst` custom agent for deeper specialist behavior.
