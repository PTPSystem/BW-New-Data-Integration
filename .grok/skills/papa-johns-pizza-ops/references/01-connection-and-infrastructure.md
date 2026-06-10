# Papa John's PPJ / OARS — Connection & Infrastructure (Current Repo)

## Two Data Layers (Important)

| Layer | What it is | Protocol | Use for |
|-------|-----------|----------|---------|
| **A — OARS cube** | PPJ's raw AtScale/OLAP cube on `ednacubes` | XMLA + MDX + Basic auth | Sales, FLMD, labor, kitchen/delivery times, OSAT, comps |
| **B — Beachwood Daily** | Published **Power BI / Fabric semantic model** | XMLA + DAX (or Power BI REST) + Entra ID | **PaFLMD**, profit estimates, goals, store regressions |

**Do not confuse these.** Querying `OARS Franchise` will **not** return DAX-only measures like `PaFLMD`. Those exist only in the **Beachwood Daily** semantic model (source: `Beachwood-PowerBI/PBIP/Beachwood Daily.SemanticModel`).

## Layer A — OARS Cube (Currently Wired in This Repo)

### Server & Endpoints
- **Primary XMLA endpoint**: `https://ednacubes.papajohns.com:10502`
- Full URL: `{server}/xmla/default`
- Protocol: HTTP + SOAP (XMLA) with Basic auth.
- Main cube: `OARS Franchise`

## Authentication (Current Standard)
**Preferred method: Azure Key Vault `kv-bw-data-integration`**

Secrets:
- `olap-username`
- `olap-password`

Modern code always loads them via:
```python
from modules.utils.keyvault import get_secret
username = get_secret("olap-username")
password = get_secret("olap-password")
```

For local development:
1. `az login`
2. Ensure your account has **Key Vault Secrets User** role on `kv-bw-data-integration`

Environment variable overrides (for `.env` during testing) are still supported in some places, but Key Vault is authoritative.

## Modern Client Code (BW-New-Data-Integration)

**Execution**:
- `modules/olap.py`:
  - `execute_xmla_mdx(server, catalog, username, password, mdx_query, ssl_verify=False, logger=None, max_retries=3, ...)`
  - Uses CDATA + retry logic with exponential backoff.

**Parsing**:
- `modules/olap.py`:
  - `parse_xmla_celldata_response(xml_response, logger=None)` — improved namespace-aware parser that builds clean DataFrames with `StoreNumber`, `CalendarDate`, and all measure columns.

**MDX Query Builders**:
- `modules/mdx_queries.py` (recommended):
  - `get_mdx_last_n_days(days=14, fiscal_year=2025)` — uses MyView filters (81=7 days, 82=14 days) for efficient incremental pulls.
  - `get_sample_mdx_queries()`
  - `get_sales_channel_daily_mdx()`
  - `get_offers_mdx()`

There are now **47+ measures** (original 33 + SMG service metrics, Singles/Doubles/Triples, Taste of Food surveys, more TTDT variants, etc.).

## Running Queries as the Agent / Analyst
Preferred helper (this skill):
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py --master --fiscal-years 2025 --stores 1334 --format table
```

The script in this skill is adapted to the current modules and always tries Key Vault first via `get_secret('olap-username')` / `get_secret('olap-password')`.

You can also call the modules directly:
```python
from modules.utils.keyvault import get_secret
from modules.olap import execute_xmla_mdx, parse_xmla_celldata_response
from modules.mdx_queries import get_mdx_last_n_days

username = get_secret("olap-username")
password = get_secret("olap-password")

mdx = get_mdx_last_n_days(days=14)
xml = execute_xmla_mdx("https://ednacubes.papajohns.com:10502", "OARS Franchise", username, password, mdx, ssl_verify=False)
df = parse_xmla_celldata_response(xml)
```

## Important Query Patterns (Current)
- Use MyView filters for recent data: `[MyView].[My View].[My View].&[81]` (last 7 days) or `&[82]` (last 14 days). Much more efficient than full fiscal year pulls.
- Always request DIMENSION PROPERTIES and CELL PROPERTIES.
- The new parser expects the CellData format returned by the cube.

## Refreshing the Knowledge Layer
Run:
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/refresh_knowledge.py
```

This should discover current measures (via code or light queries) and help keep the catalog up to date while preserving curated business meanings.

## Key Differences from Older Beachwood-Data-Integration Repo
- Key Vault moved from `sf-kv-6338` → `kv-bw-data-integration`
- OLAP secrets renamed to `olap-username` / `olap-password`
- Much improved modular structure (`modules/olap.py`, `modules/mdx_queries.py`, better parser)
- More measures exposed (service metrics, etc.)
- Use of MyView for incremental queries

Always work in `BW-New-Data-Integration` for new development.
