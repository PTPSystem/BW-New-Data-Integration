# Query Execution Guide (Current Repo)

## Layer A — OARS Cube (MDX)

**Script:**
```bash
python .grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py --last-n-days 14 --stores 000349 --format table
```

**Module:**
```python
from modules.utils.keyvault import get_secret
from modules.olap import execute_xmla_mdx, parse_xmla_celldata_response
from modules.mdx_queries import get_mdx_last_n_days

mdx = get_mdx_last_n_days(days=14)
xml = execute_xmla_mdx(
    "https://ednacubes.papajohns.com:10502",
    "OARS Franchise",
    get_secret("olap-username"),
    get_secret("olap-password"),
    mdx,
    ssl_verify=False,
)
df = parse_xmla_celldata_response(xml)
```

**Auth:** `olap-username`, `olap-password` in Key Vault.

---

## Layer B — Beachwood Daily (DAX via Power BI REST API)

**Script:**
```bash
# Latest sales date in semantic model
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset max-date

# Store profitability ranking (PaFLMD, estimated profit)
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset profitability --days 14

# Single store
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --preset store --store 349 --days 14

# Custom DAX
python .grok/skills/papa-johns-pizza-ops/scripts/query_beachwood_daily.py --dax "EVALUATE ROW(\"x\", [PaFLMD])"
```

**Module:**
```python
from modules.powerbi import execute_dax_query
from modules.powerbi_queries import get_store_paflmd_dax

df = execute_dax_query(get_store_paflmd_dax("349", days=14))
```

**Auth:** Same service principal as Dataverse — `app-client-id`, `app-client-secret`, `azure-tenant-id` in Key Vault.

**Routing (workspace / dataset):**
| Setting | Default | Override |
|---------|---------|----------|
| Workspace ID | `ba0545ee-6dee-4757-b5c2-c5946cd9e320` | `POWERBI_WORKSPACE_ID` or `powerbi-workspace-id` |
| Dataset ID | `6fd26600-b245-404f-86e4-5841e1c88e9c` | `POWERBI_DATASET_ID` or `powerbi-dataset-id` |

**Requirements:**
- Service principal `ar-bw-data-integration` must be **Member** (not Viewer) on the shared workspace.
- Power BI admin tenant setting: **Allow service principals to use Power BI APIs**.

---

## Which Layer to Use

| Question | Layer | Tool |
|----------|-------|------|
| Food cost, labor, Make/Rack, OTD, OSAT | A | `query_ppj.py` |
| PaFLMD, profit, break-even, goals | **B** | `query_beachwood_daily.py` |
| Quick PaFLMD proxy | A | `query_ppj.py` + compute Sales − FLMD |