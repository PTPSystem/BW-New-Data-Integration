# BW-New-Data-Integration — Project Rules & Agent Guidance

This is the **current active repository** for Papa John's (PPJ) franchise operations BI and OLAP-to-Dataverse pipelines.

## When Working Here
- Use the modern modular structure under `modules/` (`olap.py`, `mdx_queries.py`, `powerbi.py`, `powerbi_queries.py`, `olap_sync.py`, `keyvault.py`, etc.).
- **Two data layers**: Layer A = OARS cube (MDX on `ednacubes`); Layer B = Beachwood Daily Power BI semantic model (DAX on `api.powerbi.com`).
- All sensitive credentials come from Azure Key Vault **`kv-bw-data-integration`**.
- Profit / PaFLMD questions → Layer B (`query_beachwood_daily.py`). Ops / costs / times → Layer A (`query_ppj.py`).
- Run `az login` for local development Key Vault access.
- New pipelines and mappings live under `pipelines/`.

## Papa John's Pizza Operations Analyst Agent
For any questions involving **Papa John's store performance, sales, food/labor/FLMD costs, kitchen efficiency (Make/Rack), delivery (OTD/TTDT), customer metrics (OSAT, accuracy, new SMG/Taste surveys), or comp analysis**, use:

- **Skill**: `papa-johns-pizza-ops` (auto-activates on relevant prompts)
- **Custom agent**: `papa-pizza-ops-analyst`
- **Knowledge**: `.grok/skills/papa-johns-pizza-ops/references/` (curated measures catalog with business meanings, patterns, glossary, connection details for the current architecture)

The skill encodes the live OARS semantic model as durable agent knowledge while using the real executable modules in this repo for numbers.

See:
- `.grok/skills/papa-johns-pizza-ops/SKILL.md`
- The `references/` folder inside the skill
- `docs/KEYVAULT_SECRETS.md` for current secret layout

## General Conventions
- Key Vault is the source of truth for secrets. Environment variables in `.env` are only for local overrides.
- Use the query helper in the skill for ad-hoc Papa John's analysis: `.grok/skills/papa-johns-pizza-ops/scripts/query_ppj.py`
- When adding new measures or cubes, update both the code in `modules/mdx_queries.py` **and** the curated catalog in the skill's references.

This `AGENTS.md` + `.grok/` structure teaches both humans and Grok agents the current conventions and deep domain model for Papa John's pizza operations.
