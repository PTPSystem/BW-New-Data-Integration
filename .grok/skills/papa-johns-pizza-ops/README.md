# Papa John's Pizza Ops Skill (Current Repo)

This skill + references turns the Papa John's OARS semantic model into agent-usable knowledge inside `BW-New-Data-Integration` (the active repository).

## Folder Structure
```
.grok/skills/papa-johns-pizza-ops/
├── SKILL.md
├── README.md
├── references/          # Curated business + technical knowledge (loaded by Grok)
└── scripts/
    ├── query_ppj.py              # Layer A — OARS cube (MDX)
    ├── query_beachwood_daily.py  # Layer B — Beachwood Daily (DAX)
    └── refresh_knowledge.py
```

## Important Notes for This Repo
- Key Vault is now `kv-bw-data-integration`
- OLAP secrets: `olap-username` and `olap-password`
- Layer A: `modules/olap.py` + `modules/mdx_queries.py` + `query_ppj.py`
- Layer B: `modules/powerbi.py` + `modules/powerbi_queries.py` + `query_beachwood_daily.py`
- Beachwood Daily workspace `ba0545ee-6dee-4757-b5c2-c5946cd9e320`, dataset `6fd26600-b245-404f-86e4-5841e1c88e9c`
- Service principal `ar-bw-data-integration` must be **Member** on the shared workspace

## Usage
Ask normal Papa John's ops questions while the repo is in context. The skill should activate.

Explicit: `/papa-johns-pizza-ops`

For specialist mode: load the `papa-pizza-ops-analyst` agent.

Run queries directly with the helper script in `scripts/`.

## Maintenance
After cube changes or when new measures appear, update the references (especially the measures catalog) and re-run the refresh script.
