# Papa John's Pizza Ops Skill (Current Repo)

This skill + references turns the Papa John's OARS semantic model into agent-usable knowledge inside `BW-New-Data-Integration` (the active repository).

## Folder Structure
```
.grok/skills/papa-johns-pizza-ops/
├── SKILL.md
├── README.md
├── references/          # Curated business + technical knowledge (loaded by Grok)
└── scripts/
    ├── query_ppj.py     # Main helper the agent uses to hit the live cube
    └── refresh_knowledge.py
```

## Important Notes for This Repo
- Key Vault is now `kv-bw-data-integration`
- OLAP secrets: `olap-username` and `olap-password`
- Use modern modules: `modules/olap.py` + `modules/mdx_queries.py`
- Prefer MyView-based incremental queries for recent data

## Usage
Ask normal Papa John's ops questions while the repo is in context. The skill should activate.

Explicit: `/papa-johns-pizza-ops`

For specialist mode: load the `papa-pizza-ops-analyst` agent.

Run queries directly with the helper script in `scripts/`.

## Maintenance
After cube changes or when new measures appear, update the references (especially the measures catalog) and re-run the refresh script.
