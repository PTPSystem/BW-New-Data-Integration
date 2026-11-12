# AtScale API Token

Papa John's moved OARS off the legacy XMLA host (`ednacubes.papajohns.com:10502`) to the new AtScale Design Center. Automated queries use the **Account API Token** against the public query API (SQL), not Basic auth MDX.

## Key Vault

| Secret | Vault | Purpose |
|--------|-------|---------|
| `atscale-api-token` | `kv-bw-data-integration` | Bearer token for `https://atscale.papajohns.com/v1/query` |

Local access: `az login`, then load via the usual Key Vault helpers (same pattern as `olap-username` / `olap-password`).

## Where to get / rotate the token

1. Open [AtScale Design Center](https://atscale.papajohns.com/design-center/).
2. Sign in (username/password or **AtScale SSO**; complete MFA if prompted).
3. Click your **profile / account** icon (top right).
4. In the **Account** panel, find **API Token**.
5. Use **refresh / generate** if you need a new token, then **copy**.
6. Update Key Vault:
   ```bash
   az keyvault secret set \
     --vault-name kv-bw-data-integration \
     --name atscale-api-token \
     --value '<paste-token-here>'
   ```
7. Revoke the old token in the same Account panel when rotating.

This is the **API Token** section (public API). It is **not** the Git token field, and it is not an XMLA “MDX + Token” string.

## How to use it (smoke test)

```bash
# token from Key Vault — do not print it
TOKEN=$(az keyvault secret show \
  --vault-name kv-bw-data-integration \
  --name atscale-api-token \
  --query value -o tsv)

curl -sS -X POST 'https://atscale.papajohns.com/v1/query' \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "language": "SQL",
    "query": "SELECT SUM(`TY Net Sales USD`) FROM OARS",
    "fakeResults": false,
    "dryRun": false,
    "useLocalCache": true,
    "useAggregateCache": true,
    "timeout": "60s",
    "context": {
      "environment": { "id": "default" },
      "project": { "name": "OARS" },
      "aggregation": { "useAggregates": true, "genAggregates": false }
    }
  }'
```

Also useful:

- `GET https://atscale.papajohns.com/v1/catalogs` — list deployed catalogs/models  
- Prefer explicit columns/measures; `SELECT * FROM OARS` often fails planning

## Related endpoints (for reference)

| Kind | Endpoint | Notes |
|------|----------|-------|
| Public SQL API | `https://atscale.papajohns.com/v1/query` | Use with `atscale-api-token` |
| Catalogs | `https://atscale.papajohns.com/v1/catalogs` | Metadata |
| XMLA (MDX) | `https://atscale.papajohns.com/engine/xmla` | Needs different auth; API token does **not** work here |
| SQL/ODBC wire | `atscale-tds.papajohns.com:15432` | Often firewalled; not required if using `/v1/query` |

## Notes

- Token does not expire until revoked/regenerated in Design Center.
- Warehouse behind OARS is BigQuery connection `pj-prod-gbq`.
- Legacy secrets `olap-username` / `olap-password` were for `ednacubes` XMLA and are obsolete once that host is shut down.
