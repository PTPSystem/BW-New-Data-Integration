# NewIntegration - OLAP to Dataverse Migration

This directory contains the implementation of Step 2 of the OLAP to Dataverse migration checklist.

## Overview

The NewIntegration module syncs data from the OARS Franchise OLAP cube into Neon Postgres. Record shaping is unchanged from the Dataverse load: table names and column names are the Dataverse logical names (`crf63_*`). Only the write target moved.

## Features

- ✅ Direct XMLA/MDX queries to OLAP cube (no Excel needed)
- ✅ Azure Key Vault integration for secure credential management
- ✅ Comprehensive testing framework
- ✅ Support for all 33 OLAP measures
- ✅ Automatic data transformation and Neon Postgres upsert (`DATABASE_URL`)
- ✅ Cross-platform compatible (Windows, Linux, macOS)

## Directory Structure

```
NewIntegration/
├── olap_to_dataverse.py       # Main sync script
├── test_step2_checklist.py    # Step 2 validation tests
├── modules/
│   └── utils/
│       ├── keyvault.py         # Azure Key Vault utilities
│       └── __init__.py
├── config/                     # Configuration files (optional)
├── .env.example                # Environment variable template
└── README.md                   # This file
```

## Prerequisites

1. **Python 3.8+** with required packages:
   ```bash
   pip install requests msal python-dotenv azure-keyvault-secrets azure-identity pandas urllib3 lxml
   ```

2. **Azure CLI** (for Key Vault authentication):
   ```bash
   az login
   ```

3. **Access to Azure Key Vault** `kv-bw-data-integration`:
   - azure-tenant-id
   - dataverse-client-id
   - dataverse-client-secret
   - dataverse-environment-url

4. **OLAP Credentials**:
   - Set `OLAP_USERNAME` and `OLAP_PASSWORD` environment variables
   - Or store `olap-password` in Key Vault

5. **Neon Postgres** (write target):
   - A dedicated Neon project is created separately. This repo does not contain its id or password.
   - Set `DATABASE_URL` to a direct connection string. `sslmode=require` is applied when the URL does not already require SSL.
   - Placeholder: `postgresql://<role>:<password>@<endpoint>.<region>.aws.neon.tech/<database>?sslmode=require`
   - The loader creates the destination tables on first write (`python -m modules.neon_sink --print-ddl` prints the DDL). The Neon role needs permission to create tables, or a human can apply that DDL first.

## Quick Start

### 1. Set up environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Authenticate to Azure

```bash
az login
```

### 3. Run Step 2 Checklist Tests

This validates all checklist items:

```bash
cd NewIntegration
python test_step2_checklist.py
```

The test script will verify:
- ✓ Modules copied to NewIntegration
- ✓ Key Vault references updated
- ✓ OLAP connection test successful
- ✓ Dataverse authentication successful
- ✓ Single query test returns data
- ✓ Full sync completes without errors (placeholder)
- ⏸ Data visible in Dataverse portal (manual)
- ⏸ All 33 measures populated (manual)

### 4. Run Full Sync

Once tests pass, execute the full OLAP to Dataverse sync:

```bash
python olap_to_dataverse.py
```

This will:
1. Connect to OLAP cube
2. Execute MDX query with all 33 measures
3. Parse and transform the data
4. Upsert records to Neon table `crf63_oarsbidatas` (and the other pipeline tables when `--query all`)

## The 33 Measures

The sync captures these measures from the OARS Franchise cube:

### Sales Metrics (4)
1. TY Net Sales USD
2. L2Y Comp Net Sales USD
3. L3Y Comp Net Sales USD
4. LY Comp Net Sales USD

### Cost Metrics (7)
5. TY Target Food Cost USD
6. Actual Food Cost USD
7. FLMD USD
8. Target Profit after FLM Local (Fran)
9. Actual FLM w/o Vacation Accrual Local
10. Actual Labor $ USD
11. FLMDPC USD (Fran)

### Operations Metrics (8)
12. HS Total Actual Hours
13. Store Days
14. Make Time Minutes
15. Rack Time Minutes
16. Total OTD Time (Hours)
17. Avg TTDT
18. Mileage Cost Local
19. Total Cash Over/Short USD

### Order Metrics (8)
20. TY Orders
21. LY Orders
22. Deliveries
23. BOZOCORO Orders
24. OTD Order Count
25. TY Dispatched Delivery Orders
26. TY Total Order Accuracy Survey Count
27. Order Accuracy %
28. Discounts USD

### Customer Satisfaction (3)
29. TY Total OSAT Survey Count
30. TY OSAT Satisfied Survey Count
31. Total Calls
32. Answered Calls

### Financial (1)
33. m_ty_agg_commission_local_sum

## Historical backfill (Dataverse → Neon)

One-time copy of every table the ETL writes. It reuses Key Vault Dataverse auth, pages with `@odata.nextLink`, and upserts into Neon on the same primary keys. Re-running is safe. It does not extract from OLAP.

```bash
# Count Dataverse rows. Does not open DATABASE_URL.
python scripts/backfill_dataverse_to_neon.py --dry-run

# Upsert history into Neon. Requires DATABASE_URL.
python scripts/backfill_dataverse_to_neon.py

# Empty each Neon table, then reload.
python scripts/backfill_dataverse_to_neon.py --truncate --batch-size 1000
```

Tables: `crf63_oarsbidatas`, `crf63_saleschanneldailies`, `crf63_offerses`, `crf63_inventories`, `crf63_bw_clockinouts`, `crf63_labortimes`, `crf63_bwsalesforecasts`.

The script logs fetched / created / updated / error counts per table. `--dry-run` still reads Dataverse. Do not run it until the Neon project exists and `DATABASE_URL` is set for a real load.

## Manual Verification Steps

After running the full sync, query Neon (do not commit the connection string):

```sql
SELECT COUNT(*) FROM crf63_oarsbidatas;
SELECT crf63_businesskey, crf63_storenumber, crf63_calendardate, crf63_tynetsales
FROM crf63_oarsbidatas
ORDER BY crf63_calendardate DESC
LIMIT 20;
```

## Troubleshooting

### Key Vault Access Denied

```
Error: User does not have secrets get permission
```

**Solution**: Grant yourself "Key Vault Secrets User" role:
```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "your-email@company.com" \
  --scope "/subscriptions/YOUR_SUB/resourceGroups/YOUR_RG/providers/Microsoft.KeyVault/vaults/kv-bw-data-integration"
```

### OLAP Connection Failed

```
Error: 401 Unauthorized
```

**Solution**: Verify OLAP credentials:
- Check `OLAP_USERNAME` environment variable
- Check `OLAP_PASSWORD` environment variable or Key Vault secret

### Neon connection failed

```
RuntimeError: DATABASE_URL is not set
```

**Solution**: Export `DATABASE_URL` with `sslmode=require`. Use the direct host for the batch load. The password stays out of git.

The historical backfill and the sales-forecast source reads still authenticate to Dataverse through Key Vault. A Dataverse token error on those paths means `app-client-id`, `app-client-secret`, or `azure-tenant-id` is missing or wrong.

### No Data Returned from Query

```
Query returned 0 rows
```

**Solution**: 
- Verify fiscal year in MDX query matches data availability
- Check MDX query syntax
- Ensure OLAP cube contains data for FY2025

## Architecture

```
┌──────────────────┐
│  OARS Franchise  │
│   OLAP Cube      │
│  (Papa John's)   │
└────────┬─────────┘
         │ XMLA/MDX
         │ (HTTPS)
         ▼
┌──────────────────┐      ┌───────────────────────┐
│ olap_to_dataverse│◄────►│  Azure Key Vault      │
│      .py         │      │ kv-bw-data-integration│
└────────┬─────────┘      └───────────────────────┘
         │ Postgres (DATABASE_URL, sslmode=require)
         ▼
┌──────────────────┐
│  Neon Postgres   │
│ crf63_oarsbidatas│
│  and sibling     │
│  ETL tables      │
└──────────────────┘
```

## Benefits Over Legacy Approach

| Aspect | Legacy (Excel COM) | New (OLAP Direct) |
|--------|-------------------|-------------------|
| Platform | Windows only | Cross-platform |
| Dependencies | Excel license | None |
| Automation | Difficult | Easy |
| Containerization | Impossible | ✅ Docker/K8s ready |
| Performance | Slow | Fast |
| Reliability | Brittle | Robust |
| Secret Management | Local keyring | Azure Key Vault |

## Next Steps

After completing Step 2:

1. **Update Main Integration**: 
   - Replace `powerbi_update.py` Excel COM calls
   - Use `NewIntegration/olap_to_dataverse.py` instead

2. **Schedule Regular Syncs**:
   - Add to cron job (Linux/Mac)
   - Windows Task Scheduler
   - Kubernetes CronJob

3. **Add Monitoring**:
   - Log sync results
   - Alert on failures
   - Track data quality metrics

4. **Containerize**:
   - Create Dockerfile
   - Deploy to TrueNAS or Kubernetes
   - Use managed identity for Key Vault auth

## Support

For issues or questions, refer to:
- Main repository: [Beachwood-Data-Integration](../)
- OLAP Guide: [../OLAP_TO_DATAVERSE_GUIDE.md](../OLAP_TO_DATAVERSE_GUIDE.md)
- Key Vault Migration: [../KEYVAULT_MIGRATION.md](../KEYVAULT_MIGRATION.md)

---

**Status**: Step 2 Implementation Complete ✓
**Last Updated**: 2025-11-20
