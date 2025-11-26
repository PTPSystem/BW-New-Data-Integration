# NewIntegration - OLAP to Dataverse Migration

This directory contains the implementation of Step 2 of the OLAP to Dataverse migration checklist.

## Overview

The NewIntegration module provides a clean, tested implementation of syncing data from the OARS Franchise OLAP cube to Microsoft Dataverse, replacing the legacy Excel COM automation approach.

## Features

- ✅ Direct XMLA/MDX queries to OLAP cube (no Excel needed)
- ✅ Azure Key Vault integration for secure credential management
- ✅ Comprehensive testing framework
- ✅ Support for all 33 OLAP measures
- ✅ Automatic data transformation and Dataverse upsert
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
4. Upsert records to Dataverse table `crf63_olapbidatas`

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

## Manual Verification Steps

After running the full sync, verify in Dataverse:

1. **Open Dataverse Portal**:
   - Navigate to https://orgbf93e3c3.crm.dynamics.com
   - Go to Tables → `crf63_olapbidatas`

2. **Verify Data Present**:
   - Check that records exist
   - Verify date range (should be FY2025)
   - Check store coverage

3. **Verify All 33 Measures**:
   - Open a few sample records
   - Confirm all measure columns have values
   - Check for any null/empty columns

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

### Dataverse Authentication Failed

```
Error: AADSTS70011: The provided value for the input parameter 'scope' is not valid
```

**Solution**: Verify client ID and tenant ID in Key Vault are correct.

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
         │ Web API
         │ (HTTPS)
         ▼
┌──────────────────┐
│   Dataverse      │
│ crf63_olapbidatas│
│     Table        │
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
