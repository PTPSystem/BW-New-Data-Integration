# Azure Key Vault Secrets Reference

## Key Vault Information

- **Name**: `kv-bw-data-integration`
- **URL**: `https://kv-bw-data-integration.vault.azure.net/`
- **Resource Group**: `rg-bw-data-integration`
- **App Registration**: `ar-bw-data-integration`
- **App Client ID**: `d056223e-f0de-4b16-b4e0-fec2a24109ff`
- **Tenant ID**: `c8b6ba98-3fc0-4153-83a9-01374492c0f5`

## Complete Secret List (10 Secrets)

### 1. Azure Authentication Secrets

| Secret Name | Purpose | Used By |
|-------------|---------|---------|
| `azure-tenant-id` | Azure AD tenant ID | All modules requiring Azure authentication |
| `app-client-id` | Unified app registration client ID | Dataverse, SharePoint, Microsoft Graph |
| `app-client-secret` | Unified app registration secret | Dataverse, SharePoint, Microsoft Graph |

### 2. Dataverse Secrets

| Secret Name | Purpose | Used By |
|-------------|---------|---------|
| `dataverse-client-id` | Dataverse environment identifier | `olap_to_dataverse.py`, `labor_forecast.py` |

**Note**: Dataverse uses the same `app-client-id` and `app-client-secret` as SharePoint since they share the same app registration.

### 3. SharePoint Secrets

| Secret Name | Purpose | Used By |
|-------------|---------|---------|
| `sharepoint-site-url` | SharePoint site URL | `powerbi_update.py`, `doordash_update.py`, `labor_processing.py` |

**Note**: SharePoint uses the same `app-client-id` and `app-client-secret` as Dataverse since they share the same app registration.

### 4. OLAP Server Secrets

| Secret Name | Purpose | Used By |
|-------------|---------|---------|
| `olap-username` | OLAP server authentication username | `olap_to_dataverse.py` |
| `olap-password` | OLAP server authentication password | `olap_to_dataverse.py` |

### 5. Labor Processing Secrets

| Secret Name | Purpose | Used By |
|-------------|---------|---------|
| `files-url` | Labor files download URL (files.papajohns.com) | `labor_processing.py` |
| `files-username` | Labor files portal username | `labor_processing.py` |
| `files-password` | Labor files portal password | `labor_processing.py` |

## API Permissions Required

The app registration (`ar-bw-data-integration`) requires the following API permissions:

### Microsoft Graph
- `Mail.Send` (Delegated) - Send email notifications
- `Files.ReadWrite.All` (Delegated or Application) - Access SharePoint files

### Dynamics CRM (Dataverse)
- `user_impersonation` (Delegated) - Full access to Dataverse tables

### Azure Key Vault (RBAC)
- Role: `Key Vault Secrets User`
- Scope: Key Vault `kv-bw-data-integration`

## Access Configuration

### For Local Development (Azure CLI)

```bash
# Login to Azure
az login

# Verify Key Vault access
az keyvault secret list --vault-name kv-bw-data-integration
```

### For Service Principal (Production/Container)

Set environment variables:
```bash
export AZURE_TENANT_ID="c8b6ba98-3fc0-4153-83a9-01374492c0f5"
export AZURE_CLIENT_ID="d056223e-f0de-4b16-b4e0-fec2a24109ff"
export AZURE_CLIENT_SECRET="<your-secret>"
```

### Grant Key Vault Access

```bash
# For service principal
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee d056223e-f0de-4b16-b4e0-fec2a24109ff \
  --scope /subscriptions/<subscription-id>/resourceGroups/rg-bw-data-integration/providers/Microsoft.KeyVault/vaults/kv-bw-data-integration

# For user account
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee howard@ptpsystem.com \
  --scope /subscriptions/<subscription-id>/resourceGroups/rg-bw-data-integration/providers/Microsoft.KeyVault/vaults/kv-bw-data-integration
```

## Testing Key Vault Access

### Test All Secrets

```bash
cd /path/to/Beachwood-Data-Integration
python modules/utils/keyvault.py
```

### Test Individual Secret Retrieval

```python
from modules.utils.keyvault import get_secret

# Test OLAP credentials
olap_username = get_secret('olap-username')
olap_password = get_secret('olap-password')

# Test app registration
client_id = get_secret('app-client-id')
client_secret = get_secret('app-client-secret')
tenant_id = get_secret('azure-tenant-id')

# Test Dataverse
dataverse_client_id = get_secret('dataverse-client-id')

# Test SharePoint
sharepoint_site_url = get_secret('sharepoint-site-url')

# Test labor processing
files_url = get_secret('files-url')
files_username = get_secret('files-username')
files_password = get_secret('files-password')
```

## Secret Naming Convention

**Current Naming**: Unified app registration approach
- Single `app-client-id` and `app-client-secret` used for both Dataverse and SharePoint
- Service-specific secrets use service prefix (`olap-`, `files-`, `sharepoint-`, `dataverse-`)
- Azure common secrets use `azure-` prefix

## Troubleshooting

### Access Denied Error

```
Failed to retrieve secret from Key Vault: (Forbidden) Access denied
```

**Solution**:
1. Verify you're logged in: `az account show`
2. Check RBAC role assignment: `az role assignment list --scope /subscriptions/<sub-id>/resourceGroups/rg-bw-data-integration/providers/Microsoft.KeyVault/vaults/kv-bw-data-integration`
3. Ensure "Key Vault Secrets User" role is assigned to your identity

### Wrong Tenant Error

```
Failed to obtain access token: invalid_client
```

**Solution**:
1. Verify `AZURE_TENANT_ID` matches `c8b6ba98-3fc0-4153-83a9-01374492c0f5`
2. Verify `AZURE_CLIENT_ID` matches `d056223e-f0de-4b16-b4e0-fec2a24109ff`
3. Check client secret hasn't expired in Azure Portal

### Secret Not Found

```
Failed to retrieve secret 'secret-name' from Key Vault: (NotFound)
```

**Solution**:
1. List all secrets: `az keyvault secret list --vault-name kv-bw-data-integration`
2. Verify secret name matches exactly (case-sensitive, use hyphens not underscores)
3. Check secret is enabled and not expired

## Migration Notes

### Changes from Old Configuration

**Old Key Vault**: `sf-kv-6338`
**New Key Vault**: `kv-bw-data-integration`

**Old Secrets** (separate app registrations):
- `sharepoint-client-id` (separate)
- `sharepoint-client-secret` (separate)
- `dataverse-client-id` (separate)
- `dataverse-client-secret` (separate)

**New Secrets** (unified app registration):
- `app-client-id` (shared for both SharePoint and Dataverse)
- `app-client-secret` (shared for both SharePoint and Dataverse)
- `dataverse-client-id` (environment identifier only)

### Benefits of Unified Approach

1. **Simplified Management**: Single app registration for multiple services
2. **Easier Permissions**: Grant API permissions in one place
3. **Reduced Secrets**: Fewer secrets to manage and rotate
4. **Better Security**: Centralized access control and auditing

## Additional Resources

- [Azure Key Vault Documentation](https://docs.microsoft.com/en-us/azure/key-vault/)
- [Azure RBAC for Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/general/rbac-guide)
- [App Registration Best Practices](https://docs.microsoft.com/en-us/azure/active-directory/develop/security-best-practices-for-app-registration)
