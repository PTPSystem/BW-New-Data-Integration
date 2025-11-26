# Troubleshooting Guide - Beachwood Data Integration

Quick reference for common issues and their solutions.

## Table of Contents
- [Authentication Issues](#authentication-issues)
- [Connection Issues](#connection-issues)
- [Data Processing Issues](#data-processing-issues)
- [Container Issues](#container-issues)
- [Performance Issues](#performance-issues)

---

## Authentication Issues

### Azure Key Vault Access Denied

**Symptoms:**
```
Failed to retrieve secret from Key Vault: (Forbidden) Access denied
```

**Causes & Solutions:**

1. **Service principal lacks permissions**
   ```bash
   # Grant Key Vault Secrets User role
   az role assignment create \
     --role "Key Vault Secrets User" \
     --assignee <client-id> \
     --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/kv-bw-data-integration
   ```

2. **Wrong tenant/client ID**
   - Verify `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` environment variables
   - Check values match Azure AD app registration

3. **Expired client secret**
   - Generate new secret in Azure AD
   - Update `AZURE_CLIENT_SECRET` environment variable
   - Update secret in TrueNAS container configuration

**Test:**
```bash
docker exec beachwood-integration python modules/utils/keyvault.py
```

### Dataverse Authentication Failure

**Symptoms:**
```
Failed to obtain Dataverse access token: invalid_client
```

**Causes & Solutions:**

1. **Check credentials in Key Vault**
   ```bash
   az keyvault secret show --vault-name kv-bw-data-integration --name dataverse-client-id
   az keyvault secret show --vault-name kv-bw-data-integration --name dataverse-client-secret
   ```

2. **Verify app registration permissions**
   - Navigate to Azure AD > App Registrations
   - Check API permissions include Dataverse
   - Grant admin consent if needed

3. **Check environment URL**
   - Verify `dataverse-environment-url` in Key Vault
   - Should be: `https://orgbf93e3c3.crm.dynamics.com`

**Test:**
```bash
docker exec beachwood-integration python -c "
from modules.olap_to_dataverse import get_dataverse_access_token
from modules.utils.keyvault import get_dataverse_credentials
creds = get_dataverse_credentials()
token = get_dataverse_access_token(
    creds['dataverse-environment-url'],
    creds['dataverse-client-id'],
    creds['dataverse-client-secret'],
    creds['azure-tenant-id']
)
print('✓ Token obtained' if token else '✗ Failed')
"
```

---

## Connection Issues

### OLAP Server Connection Timeout

**Symptoms:**
```
Timeout waiting for OLAP server response
```

**Causes & Solutions:**

1. **Network connectivity**
   ```bash
   # Test from container
   docker exec beachwood-integration curl -k https://ednacubes.papajohns.com:10502
   ```

2. **Firewall blocking port 10502**
   - Check TrueNAS firewall rules
   - Verify outbound HTTPS on port 10502 is allowed

3. **OLAP server down or slow**
   - Contact Papa John's IT
   - Check OLAP server status
   - Increase timeout in config: `"timeout_seconds": 600`

4. **SSL certificate issues**
   - Verify `"ssl_verify": false` in config
   - Check certificate hasn't expired

**Test:**
```bash
docker exec beachwood-integration python -c "
from modules.olap_to_dataverse import execute_xmla_mdx
response = execute_xmla_mdx(
    'https://ednacubes.papajohns.com:10502',
    'OARS Franchise',
    'username',
    'password',
    'SELECT * FROM [OARS Franchise]',
    ssl_verify=False
)
print(f'Status: {response.status_code}')
"
```

### SharePoint API Not Accessible

**Symptoms:**
```
Failed to connect to SharePoint: Connection refused
```

**Causes & Solutions:**

1. **Network connectivity**
   ```bash
   docker exec beachwood-integration curl https://graph.microsoft.com/v1.0/me
   ```

2. **SharePoint credentials incorrect**
   - Verify secrets in Key Vault:
     - `sharepoint-client-id`
     - `sharepoint-client-secret`
     - `sharepoint-site-url`

3. **API permissions missing**
   - Check app registration has `Files.ReadWrite.All`
   - Grant admin consent

**Test:**
```bash
docker exec beachwood-integration python -c "
from modules.utils.sharepoint import test_connection
test_connection()
"
```

---

## Data Processing Issues

### OLAP Query Returns No Data

**Symptoms:**
- Empty DataFrame after parsing XMLA response
- "No data returned from OLAP query"

**Causes & Solutions:**

1. **MDX query syntax error**
   - Review query in MDX_QUERY_REFERENCE.md
   - Test query in SQL Server Management Studio
   - Check cube name and dimension names

2. **Date range issue**
   - Verify fiscal year filter
   - Check cube has data for requested period
   - Update WHERE clause in MDX query

3. **Parsing error**
   - Check XMLA response format
   - Update `parse_xmla_mdx_response()` function
   - Add debug logging to see raw XML

**Debug:**
```python
# Add to olap_to_dataverse.py
print(response.text[:1000])  # Print first 1000 chars of XML
```

### Dataverse Upsert Failures

**Symptoms:**
```
Failed to upsert record: 400 Bad Request
```

**Causes & Solutions:**

1. **Invalid field values**
   - Check data types match Dataverse schema
   - Verify required fields are populated
   - Check for null/empty values

2. **Field name mismatch**
   - Verify column names: `crf63_fieldname`
   - Check schema in Dataverse portal
   - Update field mapping in code

3. **Record doesn't exist (for updates)**
   - Check filter query logic
   - Verify unique key fields
   - Use create instead of update

**Test:**
```bash
# Check table exists
curl -X GET \
  "https://orgbf93e3c3.crm.dynamics.com/api/data/v9.2/crf63_olapbidatas" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/json"
```

### Labor Processing File Download Failures

**Symptoms:**
- "Failed to download file from FTP"
- Empty ZIP files

**Causes & Solutions:**

1. **FTP credentials expired**
   - Check `labor-processing-username` in Key Vault
   - Check `labor-processing-password` in Key Vault
   - Contact Papa John's to reset password

2. **Date format issue**
   - Verify `config_date` format: YYYYMMDD
   - Check files exist for that date
   - Try latest date: set `config_date` to empty string

3. **Network issue**
   ```bash
   docker exec beachwood-integration curl https://files.papajohns.com/
   ```

---

## Container Issues

### Container Won't Start

**Symptoms:**
- Container exits immediately
- Status: Exited (1)

**Causes & Solutions:**

1. **Check logs**
   ```bash
   docker logs beachwood-integration
   ```

2. **Missing environment variables**
   ```bash
   # Verify required vars are set
   docker inspect beachwood-integration | grep -A 20 "Env"
   ```
   Required:
   - `AZURE_TENANT_ID`
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`

3. **Python dependency issue**
   ```bash
   # Rebuild container
   docker build --no-cache -t beachwood-integration:latest .
   ```

4. **Entrypoint script error**
   ```bash
   # Test entrypoint manually
   docker run --rm -it --entrypoint /bin/bash beachwood-integration:latest
   /app/docker/entrypoint.sh python --version
   ```

### Container Crashes During Execution

**Symptoms:**
- Container runs for a while then exits
- Out of memory errors in logs

**Causes & Solutions:**

1. **Memory limit too low**
   ```bash
   # Increase memory limit
   docker update --memory 4g beachwood-integration
   ```

2. **Memory leak**
   - Check for unclosed file handles
   - Review DataFrame operations
   - Add explicit garbage collection:
     ```python
     import gc
     gc.collect()
     ```

3. **Unhandled exception**
   - Review stack trace in logs
   - Add try/except blocks
   - Improve error handling

**Monitor:**
```bash
# Watch memory usage
docker stats beachwood-integration
```

### Logs Not Appearing

**Symptoms:**
- `docker logs` shows nothing
- Can't debug issues

**Causes & Solutions:**

1. **Logging not configured**
   - Verify `PYTHONUNBUFFERED=1` is set
   - Check log level: `LOG_LEVEL=DEBUG`

2. **Logs going to file instead of stdout**
   - Update logging configuration
   - Ensure logs write to stdout/stderr

3. **Container not running**
   ```bash
   docker ps -a | grep beachwood
   ```

---

## Performance Issues

### Execution Takes Too Long

**Symptoms:**
- Job runs for > 1 hour
- Timeout errors

**Causes & Solutions:**

1. **OLAP query too slow**
   - Optimize MDX query
   - Reduce date range
   - Add WHERE filters
   - Contact DBA about cube performance

2. **Dataverse upsert too slow**
   - Batch records (100 at a time)
   - Use parallel upserts (be careful!)
   - Check Dataverse API throttling

3. **Network latency**
   - Check TrueNAS internet connection
   - Monitor API response times
   - Consider caching if appropriate

**Profile:**
```python
import time

start = time.time()
# ... code block ...
print(f"Duration: {time.time() - start:.2f}s")
```

### High Memory Usage

**Symptoms:**
- Container using > 2GB RAM
- OOM (out of memory) kills

**Causes & Solutions:**

1. **Large DataFrames**
   - Process data in chunks
   - Use iterators instead of loading all data
   - Delete unused DataFrames explicitly

2. **Memory leaks**
   - Check for unclosed connections
   - Review object lifecycles
   - Use `del` to free memory

**Example:**
```python
# Process in chunks
chunk_size = 1000
for i in range(0, len(df), chunk_size):
    chunk = df[i:i+chunk_size]
    process_chunk(chunk)
    del chunk
    gc.collect()
```

---

## General Debugging

### Enable Debug Logging

**Temporary:**
```bash
docker exec beachwood-integration env LOG_LEVEL=DEBUG python main.py
```

**Permanent:**
Update environment variable in TrueNAS container config:
```
LOG_LEVEL=DEBUG
```

### Interactive Shell

```bash
# Access container shell
docker exec -it beachwood-integration /bin/bash

# Run Python interactively
python

# Test specific module
python -c "from modules.utils.keyvault import get_secret; print(get_secret('test'))"
```

### Manual Execution

```bash
# Run full process manually
docker exec beachwood-integration python main.py

# Run specific module
docker exec beachwood-integration python -m modules.processing.labor_processing
```

### Check Configuration

```bash
# View loaded config
docker exec beachwood-integration python -c "
from modules.utils.config import load_config
import json
config = load_config()
print(json.dumps(config, indent=2))
"
```

---

## Getting Help

If issues persist:

1. **Collect Information**
   - Full error message and stack trace
   - Container logs: `docker logs beachwood-integration > logs.txt`
   - Container config: `docker inspect beachwood-integration > inspect.json`
   - Environment: `echo $ENVIRONMENT`

2. **Check Documentation**
   - README.md
   - Migrate-to-Truenas.md
   - DEPLOYMENT.md

3. **Contact Support**
   - Email: howard@ptpsystem.com
   - Include logs and error details
   - Describe what was attempted

---

**Last Updated**: 2025-11-19  
**Version**: 1.0.0
