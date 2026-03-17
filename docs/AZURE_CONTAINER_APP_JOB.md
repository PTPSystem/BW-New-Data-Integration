# Azure Container App Job - Setup Guide

This replaces the TrueNAS container with a fully managed, serverless scheduled job in Azure.

## Architecture

```
GitHub push to main
    └─► GitHub Actions (OIDC, no secrets)
            ├─► Build Docker image
            ├─► Push to Azure Container Registry (ACR)
            └─► Deploy Bicep → update Container App Job image

Azure Container App Job (Consumption, serverless)
    ├─► Runs on cron schedule (2x/day, configurable)
    ├─► Pulls image from ACR via Managed Identity
    └─► Reads Key Vault secrets via Managed Identity
            └─► Runs: python olap_to_dataverse.py --query all --length 1wk --email yes
```

**No credentials are stored in the job or in GitHub Secrets** (except non-sensitive IDs).

## Estimated Cost

| Resource | Cost |
|---|---|
| Container App Job (0.5 vCPU / 1 GiB, 15 min × 2/day × 30 days) | ~$0.80/month |
| ACR Basic | ~$5/month |
| Container App Environment | $0 (Consumption plan) |
| **Total** | **~$6/month** |

---

## One-Time Setup

### Prerequisites

- Azure CLI installed and logged in (`az login`)
- Sufficient permissions on `rg-bw-data-integration` (Owner or Contributor + User Access Administrator)
- GitHub repo: `PTPSystem/BW-New-Data-Integration`

---

### Step 1 – Deploy Infrastructure

Run this once from your local machine. It creates the ACR, Container App Environment, Container App Job, Managed Identity, and all role assignments.

```bash
cd /path/to/BW-New-Data-Integration

az deployment group create \
  --resource-group rg-bw-data-integration \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam
```

> **First deploy will fail on the image pull** because no image exists in ACR yet.
> That is expected — the GitHub Actions workflow pushes the first image in Step 4.

---

### Step 2 – Configure OIDC for GitHub Actions

GitHub Actions authenticates to Azure using **OpenID Connect (OIDC)** — no client secret is stored in GitHub.

You will configure the existing `ar-bw-data-integration` app registration to trust GitHub Actions.

**2a. Get the Object ID of the app registration**

```bash
az ad app show --id d056223e-f0de-4b16-b4e0-fec2a24109ff --query id -o tsv
# Copy the output (the Object ID, different from the Client ID)
```

**2b. Add a federated credential** (trusts pushes to `main`)

```bash
APP_OBJECT_ID="<paste-object-id-from-above>"

az ad app federated-credential create \
  --id $APP_OBJECT_ID \
  --parameters '{
    "name": "github-actions-main",
    "issuer": "https://token.actions.githubusercontent.com",
    "subject": "repo:PTPSystem/BW-New-Data-Integration:ref:refs/heads/main",
    "audiences": ["api://AzureADTokenExchange"]
  }'
```

> If you also want `workflow_dispatch` from any branch to work, add a second credential with `subject: "repo:PTPSystem/BW-New-Data-Integration:environment:Production"` or use `"repo:PTPSystem/BW-New-Data-Integration:*"`.

---

### Step 3 – Grant the App Registration Required Roles

The `ar-bw-data-integration` service principal needs two roles to deploy from GitHub Actions.

**3a. Get the Service Principal Object ID** (different from the App Object ID above)

```bash
SP_OBJECT_ID=$(az ad sp show --id d056223e-f0de-4b16-b4e0-fec2a24109ff --query id -o tsv)
echo $SP_OBJECT_ID
```

**3b. Get your Subscription ID**

```bash
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
echo $SUBSCRIPTION_ID
```

**3c. Grant Contributor on the resource group** (needed to deploy Bicep and update the job)

```bash
az role assignment create \
  --assignee-object-id $SP_OBJECT_ID \
  --assignee-principal-type ServicePrincipal \
  --role Contributor \
  --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/rg-bw-data-integration
```

**3d. Grant AcrPush on the ACR** (needed to push Docker images)

```bash
ACR_ID=$(az acr show --name acrbwdataintegration --resource-group rg-bw-data-integration --query id -o tsv)

az role assignment create \
  --assignee-object-id $SP_OBJECT_ID \
  --assignee-principal-type ServicePrincipal \
  --role AcrPush \
  --scope $ACR_ID
```

---

### Step 4 – Set GitHub Secrets

Go to: **GitHub → Repository → Settings → Secrets and variables → Actions → New repository secret**

| Secret Name | Value |
|---|---|
| `AZURE_CLIENT_ID` | `d056223e-f0de-4b16-b4e0-fec2a24109ff` (the app registration Client ID) |
| `AZURE_TENANT_ID` | `c8b6ba98-3fc0-4153-83a9-01374492c0f5` |
| `AZURE_SUBSCRIPTION_ID` | (your Azure subscription ID from Step 3b) |

> These are non-sensitive IDs — not secrets or passwords. OIDC means no `AZURE_CLIENT_SECRET` is ever stored.

---

### Step 5 – Trigger the First Deployment

Push any change to `main` (or trigger manually from the **Actions** tab → **Deploy to Azure Container App Job** → **Run workflow**).

The workflow will:
1. Build the Docker image
2. Push it to `acrbwdataintegration.azurecr.io/bw-data-integration:<sha>`
3. Deploy the Bicep template (this time the image exists, so everything succeeds)

---

### Step 6 – Verify

**Check that the job exists and is scheduled:**

```bash
az containerapp job show \
  --name job-bw-data-integration \
  --resource-group rg-bw-data-integration \
  --query "{name:name, state:properties.provisioningState, cron:properties.configuration.scheduleTriggerConfig.cronExpression}" \
  -o table
```

**Run the job manually right now** (to verify it works before the next scheduled run):

```bash
az containerapp job start \
  --name job-bw-data-integration \
  --resource-group rg-bw-data-integration
```

**Watch the execution logs:**

```bash
# List recent executions
az containerapp job execution list \
  --name job-bw-data-integration \
  --resource-group rg-bw-data-integration \
  --query "[].{name:name, status:properties.status, started:properties.startTime}" \
  -o table

# Stream logs from an execution (replace <execution-name> with the name from above)
az containerapp logs show \
  --name job-bw-data-integration \
  --resource-group rg-bw-data-integration \
  --type system \
  --follow
```

---

### Step 7 – Decommission TrueNAS Container

Once you've confirmed **at least two scheduled runs** completed successfully on Azure, disable/remove the TrueNAS cron job to avoid duplicate runs.

---

## Changing the Schedule

Edit [infra/main.bicepparam](../infra/main.bicepparam) and update `cronSchedule`. Then push to `main` — the workflow redeploys automatically.

```bicepparam
// 8:00 AM and 8:00 PM Central Standard Time (UTC-6) = 2:00 PM and 2:00 AM UTC
param cronSchedule = '0 14,2 * * *'
```

Or manually:

```bash
az containerapp job update \
  --name job-bw-data-integration \
  --resource-group rg-bw-data-integration \
  --cron-expression '0 14,2 * * *'
```

---

## Troubleshooting

### Job fails at startup ("Key Vault authentication failed")

The managed identity hasn't finished propagating. Wait 2-3 minutes after the first Bicep deploy and retry, or run:

```bash
# Verify the role assignment exists
az role assignment list \
  --scope $(az keyvault show --name kv-bw-data-integration --query id -o tsv) \
  --query "[?principalType=='ServicePrincipal'].{principal:principalName,role:roleDefinitionName}" \
  -o table
```

### Job fails to pull image ("unauthorized")

Verify the ACR pull role assignment for the managed identity:

```bash
az role assignment list \
  --scope $(az acr show --name acrbwdataintegration --query id -o tsv) \
  --query "[?principalType=='ServicePrincipal']" \
  -o table
```

### GitHub Actions fails with "AADSTS70021: No matching federated identity record found"

The OIDC federated credential subject must match exactly. If you're pushing from a branch other than `main`, add another federated credential or trigger via `workflow_dispatch` with the `main` branch.

### OLAP Server connectivity

The OLAP server (`ednacubes.papajohns.com:10502`) must be accessible from Azure Container Apps. If the OLAP server has IP allowlisting, you'll need to either:
- Request that the Azure Container Apps outbound IPs be allowed, **or**
- Use a Container App Environment with a VNet and a static outbound IP (adds cost)

To find the outbound IPs of your Container App Environment:

```bash
az containerapp env show \
  --name cae-bw-data-integration \
  --resource-group rg-bw-data-integration \
  --query properties.staticIp -o tsv
```
