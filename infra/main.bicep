// =============================================================================
// BW Data Integration - Azure Container App Job
// =============================================================================
// Provisions all resources needed to run olap_to_dataverse.py on a schedule
// via Azure Container Apps Jobs (Consumption plan - serverless, pay-per-use).
//
// Resources created:
//   - User-assigned Managed Identity (no stored secrets in the job)
//   - Azure Container Registry (Basic SKU ~$5/month)
//   - Log Analytics Workspace (for job log streaming)
//   - Container App Environment (Consumption workload profile)
//   - Container App Job (scheduled, 2x/day)
//   - Role assignments (AcrPull + Key Vault Secrets User)
//
// Resources referenced (must already exist):
//   - Resource group: rg-bw-data-integration
//   - Key Vault: kv-bw-data-integration
// =============================================================================

@description('Azure region. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Docker image tag to deploy. Set to a specific git SHA by GitHub Actions.')
param imageTag string = 'latest'

@description('Cron schedule (UTC). Default: 6:00 AM and 6:00 PM UTC every day.')
param cronSchedule string = '0 6,18 * * *'

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------
var acrName = 'acrbwdataintegration'        // Globally unique; lowercase alphanumeric
var environmentName = 'cae-bw-data-integration'
var jobName = 'job-bw-data-integration'
var identityName = 'id-bw-data-integration'
var keyVaultName = 'kv-bw-data-integration' // Already exists in this resource group
var logAnalyticsName = 'log-bw-data-integration'

// ---------------------------------------------------------------------------
// Reference existing Key Vault
// ---------------------------------------------------------------------------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

// ---------------------------------------------------------------------------
// User-Assigned Managed Identity
// The job authenticates to ACR and Key Vault through this identity.
// No credentials are stored anywhere in the job or GitHub Actions.
// ---------------------------------------------------------------------------
resource managedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

// ---------------------------------------------------------------------------
// Azure Container Registry (Basic SKU ~$5/month)
// Stores the Docker image built by GitHub Actions.
// ---------------------------------------------------------------------------
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false // Use managed identity instead of admin credentials
  }
}

// Role: AcrPull → managed identity on ACR
// Allows the Container App Job to pull the image without any stored credentials.
resource acrPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, managedIdentity.id, 'acrpull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d') // AcrPull
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Role: Key Vault Secrets User → managed identity on existing Key Vault
// Allows olap_to_dataverse.py to read all secrets via DefaultAzureCredential.
resource kvSecretsUserRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, managedIdentity.id, 'kvsecretsuser')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6') // Key Vault Secrets User
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------------------------------------------------------------------------
// Log Analytics Workspace
// Required for log streaming from az CLI and Azure Portal.
// Pay-per-use: ~$0 at this log volume (first 5 GB/month free).
// ---------------------------------------------------------------------------
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ---------------------------------------------------------------------------
// Container App Environment (Consumption / Serverless)
// No baseline cost - you only pay when jobs are actually executing.
// ---------------------------------------------------------------------------
resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Container App Job
//
// Trigger:   Schedule (cron, UTC)
// Command:   entrypoint.sh → python olap_to_dataverse.py --query all --length 1wk --email yes
// Auth:      User-assigned managed identity (no secrets)
// Resources: 0.5 vCPU / 1 GiB RAM (matches your ~15 min runtime comfortably)
// Timeout:   30 min (safety margin above the ~15 min runtime)
// ---------------------------------------------------------------------------
resource job 'Microsoft.App/jobs@2024-03-01' = {
  name: jobName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentity.id}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: {
      triggerType: 'Schedule'
      replicaTimeout: 2700       // 45 minutes
      replicaRetryLimit: 1       // Retry once on failure
      scheduleTriggerConfig: {
        cronExpression: cronSchedule
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: acr.properties.loginServer
          identity: managedIdentity.id  // Pull image via managed identity - no password stored
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'bw-data-integration'
          image: '${acr.properties.loginServer}/bw-data-integration:${imageTag}'
          // 'command' is intentionally omitted - the Dockerfile ENTRYPOINT (entrypoint.sh) is used.
          // 'args' overrides the Dockerfile CMD, passing the correct script and flags.
          args: [
            'python'
            'olap_to_dataverse.py'
            '--query'
            'all'
            '--length'
            '1wk'
            '--email'
            'yes'
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            {
              // Required so DefaultAzureCredential picks the right managed identity
              // when multiple identities could be present on the host.
              name: 'AZURE_CLIENT_ID'
              value: managedIdentity.properties.clientId
            }
            {
              name: 'ENVIRONMENT'
              value: 'production'
            }
            {
              name: 'LOG_LEVEL'
              value: 'INFO'
            }
          ]
        }
      ]
    }
  }
  dependsOn: [
    acrPullRole
    kvSecretsUserRole
  ]
}

// ---------------------------------------------------------------------------
// Outputs  (referenced by GitHub Actions workflow)
// ---------------------------------------------------------------------------
output acrLoginServer string = acr.properties.loginServer
output acrName string = acr.name
output jobName string = job.name
output managedIdentityClientId string = managedIdentity.properties.clientId
