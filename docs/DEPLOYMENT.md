# Deployment Guide - Beachwood Data Integration

This guide covers deploying the containerized Beachwood Data Integration system to TrueNAS.

## Prerequisites

- TrueNAS server with container support (Docker or Kubernetes)
- Container registry access (Docker Hub, Azure Container Registry, etc.)
- Azure service principal with Key Vault access
- Network access from TrueNAS to:
  - Azure Key Vault (vault.azure.net)
  - Microsoft Graph API (graph.microsoft.com)
  - Dataverse (crm.dynamics.com)
  - OLAP Server (ednacubes.papajohns.com:10502)
  - SharePoint Online (sharepoint.com)

## Build and Push Container Image

### 1. Build the Image

```bash
cd NewIntegration

# Build for production
docker build -t beachwood-integration:latest .

# Tag with version
docker tag beachwood-integration:latest beachwood-integration:v1.0.0
```

### 2. Push to Container Registry

#### Docker Hub
```bash
docker login
docker tag beachwood-integration:latest yourusername/beachwood-integration:v1.0.0
docker push yourusername/beachwood-integration:v1.0.0
```

#### Azure Container Registry
```bash
az acr login --name yourregistry
docker tag beachwood-integration:latest yourregistry.azurecr.io/beachwood-integration:v1.0.0
docker push yourregistry.azurecr.io/beachwood-integration:v1.0.0
```

## Deploy to TrueNAS

### Option 1: Docker Container

1. **Create Container via TrueNAS UI**
   - Navigate to: Apps > Available Applications
   - Click "Launch Docker Image"
   - Configure:
     - **Image Repository**: `yourregistry/beachwood-integration`
     - **Tag**: `v1.0.0`
     - **Container Name**: `beachwood-integration`
     - **Restart Policy**: `unless-stopped`

2. **Configure Environment Variables**
   ```
   ENVIRONMENT=production
   AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5
   AZURE_CLIENT_ID=<your-client-id>
   AZURE_CLIENT_SECRET=<your-client-secret>
   LOG_LEVEL=INFO
   ```

3. **Configure Resource Limits**
   - Memory: 2 GB
   - CPU: 2 cores

4. **Configure Volumes** (optional)
   - Mount point: `/app/logs`
   - Host path: `/mnt/pool/apps/beachwood-integration/logs`

5. **Start Container**

### Option 2: TrueNAS Cron Job

1. **Create Shell Script**
   
   Create `/mnt/pool/scripts/beachwood-integration.sh`:
   ```bash
   #!/bin/bash
   
   docker run --rm \
     --name beachwood-integration-$(date +%Y%m%d-%H%M%S) \
     -e ENVIRONMENT=production \
     -e AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5 \
     -e AZURE_CLIENT_ID=<your-client-id> \
     -e AZURE_CLIENT_SECRET=<your-client-secret> \
     -e LOG_LEVEL=INFO \
     -v /mnt/pool/apps/beachwood-integration/logs:/app/logs \
     yourregistry/beachwood-integration:v1.0.0
   ```

2. **Make Executable**
   ```bash
   chmod +x /mnt/pool/scripts/beachwood-integration.sh
   ```

3. **Configure Cron Job in TrueNAS**
   - Navigate to: Tasks > Cron Jobs
   - Add new cron job:
     - **Description**: Beachwood Data Integration
     - **Command**: `/mnt/pool/scripts/beachwood-integration.sh`
     - **Schedule**: Daily at 2:00 AM
     - **User**: root

### Option 3: Kubernetes CronJob

If TrueNAS has Kubernetes support:

1. **Create Secret for Azure Credentials**
   ```bash
   kubectl create secret generic azure-credentials \
     --from-literal=tenant-id=c8b6ba98-3fc0-4153-83a9-01374492c0f5 \
     --from-literal=client-id=<your-client-id> \
     --from-literal=client-secret=<your-client-secret> \
     -n production
   ```

2. **Create CronJob Manifest**
   
   Save as `beachwood-cronjob.yaml`:
   ```yaml
   apiVersion: batch/v1
   kind: CronJob
   metadata:
     name: beachwood-integration
     namespace: production
   spec:
     schedule: "0 2 * * *"  # Daily at 2 AM
     successfulJobsHistoryLimit: 3
     failedJobsHistoryLimit: 3
     jobTemplate:
       spec:
         template:
           spec:
             containers:
             - name: integration
               image: yourregistry/beachwood-integration:v1.0.0
               env:
               - name: ENVIRONMENT
                 value: "production"
               - name: LOG_LEVEL
                 value: "INFO"
               - name: AZURE_TENANT_ID
                 valueFrom:
                   secretKeyRef:
                     name: azure-credentials
                     key: tenant-id
               - name: AZURE_CLIENT_ID
                 valueFrom:
                   secretKeyRef:
                     name: azure-credentials
                     key: client-id
               - name: AZURE_CLIENT_SECRET
                 valueFrom:
                   secretKeyRef:
                     name: azure-credentials
                     key: client-secret
               resources:
                 limits:
                   memory: "2Gi"
                   cpu: "2000m"
                 requests:
                   memory: "1Gi"
                   cpu: "500m"
             restartPolicy: OnFailure
   ```

3. **Apply Manifest**
   ```bash
   kubectl apply -f beachwood-cronjob.yaml
   ```

## Verify Deployment

### Check Container Status
```bash
# Docker
docker ps -a | grep beachwood

# Kubernetes
kubectl get cronjobs -n production
kubectl get jobs -n production
```

### View Logs
```bash
# Docker
docker logs beachwood-integration

# Kubernetes
kubectl logs -n production -l app=beachwood-integration
```

### Test Execution
```bash
# Docker - Run manually
docker exec beachwood-integration python main.py

# Kubernetes - Trigger CronJob manually
kubectl create job --from=cronjob/beachwood-integration manual-test -n production
```

## Monitoring

### Container Health
- Monitor container status in TrueNAS dashboard
- Set up alerts for container failures
- Check logs regularly for errors

### Application Metrics
- Monitor email notifications for execution summaries
- Check Dataverse for updated records
- Verify SharePoint files are being updated

### Resource Usage
- Monitor memory usage (should stay under 2GB)
- Monitor CPU usage
- Check disk space for logs

## Troubleshooting

### Container Won't Start
1. Check environment variables are set correctly
2. Verify Azure credentials are valid
3. Check network connectivity to Azure services
4. Review container logs for startup errors

### Key Vault Access Denied
1. Verify service principal has "Key Vault Secrets User" role
2. Check tenant ID and client ID are correct
3. Verify client secret hasn't expired
4. Test Key Vault access from container:
   ```bash
   docker exec beachwood-integration python modules/utils/keyvault.py
   ```

### Integration Failures
1. Check logs for specific error messages
2. Verify all external services are accessible
3. Test individual components:
   ```bash
   # Test OLAP connection
   docker exec beachwood-integration python -c "from modules.olap_to_dataverse import execute_xmla_mdx; print('OLAP OK')"
   
   # Test Dataverse connection
   docker exec beachwood-integration python -c "from modules.olap_to_dataverse import get_dataverse_access_token; print('Dataverse OK')"
   ```

## Updating

### Update Container Image

1. Build and push new version:
   ```bash
   docker build -t beachwood-integration:v1.0.1 .
   docker push yourregistry/beachwood-integration:v1.0.1
   ```

2. Update TrueNAS container:
   - Stop existing container
   - Update image tag to v1.0.1
   - Start container

3. Or update via CLI:
   ```bash
   docker stop beachwood-integration
   docker rm beachwood-integration
   docker run -d --name beachwood-integration ... yourregistry/beachwood-integration:v1.0.1
   ```

### Rollback

If issues occur after update:
1. Stop current container
2. Start container with previous version tag
3. Investigate issues
4. Plan fix

## Maintenance

### Log Rotation
Set up log rotation to prevent disk space issues:
```bash
# Docker log rotation in daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```

### Backup
- Container configuration backed up via TrueNAS
- Code in git repository
- Secrets in Azure Key Vault
- Data in Dataverse and SharePoint

### Updates
- Review and update Python dependencies quarterly
- Update base image for security patches
- Test updates in staging before production

## Support

For issues:
1. Check logs first
2. Review TROUBLESHOOTING.md
3. Contact: howard@ptpsystem.com

---

**Last Updated**: 2025-11-19  
**Version**: 1.0.0
