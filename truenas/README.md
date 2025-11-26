# TrueNAS Setup for Beachwood Data Integration

This directory contains the configuration files needed to deploy the Beachwood OLAP-to-Dataverse sync on TrueNAS using either direct Docker commands or Portainer.

## Deployment Options

### Option 1: Portainer (Recommended for GUI Management)

Portainer provides a web-based UI for managing Docker containers and stacks on TrueNAS.

#### Files for Portainer:
- `portainer-stack.yml` - Complete Portainer deployment guide with docker-compose.yml
- `docker-compose.yml` - Compatible with Portainer stack deployment

#### Portainer Deployment Steps:

1. **Access Portainer** on your TrueNAS system (usually at `http://truenas-ip:9000`)

2. **Create New Stack**:
   - Go to **Stacks** → **Add Stack**
   - Name: `beachwood-integration`
   - Copy the docker-compose.yml content from `portainer-stack.yml`

3. **Set Environment Variables** in Portainer:
   ```
   AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5
   AZURE_CLIENT_ID=d056223e-f0de-4b16-b4e0-fec2a24109ff
   AZURE_CLIENT_SECRET=[get from Azure Key Vault kv-bw-data-integration]
   ```

4. **Deploy the Stack**

5. **Set up Scheduling** (see Scheduling section below)

### Option 2: Direct Docker Commands

Use the traditional docker-compose approach with command-line deployment.

#### Files for Direct Docker:
- `docker-compose.yml` - Production container configuration
- `deploy.sh` - Automated deployment script
- `cron-jobs.txt` - Cron job definitions

#### Direct Docker Deployment Steps:

```bash
# Copy files to TrueNAS
scp -r truenas/ root@truenas:/tmp/

# Run deployment script
ssh root@truenas
cd /tmp/truenas
./deploy.sh

# Configure environment
nano /mnt/truenas/beachwood/integration/.env

# Build and deploy
cd /mnt/truenas/beachwood/integration
docker-compose build
docker-compose up -d
```

## Scheduling Jobs

### For Portainer Users:

Since Portainer doesn't have built-in cron scheduling, use TrueNAS cron jobs:

1. **Go to TrueNAS Web UI** → **System** → **Advanced** → **Cron Jobs**
2. **Add Cron Job**:
   - **Description**: Beachwood Daily OLAP Sync
   - **Command**: `docker exec beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks >> /mnt/truenas/beachwood/logs/cron.log 2>&1`
   - **Run As**: root
   - **Schedule**: `0 2 * * *` (Daily at 2:00 AM)
   - **Enabled**: ✓

3. **Add Weekly Full Sync**:
   - **Description**: Beachwood Weekly Full Sync
   - **Command**: `docker exec beachwood-olap-sync python olap_to_dataverse.py --query-type full_bi_data >> /mnt/truenas/beachwood/logs/cron-full.log 2>&1`
   - **Schedule**: `0 3 * * 0` (Sundays at 3:00 AM)

### For Direct Docker Users:

Use the cron jobs from `cron-jobs.txt` in TrueNAS cron configuration.

## Prerequisites

### TrueNAS Configuration

- Docker service enabled in TrueNAS
- Storage pool with sufficient space for logs and temp files
- Network access to:
  - Azure Key Vault (`vault.azure.net`)
  - Dataverse (`crm.dynamics.com`)
  - OLAP Server (`ednacubes.papajohns.com:10502`)

### Azure Configuration

- Azure Key Vault `kv-bw-data-integration` with secrets:
  - `azure-tenant-id`
  - `app-client-id`
  - `app-client-secret`
  - `olap-username`
  - `olap-password`

## Deployment Steps

### 1. Run the Deployment Script

```bash
# Copy files to TrueNAS
scp -r truenas/ root@truenas:/tmp/

# SSH to TrueNAS and run deployment
ssh root@truenas
cd /tmp/truenas
chmod +x deploy.sh
./deploy.sh
```

### 2. Configure Environment Variables

Edit the `.env` file that was created:

```bash
nano /mnt/truenas/beachwood/integration/.env
```

Set the `AZURE_CLIENT_SECRET` with the value from your Azure Key Vault.

### 3. Build the Docker Image

```bash
cd /mnt/truenas/beachwood/integration

# Copy the application code
# (Assuming you have the NewIntegration directory available)
cp -r /path/to/NewIntegration/* .

# Build the image
docker-compose build
```

### 4. Test the Setup

```bash
# Test Key Vault connectivity
docker-compose run --rm beachwood-olap-sync python -c "
from modules.utils.keyvault import get_secret
print('Key Vault test:', get_secret('azure-tenant-id'))
"

# Test OLAP sync (dry run)
docker-compose run --rm beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks
```

### 5. Set Up Cron Jobs

In TrueNAS Web UI:
1. Go to **System > Advanced > Cron Jobs**
2. Click **Add**
3. Configure:
   - **Description**: Beachwood OLAP Sync
   - **Command**: `cd /mnt/truenas/beachwood/integration && docker-compose run --rm beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks >> /mnt/truenas/beachwood/logs/cron.log 2>&1`
   - **Run As User**: root
   - **Schedule**: Daily at 2:00 AM
   - **Enabled**: ✓

### 6. Set Up Weekly Full Sync

Add a second cron job for weekly full syncs:
- **Description**: Beachwood Full OLAP Sync
- **Command**: `cd /mnt/truenas/beachwood/integration && docker-compose run --rm beachwood-olap-sync python olap_to_dataverse.py --query-type full_bi_data >> /mnt/truenas/beachwood/logs/cron-full.log 2>&1`
- **Schedule**: Weekly on Sundays at 3:00 AM

## Monitoring

### Logs
- **Daily sync logs**: `/mnt/truenas/beachwood/logs/cron.log`
- **Weekly full sync logs**: `/mnt/truenas/beachwood/logs/cron-full.log`
- **Container logs**: `docker-compose logs beachwood-olap-sync`

### Health Checks
```bash
# Check container status
docker-compose ps

# Check logs
docker-compose logs -f beachwood-olap-sync

# Manual test run
docker-compose run --rm beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks
```

### Alerts
Consider setting up alerts for:
- Cron job failures
- High error rates in logs
- Container restarts
- Storage space warnings

## Troubleshooting

### Common Issues

#### Docker Build Fails
```bash
# Check Docker service status
systemctl status docker

# Check available disk space
df -h

# Clean up old images
docker system prune -f
```

#### Key Vault Access Denied
```bash
# Test Azure CLI login
az login

# Check Key Vault permissions
az keyvault list --resource-group rg-bw-data-integration
```

#### Network Connectivity Issues
```bash
# Test outbound connectivity
curl -I https://vault.azure.net
curl -I https://orgbf93e3c3.crm.dynamics.com
curl -I https://ednacubes.papajohns.com:10502
```

#### Permission Issues
```bash
# Check TrueNAS storage permissions
ls -la /mnt/truenas/beachwood/

# Fix permissions if needed
chown -R root:root /mnt/truenas/beachwood/
chmod -R 755 /mnt/truenas/beachwood/
```

### Log Analysis
```bash
# View recent logs
tail -50 /mnt/truenas/beachwood/logs/cron.log

# Search for errors
grep -i error /mnt/truenas/beachwood/logs/cron.log

# Check sync success
grep "✅ Sync Complete" /mnt/truenas/beachwood/logs/cron.log
```

## Backup and Recovery

### Configuration Backup
```bash
# Backup configuration
tar -czf /mnt/truenas/backups/beachwood-config-$(date +%Y%m%d).tar.gz \
  /mnt/truenas/beachwood/integration/
```

### Log Rotation
Logs are automatically rotated monthly by the cron job. Manual cleanup:
```bash
# Remove logs older than 90 days
find /mnt/truenas/beachwood/logs -name "*.log" -mtime +90 -delete
```

### Container Recovery
```bash
# Restart services
docker-compose down
docker-compose up -d

# Rebuild if needed
docker-compose build --no-cache
docker-compose up -d
```

## Performance Tuning

### Resource Allocation
Adjust resources in `docker-compose.yml` based on your TrueNAS capacity:

```yaml
deploy:
  resources:
    limits:
      memory: 4G    # Increase if processing large datasets
      cpus: '4.0'   # Increase for parallel processing
```

### Sync Optimization
- **Daily syncs**: Use `last_2_weeks` for incremental updates (fast)
- **Weekly syncs**: Use `full_bi_data` for complete refresh (slower)
- **Batch sizes**: Default 100 records - monitor and adjust if needed

## Security Considerations

- Environment variables contain sensitive Azure credentials
- `.env` file should have restricted permissions (600)
- Consider using TrueNAS secrets management for credentials
- Regularly rotate Azure service principal secrets
- Monitor for unauthorized access attempts

## Support

For issues:
1. Check the logs in `/mnt/truenas/beachwood/logs/`
2. Verify Azure Key Vault access
3. Test network connectivity
4. Review TrueNAS Docker service status
5. Check Dataverse and OLAP server availability

## Version History

- **v1.0.0**: Initial TrueNAS deployment configuration
- Supports OLAP-to-Dataverse sync with Azure Key Vault integration
- Daily incremental syncs and weekly full syncs
- Comprehensive logging and monitoring</content>
<parameter name="filePath">/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration/NewIntegration/truenas/README.mdTrueNAS IP: 192.168.1.46
