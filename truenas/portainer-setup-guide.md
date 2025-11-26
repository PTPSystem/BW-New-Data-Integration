# Portainer Setup Guide for TrueNAS

## Prerequisites

- TrueNAS server with Docker service enabled
- Administrative access to TrueNAS
- Network access to Azure services (for the Beachwood integration)

## Step 1: Install Portainer on TrueNAS

### Option A: Install via TrueNAS Web UI (Recommended)

1. **Access TrueNAS Web Interface**
   - Open your TrueNAS web interface
   - Go to **System** → **Add-ons** (or **Apps** in newer versions)

2. **Install Portainer**
   - Search for "Portainer"
   - Click **Install**
   - Configure:
     - **Name**: `portainer`
     - **Version**: Latest stable
     - **Enable Host Path Safety Checks**: ✅ Enabled
     - **Storage**: Use default or configure custom storage
   - Click **Install**

3. **Start Portainer**
   - Wait for installation to complete
   - Portainer will be available at: `http://truenas-ip:9000`
   - Or `http://truenas-ip:9443` (HTTPS)

### Option B: Install via Docker CLI

If the web UI installation doesn't work:

```bash
# SSH to TrueNAS
ssh root@truenas

# Create Portainer volume
docker volume create portainer_data

# Run Portainer container
docker run -d \
  -p 8000:8000 \
  -p 9443:9443 \
  --name portainer \
  --restart=always \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v portainer_data:/data \
  portainer/portainer-ce:latest

# Portainer will be available at https://truenas-ip:9443
```

## Step 2: Initial Portainer Configuration

1. **Access Portainer**
   - Open `https://truenas-ip:9443` (or `http://truenas-ip:9000`)
   - Accept the self-signed certificate warning

2. **Create Admin User**
   - **Username**: `admin` (or your preference)
   - **Password**: Set a strong password
   - Click **Create user**

3. **Connect to Docker Environment**
   - **Environment Type**: Docker
   - **Name**: `TrueNAS Docker`
   - **Docker API URL**: `unix:///var/run/docker.sock`
   - Click **Connect**

## Step 3: Prepare TrueNAS Storage

```bash
# SSH to TrueNAS
ssh root@truenas

# Create directories for Beachwood integration
mkdir -p /mnt/truenas/beachwood/{logs,temp/labor,temp/doordash,integration}

# Set permissions
chmod -R 755 /mnt/truenas/beachwood/
```

## Step 4: Build Beachwood Integration Image

### Option A: Build on TrueNAS (Recommended)

```bash
# SSH to TrueNAS
ssh root@truenas

# Navigate to integration directory
cd /mnt/truenas/beachwood/integration

# Copy your application files (adjust path as needed)
# Assuming you have the files available on TrueNAS
cp -r /path/to/NewIntegration/* .

# Build the Docker image
docker build -t beachwood-integration:latest .
```

### Option B: Build Locally and Push

If building locally:

```bash
# On your local machine
cd /path/to/NewIntegration

# Build and tag for your TrueNAS registry
docker build -t truenas-ip:5000/beachwood-integration:latest .

# Push to TrueNAS (if you have a registry)
docker push truenas-ip:5000/beachwood-integration:latest
```

## Step 5: Deploy Beachwood Stack in Portainer

1. **Access Portainer**
   - Go to **Stacks** → **Add Stack**

2. **Configure Stack**
   - **Name**: `beachwood-integration`
   - **Stack deployment method**: **Web editor**

3. **Copy Docker Compose Configuration**

   Copy the entire content from `/path/to/NewIntegration/truenas/portainer-stack.yml` (the ```yaml``` section):

   ```yaml
   version: '3.8'

   services:
     beachwood-olap-sync:
       image: beachwood-integration:latest
       container_name: beachwood-olap-sync
       restart: unless-stopped

       environment:
         - ENVIRONMENT=production
         - LOG_LEVEL=INFO
         - PYTHONUNBUFFERED=1
         - AZURE_TENANT_ID=${AZURE_TENANT_ID}
         - AZURE_CLIENT_ID=${AZURE_CLIENT_ID}
         - AZURE_CLIENT_SECRET=${AZURE_CLIENT_SECRET}
         - POSTGRES_HOST=${POSTGRES_HOST:-172.17.0.1}
         - POSTGRES_PORT=${POSTGRES_PORT:-5432}

       volumes:
         - /mnt/truenas/beachwood/logs:/app/logs
         - /mnt/truenas/beachwood/temp/labor:/tmp/labor
         - /mnt/truenas/beachwood/temp/doordash:/tmp/doordash

       network_mode: host

       deploy:
         resources:
           limits:
             memory: 2G
             cpus: '2.0'
           reservations:
             memory: 1G
             cpus: '1.0'

       healthcheck:
         test: ["CMD", "python", "-c", "import sys; sys.exit(0)"]
         interval: 30s
         timeout: 10s
         retries: 3
         start_period: 40s

       logging:
         driver: "json-file"
         options:
           max-size: "10m"
           max-file: "3"
   ```

4. **Set Environment Variables**

   In Portainer, add these environment variables:

   | Variable | Value | Notes |
   |----------|-------|-------|
   | `AZURE_TENANT_ID` | `c8b6ba98-3fc0-4153-83a9-01374492c0f5` | Fixed |
   | `AZURE_CLIENT_ID` | `d056223e-f0de-4b16-b4e0-fec2a24109ff` | Fixed |
   | `AZURE_CLIENT_SECRET` | `[your-secret]` | Get from Azure Key Vault |
   | `POSTGRES_HOST` | `172.17.0.1` | Docker gateway (for PostgreSQL access) |
   | `POSTGRES_PORT` | `5432` | PostgreSQL port |

5. **Deploy the Stack**
   - Click **Deploy the stack**
   - Wait for deployment to complete
   - Check the **Containers** section to verify the container is running

## Step 6: Verify Deployment

1. **Check Container Status**
   - In Portainer, go to **Containers**
   - Verify `beachwood-olap-sync` is running (green status)

2. **Check Logs**
   - Click on the container name
   - Go to **Logs** tab
   - Look for successful startup messages

3. **Test Azure Key Vault Connection**
   - In Portainer, go to **Containers** → `beachwood-olap-sync` → **Console**
   - Run: `python -c "from modules.utils.keyvault import get_secret; print('KV Test:', get_secret('azure-tenant-id'))"`

4. **Test OLAP Sync (Manual)**
   - In container console, run:
   ```bash
   python olap_to_dataverse.py --query-type last_2_weeks
   ```

## Step 7: Set Up Cron Jobs for Automation

Since Portainer doesn't have built-in scheduling, set up TrueNAS cron jobs:

1. **Access TrueNAS Web UI**
   - Go to **System** → **Advanced** → **Cron Jobs**

2. **Add Daily Sync Job**
   - **Description**: Beachwood Daily OLAP Sync
   - **Command**:
     ```bash
     docker exec beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks >> /mnt/truenas/beachwood/logs/cron.log 2>&1
     ```
   - **Run As**: `root`
   - **Schedule**: `0 2 * * *` (Daily at 2:00 AM)
   - **Enabled**: ✅

3. **Add Weekly Full Sync Job**
   - **Description**: Beachwood Weekly Full Sync
   - **Command**:
     ```bash
     docker exec beachwood-olap-sync python olap_to_dataverse.py --query-type full_bi_data >> /mnt/truenas/beachwood/logs/cron-full.log 2>&1
     ```
   - **Run As**: `root`
   - **Schedule**: `0 3 * * 0` (Sundays at 3:00 AM)
   - **Enabled**: ✅

## Step 8: Monitoring and Maintenance

### Portainer Monitoring
- **Dashboard**: Overview of all stacks and containers
- **Container Stats**: CPU, memory, and network usage
- **Logs**: Real-time log viewing
- **Console Access**: Direct shell access to containers

### Log Locations
- **Daily sync logs**: `/mnt/truenas/beachwood/logs/cron.log`
- **Weekly sync logs**: `/mnt/truenas/beachwood/logs/cron-full.log`
- **Container logs**: Available in Portainer

### Backup Strategy
```bash
# Backup configuration
tar -czf /mnt/truenas/backups/beachwood-$(date +%Y%m%d).tar.gz \
  /mnt/truenas/beachwood/integration \
  /mnt/truenas/beachwood/logs
```

## Troubleshooting

### Common Issues

#### Portainer Won't Start
```bash
# Check Portainer logs
docker logs portainer

# Restart Portainer
docker restart portainer
```

#### Beachwood Container Won't Start
```bash
# Check container logs in Portainer
# Or via CLI:
docker logs beachwood-olap-sync

# Common issues:
# - Missing environment variables
# - Azure Key Vault access denied
# - Storage permissions
```

#### Azure Key Vault Access Issues
```bash
# Test from container console in Portainer
az login  # If using CLI authentication
# Or verify AZURE_CLIENT_SECRET is correct
```

#### Storage Permission Issues
```bash
# SSH to TrueNAS
chmod -R 755 /mnt/truenas/beachwood/
chown -R root:root /mnt/truenas/beachwood/
```

### Performance Tuning

- **Memory**: Increase to 4GB if processing large datasets
- **CPU**: Increase to 4 cores for parallel processing
- **Storage**: Monitor disk space usage in logs directory

## Security Best Practices

1. **Change Default Portainer Password**: Use a strong, unique password
2. **Enable HTTPS**: Configure SSL certificates for Portainer
3. **Network Security**: Use TrueNAS firewall rules
4. **Access Control**: Create additional users with limited permissions
5. **Regular Updates**: Keep Portainer and containers updated

## Support

- **Portainer Documentation**: https://docs.portainer.io/
- **TrueNAS Documentation**: https://www.truenas.com/docs/
- **Logs Location**: Check `/mnt/truenas/beachwood/logs/` for application logs

---

**Next Steps After Setup:**
1. Monitor the first few automated runs
2. Verify data is being synced to Dataverse
3. Set up alerts for failures
4. Consider enabling Watchtower for automatic updates</content>
<parameter name="filePath">/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration/NewIntegration/truenas/portainer-setup-guide.md