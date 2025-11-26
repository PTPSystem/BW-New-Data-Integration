# GitHub Pull-Based Deployment Setup

This setup uses **Watchtower** to automatically pull new Docker images from GitHub Container Registry whenever you push code changes.

## How It Works

1. **Push code** to GitHub main branch
2. **GitHub Actions** automatically builds Docker image and pushes to GitHub Container Registry (ghcr.io)
3. **Watchtower** on TrueNAS checks for new images every 5 minutes
4. **Watchtower** pulls new image and recreates container automatically
5. **No ports** need to be opened on TrueNAS

## Setup Steps

### 1. Make GitHub Container Registry Public

Since TrueNAS needs to pull without authentication:

1. Go to https://github.com/orgs/PTPSystem/packages
2. Find `beachwood-integration` package
3. Click **Package settings**
4. Scroll to **Danger Zone**
5. Click **Change visibility** → **Public**

### 2. Update TrueNAS Docker Compose

SSH to TrueNAS and update the docker-compose.yml:

```bash
ssh howardshen@192.168.1.46

# Backup current setup
sudo cp ~/docker-compose.yml ~/docker-compose.yml.backup

# Download new compose file from repo
curl -o ~/docker-compose.yml https://raw.githubusercontent.com/PTPSystem/BW-New-Data-Integration/main/truenas/docker-compose.yml
```

Or manually copy the updated `truenas/docker-compose.yml` from this repo.

### 3. Deploy with Watchtower

```bash
ssh howardshen@192.168.1.46

# Stop current container
sudo docker stop beachwood-olap-sync

# Remove old container
sudo docker rm beachwood-olap-sync

# Pull initial image from GitHub
sudo docker pull ghcr.io/ptpsystem/beachwood-integration:latest

# Start both containers (app + watchtower)
cd /mnt/Applications/beachwood
sudo docker-compose -f ~/docker-compose.yml up -d
```

### 4. Verify Watchtower is Running

```bash
# Check both containers are running
sudo docker ps | grep -E 'beachwood|watchtower'

# Check watchtower logs
sudo docker logs beachwood-watchtower

# You should see: "Watchtower 1.x.x"
```

## Testing the Auto-Deploy

### Make a test change and push:

```bash
# On your local machine
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration

# Make a small change (e.g., add a comment)
echo "# Test deployment" >> olap_to_dataverse.py

# Commit and push
git add olap_to_dataverse.py
git commit -m "Test auto-deployment"
git push origin main
```

### Monitor the deployment:

```bash
# Watch GitHub Actions (on github.com)
# Go to: https://github.com/PTPSystem/BW-New-Data-Integration/actions

# Watch Watchtower logs on TrueNAS
ssh howardshen@192.168.1.46
sudo docker logs -f beachwood-watchtower

# After ~5 minutes, you should see:
# "Found new image for beachwood-olap-sync"
# "Stopping beachwood-olap-sync"
# "Creating beachwood-olap-sync"
```

## Configuration Options

### Change Update Frequency

Edit docker-compose.yml:

```yaml
environment:
  - WATCHTOWER_POLL_INTERVAL=300  # Change to 3600 for hourly, 86400 for daily
```

### Update Only on Schedule

```yaml
environment:
  - WATCHTOWER_SCHEDULE=0 0 2 * * *  # Update daily at 2:00 AM (cron format)
```

### Get Notifications

Add email notifications:

```yaml
environment:
  - WATCHTOWER_NOTIFICATIONS=email
  - WATCHTOWER_NOTIFICATION_EMAIL_FROM=watchtower@yourdomain.com
  - WATCHTOWER_NOTIFICATION_EMAIL_TO=your@email.com
  - WATCHTOWER_NOTIFICATION_EMAIL_SERVER=smtp.gmail.com
  - WATCHTOWER_NOTIFICATION_EMAIL_SERVER_PORT=587
  - WATCHTOWER_NOTIFICATION_EMAIL_SERVER_USER=your@email.com
  - WATCHTOWER_NOTIFICATION_EMAIL_SERVER_PASSWORD=${EMAIL_PASSWORD}
```

## Advantages of This Approach

✅ **No open ports** - TrueNAS initiates all connections outbound  
✅ **Automatic updates** - Push to GitHub and forget  
✅ **Safe rollback** - Tagged images available in registry  
✅ **Audit trail** - GitHub Actions logs every deployment  
✅ **No manual steps** - Completely automated  
✅ **Works with Portainer** - Compatible with existing setup

## Troubleshooting

### Watchtower not pulling updates

```bash
# Check watchtower logs
sudo docker logs beachwood-watchtower

# Manually trigger update
sudo docker restart beachwood-watchtower

# Check if image exists in registry
curl -s https://ghcr.io/v2/ptpsystem/beachwood-integration/tags/list
```

### Authentication errors

If the registry is private, you need to authenticate:

```bash
# Create GitHub personal access token with read:packages permission
# Then login on TrueNAS:
sudo docker login ghcr.io -u YOUR_GITHUB_USERNAME -p YOUR_TOKEN

# Restart watchtower
sudo docker restart beachwood-watchtower
```

### Container not updating

Check the label is set:

```bash
sudo docker inspect beachwood-olap-sync | grep watchtower.enable
# Should show: "com.centurylinklabs.watchtower.enable": "true"
```

## Alternative: Manual Pull Script

If you prefer manual updates, create this script on TrueNAS:

```bash
#!/bin/bash
# /root/update-beachwood.sh

echo "Checking for updates..."
docker pull ghcr.io/ptpsystem/beachwood-integration:latest

echo "Recreating container..."
docker stop beachwood-olap-sync
docker rm beachwood-olap-sync
docker-compose -f ~/docker-compose.yml up -d beachwood-olap-sync

echo "Cleanup old images..."
docker image prune -f

echo "Deployment complete!"
```

Then run manually or via cron when you push changes.

## Summary

**Workflow:**
1. Developer pushes code → GitHub
2. GitHub Actions builds image → GitHub Container Registry
3. Watchtower on TrueNAS pulls new image → Updates container
4. Zero configuration, zero open ports, fully automated! 🎉
