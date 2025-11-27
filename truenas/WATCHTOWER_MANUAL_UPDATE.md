# Watchtower Manual Update Guide

## Overview

Watchtower is configured to automatically check for updates **once per day (every 24 hours)**. However, you can manually trigger an update at any time using the methods below.

---

## Method 1: Using HTTP API (Recommended)

Watchtower exposes an HTTP API on port 8080 for manual updates.

### Trigger Manual Update

From any machine that can access your TrueNAS server:

```bash
# Replace <TRUENAS_IP> with your TrueNAS server IP address
curl -H "Authorization: Bearer beachwood-manual-trigger-2025" \
  http://<TRUENAS_IP>:8080/v1/update
```

Example:
```bash
curl -H "Authorization: Bearer beachwood-manual-trigger-2025" \
  http://192.168.1.100:8080/v1/update
```

**Response:**
- HTTP 200: Update triggered successfully
- HTTP 401: Invalid token
- HTTP 500: Update failed

### From TrueNAS Shell

If you're logged into the TrueNAS shell:

```bash
curl -H "Authorization: Bearer beachwood-manual-trigger-2025" \
  http://localhost:8080/v1/update
```

---

## Method 2: Restart Watchtower Container

Restarting Watchtower will immediately trigger a check for updates:

```bash
# SSH into TrueNAS
ssh root@<TRUENAS_IP>

# Restart Watchtower container
docker restart beachwood-watchtower

# Watch the logs to see the update progress
docker logs -f beachwood-watchtower
```

---

## Method 3: Send USR1 Signal

Send a UNIX signal to Watchtower to trigger an immediate check:

```bash
# SSH into TrueNAS
ssh root@<TRUENAS_IP>

# Send USR1 signal to trigger update
docker kill --signal=USR1 beachwood-watchtower

# Watch the logs
docker logs -f beachwood-watchtower
```

---

## Method 4: Force Pull and Recreate (Nuclear Option)

If Watchtower isn't working, you can manually pull and recreate the container:

```bash
# SSH into TrueNAS
ssh root@<TRUENAS_IP>

# Navigate to deployment directory
cd /mnt/Applications/beachwood

# Stop the application container
docker stop beachwood-olap-sync

# Pull the latest image
docker pull ghcr.io/ptpsystem/beachwood-integration:latest

# Recreate containers with new image
docker-compose up -d

# Verify the update
docker ps
docker logs beachwood-olap-sync
```

---

## Verify Update Status

### Check Current Image Version

```bash
# Check when the current image was created
docker inspect ghcr.io/ptpsystem/beachwood-integration:latest \
  | grep -A 5 "Created"

# Check container creation time
docker inspect beachwood-olap-sync \
  | grep -A 2 "Created"
```

### Check Watchtower Logs

```bash
# View recent Watchtower activity
docker logs --tail 100 beachwood-watchtower

# Follow logs in real-time
docker logs -f beachwood-watchtower
```

Look for messages like:
- `Found new ghcr.io/ptpsystem/beachwood-integration:latest image`
- `Stopping /beachwood-olap-sync (abc123) with SIGTERM`
- `Creating /beachwood-olap-sync`

---

## Current Configuration

**Automatic Updates:**
- **Frequency:** Once per day (every 24 hours)
- **Poll Interval:** 86400 seconds
- **Scope:** Only containers with `com.centurylinklabs.watchtower.enable=true` label
- **Cleanup:** Old images are automatically removed after update

**Manual API:**
- **Enabled:** Yes
- **Port:** 8080
- **Token:** `beachwood-manual-trigger-2025`

---

## Troubleshooting

### Manual Update Not Working

1. **Check if Watchtower is running:**
   ```bash
   docker ps | grep watchtower
   ```

2. **Check Watchtower logs for errors:**
   ```bash
   docker logs beachwood-watchtower --tail 50
   ```

3. **Verify HTTP API is accessible:**
   ```bash
   curl http://localhost:8080/v1/update
   # Should return 401 (Unauthorized) if API is working
   ```

### Update Takes Too Long

The update process can take 2-5 minutes:
1. Pull new image from GitHub (30-60 seconds)
2. Stop old container gracefully (10-30 seconds)
3. Start new container (10-30 seconds)
4. Health check validation (30-40 seconds)

Watch the logs in real-time:
```bash
docker logs -f beachwood-watchtower
```

### Check GitHub Container Registry

Verify that a new image was actually pushed:
1. Go to: https://github.com/orgs/PTPSystem/packages
2. Find: `beachwood-integration`
3. Check the latest tag timestamp

---

## Security Note

The HTTP API token (`beachwood-manual-trigger-2025`) is stored in plain text in the docker-compose.yml file. This is acceptable for internal networks, but if you need to expose this publicly, consider:

1. Using stronger token authentication
2. Setting up reverse proxy with HTTPS
3. Using firewall rules to restrict access
4. Rotating the token periodically

To change the token:
1. Edit `truenas/docker-compose.yml`
2. Update `WATCHTOWER_HTTP_API_TOKEN` value
3. Run `docker-compose up -d watchtower`
4. Update this documentation with the new token

---

## Quick Reference

| Method | Command | Speed | Recommended |
|--------|---------|-------|-------------|
| HTTP API | `curl -H "Authorization: Bearer beachwood-manual-trigger-2025" http://localhost:8080/v1/update` | Fast | ✅ Yes |
| Restart | `docker restart beachwood-watchtower` | Fast | ✅ Yes |
| Signal | `docker kill --signal=USR1 beachwood-watchtower` | Fast | ⚠️ Advanced |
| Force Pull | `docker pull ... && docker-compose up -d` | Slow | ⚠️ Emergency Only |

---

## Related Documentation

- [GitHub Pull Deployment Guide](../docs/GITHUB_PULL_DEPLOYMENT.md)
- [TrueNAS Deployment Guide](README.md)
- [Watchtower Official Docs](https://containrrr.dev/watchtower/)
