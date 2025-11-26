# GitHub Actions Integration with Portainer

Portainer supports several methods for automated deployments from GitHub Actions. Here are the available options:

## Method 1: Portainer API Integration (Recommended)

### GitHub Actions Workflow

Create `.github/workflows/deploy.yml` in your repository:

```yaml
name: Deploy to Portainer

on:
  push:
    branches: [ main, feature/olap-to-dataverse ]
  pull_request:
    branches: [ main ]

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: actions/setup-buildx-action@v3

    - name: Log in to Container Registry
      uses: docker/login-action@v3
      with:
        registry: ${{ secrets.REGISTRY_URL }}
        username: ${{ secrets.REGISTRY_USERNAME }}
        password: ${{ secrets.REGISTRY_PASSWORD }}

    - name: Build and push Docker image
      uses: docker/build-push-action@v5
      with:
        context: ./NewIntegration
        push: true
        tags: |
          ${{ secrets.REGISTRY_URL }}/beachwood-integration:latest
          ${{ secrets.REGISTRY_URL }}/beachwood-integration:${{ github.sha }}

    - name: Deploy to Portainer
      run: |
        # Get Portainer API token
        TOKEN=$(curl -X POST \
          -H "Content-Type: application/json" \
          -d "{\"username\":\"${{ secrets.PORTAINER_USERNAME }}\",\"password\":\"${{ secrets.PORTAINER_PASSWORD }}\"}" \
          https://${{ secrets.TRUENAS_IP }}:9443/api/auth | jq -r .jwt)

        # Update the stack
        curl -X PUT \
          -H "Authorization: Bearer $TOKEN" \
          -H "Content-Type: application/json" \
          -d "{
            \"env\": [
              {\"name\":\"AZURE_TENANT_ID\",\"value\":\"${{ secrets.AZURE_TENANT_ID }}\"},
              {\"name\":\"AZURE_CLIENT_ID\",\"value\":\"${{ secrets.AZURE_CLIENT_ID }}\"},
              {\"name\":\"AZURE_CLIENT_SECRET\",\"value\":\"${{ secrets.AZURE_CLIENT_SECRET }}\"}
            ]
          }" \
          https://${{ secrets.TRUENAS_IP }}:9443/api/stacks/${{ secrets.PORTAINER_STACK_ID }}/update
```

### Required GitHub Secrets

Set these in your repository settings:

```
REGISTRY_URL=your-registry.com
REGISTRY_USERNAME=your-username
REGISTRY_PASSWORD=your-password
TRUENAS_IP=192.168.1.100
PORTAINER_USERNAME=admin
PORTAINER_PASSWORD=your-portainer-password
PORTAINER_STACK_ID=your-stack-id
AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5
AZURE_CLIENT_ID=d056223e-f0de-4b16-b4e0-fec2a24109ff
AZURE_CLIENT_SECRET=your-secret
```

## Method 2: Portainer Webhooks

### Enable Webhooks in Portainer

1. **In Portainer**: Go to your stack → **Settings** → Enable **Webhook**
2. **Copy the webhook URL** (it will look like: `https://truenas-ip:9443/api/webhooks/webhook-id`)

### GitHub Actions Workflow for Webhooks

```yaml
name: Deploy via Portainer Webhook

on:
  push:
    branches: [ main ]

jobs:
  build-and-notify:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Build and push Docker image
      run: |
        # Build and push your image here
        echo "Building and pushing image..."

    - name: Trigger Portainer deployment
      run: |
        curl -X POST ${{ secrets.PORTAINER_WEBHOOK_URL }}
```

## Method 3: Portainer GitOps (Limited)

Portainer has basic GitOps capabilities:

1. **In Portainer**: Create a stack from **Git Repository**
2. **Repository URL**: Your GitHub repo
3. **Reference**: Branch name
4. **Compose path**: `NewIntegration/truenas/docker-compose.yml`

However, this method has limitations:
- No automatic image rebuilding
- Manual environment variable management
- Less flexible than API/webhook methods

## Method 4: Watchtower + GitHub Registry

Use Watchtower for automatic updates:

### Enable Watchtower in your stack:

```yaml
services:
  watchtower:
    image: containrrr/watchtower
    environment:
      - WATCHTOWER_CLEANUP=true
      - WATCHTOWER_POLL_INTERVAL=300  # Check every 5 minutes
      - WATCHTOWER_INCLUDE_STOPPED=true
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    profiles:
      - watchtower
```

### GitHub Actions Workflow:

```yaml
name: Build and Push Image

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Build and push
      run: |
        # Build and push to registry
        # Watchtower will automatically update the container
```

## Recommended Setup: Method 1 (Portainer API)

### Why Method 1?

- ✅ **Full control** over deployments
- ✅ **Environment variable management**
- ✅ **Rollback capabilities**
- ✅ **Integration with GitHub** workflow
- ✅ **Secure** (uses Portainer API tokens)

### Implementation Steps:

1. **Get your Portainer Stack ID:**
   ```bash
   # Use Portainer API to find stack ID
   curl -H "Authorization: Bearer YOUR_TOKEN" \
     https://truenas-ip:9443/api/stacks | jq '.[] | select(.Name=="beachwood-integration") | .Id'
   ```

2. **Set up GitHub Secrets** (see above)

3. **Test the workflow** with a manual trigger

4. **Monitor deployments** in Portainer

## Alternative: Use a Registry with Auto-Update

If you prefer simpler setup:

1. **Push images** to a registry (Docker Hub, GitHub Container Registry, etc.)
2. **Configure Portainer** to always pull latest image
3. **Use Watchtower** for automatic updates

### GitHub Actions for Registry Approach:

```yaml
name: Build and Push

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout
      uses: actions/checkout@v4

    - name: Build and push to GitHub Container Registry
      uses: docker/build-push-action@v5
      with:
        context: ./NewIntegration
        push: true
        tags: |
          ghcr.io/${{ github.repository }}/beachwood-integration:latest
          ghcr.io/${{ github.repository }}/beachwood-integration:${{ github.sha }}
```

## Security Considerations

1. **Use GitHub Secrets** for all sensitive data
2. **Rotate API tokens** regularly
3. **Use HTTPS** for all communications
4. **Limit repository access** to necessary secrets
5. **Monitor deployment logs** in Portainer

## Troubleshooting

### Common Issues:

#### API Authentication Fails
```bash
# Test API connection
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"your-password"}' \
  https://truenas-ip:9443/api/auth
```

#### Stack Update Fails
- Check stack ID is correct
- Verify environment variables are properly formatted
- Check Portainer logs for detailed errors

#### Image Pull Fails
- Ensure registry credentials are correct
- Check network connectivity from TrueNAS to registry
- Verify image was pushed successfully

## Summary

**Yes, Portainer can be automatically updated from GitHub Actions!** The recommended approach is **Method 1 (Portainer API)** for full control and reliability.

- **Best for production**: Portainer API integration
- **Simplest setup**: Webhooks
- **Most automated**: Watchtower + Registry
- **Most basic**: GitOps (limited functionality)

Choose based on your automation needs and security requirements.</content>
<parameter name="filePath">/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration/NewIntegration/truenas/github-actions-integration.md