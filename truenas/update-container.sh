#!/bin/bash

# Load secrets
source /mnt/Applications/beachwood/.env

# Function to clean up old unused images (older than 2 weeks)
cleanup_old_images() {
    echo "Checking for old unused images..."
    
    # Get images older than 2 weeks that are not in use
    TWO_WEEKS_AGO=$(date -d '2 weeks ago' +%s 2>/dev/null || date -v-2w +%s)
    
    docker images --format "{{.ID}}|{{.Repository}}|{{.Tag}}|{{.CreatedAt}}" | while IFS='|' read -r IMAGE_ID REPO TAG CREATED; do
        # Skip if image is currently in use by any container
        if docker ps -a --format "{{.Image}}" | grep -q "$IMAGE_ID\|$REPO:$TAG"; then
            continue
        fi
        
        # Parse creation date and check if older than 2 weeks
        IMAGE_DATE=$(date -d "$CREATED" +%s 2>/dev/null || date -j -f "%Y-%m-%d %H:%M:%S" "$CREATED" +%s 2>/dev/null)
        
        if [ -n "$IMAGE_DATE" ] && [ "$IMAGE_DATE" -lt "$TWO_WEEKS_AGO" ]; then
            echo "Removing old unused image: $REPO:$TAG ($IMAGE_ID)"
            docker rmi "$IMAGE_ID" 2>/dev/null || echo "  Unable to remove $IMAGE_ID (may have dependencies)"
        fi
    done
    
    echo "Cleanup complete."
}

# Pull latest image
docker pull ghcr.io/ptpsystem/beachwood-integration:latest

# Get current image ID
OLD_IMAGE=$(docker inspect beachwood-olap-sync --format='{{.Image}}' 2>/dev/null)
NEW_IMAGE=$(docker inspect ghcr.io/ptpsystem/beachwood-integration:latest --format='{{.Id}}')

# Only recreate if image changed
if [ "$OLD_IMAGE" != "$NEW_IMAGE" ]; then
    echo "New image detected, updating container..."
    docker stop beachwood-olap-sync
    docker rm beachwood-olap-sync
    docker run -d \
      --name beachwood-olap-sync \
      --restart unless-stopped \
      -e ENVIRONMENT=production \
      -e LOG_LEVEL=INFO \
      -e PYTHONUNBUFFERED=1 \
      -e AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5 \
      -e AZURE_CLIENT_ID=d056223e-f0de-4b16-b4e0-fec2a24109ff \
      -e AZURE_CLIENT_SECRET="${AZURE_CLIENT_SECRET}" \
      -e KEYVAULT_NAME=kv-bw-data-integration \
      -v /mnt/Applications/beachwood/logs:/app/logs \
      -v /mnt/Applications/beachwood/temp/labor:/tmp/labor \
      -v /mnt/Applications/beachwood/temp/doordash:/tmp/doordash \
      ghcr.io/ptpsystem/beachwood-integration:latest \
      tail -f /dev/null
    echo "Container updated!"
    
    # Clean up old images after successful update
    cleanup_old_images
else
    echo "No new image, skipping update."
fi
