#!/bin/bash
# TrueNAS Deployment Script for Beachwood Data Integration
# This script sets up the OLAP-to-Dataverse sync on TrueNAS

set -e

echo "=========================================="
echo "Beachwood Data Integration - TrueNAS Setup"
echo "=========================================="
echo

# Configuration
INTEGRATION_DIR="/mnt/truenas/beachwood/integration"
LOGS_DIR="/mnt/truenas/beachwood/logs"
TEMP_DIR="/mnt/truenas/beachwood/temp"
DOCKER_IMAGE="beachwood-integration:latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status messages
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if running on TrueNAS
if ! command -v truenas &> /dev/null; then
    print_warning "This script is designed for TrueNAS. Some features may not work on other systems."
fi

# Create directory structure
echo "Creating directory structure..."
mkdir -p "$INTEGRATION_DIR"
mkdir -p "$LOGS_DIR"
mkdir -p "$TEMP_DIR/labor"
mkdir -p "$TEMP_DIR/doordash"
print_status "Directory structure created"

# Copy configuration files
echo "Copying configuration files..."
cp docker-compose.yml "$INTEGRATION_DIR/"
cp .env.template "$INTEGRATION_DIR/.env"
print_status "Configuration files copied"

# Set proper permissions
echo "Setting permissions..."
chmod 755 "$INTEGRATION_DIR"
chmod 644 "$INTEGRATION_DIR/docker-compose.yml"
chmod 600 "$INTEGRATION_DIR/.env"  # Secure environment file
chmod 755 "$LOGS_DIR"
chmod 755 "$TEMP_DIR"
print_status "Permissions set"

# Check Docker availability
if command -v docker &> /dev/null; then
    print_status "Docker is available"
else
    print_error "Docker is not available. Please ensure Docker is installed on TrueNAS."
    exit 1
fi

# Check if Docker Compose is available
if command -v docker-compose &> /dev/null; then
    print_status "Docker Compose is available"
else
    print_error "Docker Compose is not available. Please install Docker Compose."
    exit 1
fi

echo
echo "=========================================="
echo "Next Steps - Manual Configuration Required"
echo "=========================================="
echo
echo "1. Edit the environment file:"
echo "   nano $INTEGRATION_DIR/.env"
echo
echo "2. Set your Azure credentials:"
echo "   - AZURE_CLIENT_SECRET: Get from Azure Key Vault kv-bw-data-integration"
echo
echo "3. Test the configuration:"
echo "   cd $INTEGRATION_DIR"
echo "   docker-compose config"
echo
echo "4. Build and test the container:"
echo "   docker-compose build"
echo "   docker-compose run --rm beachwood-olap-sync python olap_to_dataverse.py --query-type last_2_weeks"
echo
echo "5. Set up cron jobs:"
echo "   - Copy the cron configuration from cron-jobs.txt"
echo "   - Add to TrueNAS cron jobs (System > Advanced > Cron Jobs)"
echo
echo "6. Monitor logs:"
echo "   tail -f $LOGS_DIR/cron.log"
echo
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="</content>
<parameter name="filePath">/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration/NewIntegration/truenas/deploy.sh