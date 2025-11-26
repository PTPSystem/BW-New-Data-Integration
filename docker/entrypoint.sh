#!/bin/sh
# Entrypoint script for Beachwood Data Integration container

set -e

echo "=========================================="
echo "Beachwood Data Integration Container"
echo "=========================================="
echo ""

# Display environment info
echo "Environment: ${ENVIRONMENT:-production}"
echo "Log Level: ${LOG_LEVEL:-INFO}"
echo "Python Version: $(python --version)"
echo ""

# Verify Azure authentication (if using service principal)
if [ -n "$AZURE_CLIENT_ID" ] && [ -n "$AZURE_CLIENT_SECRET" ] && [ -n "$AZURE_TENANT_ID" ]; then
    echo "✓ Azure service principal credentials detected"
else
    echo "⚠ No Azure service principal credentials found"
    echo "  Will attempt to use Azure CLI credentials or Managed Identity"
fi
echo ""

# Test Azure Key Vault connectivity
echo "Testing Azure Key Vault connectivity..."
python -c "
from modules.utils.keyvault import get_secret_client
try:
    client = get_secret_client()
    print('✓ Successfully authenticated to Azure Key Vault')
except Exception as e:
    print(f'✗ Failed to authenticate to Key Vault: {e}')
    exit(1)
" || {
    echo ""
    echo "❌ Key Vault authentication failed!"
    echo "Please ensure:"
    echo "  1. Azure credentials are configured correctly"
    echo "  2. Service principal has 'Key Vault Secrets User' role"
    echo "  3. Key Vault kv-bw-data-integration is accessible from this network"
    exit 1
}
echo ""

# Create temporary directories if they don't exist
mkdir -p /tmp/labor/download /tmp/labor/extract /tmp/doordash
echo "✓ Temporary directories created"
echo ""

echo "=========================================="
echo "Starting Integration Process"
echo "=========================================="
echo ""

# Execute the command passed to the container
exec "$@"
