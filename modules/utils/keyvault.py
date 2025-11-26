"""
Azure Key Vault utility for retrieving secrets.
Supports both DefaultAzureCredential (preferred) and client credentials.
"""

import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential

# Key Vault configuration
KEY_VAULT_NAME = "kv-bw-data-integration"
KEY_VAULT_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net/"


def get_secret_client(use_client_credentials=False):
    """
    Get an authenticated SecretClient for Azure Key Vault.
    
    Args:
        use_client_credentials: If True, use ClientSecretCredential with explicit credentials.
                               If False, use DefaultAzureCredential (recommended).
    
    Returns:
        SecretClient: Authenticated client for Key Vault operations
    """
    if use_client_credentials:
        # Explicit client credentials (for scenarios where DefaultAzureCredential doesn't work)
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError(
                "When using client credentials, you must set: "
                "AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET"
            )
        
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
    else:
        # DefaultAzureCredential tries multiple authentication methods in order:
        # 1. EnvironmentCredential (env vars)
        # 2. ManagedIdentityCredential (Azure VM/Container/Function)
        # 3. AzureCliCredential (az cli login)
        # 4. VisualStudioCodeCredential
        # 5. AzurePowerShellCredential
        credential = DefaultAzureCredential()
    
    return SecretClient(vault_url=KEY_VAULT_URL, credential=credential)


def get_secret(secret_name, use_client_credentials=False):
    """
    Retrieve a secret from Azure Key Vault.
    
    Args:
        secret_name: Name of the secret to retrieve
        use_client_credentials: Use explicit client credentials instead of DefaultAzureCredential
    
    Returns:
        str: The secret value
        
    Raises:
        Exception: If secret cannot be retrieved
    """
    try:
        client = get_secret_client(use_client_credentials=use_client_credentials)
        secret = client.get_secret(secret_name)
        return secret.value
    except Exception as e:
        raise Exception(f"Failed to retrieve secret '{secret_name}' from Key Vault: {str(e)}")


def get_all_secrets(secret_names, use_client_credentials=False):
    """
    Retrieve multiple secrets from Azure Key Vault.
    
    Args:
        secret_names: List of secret names to retrieve
        use_client_credentials: Use explicit client credentials instead of DefaultAzureCredential
    
    Returns:
        dict: Dictionary mapping secret names to their values
    """
    client = get_secret_client(use_client_credentials=use_client_credentials)
    secrets = {}
    
    for name in secret_names:
        try:
            secret = client.get_secret(name)
            secrets[name] = secret.value
        except Exception as e:
            print(f"Warning: Could not retrieve secret '{name}': {str(e)}")
            secrets[name] = None
    
    return secrets


# Convenience functions for specific secrets
def get_dataverse_credentials():
    """Get Dataverse-related secrets."""
    secrets = get_all_secrets([
        'azure-tenant-id',
        'app-client-id',
        'app-client-secret',
        'dataverse-client-id'
    ])
    
    # Map to expected structure
    return {
        'environment_url': 'https://orgbf93e3c3.crm.dynamics.com',
        'tenant_id': secrets.get('azure-tenant-id'),
        'client_id': secrets.get('app-client-id'),
        'client_secret': secrets.get('app-client-secret')
    }


def get_sharepoint_credentials():
    """Get SharePoint-related secrets."""
    return get_all_secrets([
        'azure-tenant-id',
        'app-client-id',
        'app-client-secret',
        'sharepoint-site-url'
    ])


def get_olap_password():
    """Get OLAP password if stored in Key Vault."""
    try:
        return get_secret('olap-password')
    except:
        # OLAP password might not be in Key Vault yet
        return None


if __name__ == "__main__":
    """Test Key Vault connectivity."""
    print("=" * 70)
    print("Azure Key Vault Connection Test")
    print("=" * 70)
    print(f"Key Vault: {KEY_VAULT_NAME}")
    print(f"URL: {KEY_VAULT_URL}")
    print()
    
    print("Testing authentication...")
    try:
        client = get_secret_client()
        print("✓ Successfully authenticated to Key Vault")
        print()
        
        print("Listing available secrets:")
        secret_properties = client.list_properties_of_secrets()
        for prop in secret_properties:
            enabled = "✓" if prop.enabled else "✗"
            print(f"  {enabled} {prop.name}")
        
        print()
        print("Testing Dataverse credentials retrieval...")
        dataverse_creds = get_dataverse_credentials()
        print(f"  ✓ azure-tenant-id: {'***' + dataverse_creds['azure-tenant-id'][-4:] if dataverse_creds['azure-tenant-id'] else 'NOT FOUND'}")
        print(f"  ✓ dataverse-client-id: {'***' + dataverse_creds['dataverse-client-id'][-4:] if dataverse_creds['dataverse-client-id'] else 'NOT FOUND'}")
        print(f"  ✓ dataverse-client-secret: {'***' + dataverse_creds['dataverse-client-secret'][-4:] if dataverse_creds['dataverse-client-secret'] else 'NOT FOUND'}")
        print(f"  ✓ dataverse-environment-url: {dataverse_creds['dataverse-environment-url']}")
        
        print()
        print("✅ All tests passed!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print()
        print("Make sure you're authenticated:")
        print("  1. Run: az login")
        print("  2. Ensure your account has 'Key Vault Secrets User' role on sf-kv-6338")
        print()
        print("Or set environment variables for client credentials:")
        print("  export AZURE_TENANT_ID='...'")
        print("  export AZURE_CLIENT_ID='...'")
        print("  export AZURE_CLIENT_SECRET='...'")
