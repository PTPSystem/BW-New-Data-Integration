#!/usr/bin/env python3
"""
Test SharePoint access using app registration credentials from Key Vault.
This validates that we can connect to SharePoint and list files.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.utils.keyvault import get_secret
from msal import ConfidentialClientApplication
import requests
import json


def get_access_token():
    """Get access token for SharePoint using app registration."""
    print("🔐 Retrieving credentials from Key Vault...")
    
    try:
        tenant_id = get_secret('azure-tenant-id')
        client_id = get_secret('app-client-id')
        client_secret = get_secret('app-client-secret')
        sharepoint_site_url = get_secret('sharepoint-site-url')
        
        print(f"   ✓ Tenant ID: {tenant_id}")
        print(f"   ✓ Client ID: {client_id}")
        print(f"   ✓ SharePoint Site URL: {sharepoint_site_url}")
        
    except Exception as e:
        print(f"   ✗ Failed to retrieve secrets: {e}")
        return None, None
    
    print("\n🔑 Authenticating to Microsoft Graph...")
    
    try:
        # Create MSAL confidential client
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority
        )
        
        # Get token for Microsoft Graph (for SharePoint access)
        scopes = ["https://graph.microsoft.com/.default"]
        result = app.acquire_token_for_client(scopes=scopes)
        
        if "access_token" in result:
            print(f"   ✓ Successfully authenticated")
            return result["access_token"], sharepoint_site_url
        else:
            error = result.get("error_description", result.get("error", "Unknown error"))
            print(f"   ✗ Authentication failed: {error}")
            return None, None
            
    except Exception as e:
        print(f"   ✗ Authentication error: {e}")
        return None, None


def parse_sharepoint_url(site_url):
    """Parse SharePoint URL to extract tenant and site path."""
    # Example: https://ptpsystem.sharepoint.com/sites/ITProject
    if not site_url:
        return None, None, None
    
    parts = site_url.replace('https://', '').split('/')
    tenant = parts[0]  # ptpsystem.sharepoint.com
    site_path = '/'.join(parts[1:]) if len(parts) > 1 else 'sites/ITProject'
    
    return tenant, site_path, f"https://{tenant}"


def test_sharepoint_connection(access_token, sharepoint_site_url):
    """Test SharePoint access by retrieving site information."""
    print("\n🔍 Testing SharePoint Connection...")
    
    tenant, site_path, base_url = parse_sharepoint_url(sharepoint_site_url)
    
    if not tenant:
        print(f"   ✗ Invalid SharePoint URL: {sharepoint_site_url}")
        return False
    
    print(f"   • Tenant: {tenant}")
    print(f"   • Site Path: {site_path}")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    # Get site information
    try:
        # Use Graph API to get site
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{tenant}:/{site_path}"
        print(f"\n   📡 GET {graph_url}")
        
        response = requests.get(graph_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            site_info = response.json()
            print(f"   ✓ Site found: {site_info.get('displayName', 'Unknown')}")
            print(f"   • Site ID: {site_info.get('id', 'Unknown')}")
            print(f"   • Web URL: {site_info.get('webUrl', 'Unknown')}")
            return site_info.get('id')
        else:
            print(f"   ✗ Failed to get site info: {response.status_code}")
            print(f"   Error: {response.text}")
            return None
            
    except Exception as e:
        print(f"   ✗ Error connecting to SharePoint: {e}")
        return None


def list_document_libraries(access_token, site_id):
    """List document libraries in the SharePoint site."""
    print("\n📚 Listing Document Libraries...")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    try:
        # Get drives (document libraries)
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        print(f"   📡 GET {graph_url}")
        
        response = requests.get(graph_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            drives = response.json().get('value', [])
            print(f"   ✓ Found {len(drives)} document libraries:")
            for drive in drives:
                print(f"      • {drive.get('name', 'Unknown')} (ID: {drive.get('id', 'Unknown')})")
            return drives
        else:
            print(f"   ✗ Failed to list libraries: {response.status_code}")
            print(f"   Error: {response.text}")
            return []
            
    except Exception as e:
        print(f"   ✗ Error listing libraries: {e}")
        return []


def test_file_access(access_token, site_id, drives):
    """Test accessing files in the 'BI Import' library."""
    print("\n📁 Testing File Access in 'BI Import' Library...")
    
    # Find 'BI Import' or similar library
    bi_import_drive = None
    for drive in drives:
        drive_name = drive.get('name', '').lower()
        if 'bi import' in drive_name or 'general' in drive_name:
            bi_import_drive = drive
            print(f"   • Using library: {drive.get('name')}")
            break
    
    if not bi_import_drive:
        print("   ⚠️  'BI Import' library not found, trying first available library...")
        if drives:
            bi_import_drive = drives[0]
            print(f"   • Using library: {bi_import_drive.get('name')}")
        else:
            print("   ✗ No libraries available")
            return False
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    try:
        drive_id = bi_import_drive.get('id')
        
        # List root folder contents
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
        print(f"   📡 GET {graph_url}")
        
        response = requests.get(graph_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            items = response.json().get('value', [])
            print(f"   ✓ Successfully accessed library! Found {len(items)} items:")
            
            # Show first 10 items
            for item in items[:10]:
                item_type = "📁 Folder" if 'folder' in item else "📄 File"
                item_name = item.get('name', 'Unknown')
                print(f"      {item_type}: {item_name}")
            
            if len(items) > 10:
                print(f"      ... and {len(items) - 10} more items")
            
            return True
        else:
            print(f"   ✗ Failed to access files: {response.status_code}")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ✗ Error accessing files: {e}")
        return False


def main():
    """Main test function."""
    print("=" * 70)
    print("SharePoint Access Test".center(70))
    print("=" * 70)
    
    # Step 1: Get access token
    access_token, sharepoint_site_url = get_access_token()
    if not access_token:
        print("\n❌ FAILED: Could not obtain access token")
        return False
    
    # Step 2: Test SharePoint connection
    site_id = test_sharepoint_connection(access_token, sharepoint_site_url)
    if not site_id:
        print("\n❌ FAILED: Could not connect to SharePoint site")
        return False
    
    # Step 3: List document libraries
    drives = list_document_libraries(access_token, site_id)
    if not drives:
        print("\n❌ FAILED: Could not list document libraries")
        return False
    
    # Step 4: Test file access
    file_access = test_file_access(access_token, site_id, drives)
    if not file_access:
        print("\n❌ FAILED: Could not access files in library")
        return False
    
    # All tests passed
    print("\n" + "=" * 70)
    print("✅ SUCCESS: All SharePoint access tests passed!".center(70))
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
