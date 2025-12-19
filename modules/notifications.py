import requests
import msal
import os
import traceback
from modules.utils.keyvault import get_secret
from modules.utils.config import load_config

def get_graph_access_token(client_id, client_secret, tenant_id, logger=None):
    """Obtain an access token for Microsoft Graph API."""
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        scope = ["https://graph.microsoft.com/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            log(f"Microsoft Graph access token obtained")
            return result["access_token"]
        else:
            log(f"Failed to obtain Graph access token: {result.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        log(f"Error obtaining Graph access token: {e}")
        return None

def send_email_notification(subject, body, recipients=None, is_html=False, logger=None):
    """
    Send email notification using Microsoft Graph API.
    
    Args:
        subject: Email subject line
        body: Email body content
        recipients: List of email addresses (defaults to config file recipients)
        is_html: Whether body is HTML (default: False for plain text)
        logger: Optional logger
    
    Returns:
        True if successful, False otherwise
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        # Load config to get recipients and Azure credentials
        config = load_config()
        
        # Check if email notifications are enabled
        if not config.get('email_notifications', {}).get('enabled', False):
            log("Email notifications are disabled in config")
            return False
        
        # Use provided recipients or fall back to config
        if recipients is None:
            recipients = config.get('email_notifications', {}).get('recipients', [])
        
        if not recipients:
            log("No email recipients configured")
            return False
        
        # Get Azure credentials
        tenant_id = config['azure']['tenant_id']
        client_id = config['azure']['app_client_id']
        client_secret = os.getenv('AZURE_CLIENT_SECRET')
        
        if not client_secret:
            # Try to get from Key Vault
            client_secret = get_secret('app-client-secret')
        
        if not client_secret:
            log("Failed to get Azure client secret for email")
            return False
        
        # Get Graph API access token
        access_token = get_graph_access_token(client_id, client_secret, tenant_id, logger)
        
        if not access_token:
            log("Failed to get Microsoft Graph access token")
            return False
        
        # Build recipient list
        to_recipients = [{"emailAddress": {"address": email}} for email in recipients]
        
        # Build email message
        content_type = "HTML" if is_html else "Text"
        message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": content_type,
                    "content": body
                },
                "toRecipients": to_recipients
            },
            "saveToSentItems": "true"
        }
        
        # Send email using Graph API (application permission - send as specific user)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Get sender email from config or use first recipient
        sender_email = config.get('email_notifications', {}).get('sender', recipients[0] if recipients else None)
        
        if not sender_email:
            log("No sender email configured")
            return False
        
        # Note: This requires Mail.Send application permission with admin consent
        # Sends on behalf of the specified user
        graph_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        
        response = requests.post(graph_url, headers=headers, json=message)
        
        if response.status_code == 202:
            log(f"✓ Email sent successfully to {len(recipients)} recipient(s)")
            return True
        else:
            log(f"✗ Failed to send email: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        log(f"Error sending email notification: {e}")
        log(traceback.format_exc())
        return False
