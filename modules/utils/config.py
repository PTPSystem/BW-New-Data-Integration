import os
import json

def load_config():
    """Load configuration from JSON file based on environment."""
    # Determine the root directory (assuming this file is in modules/utils/)
    # We need to go up two levels to get to the root where config/ is
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    env = os.getenv('ENVIRONMENT', 'production')
    config_path = os.path.join(root_dir, 'config', f'config.{env}.json')
    
    if not os.path.exists(config_path):
        # Fallback to production config
        config_path = os.path.join(root_dir, 'config', 'config.production.json')
    
    with open(config_path, 'r') as f:
        return json.load(f)
