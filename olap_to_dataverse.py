"""
Query OLAP cube via XMLA and load data into Dataverse.

This script is a wrapper around the modularized implementation in modules/olap_sync.py.
"""
import sys
import os

# Add the current directory to sys.path to ensure modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.olap_sync import main

if __name__ == "__main__":
    sys.exit(main())
