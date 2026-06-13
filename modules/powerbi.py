"""
Power BI REST API client for Beachwood Daily semantic model (Layer B).

Uses the ar-bw-data-integration service principal (app-client-id / app-client-secret
from Key Vault) to execute DAX queries via api.powerbi.com executeQueries.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

import pandas as pd
import requests

from modules.utils.keyvault import get_dataverse_credentials

POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# Beachwood Daily — Shared Workspace (defaults; override via Key Vault or env)
DEFAULT_WORKSPACE_ID = "ba0545ee-6dee-4757-b5c2-c5946cd9e320"
DEFAULT_DATASET_ID = "6fd26600-b245-404f-86e4-5841e1c88e9c"
DEFAULT_DATASET_NAME = "Beachwood Daily"


def _optional_secret(name: str) -> Optional[str]:
    try:
        from modules.utils.keyvault import get_secret
        return get_secret(name)
    except Exception:
        return None


def get_powerbi_config() -> dict[str, str]:
    """
    Resolve Power BI workspace/dataset IDs.

    Priority: environment variables → Key Vault secrets → built-in defaults.
    """
    def resolve(env_key: str, secret_name: str, default: str) -> str:
        return (
            os.getenv(env_key)
            or _optional_secret(secret_name)
            or default
        )

    return {
        "workspace_id": resolve(
            "POWERBI_WORKSPACE_ID", "powerbi-workspace-id", DEFAULT_WORKSPACE_ID
        ),
        "dataset_id": resolve(
            "POWERBI_DATASET_ID", "powerbi-dataset-id", DEFAULT_DATASET_ID
        ),
        "dataset_name": resolve(
            "POWERBI_DATASET_NAME", "powerbi-dataset-name", DEFAULT_DATASET_NAME
        ),
    }


def get_powerbi_access_token(logger=None) -> Optional[str]:
    """Obtain a Power BI API token using the service principal in Key Vault."""
    def log(msg: str) -> None:
        if logger:
            logger.info(msg)
        else:
            print(msg)

    creds = get_dataverse_credentials()
    tenant_id = creds.get("tenant_id")
    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")

    if not all([tenant_id, client_id, client_secret]):
        log("Missing app-client-id / app-client-secret / azure-tenant-id for Power BI auth")
        return None

    try:
        response = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": POWERBI_SCOPE,
            },
            timeout=30,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if token:
            log("Power BI access token obtained")
        return token
    except Exception as exc:
        log(f"Error obtaining Power BI access token: {exc}")
        return None


def _sanitize_column(name: str) -> str:
    """Strip DAX bracket notation from result column names."""
    name = name.strip()
    if name.startswith("[") and name.endswith("]"):
        return name[1:-1]
    return name


def parse_execute_queries_response(response_json: dict[str, Any]) -> pd.DataFrame:
    """Convert executeQueries JSON into a flat DataFrame."""
    results = response_json.get("results") or []
    if not results:
        return pd.DataFrame()

    tables = results[0].get("tables") or []
    if not tables:
        return pd.DataFrame()

    rows = tables[0].get("rows") or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.rename(columns={col: _sanitize_column(col) for col in df.columns})
    return df


def execute_dax_query(
    dax_query: str,
    *,
    workspace_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    access_token: Optional[str] = None,
    include_nulls: bool = True,
    timeout: int = 120,
    logger=None,
) -> pd.DataFrame:
    """
    Execute a DAX query against the Beachwood Daily semantic model.

    Returns a pandas DataFrame. Raises on HTTP or Power BI API errors.
    """
    config = get_powerbi_config()
    workspace_id = workspace_id or config["workspace_id"]
    dataset_id = dataset_id or config["dataset_id"]

    token = access_token or get_powerbi_access_token(logger=logger)
    if not token:
        raise RuntimeError("Could not obtain Power BI access token")

    url = (
        f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "queries": [{"query": dax_query}],
        "serializerSettings": {"includeNulls": include_nulls},
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Power BI executeQueries failed ({response.status_code}): {response.text[:800]}"
        )

    return parse_execute_queries_response(response.json())


def normalize_store_label(store: str) -> str:
    """Format store number as 6-digit label used in Beachwood Daily."""
    digits = re.sub(r"\D", "", str(store))
    return digits.zfill(6) if digits else str(store)