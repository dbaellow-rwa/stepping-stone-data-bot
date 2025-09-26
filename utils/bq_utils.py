# bq_utils.py
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# (Optional) Streamlit is not required; if present, we'll use st.error for nicer UX
try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # graceful fallback when not using Streamlit

# (Optional) Secret Manager helper — import if your app provides one
try:
    from sources_of_truth.secret_manager_utils import get_secret  # type: ignore
except Exception:
    # Fallback stub; replace with your own secret retrieval for local dev.
    def get_secret(secret_id: str, project_id: Optional[str] = None) -> Optional[str]:
        # Try env var mirror (e.g., SECRET__{secret_id})
        return os.environ.get(f"SECRET__{secret_id}")

# ──────────────────────────────────────────────────────────────────────────────
# Configuration & Credential Loading
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class AuthConfig:
    """
    Configure how credentials are loaded.

    Choose one of:
    - mode="secret_manager": uses get_secret(secret_id=..., project_id=...) that returns a JSON key string.
    - mode="env_json": reads JSON SA key from env var GOOGLE_APPLICATION_CREDENTIALS_JSON.
    - mode="env_path": reads file path from env var GOOGLE_APPLICATION_CREDENTIALS_PATH.
    - mode="adc": uses Application Default Credentials (workload identity / gcloud auth).

    You may also provide OPENAI_API_KEY via:
    - secret_manager: openai_secret_id (string returned directly)
    - env var: OPENAI_API_KEY
    """
    mode: str = "adc"  # "secret_manager" | "env_json" | "env_path" | "adc"
    # Secret Manager (if used)
    gcp_sa_secret_id: Optional[str] = None
    gcp_sa_secret_project: Optional[str] = None
    openai_secret_id: Optional[str] = None
    openai_secret_project: Optional[str] = None
    # Project override (if you want to pin a project)
    project_id: Optional[str] = None


@dataclass
class LoadedCreds:
    bq_credentials: Optional[service_account.Credentials]
    project_id: str
    openai_api_key: Optional[str]


def _err(msg: str) -> None:
    if st is not None:
        st.error(msg)
    else:
        print(f"[bq_utils] ERROR: {msg}")


def load_credentials(cfg: AuthConfig) -> LoadedCreds:
    """
    Load Google & OpenAI credentials based on the provided AuthConfig.
    Returns LoadedCreds(bq_credentials, project_id, openai_api_key).
    """
    creds: Optional[service_account.Credentials] = None
    project_id: Optional[str] = cfg.project_id
    openai_key: Optional[str] = None

    mode = cfg.mode.lower().strip()

    try:
        if mode == "secret_manager":
            # Expect a full JSON service account string from Secret Manager
            if not cfg.gcp_sa_secret_id:
                raise ValueError("AuthConfig.gcp_sa_secret_id is required for mode='secret_manager'.")
            json_key_str = get_secret(cfg.gcp_sa_secret_id, project_id=cfg.gcp_sa_secret_project)
            if not json_key_str:
                raise ValueError("Service account JSON not found via Secret Manager.")
            json_key = json.loads(json_key_str)
            creds = service_account.Credentials.from_service_account_info(json_key)
            project_id = project_id or json_key.get("project_id")

            if cfg.openai_secret_id:
                openai_key = get_secret(cfg.openai_secret_id, project_id=cfg.openai_secret_project)
            else:
                openai_key = os.environ.get("OPENAI_API_KEY")

        elif mode == "env_json":
            # GOOGLE_APPLICATION_CREDENTIALS_JSON holds the JSON content
            json_key_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if not json_key_str:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_JSON not set.")
            json_key = json.loads(json_key_str)
            creds = service_account.Credentials.from_service_account_info(json_key)
            project_id = project_id or json_key.get("project_id")
            openai_key = os.environ.get("OPENAI_API_KEY")

        elif mode == "env_path":
            # GOOGLE_APPLICATION_CREDENTIALS_PATH points to a key file
            path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_PATH")
            if not path or not os.path.exists(path):
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_PATH not set or file not found.")
            with open(path, "r", encoding="utf-8") as f:
                json_key = json.load(f)
            creds = service_account.Credentials.from_service_account_info(json_key)
            project_id = project_id or json_key.get("project_id")
            openai_key = os.environ.get("OPENAI_API_KEY")

        elif mode == "adc":
            # Use ADC; project may come from env GOOGLE_CLOUD_PROJECT or be passed explicitly
            creds = None  # bigquery.Client() will use ADC if available
            project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
            if not project_id:
                # Client can still work without explicit project, but it's clearer to set it
                _err("Project ID not set; consider setting GOOGLE_CLOUD_PROJECT for clarity.")
            openai_key = os.environ.get("OPENAI_API_KEY")

        else:
            raise ValueError(f"Unknown auth mode: {cfg.mode}")

        if openai_key is None:
            _err("OPENAI_API_KEY not provided (set in Secret Manager or env var).")

        if not project_id:
            raise ValueError("Could not determine GCP project_id from credentials or configuration.")

        return LoadedCreds(bq_credentials=creds, project_id=project_id, openai_api_key=openai_key)

    except Exception as e:
        _err(f"Failed to load credentials: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# BigQuery Client & Helpers
# ──────────────────────────────────────────────────────────────────────────────

def make_bq_client(loaded: LoadedCreds) -> bigquery.Client:
    """
    Create a BigQuery client using either explicit service_account creds or ADC.
    """
    if loaded.bq_credentials is not None:
        return bigquery.Client(project=loaded.project_id, credentials=loaded.bq_credentials)
    return bigquery.Client(project=loaded.project_id)  # ADC

def fqn(project: str, dataset: str, table: str) -> str:
    """Build fully-qualified table name."""
    return f"{project}.{dataset}.{table}"

def parse_fqn(table_ref: str) -> Tuple[str, str, str]:
    """
    Parse 'project.dataset.table' into components.
    If input is 'dataset.table', caller must supply project elsewhere.
    """
    parts = table_ref.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return "", parts[0], parts[1]
    raise ValueError("Table reference must be 'dataset.table' or 'project.dataset.table'.")

def to_snake_case(name: str) -> str:
    """Convert labels to snake_case for friendly DataFrame columns."""
    name = re.sub(r"[\s\-]+", "_", name.strip())
    name = re.sub(r"[^0-9a-zA-Z_]", "", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


# ──────────────────────────────────────────────────────────────────────────────
# Schema & Metadata
# ──────────────────────────────────────────────────────────────────────────────

def extract_table_schema(client: bigquery.Client, table_ref: str) -> Dict[str, Any]:
    """
    Extract table description and fields.
    Accepts 'dataset.table' or 'project.dataset.table'.
    """
    try:
        # If missing project, rely on client's default
        project, dataset, table = parse_fqn(table_ref)
        full = f"{dataset}.{table}" if not project else f"{project}.{dataset}.{table}"
        tbl = client.get_table(full)
        return {
            "table_id": full,
            "description": tbl.description or "",
            "fields": [
                {
                    "name": f.name,
                    "type": f.field_type,
                    "mode": f.mode,
                    "description": f.description or "",
                }
                for f in tbl.schema
            ],
        }
    except Exception as e:
        _err(f"Error extracting schema for {table_ref}: {e}")
        return {"table_id": table_ref, "description": "", "fields": []}

def list_tables(client: bigquery.Client, dataset: str) -> List[str]:
    """List tables in a dataset (returns 'dataset.table' names)."""
    try:
        tables = client.list_tables(dataset)
        return [f"{dataset}.{t.table_id}" for t in tables]
    except Exception as e:
        _err(f"Error listing tables for {dataset}: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Query Execution
# ──────────────────────────────────────────────────────────────────────────────

def run_bigquery(
    client: bigquery.Client,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    job_labels: Optional[Dict[str, str]] = None,
    dry_run: bool = False,
    maximum_bytes_billed: Optional[int] = None,
    timeout: Optional[int] = 180,
    to_snake: bool = True,
) -> pd.DataFrame:
    """
    Execute BigQuery SQL and return a DataFrame.
    - params: dict of query params (auto-infers types)
    - job_labels: labels for auditing (e.g., {"app":"chatbot","env":"prod"})
    - dry_run: set True to validate & estimate cost without running
    - maximum_bytes_billed: cap scan size (e.g., 10*1024**3 for 10GB)
    - timeout: job timeout seconds
    - to_snake: convert column names to snake_case
    """
    try:
        job_config = bigquery.QueryJobConfig(
            labels=job_labels or {},
            dry_run=dry_run,
            use_query_cache=True,
            maximum_bytes_billed=maximum_bytes_billed,
        )

        if params:
            # Infer parameter types — simple heuristic; expand as needed
            bq_params: List[bigquery.ScalarQueryParameter] = []
            for k, v in params.items():
                if isinstance(v, bool):
                    typ = "BOOL"
                elif isinstance(v, int):
                    typ = "INT64"
                elif isinstance(v, float):
                    typ = "FLOAT64"
                elif isinstance(v, pd.Timestamp):
                    typ = "TIMESTAMP"
                else:
                    typ = "STRING"
                bq_params.append(bigquery.ScalarQueryParameter(k, typ, v))
            job_config.query_parameters = bq_params

        job = client.query(query, job_config=job_config)  # type: ignore[arg-type]

        if dry_run:
            # Return a small summary payload in a DF for convenience
            est_bytes = getattr(job, "total_bytes_processed", None)
            return pd.DataFrame(
                [{"dry_run": True, "estimated_bytes_processed": est_bytes}]
            )

        df = job.result(timeout=timeout).to_dataframe(create_bqstorage_client=True)
        if to_snake:
            df.columns = [to_snake_case(c) for c in df.columns]
        return df

    except Exception as e:
        _err(f"BigQuery query failed: {e}\n---\n{query}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Convenience: Simple end-to-end example (optional)
# ──────────────────────────────────────────────────────────────────────────────

def example_connect_and_query() -> pd.DataFrame:
    """
    Minimal example showing how to connect and run a query.
    Adjust AuthConfig to match your runtime.
    """
    cfg = AuthConfig(
        mode=os.environ.get("BQ_AUTH_MODE", "adc"),  # "adc" by default
        gcp_sa_secret_id=os.environ.get("BQ_SA_SECRET_ID"),
        gcp_sa_secret_project=os.environ.get("BQ_SA_SECRET_PROJECT"),
        openai_secret_id=os.environ.get("OPENAI_SECRET_ID"),
        openai_secret_project=os.environ.get("OPENAI_SECRET_PROJECT"),
        project_id=os.environ.get("GOOGLE_CLOUD_PROJECT"),
    )
    loaded = load_credentials(cfg)
    client = make_bq_client(loaded)

    # Replace with a small query safe for your environment
    query = "SELECT 1 AS one, 'apples' AS sku_name"
    return run_bigquery(client, query, job_labels={"app": "demo"})
