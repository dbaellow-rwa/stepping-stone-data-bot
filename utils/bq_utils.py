import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import streamlit as st # Only needed for st.error if you want to display errors at this low level, otherwise remove.

from sources_of_truth.secret_manager_utils import get_secret

def load_credentials(use_local: int):
    """
    Loads Google Cloud and OpenAI credentials based on the USE_LOCAL flag.

    Args:
        use_local (int): 1 for local development (using Secret Manager),
                         0 for production (using environment variables).

    Returns:
        tuple: (credentials, project_id, openai_key)
    """
    credentials = None
    project_id = None
    openai_key = None

    if use_local:
        json_key_str = get_secret(secret_id="stepping-stone-data-bot-sa", project_id="stepping-stone")
        if not json_key_str:
            raise ValueError("Service account key not found via secret manager.")
        json_key = json.loads(json_key_str)
        credentials = service_account.Credentials.from_service_account_info(json_key)
        project_id = json_key["project_id"]
        openai_key = get_secret("openai_rwa_1", project_id="stepping-stone")
        if not openai_key:
            raise ValueError("OpenAI API key not found via secret manager.")
    else:
        json_key_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT")
        if not json_key_str:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT environment variable not set for production.")
        json_key = json.loads(json_key_str)
        credentials = service_account.Credentials.from_service_account_info(json_key)
        project_id = json_key["project_id"]
        openai_key = os.environ.get("OPENAI_API_KEY")
        if not openai_key:
            raise ValueError("OPENAI_API_KEY environment variable not set for production.")

    return credentials, project_id, openai_key

def extract_table_schema(client: bigquery.Client, dataset_id: str, table_id: str) -> dict:
    """
    Extracts the schema (description and field details) for a given BigQuery table.

    Args:
        client (bigquery.Client): An initialized BigQuery client.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.

    Returns:
        dict: A dictionary containing the table's description and a dictionary of
              field names to their descriptions/types.
    """
    try:
        table = client.get_table(f"{dataset_id}.{table_id}")
        return {
            "description": table.description or "",
            "fields": {field.name: field.description or str(field.field_type) for field in table.schema}
        }
    except Exception as e:
        st.error(f"Error extracting schema for {dataset_id}.{table_id}: {e}")
        return {"description": "", "fields": {}} # Return empty structure on error

def run_bigquery(query: str, client: bigquery.Client) -> pd.DataFrame:
    """
    Executes a BigQuery SQL query and returns the results as a Pandas DataFrame.

    Args:
        query (str): The SQL query string to execute.
        client (bigquery.Client): An initialized BigQuery client.

    Returns:
        pd.DataFrame: A DataFrame containing the query results.
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        # In a production app, you might re-raise after logging or
        # handle more gracefully, but for now, Streamlit's error logging
        # will catch it higher up.
        raise e # Re-raise the exception to be caught in Home.py's try-except block
