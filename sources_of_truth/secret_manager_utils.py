# file: sources_of_truth/secret_manager_utils.py

import os
from google.cloud import secretmanager
from google.oauth2 import service_account

def get_secret(secret_id, version_id="latest", project_id=None):
    """
    Fetches a secret from Google Secret Manager.

    - If GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT is set, load that JSON key
      and use it for authentication.
    - Otherwise, rely on ADC (e.g. `gcloud auth application-default login`)
      and require either project_id or GOOGLE_CLOUD_PROJECT to be set.

    Args:
        secret_id (str):   The ID of the secret to retrieve.
        version_id (str):  The version of the secret to retrieve (default 'latest').
        project_id (str):  The GCP project ID where the secret lives. If omitted,
                           we fall back to GOOGLE_CLOUD_PROJECT (ADC path) or
                           to the project in the key file (custom key path).

    Returns:
        str: The secret value (decoded as UTF-8).
    """
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT")

    if key_path:
        # 1) Use custom JSON key for Trilytx
        if not os.path.isfile(key_path):
            raise EnvironmentError(
                f"GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT is set to '{key_path}', "
                "but that file does not exist or is not readable."
            )
        creds = service_account.Credentials.from_service_account_file(key_path)
        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        if not project_id:
            project_id = creds.project_id

    else:
        # 2) Fall back to ADC
        if not project_id:
            adc_proj = os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not adc_proj:
                raise EnvironmentError(
                    "No GOOGLE_APPLICATION_CREDENTIALS_STEPPING_STONE_BOT found and no project_id passed. "
                    "Set GOOGLE_CLOUD_PROJECT for ADC or provide project_id explicitly."
                )
            project_id = adc_proj
        client = secretmanager.SecretManagerServiceClient()

    # 3) Build the secret resource name and fetch it
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
