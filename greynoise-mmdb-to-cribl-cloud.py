import gzip
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

import maxminddb
import requests

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("greynoise-mmdb-to-cribl-cloud.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def validate_mmdb_file(mmdb_path):
    """Validate if MMDB file exists and can be opened."""
    try:
        if not os.path.exists(mmdb_path):
            logger.error(f"ERROR: MMDB file not found: {mmdb_path}")
            return False

        with maxminddb.open_database(mmdb_path) as reader:
            # Try to read metadata to verify file is valid
            metadata = reader.metadata()
            logger.info(f"INFO: MMDB file validation successful. Database type: {metadata.database_type}")
            return True
    except Exception as e:
        logger.error(f"ERROR: MMDB file validation failed - {str(e)}")
        return False


def download_mmdb_file(api_key, temp_dir):
    """Download MMDB file from GreyNoise Psychic API.

    Args:
        api_key: GreyNoise API key
        temp_dir: Temporary directory to save the file

    Returns:
        str: Path to the downloaded MMDB file
    """
    try:
        # Use today's date if not specified
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        version = "3"

        # Construct the download URL
        url = f"https://psychic.labs.greynoise.io/v1/psychic/generate/{yesterday}/{today}/{version}/mmdb"

        # Set up headers
        headers = {"key": api_key, "User-Agent": "greynoise-mmdb-to-cribl-cloud/1.0.0"}

        # Generate filename
        mmdb_filename = f"m{version}-{today}.mmdb"
        mmdb_path = os.path.join(temp_dir, mmdb_filename)

        logger.info(f"INFO: Downloading MMDB file from: {url}")
        logger.info(f"INFO: Saving to: {mmdb_path}")

        # Download the file
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        # Check if response contains actual MMDB data
        content_type = response.headers.get("content-type", "")
        if "application/octet-stream" not in content_type and "application/x-mmdb" not in content_type:
            logger.warning(f"WARNING: Unexpected content type: {content_type}")

        # Write the file
        with open(mmdb_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # Verify file was downloaded and is valid
        if not os.path.exists(mmdb_path):
            raise Exception("Downloaded file was not created")

        file_size = os.path.getsize(mmdb_path)
        logger.info(f"INFO: MMDB file downloaded successfully. Size: {file_size} bytes")

        # Validate the downloaded MMDB file
        if not validate_mmdb_file(mmdb_path):
            raise Exception("Downloaded MMDB file is not valid")

        return mmdb_path

    except requests.exceptions.RequestException as e:
        logger.error(f"ERROR: HTTP error downloading MMDB file: {str(e)}")
        raise Exception(f"Failed to download MMDB file: {str(e)}")
    except Exception as e:
        logger.error(f"ERROR: Error downloading MMDB file: {str(e)}")
        raise Exception(f"Failed to download MMDB file: {str(e)}")


def process_mmdb_file(api_key, temp_dir):
    try:
        # Download MMDB file from GreyNoise Psychic API
        mmdb_path = download_mmdb_file(api_key, temp_dir)

        # Check if file exists
        if not os.path.exists(mmdb_path):
            logger.error(f"ERROR: MMDB file not found: {mmdb_path}")
            raise FileNotFoundError("MMDB file not found")

        # Rename the downloaded file to ti_greynoise_indicators-simple.mmdb
        target_filename = "ti_greynoise_indicators-simple.mmdb"
        target_path = os.path.join(temp_dir, target_filename)

        logger.info(f"INFO: Renaming {mmdb_path} to {target_path}")
        os.rename(mmdb_path, target_path)
        mmdb_path = target_path
        logger.info(f"INFO: File successfully renamed to {target_filename}")

        with maxminddb.open_database(mmdb_path) as reader:
            # Count entries by iterating through all networks
            number_of_entries = 0
            logger.info("Counting entries in MMDB file... This may take a while for large databases.")

            for network, data in reader:
                number_of_entries += 1
                # Print progress every 100,000 entries
                if number_of_entries % 100000 == 0:
                    logger.info(f"Processed {number_of_entries:,} entries...")

            logger.info(f"The number of entries in the MMDB file is: {number_of_entries:,}")

            # Also print some metadata about the database
            logger.info("Database metadata:")
            logger.info(f"  Build epoch: {reader.metadata().build_epoch}")
            logger.info(f"  Database type: {reader.metadata().database_type}")

        return target_filename

    except Exception as e:
        logger.error(f"ERROR: Error processing {target_filename}: {str(e)}")
        raise Exception(f"Error processing {target_filename}: {str(e)}")


def get_bearer_token(client_id, client_secret):
    if not client_id or not client_secret:
        raise ValueError(
            "CRIBL_CLIENT_ID and CRIBL_CLIENT_SECRET must be provided via arguments or configuration file."
        )

    url = "https://login.cribl.cloud/oauth/token"
    headers = {"Content-Type": "application/json"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "https://api.cribl.cloud",
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to obtain bearer token: {e}")
        return None


def check_lookup_exists(token, organization_id, worker_group, lookup_filename):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/m/{worker_group}/system/lookups/{lookup_filename}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        if not data or "items" not in data or not data["items"]:
            return False
        return any(item.get("id") == lookup_filename for item in data["items"])

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check if lookup '{lookup_filename}' exists in {worker_group}: {e}")
        return False


def upload_lookup_file(token, organization_id, worker_group, lookup_filename):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/m/{worker_group}/system/lookups?filename={lookup_filename}"
    content_type = "text/csv" if lookup_filename.endswith(".csv") else "application/gzip"

    headers = {"Authorization": f"Bearer {token}", "Content-type": content_type, "accept": "application/json"}

    try:
        # Open file in appropriate mode based on extension
        open_func = gzip.open if lookup_filename.endswith(".gz") else open
        mode = "rb"

        with open_func(lookup_filename, mode) as f:
            response = requests.put(url, headers=headers, data=f)
        response.raise_for_status()

        response_data = response.json()

        temp_filename = response_data.get("filename")

        if not temp_filename:
            logger.error(f"Upload response missing 'filename' or 'version': {response_data}")
            return None
        if not temp_filename.startswith(lookup_filename.split(".")[0]):  # Check base filename
            logger.error(f"Unexpected temporary filename '{temp_filename}' in response: {response_data}")
            return None

        return temp_filename
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to upload '{lookup_filename}' to {worker_group}: {e}")
        return None


def create_lookup(token, organization_id, worker_group, lookup_filename, temp_filename):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/m/{worker_group}/system/lookups"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"id": lookup_filename, "fileInfo": {"filename": temp_filename}}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Created new lookup '{lookup_filename}' in {worker_group}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create lookup '{lookup_filename}' in {worker_group}: {e}")
        return False


def update_lookup(token, organization_id, worker_group, lookup_filename, temp_filename):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/m/{worker_group}/system/lookups/{lookup_filename}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "accept": "application/json"}
    payload = {"id": lookup_filename, "fileInfo": {"filename": temp_filename}}

    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Updated existing lookup '{lookup_filename}' in {worker_group}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update lookup '{lookup_filename}' in {worker_group}: {e}")
        return False


def commit_changes(token, organization_id, worker_group, lookup_filename):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/m/{worker_group}/version/commit"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "message": "Automated lookup file update",
        "group": worker_group,
        "files": [
            f"groups/{worker_group}/data/lookups/{lookup_filename}",
            f"groups/{worker_group}/data/lookups/{Path(lookup_filename).with_suffix('.yml')}",
        ],
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()

        commit_id = response_data["items"][0].get("commit")

        if not commit_id:
            logger.error(f"Commit response missing 'commit' ID: {response.json()}")
            return None
        return commit_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to commit changes for '{lookup_filename}' in {worker_group}: {e}")
        return None


def deploy_changes(token, organization_id, worker_group, commit_id):
    url = f"https://app.cribl.cloud/organizations/{organization_id}/workspaces/main/app/api/v1/master/groups/{worker_group}/deploy"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "accept": "application/json"}
    payload = {"version": commit_id}

    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to deploy changes to {worker_group}: {e}")
        return False


def main():
    try:
        logger.info("Starting capture of GreyNoise MMDB file and upload to Cribl Cloud.")

        # inputs
        api_key = os.getenv("GREYNOISE_API_KEY")
        temp_dir = "."
        client_id = os.getenv("CRIBL_CLIENT_ID")
        client_secret = os.getenv("CRIBL_CLIENT_SECRET")
        organization_id = os.getenv("CRIBL_ORGANIZATION_ID")
        worker_group = os.getenv("CRIBL_WORKER_GROUP")
        lookup_filename = "ti_greynoise_indicators-simple.mmdb"

        logger.info("Getting Cribl token")
        token = get_bearer_token(client_id, client_secret)
        if not token:
            raise Exception("Failed to get Cribl token")
        logger.info("Cribl token generated.")

        # process mmdb file
        lookup_filename = process_mmdb_file(api_key, temp_dir)
        if not lookup_filename:
            raise Exception("Failed to process MMDB file")
        logger.info("MMDB file processed successfully.")

        temp_filename = upload_lookup_file(token, organization_id, worker_group, lookup_filename)
        if not temp_filename:
            raise Exception("Failed to upload lookup file")
        logger.info(f"Uploaded '{lookup_filename}' to {worker_group}, temporary filename: '{temp_filename}'")

        if check_lookup_exists(token, organization_id, worker_group, lookup_filename):
            logger.info("Does exist on target.")
            if not update_lookup(token, organization_id, worker_group, lookup_filename, temp_filename):
                raise Exception("Failed to update lookup file")
        else:
            logger.info("Does not exist on target.")
            if not create_lookup(token, organization_id, worker_group, lookup_filename, temp_filename):
                raise Exception("Failed to create lookup file")

        # Commit the changes
        commit_id = commit_changes(token, organization_id, worker_group, lookup_filename)
        if not commit_id:
            raise Exception("Failed to commit changes")
        logger.info(f"Changes committed with ID: {commit_id}")

        # Deploy the changes
        if not deploy_changes(token, organization_id, worker_group, commit_id):
            raise Exception("Failed to deploy changes")
        logger.info(f"Successfully deployed changes to {worker_group}")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise Exception(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
