import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def download_jira_data():
    """
    Downloads raw Jira data stored in Azure Blob Storage.

    This function loads credentials from a local configuration file (.env), 
    authenticates with Azure via Service Principal, and extracts a specific 
    JSON file from a container. The content is downloaded as a binary data 
    stream and physically saved in the 'Bronze' layer directory.

    Args:
        Does not receive direct arguments. Relies on environment variables:
        - ACCOUNT_URL: Azure Storage Account URL.
        - CONTAINER_NAME: Name of the container where the blob is located.
        - BLOB_NAME: Exact name of the .json file in the cloud.
        - AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET: Access credentials.

    Returns:
        None: The function saves the 'bronze_issues.json' file to disk and 
            displays status messages in the console.
    """

    print("Starting bronze layer processing...")

    # Load variables from .env file into local memory
    load_dotenv()
    
    # Azure authentication credentials
    # Automatically uses AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET 
    # variables from .env, already loaded into memory by load_dotenv()
    credential_env = DefaultAzureCredential()

    # Account address where the cloud file to be downloaded is located
    account_url_env = os.getenv("ACCOUNT_URL") # ACCOUNT_URL: variable from .env file

    # Container where the file is stored
    container_env = os.getenv("CONTAINER_NAME") # CONTAINER_NAME: variable from .env file

    # File name including extension
    blob_env = os.getenv("BLOB_NAME") # BLOB_NAME: variable from .env file

    # Cloud connection setup
    blob_service_client = BlobServiceClient(
        account_url=account_url_env,
        credential=credential_env
    )

    # Reference to the specific blob to be downloaded
    blob_client = blob_service_client.get_blob_client(
        container=container_env,
        blob=blob_env
    )

    print(f"    Starting download: {blob_env}...")
    
    # Create connection via stream object
    download_stream = blob_client.download_blob()
    
    # Download the cloud file into local memory
    data = download_stream.readall()
    
    # Local file path to receive the cloud file content
    output_path = os.path.join("data", "bronze", "bronze_issues.json")
    
    # Write the cloud file binary into the local physical file
    with open(output_path, "wb") as file:
        file.write(data)
        
    print(f"    File saved at: {output_path}")
    print("Bronze layer successfully finished.")