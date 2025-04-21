from azure.storage.blob import BlobServiceClient
from models.config import Config

def get_blob_client(blob_name: str):
    blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(Config.AZURE_BLOB_CONTAINER_NAME)
    return container_client.get_blob_client(blob_name)

def read_azure_file(blob_name: str) -> str:
    blob_client = get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode('utf-8')

def write_azure_file(blob_name: str, content: str) -> None:
    blob_client = get_blob_client(blob_name)
    try:
        blob_client.delete_blob()
    except Exception:
        pass
    blob_client.upload_blob(content.encode('utf-8'), overwrite=True)