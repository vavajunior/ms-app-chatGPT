from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, unquote

def get_blob (blob_path, account_url, account_key):
    blob_service_client = BlobServiceClient(  
        account_url=account_url,
        credential=account_key
    )

    container_name, blob_name = extract_container_and_blob_name(blob_path)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    blob_properties = blob_client.get_blob_properties()
    sas_token = generate_sas_url(blob_client)
    blob_url = f"{blob_client.url}?{sas_token}"
    return blob_url, blob_properties.metadata

def extract_container_and_blob_name(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.lstrip('/').split('/', 1)  
      
    if len(path_parts) < 2:  
        raise ValueError("O URL fornecido não possui um caminho válido para o contêiner e o blob.")  
    
    container_name = path_parts[0]
    blob_name = unquote(path_parts[1])
    return container_name, blob_name  

# Create a SAS token that's valid for one hour
def generate_sas_url(blob_client): 
    start_time = datetime.now(timezone.utc)
    expiry_time = start_time + timedelta(hours=1)

    sas_token = generate_blob_sas(  
        account_name=blob_client.account_name,  
        account_key=blob_client.credential.account_key,  
        container_name=blob_client.container_name,  
        blob_name=blob_client.blob_name,  
        permission=BlobSasPermissions(read=True), 
        expiry=expiry_time,
        protocol='https'
    )
    return sas_token