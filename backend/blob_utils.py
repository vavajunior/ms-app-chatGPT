import logging
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote

def get_blob_details (prefix, blob_name, container_name, account_url, account_key):
    try:
        blob_service_client = BlobServiceClient(  
            account_url=account_url,
            credential=account_key
        )

        blob_full_name = f"{prefix}/{blob_name}" if prefix else blob_name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_full_name)
        blob_properties = blob_client.get_blob_properties()
        blob_metadata = blob_properties.metadata

        sas_token = generate_sas_url(blob_client)
        blob_url = f"{blob_client.url}?{sas_token}"

        blob_titulo, blob_metadata_ajustado = format_metadata(blob_metadata, blob_name)    
        return blob_url, blob_titulo, blob_metadata_ajustado
    
    except ResourceNotFoundError:
        logging.exception(f"O blob '{blob_name}' não foi encontrado no container '{container_name}'.")
        return None, None, None

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

def format_metadata(blob_metadata, blob_name):
    # Recupera o Titulo e caso não encontre o valor padrão é o do blob_name
    # Remove o Titulo da lista blob_metadata
    blob_titulo = unquote(blob_metadata.get("Titulo", blob_name))
    blob_codigo = blob_metadata.get("Codigo", "")
    blob_titulo_aux = f"{blob_codigo} - {blob_titulo}" if blob_codigo else blob_titulo
    if "Titulo" in blob_metadata:
        del blob_metadata["Titulo"]

    # Formata a DataPublicacao
    if "DataPublicacao" in blob_metadata:
        data_original  = blob_metadata["DataPublicacao"]
        if data_original:
            try:
                ano, mes, dia = map(int, data_original.split('-'))
                blob_metadata["DataPublicacao"] = f"{dia:02d}/{mes:02d}/{ano}"
            except ValueError:
                logging.exception("Formato de data inválido. Mantendo o valor original.")

    # Ajustar nomes dos metadados
    # As outras chaves que não estão aqui serão mantidas como estão
    mapeamento_chaves = {
        "Codigo": "Código",
        "AreaGestora": "Área Gestora",
        "DataPublicacao": "Data Publicação",
        "Situacao": "Situação", 
    }
    blob_metadata_ajustado = {mapeamento_chaves.get(k, k): v for k, v in blob_metadata.items()}
    return blob_titulo_aux, blob_metadata_ajustado