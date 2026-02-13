import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def download_jira_data():
    # Carrega variáveis do arquivo .env
    load_dotenv()

    # Define as variáveis de ambiente necessárias para o DefaultAzureCredential
    # O SDK do Azure busca automaticamente por estas chaves:
    os.environ["AZURE_TENANT_ID"] = os.getenv("AZURE_TENANT_ID")
    os.environ["AZURE_CLIENT_ID"] = os.getenv("AZURE_CLIENT_ID")
    os.environ["AZURE_CLIENT_SECRET"] = os.getenv("AZURE_CLIENT_SECRET")

    account_url = os.getenv("ACCOUNT_URL")
    container_name = os.getenv("CONTAINER_NAME")
    blob_name = os.getenv("BLOB_NAME")

    try:
        # Autenticação robusta via Service Principal
        token_credential = DefaultAzureCredential()
        
        blob_service_client = BlobServiceClient(
            account_url, 
            credential=token_credential
        )

        # Referência ao blob específico
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )

        print(f"Iniciando download: {blob_name}...")
        
        # Download do conteúdo
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        
        # Exemplo de salvamento na camada bronze local
        output_path = os.path.join("data", "bronze", blob_name)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, "wb") as file:
            file.write(data)
            
        print(f"Sucesso! Arquivo salvo em: {output_path}")

        return blob_name

    except Exception as e:
        print(f"Erro na conexão ou download: {e}")